/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.SystemErasureCodingPolicies;
import org.apache.hadoop.hdfs.security.token.block.BlockKey;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.server.datanode.CausynthCluster;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeRpcServer;
import org.apache.hadoop.ipc.CausynthMessagePropagation;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * HDFS-17897: a striped file checksum whose DataNode missed the NameNode's key
 * update cannot resolve the encryption key the client presents.
 */
public class TestCausynthStripedChecksum {
  /** Block-key serial number pinned by {@link #pinBlockKeys}. */
  private static final int SERIAL_NO = Integer.MAX_VALUE / 17897;

  @Test
  public void testStaleEncryptionKeyFailsStripedChecksum() throws Exception {
    // Nothing set up before the window expires on the wall clock.
    Configuration conf = CausynthCluster.configure(new HdfsConfiguration());
    conf.setBoolean(DFSConfigKeys.DFS_ENCRYPT_DATA_TRANSFER_KEY, true);
    conf.setBoolean(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
    // Each DataNode has IPC connections of its own, as a DataNode process
    // does (CausynthCluster.dataNodeOverlays).
    try (MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(3)
        .dataNodeConfOverlays(CausynthCluster.dataNodeOverlays(3)).build();
         DistributedFileSystem fs = cluster.getFileSystem()) {
      cluster.waitActive();
      DFSClient client = DFSClientAdapter.getDFSClient(fs);

      ErasureCodingPolicy policy = SystemErasureCodingPolicies.getByID(
          SystemErasureCodingPolicies.XOR_2_1_POLICY_ID);
      fs.enableErasureCodingPolicy(policy.getName());
      Path directory = new Path("/hdfs-17897");
      Path file = new Path(directory, "file");
      fs.mkdirs(directory);
      fs.setErasureCodingPolicy(directory, policy.getName());

      try (FSDataOutputStream out = fs.create(file)) {
        out.write(new byte[1024]);
      }
      registerSources(cluster, client, file);
      BlockTokenSecretManager master = cluster.getNamesystem()
          .getBlockManager().getBlockTokenSecretManager();
      pinBlockKeys(cluster, master);
      // Every heartbeat of the window is the workload's (CausynthCluster).
      CausynthCluster.driveHeartbeats();
      CausynthCluster.startRecording();

      // THE KEY FLOWS THE RPC'S WAY (experiments/hadoop/lib/README.md).  The
      // master rotates twice; an explicit rotation marks no DataNode for a key
      // update, so the workload marks every one.
      int initialKeyId = master.getCurrentKey().getKeyId();
      for (int rotation = 0; rotation < 2; rotation++) {
        long causynthRotation = CausynthMessagePropagation.beginTick(
            master, "KEY_MANAGER_TICK");
        try {
          master.updateKeys(Long.MAX_VALUE);
        } finally {
          CausynthMessagePropagation.endTick(causynthRotation);
        }
      }
      CausynthCluster.recordingPrecondition(
          () -> master.getCurrentKey().getKeyId() == initialKeyId + 2,
          "the recorded window must rotate the master twice");
      int currentKeyId = master.getCurrentKey().getKeyId();
      String blockPoolId = cluster.getNamesystem().getBlockPoolId();
      for (DataNode node : cluster.getDataNodes()) {
        cluster.getNamesystem().getBlockManager().getDatanodeManager()
            .getDatanode(node.getDatanodeId()).setNeedKeyUpdate(true);
      }
      // Every DataNode that verifies the checksum's key takes the master's
      // new keys over ITS OWN heartbeat, one each, in the order of the nodes'
      // names (dn<i> is the i-th location of the block group).  A delivery a
      // witness fails -- the request never reaching the NameNode, or the
      // NameNode's answer lost after it cleared the node's key-update mark --
      // is not repeated in the window.
      for (DataNode node : CausynthCluster.dataNodes()) {
        CausynthCluster.refreshKeysFromNameNode(node, "heartbeat");
      }

      // The client then takes the master's CURRENT key over its own RPC and
      // presents it to every DataNode of the group.
      client.clearDataEncryptionKey();
      long cacheRequest = CausynthMessagePropagation.beginRequest(
          client, "cache-encryption-key");
      try {
        client.newDataEncryptionKey();
      } finally {
        CausynthMessagePropagation.endRequest(
            cacheRequest, "cache-encryption-key");
      }
      CausynthCluster.recordingPrecondition(
          () -> client.getEncryptionKey().keyId == currentKeyId,
          "the client must present the master's current key");
      for (DataNode node : cluster.getDataNodes()) {
        BlockTokenSecretManager keys =
            node.getBlockPoolTokenSecretManager().get(blockPoolId);
        CausynthCluster.recordingPrecondition(
            () -> keys.hasKey(currentKeyId),
            "every DataNode must hold the key the client presents, or the"
                + " recording is already the failure");
      }

      long checksumRequest = CausynthMessagePropagation.beginRequest(
          client, "striped-file-checksum");
      try {
        fs.getFileChecksum(file);
      } finally {
        CausynthMessagePropagation.endRequest(
            checksumRequest, "striped-file-checksum");
      }
    }
  }

  /**
   * Makes every block-key id the recorded window sees identical across
   * GraphChecker replay sessions.  The manager seeds serialNo from
   * SecureRandom once per JVM and each replay task is its own JVM.
   * setSerialNo only moves the counter, so the two constructor-time keys keep
   * their random ids; two rotations retire them: the first mints
   * SERIAL_NO + 1 as nextKey, the second makes it currentKey and mints
   * SERIAL_NO + 2.  DataNodes
   * mint nothing themselves (exportKeys at registration, addKeys afterwards),
   * so delivering the master's export in-process here is the same state a
   * KeyUpdateCommand delivers; it stays out of the trace because nothing is
   * recorded yet.
   */
  private static void pinBlockKeys(MiniDFSCluster cluster,
      BlockTokenSecretManager master) throws Exception {
    master.setSerialNo(SERIAL_NO);
    { // causynth-d3-rotation-scope
      long causynthRotation = org.apache.hadoop.ipc.CausynthMessagePropagation.beginTick(
          master, "KEY_MANAGER_TICK");
      try {
        master.updateKeys(Long.MAX_VALUE);
      } finally {
        org.apache.hadoop.ipc.CausynthMessagePropagation.endTick(
            causynthRotation);
      }
    }
    { // causynth-d3-rotation-scope
      long causynthRotation = org.apache.hadoop.ipc.CausynthMessagePropagation.beginTick(
          master, "KEY_MANAGER_TICK");
      try {
        master.updateKeys(Long.MAX_VALUE);
      } finally {
        org.apache.hadoop.ipc.CausynthMessagePropagation.endTick(
            causynthRotation);
      }
    }
    assertEquals(SERIAL_NO + 1, master.getCurrentKey().getKeyId());
    // The two retired constructor-time keys still sit in every allKeys map
    // (they expire long after the run) and would reach the target as a
    // second, unpinned key family.  Drop them everywhere before recording, so
    // every manager starts the recorded window holding exactly the pinned
    // pair; managers created later (a Balancer/SPS KeyManager) copy the
    // pruned NameNode export and never see them.
    Set<Integer> pinned =
        new TreeSet<>(Arrays.asList(SERIAL_NO + 1, SERIAL_NO + 2));
    retainKeys(master, pinned);
    assertEquals(pinned, keyIds(master));
    String blockPoolId = cluster.getNamesystem().getBlockPoolId();
    for (DataNode node : cluster.getDataNodes()) {
      BlockTokenSecretManager manager =
          node.getBlockPoolTokenSecretManager().get(blockPoolId);
      manager.addKeys(master.exportKeys());
      retainKeys(manager, pinned);
      assertEquals(pinned, keyIds(manager),
          "DataNode " + node.getDatanodeId() + " holds unpinned keys");
      assertEquals(SERIAL_NO + 1, manager.getCurrentKey().getKeyId());
      assertTrue(manager.hasKey(SERIAL_NO + 1) && manager.hasKey(SERIAL_NO + 2));
    }
  }

  /** The private allKeys map; BlockTokenSecretManager has no test accessor. */
  @SuppressWarnings("unchecked")
  private static Map<Integer, BlockKey> allKeys(
      BlockTokenSecretManager manager) throws ReflectiveOperationException {
    Field field = BlockTokenSecretManager.class.getDeclaredField("allKeys");
    field.setAccessible(true);
    return (Map<Integer, BlockKey>) field.get(manager);
  }

  private static void retainKeys(BlockTokenSecretManager manager,
      Set<Integer> keyIds) throws ReflectiveOperationException {
    synchronized (manager) {
      allKeys(manager).keySet().retainAll(keyIds);
    }
  }

  private static Set<Integer> keyIds(BlockTokenSecretManager manager)
      throws ReflectiveOperationException {
    synchronized (manager) {
      return new TreeSet<>(allKeys(manager).keySet());
    }
  }

  private static void registerSources(MiniDFSCluster cluster,
      DFSClient client, Path file) throws Exception {
    CausynthMessagePropagation.registerSource(
        client, "EXTERNAL_APP", "DFS_CLIENT", "hdfs-17897/client", 0);
    // Every node, registered whole, before any traffic the recording
    // depends on (CausynthCluster).  dn<i> is the i-th location of the
    // block group, so dn0 is the checksum leader the case is about.
    CausynthCluster.registerNameNode(cluster, 0, "hdfs-17897/nn0");
    DatanodeInfo[] locations = client.getLocatedBlocks(
        file.toString(), 0).get(0).getLocations();
    int nextIndex = locations.length;
    for (DataNode node : cluster.getDataNodes()) {
      int index = -1;
      for (int locationIndex = 0; locationIndex < locations.length;
           locationIndex++) {
        if (node.getDatanodeUuid().equals(
            locations[locationIndex].getDatanodeUuid())) {
          index = locationIndex;
          break;
        }
      }
      if (index < 0) {
        index = nextIndex++;
      }
      CausynthCluster.registerDataNode(node, "hdfs-17897/dn" + index);
    }
  }
}
