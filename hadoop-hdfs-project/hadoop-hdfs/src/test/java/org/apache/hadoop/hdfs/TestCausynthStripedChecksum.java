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
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeRpcServer;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.ipc.CausynthMessagePropagation;
import org.apache.hadoop.ipc.RPC;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Causal workload for the stale-key striped-checksum failure in HDFS-17897. */
public class TestCausynthStripedChecksum {
  /** Block-key serial number pinned by {@link #pinBlockKeys}. */
  private static final int SERIAL_NO = Integer.MAX_VALUE / 17897;

  @Test
  public void testStaleEncryptionKeyFailsStripedChecksum() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(DFSConfigKeys.DFS_ENCRYPT_DATA_TRANSFER_KEY, true);
    conf.setBoolean(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
    conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 3600);
    // Keep the block-key lifecycle inside a realistic clock window.  With the
    // 600-minute defaults every key in this workload expires 20-40 hours after
    // the recorded checksum, so removeExpiredKeys() can never fire.  One minute
    // is the smallest value both keys accept (BlockManager multiplies them by
    // 60 * 1000), which puts the retiring NameNode key at t_update + U + L =
    // +2 minutes and the DataNodes' pre-recording copy of that same key (made
    // current by pinBlockKeys) at t_pin + 2U + L = +3 minutes from the
    // recorded checksum clock.
    conf.setLong(DFSConfigKeys.DFS_BLOCK_ACCESS_KEY_UPDATE_INTERVAL_KEY, 1L);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_LIFETIME_KEY, 1L);
    try (MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(3).build();
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
      CausynthMessagePropagation.registerSourceAlias(master,
          ((NameNodeRpcServer) cluster.getNameNodeRpc()).getClientRpcServer());
      CausynthMessagePropagation.startRecording();
      client.clearDataEncryptionKey();
      long cacheRequest = CausynthMessagePropagation.beginRequest(
          client, "cache-encryption-key");
      try {
        client.newDataEncryptionKey();
      } finally {
        CausynthMessagePropagation.endRequest(
            cacheRequest, "cache-encryption-key");
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
      refreshDataNodes(cluster, conf, fs);

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
   * SERIAL_NO + 1 as nextKey, the second makes it currentKey (the key the
   * recorded DEK fetch is minted from) and mints SERIAL_NO + 2.  DataNodes
   * mint nothing themselves (exportKeys at registration, addKeys afterwards),
   * so delivering the master's export in-process here is the same state the
   * recorded RPC refresh delivers later; it stays out of the trace because
   * nothing is recorded yet.  The recorded window's own two rotations are
   * unchanged.
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

  private static void refreshDataNodes(MiniDFSCluster cluster,
      Configuration conf, DistributedFileSystem fs) throws Exception {
    NamenodeProtocol namenode = NameNodeProxies.createProxy(
        conf, fs.getUri(), NamenodeProtocol.class).getProxy();
    try {
      String blockPoolId = cluster.getNamesystem().getBlockPoolId();
      for (DataNode node : cluster.getDataNodes()) {
        BlockTokenSecretManager manager =
            node.getBlockPoolTokenSecretManager().get(blockPoolId);
        CausynthMessagePropagation.registerSourceAlias(
            manager, node.getDatanodeId());
        long request = CausynthMessagePropagation.beginRequest(
            node.getDatanodeId(), "refresh-block-keys");
        try {
          ExportedBlockKeys fresh = namenode.getBlockKeys();
          manager.addKeys(fresh);
        } finally {
          CausynthMessagePropagation.endRequest(request, "refresh-block-keys");
        }
      }
    } finally {
      RPC.stopProxy(namenode);
    }
  }

  private static void registerSources(MiniDFSCluster cluster,
      DFSClient client, Path file) throws Exception {
    NameNodeRpcServer namenode = (NameNodeRpcServer) cluster.getNameNodeRpc();
    CausynthMessagePropagation.registerSource(
        client, "EXTERNAL_APP", "DFS_CLIENT", "hdfs-17897/client", 0);
    CausynthMessagePropagation.registerSource(
        namenode.getClientRpcServer(), "CLUSTER_NODE", "NAMENODE",
        "hdfs-17897/nn0", 0);
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
      CausynthMessagePropagation.registerSource(
          node.getDatanodeId(), "CLUSTER_NODE", "DATANODE",
          "hdfs-17897/dn" + index, 0);
    }
  }
}
