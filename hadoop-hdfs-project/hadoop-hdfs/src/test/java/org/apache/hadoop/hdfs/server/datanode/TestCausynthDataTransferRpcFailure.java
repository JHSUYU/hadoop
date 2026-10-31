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
package org.apache.hadoop.hdfs.server.datanode;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo.DatanodeInfoBuilder;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.security.token.block.BlockKey;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.server.namenode.NameNodeRpcServer;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.HeartbeatResponse;
import org.apache.hadoop.ipc.CausynthMessagePropagation;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * HDFS-17899 bug3, NameNode -> source DataNode -> target DataNode: the source
 * replicates a block to a target whose key delivery failed, and the target
 * cannot resolve the encryption key the source presents.  With data-transfer
 * encryption off, the same delivery failure leaves the target unable to
 * resolve the block token's key instead (hdfs-adhoc-12).
 */
public class TestCausynthDataTransferRpcFailure {
  /** Block-key serial number pinned by {@link #pinBlockKeys}. */
  private static final int SERIAL_NO = Integer.MAX_VALUE / 17899 + 2;

  @Test
  @Timeout(120)
  public void testReplicationAfterModeledKeyRefreshRpcFailure()
      throws Exception {
    replicateAfterModeledKeyRefreshRpcFailure(true, "hdfs-17899", 2, 0);
  }

  /**
   * hdfs-inj-03: the same encrypted replication after ONE rotation.  On a
   * NameNode that publishes its next block key ahead of time, every DataNode
   * holds the key that becomes current at the rotation before it, so a lost
   * key refresh is harmless; on one that does not, the target DataNode holds
   * that key only if its own refresh delivered it.  The DataNodes' daemon
   * heartbeats wait six hours, so the window's key refreshes are the
   * workload's own (a daemon heartbeat would take the key-update mark).
   */
  @Test
  @Timeout(120)
  public void testReplicationAfterOneRotation() throws Exception {
    replicateAfterModeledKeyRefreshRpcFailure(true, "hdfs-inj-03", 1,
        TimeUnit.HOURS.toSeconds(6));
  }

  /**
   * hdfs-adhoc-12, BlockTokenSecretManager#retrievePassword's missing key:
   * the same replication unencrypted, so the target checks the source's
   * block token (DataXceiver#checkAccess) with no SASL key check before it;
   * encrypted, that check (retrieveDataEncryptionKey) always fires first.
   */
  @Test
  @Timeout(120)
  public void testUnencryptedReplicationAfterModeledKeyRefreshRpcFailure()
      throws Exception {
    replicateAfterModeledKeyRefreshRpcFailure(false, "hdfs-adhoc-12", 2, 0);
  }

  /**
   * @param encrypted   whether data transfer is encrypted
   * @param prefix      the name prefix of the case's nodes
   * @param rotations   the master's block-key rotations inside the window
   * @param heartbeatS  the DataNodes' heartbeat interval in seconds, or 0
   *                    for the default
   */
  private static void replicateAfterModeledKeyRefreshRpcFailure(
      boolean encrypted, String prefix, int rotations, long heartbeatS)
      throws Exception {
    // Nothing set up before the window expires on the wall clock.
    Configuration conf = CausynthCluster.configure(new HdfsConfiguration());
    conf.setBoolean(DFSConfigKeys.DFS_ENCRYPT_DATA_TRANSFER_KEY, encrypted);
    conf.setBoolean(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
    conf.setInt(
        DFSConfigKeys.DFS_CLIENT_BLOCK_WRITE_LOCATEFOLLOWINGBLOCK_RETRIES_KEY,
        12);
    conf.setInt(DFSConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY, 10 * 60 * 1000);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY,
        10 * 60 * 1000);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_ACCESS_KEY_UPDATE_INTERVAL_KEY, 60);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_LIFETIME_KEY, 1);
    if (heartbeatS > 0) {
      conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, heartbeatS);
    }
    // Each DataNode has IPC connections of its own, as a DataNode process
    // does (CausynthCluster.dataNodeOverlays).
    try (MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(2)
        .dataNodeConfOverlays(CausynthCluster.dataNodeOverlays(2)).build();
         DistributedFileSystem fs = cluster.getFileSystem()) {
      cluster.waitActive();
      Path path = new Path("/causynth-hdfs-17899-bug3");
      // Pinned to the first DataNode: the case's roles are read off this
      // block, and random placement would make a different started node play
      // them in every run (CausynthCluster.createOnNode).
      try (FSDataOutputStream out = CausynthCluster.createOnNode(fs, path,
          cluster.getDataNodes().get(0))) {
        out.write(1);
      }

      LocatedBlock located = DFSTestUtil.getAllBlocks(fs, path).get(0);
      ExtendedBlock block = located.getBlock();
      List<DataNode> nodes = cluster.getDataNodes();
      DataNode source = find(nodes, located.getLocations()[0]);
      assertEquals(nodes.get(0), source,
          "the block must be on the DataNode the workload pinned it to");
      DataNode target = nodes.get(0) == source ? nodes.get(1) : nodes.get(0);
      pinBlockKeys(cluster);
      registerSources(cluster, source, target, prefix);

      BlockTokenSecretManager master = cluster.getNamesystem()
          .getBlockManager().getBlockTokenSecretManager();
      CausynthCluster.startRecording();

      int initialKeyId = currentKeyId(master);
      // THE KEY FLOWS THE RPC'S WAY (experiments/hadoop/lib/README.md).  The
      // master rotates, as the NameNode's own request; an explicit
      // rotation marks no DataNode for a key update, so the workload marks
      // the two it drives.
      long rotateRequest = CausynthMessagePropagation.beginRequest(
          ((NameNodeRpcServer) cluster.getNameNodeRpc()).getClientRpcServer(),
          "rotate-block-keys");
      try {
        for (int rotation = 0; rotation < rotations; rotation++) {
          long causynthRotation = CausynthMessagePropagation.beginTick(
              master, "KEY_MANAGER_TICK");
          try {
            master.updateKeys(Long.MAX_VALUE);
          } finally {
            CausynthMessagePropagation.endTick(causynthRotation);
          }
        }
      } finally {
        CausynthMessagePropagation.endRequest(rotateRequest,
            "rotate-block-keys");
      }
      CausynthCluster.recordingPrecondition(
          () -> currentKeyId(master) == initialKeyId + rotations,
          "the recorded window must rotate the master " + rotations
              + " time(s)");
      int currentKeyId = currentKeyId(master);
      for (DataNode node : new DataNode[]{target, source}) {
        cluster.getNamesystem().getBlockManager().getDatanodeManager()
            .getDatanode(node.getDatanodeId()).setNeedKeyUpdate(true);
      }

      // The verifier first, then the consumer: each takes the master's new
      // keys over ITS OWN heartbeat, one each, the KeyUpdateCommand in the
      // answer.  The target's is the delivery a witness fails -- its
      // request never reaching the NameNode, or the NameNode's answer lost
      // after it cleared the node's key-update mark -- and nothing else in
      // the window delivers the key again.
      BlockTokenSecretManager targetKeys = target
          .getBlockPoolTokenSecretManager().get(block.getBlockPoolId());
      BlockTokenSecretManager sourceKeys = source
          .getBlockPoolTokenSecretManager().get(block.getBlockPoolId());
      if (refreshKeysFromNameNode(target)) {
        GenericTestUtils.waitFor(
            () -> currentKeyId(targetKeys) == currentKeyId, 100,
            CausynthCluster.WINDOW_WAIT_MS);
      }
      if (refreshKeysFromNameNode(source)) {
        GenericTestUtils.waitFor(
            () -> currentKeyId(sourceKeys) == currentKeyId, 100,
            CausynthCluster.WINDOW_WAIT_MS);
      }
      // The source signs the transfer's encryption key with the master's
      // CURRENT key, which the target holds only because its own
      // heartbeat delivered it.
      CausynthCluster.recordingPrecondition(
          () -> currentKeyId(sourceKeys) == currentKeyId,
          "the source must present the master's current key");
      CausynthCluster.recordingPrecondition(
          () -> targetKeys.hasKey(currentKeyId),
          "the target must hold the key the source presents, or the"
              + " recording is already the failure");

      long request = CausynthMessagePropagation.beginRequest(
          source.getDatanodeId(), "replicate-block");
      try {
        source.transferBlock(block,
            new DatanodeInfo[]{new DatanodeInfoBuilder()
                .setNodeID(target.getDatanodeId()).build()},
            new StorageType[]{StorageType.DISK}, new String[0]);
        GenericTestUtils.waitFor(
            () -> target.getFSDataset().isValidBlock(block), 20, CausynthCluster.WINDOW_WAIT_MS);
        assertTrue(target.getFSDataset().isValidBlock(block));
      } finally {
        CausynthMessagePropagation.endRequest(request, "replicate-block");
      }
    }
  }

  /**
   * Makes every block-key id the recorded window sees identical across
   * GraphChecker replay sessions.  The NameNode manager seeds serialNo from
   * SecureRandom once per JVM and each replay task is its own JVM, so without
   * this the replay groups of one campaign mint unrelated ids.  setSerialNo
   * only moves the counter, so the two constructor-time keys keep their
   * random ids; two rotations retire them: the first mints SERIAL_NO + 1 as
   * nextKey, the second makes it currentKey and mints SERIAL_NO + 2 (held
   * in allKeys only by a master that publishes its next key ahead of
   * time).  DataNodes mint nothing themselves (they only
   * addKeys what the NameNode exports), so delivering the master's export
   * in-process here is the same state a KeyUpdateCommand delivers; it stays
   * out of the trace because nothing is recorded yet.  The recorded window's
   * own rotations and refreshes are unchanged.
   */
  private static void pinBlockKeys(MiniDFSCluster cluster) throws Exception {
    BlockTokenSecretManager master = cluster.getNamesystem()
        .getBlockManager().getBlockTokenSecretManager();
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
    assertPinned(pinned, keyIds(master), "the master");
    String blockPoolId = cluster.getNamesystem().getBlockPoolId();
    for (DataNode node : cluster.getDataNodes()) {
      BlockTokenSecretManager manager =
          node.getBlockPoolTokenSecretManager().get(blockPoolId);
      manager.addKeys(master.exportKeys());
      retainKeys(manager, pinned);
      assertPinned(pinned, keyIds(manager),
          "DataNode " + node.getDatanodeId());
      assertEquals(SERIAL_NO + 1, manager.getCurrentKey().getKeyId());
    }
  }

  /**
   * The pinned keys a manager holds: the current SERIAL_NO + 1, and
   * SERIAL_NO + 2 where the master published its next key ahead of time.
   */
  private static void assertPinned(Set<Integer> pinned, Set<Integer> held,
      String who) {
    assertTrue(pinned.containsAll(held), who + " holds unpinned keys");
    assertTrue(held.contains(SERIAL_NO + 1),
        who + " lacks the current key");
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

  private static int currentKeyId(BlockTokenSecretManager manager) {
    synchronized (manager) {
      return manager.getCurrentKey().getKeyId();
    }
  }

  /**
   * One key-refresh heartbeat: the one shared helper's (CausynthCluster).
   * False when a modeled transport failure left the node's keys as they
   * were, so the caller skips waiting for keys that will never arrive.
   */
  private static boolean refreshKeysFromNameNode(DataNode datanode)
      throws IOException {
    return CausynthCluster.refreshKeysFromNameNode(datanode, "heartbeat");
  }

  private static DataNode find(List<DataNode> nodes, DatanodeInfo location) {
    return nodes.stream().filter(node -> node.getDatanodeUuid()
        .equals(location.getDatanodeUuid())).findFirst()
        .orElseThrow(IllegalStateException::new);
  }

  private static void registerSources(MiniDFSCluster cluster,
      DataNode source, DataNode target, String prefix) {
    // Every node, registered whole, before any traffic the recording depends
    // on (CausynthCluster).
    CausynthCluster.registerNameNode(cluster, 0, prefix + "/nn0");
    CausynthCluster.registerDataNode(source, prefix + "/source-dn");
    CausynthCluster.registerDataNode(target, prefix + "/target-dn");
    CausynthCluster.registerOtherDataNodes(cluster, prefix);
  }
}
