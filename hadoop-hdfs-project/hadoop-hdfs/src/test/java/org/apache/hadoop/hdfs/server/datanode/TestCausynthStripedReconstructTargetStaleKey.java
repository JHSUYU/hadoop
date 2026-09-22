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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import org.apache.hadoop.hdfs.security.token.block.BlockKey;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.server.protocol.BlockECReconstructionCommand.BlockECReconstructionInfo;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.HeartbeatResponse;
import org.apache.hadoop.hdfs.server.namenode.NameNodeRpcServer;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.apache.hadoop.io.erasurecode.ECSchema;
import org.apache.hadoop.ipc.CausynthMessagePropagation;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Erasure-coding reconstruction target stale-key workload for HDFS-17967
 * path C ({@code StripedBlockWriter} / {@code StripedWriter.java:122}).
 *
 * <p>When an internal block of a striped group is lost, one DataNode that
 * still holds a live internal block reconstructs it and WRITES it to a fresh
 * target DataNode.  That write opens its own SASL connection
 * ({@code StripedBlockWriter.init}, {@code datanode.getSaslClient().socketSend})
 * carrying a data-encryption key derived from the reconstructing node's
 * current block key.  When the target can no longer resolve that key it
 * answers ERROR_UNKNOWN_KEY, the writer is struck off, and with no writers
 * left {@code StripedWriter.init} gives the whole group up with
 * "All targets are failed." -- the annotated site -- instead of clearing the
 * cached key and retrying once, which is what PR 8698 adds.
 *
 * <p>Nothing here waits on the NameNode's redundancy monitor.  The task is
 * built and handed to the chosen DataNode's {@code ErasureCodingWorker}
 * directly, so the reconstructing node, the two sources, the missing index
 * and the target are all fixed by this test rather than by placement, which
 * is what makes the recording reproducible.  {@code StripedBlockReconstructor}
 * carries the request scope into the pool thread and stamps it with the node,
 * the same way {@code Server.Call} does for the IPC handler pool.
 *
 * <p>THE RECORDING MUST LEAVE A ROTATION GAP, and it must be ASYMMETRIC.
 * Every node takes the master's rotated keys so all of them can still verify
 * block tokens; only the RECONSTRUCTING node's currentKey is rolled back.
 * The target still RETAINS that key, so the recorded reconstruction SUCCEEDS
 * and a witness has to buy the target's forgetting with a clock move.  A zero
 * gap leaves every composed path either self-contradictory or satisfied at
 * the recorded valuation -- measured on hdfs-17899 -- and no witness at all.
 *
 * <p>The policy is a user-defined XOR-2-1 at a 1 KiB cell rather than the
 * 1 MiB system policy: the whole workload, setup included, is replayed on an
 * interpreter-only concolic JVM, so the recorded window moves 2 KiB instead
 * of 2 MiB.  XOR-2-1 also needs only four DataNodes -- two data, one parity,
 * one reconstruction target.
 */
public class TestCausynthStripedReconstructTargetStaleKey {
  /** Block-key serial number pinned by {@link #pinBlockKeys}. */
  private static final int SERIAL_NO = Integer.MAX_VALUE / 17967 + 6;
  private static final int CELL_SIZE = 1024;
  private static final int BLOCK_SIZE = CELL_SIZE * 4;

  @Test
  @Timeout(240)
  public void testReconstructionAfterKeyRotations() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(DFSConfigKeys.DFS_ENCRYPT_DATA_TRANSFER_KEY, true);
    conf.setBoolean(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
    conf.setInt(DFSConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY, 10 * 60 * 1000);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY,
        10 * 60 * 1000);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_ACCESS_KEY_UPDATE_INTERVAL_KEY, 60);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_LIFETIME_KEY, 1);
    // A 1 KiB cell needs the block-size floor lowered; the default is 1 MiB.
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, CELL_SIZE);
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_EC_POLICIES_USERPOLICIES_ALLOWED_KEY,
        true);
    conf.setInt(
        DFSConfigKeys.DFS_DN_EC_RECONSTRUCTION_STRIPED_READ_BUFFER_SIZE_KEY,
        CELL_SIZE);
    // A concolic replay is slow, and one held to its schedule prefix can
    // hold a heartbeat until its turn: placement must not read a slow node
    // as a stale one.
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_KEY,
        TimeUnit.HOURS.toMillis(6));
    conf.setBoolean(
        DFSConfigKeys.DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_WRITE_KEY, false);
    conf.setBoolean(
        DFSConfigKeys.DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_READ_KEY, false);
    // The redundancy monitor must not schedule a reconstruction of its own:
    // this test drives the one it is about, explicitly.
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_INTERVAL_SECONDS_KEY,
        3600);

    ErasureCodingPolicy policy =
        new ErasureCodingPolicy(new ECSchema("xor", 2, 1), CELL_SIZE);
    try (MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(4).build();
         DistributedFileSystem fs = cluster.getFileSystem()) {
      cluster.waitActive();
      policy = fs.addErasureCodingPolicies(
          new ErasureCodingPolicy[]{policy})[0].getPolicy();
      fs.enableErasureCodingPolicy(policy.getName());
      Path dir = new Path("/causynth-hdfs-17967-c");
      fs.mkdirs(dir);
      fs.setErasureCodingPolicy(dir, policy.getName());

      // The striped group is written BEFORE anything is recorded: 2 cells, so
      // both data blocks and the parity block are exactly one full cell.
      Path path = new Path(dir, "group");
      byte[] payload = new byte[2 * CELL_SIZE];
      for (int i = 0; i < payload.length; i++) {
        payload[i] = (byte) (i % 97);
      }
      try (FSDataOutputStream out = fs.create(path)) {
        out.write(payload);
      }
      LocatedStripedBlock group = (LocatedStripedBlock)
          DFSTestUtil.getAllBlocks(fs, path).get(0);
      ExtendedBlock blockGroup = group.getBlock();
      byte[] blockIndices = group.getBlockIndices();
      DatanodeInfo[] locations = group.getLocations();
      assertEquals(3, locations.length,
          "an XOR-2-1 group is three internal blocks");
      DatanodeInfo[] byIndex = new DatanodeInfo[3];
      for (int i = 0; i < locations.length; i++) {
        byIndex[blockIndices[i]] = locations[i];
      }
      for (int index = 0; index < byIndex.length; index++) {
        assertNotNull(byIndex[index], "no location for internal block "
            + index + "; the group is not fully placed");
      }

      List<DataNode> nodes = cluster.getDataNodes();
      // Index 0 is the one we declare lost.  Its holder takes no part: the
      // reconstruction reads index 1 and the parity index 2, and writes the
      // rebuilt index 0 to the one DataNode that holds nothing.
      DataNode worker = find(nodes, byIndex[1]);
      DataNode helper = find(nodes, byIndex[2]);
      DataNode lost = find(nodes, byIndex[0]);
      DataNode target = null;
      for (DataNode node : nodes) {
        if (node != worker && node != helper && node != lost) {
          target = node;
        }
      }
      assertNotNull(target, "one DataNode must hold no internal block");

      pinBlockKeys(cluster);
      registerSources(cluster, worker, helper, lost, target);

      BlockTokenSecretManager master = cluster.getNamesystem()
          .getBlockManager().getBlockTokenSecretManager();
      String blockPoolId = blockGroup.getBlockPoolId();
      NameNodeRpcServer namenode =
          (NameNodeRpcServer) cluster.getNameNodeRpc();
      CausynthCluster.startRecording();

      int initialKeyId = currentKeyId(master);
      long rotations = CausynthMessagePropagation.beginRequest(
          namenode.getClientRpcServer(), "rotate-block-keys");
      try {
        for (int rotation = 0; rotation < 2; rotation++) {
          long causynthRotation = CausynthMessagePropagation.beginTick(
              master, "KEY_MANAGER_TICK");
          try {
            master.updateKeys(Long.MAX_VALUE);
          } finally {
            CausynthMessagePropagation.endTick(causynthRotation);
          }
        }
      } finally {
        CausynthMessagePropagation.endRequest(rotations, "rotate-block-keys");
      }
      assertEquals(initialKeyId + 2, currentKeyId(master),
          "the recorded window must rotate the master twice");
      for (DataNode node : nodes) {
        cluster.getNamesystem().getBlockManager().getDatanodeManager()
            .getDatanode(node.getDatanodeId()).setNeedKeyUpdate(true);
      }
      for (DataNode node : nodes) {
        refreshKeysFromNameNode(node, "heartbeat");
      }

      BlockTokenSecretManager workerKeys =
          worker.getBlockPoolTokenSecretManager().get(blockPoolId);
      BlockTokenSecretManager targetKeys =
          target.getBlockPoolTokenSecretManager().get(blockPoolId);
      rollCurrentKeyBackTo(workerKeys, SERIAL_NO + 1);
      assertEquals(SERIAL_NO + 1, currentKeyId(workerKeys),
          "the reconstructing node must present the pinned key, or there is"
              + " no gap");
      assertEquals(currentKeyId(master), currentKeyId(targetKeys),
          "the target must be current, or the two are symmetric");
      assertTrue(targetKeys.hasKey(SERIAL_NO + 1),
          "the target must still RETAIN the worker's key, or the recording is"
              + " already the failure");

      DatanodeInfo targetInfo = new DatanodeInfoBuilder()
          .setNodeID(target.getDatanodeId()).build();
      BlockECReconstructionInfo task = new BlockECReconstructionInfo(
          blockGroup,
          new DatanodeInfo[]{byIndex[1], byIndex[2]},
          new DatanodeInfo[]{targetInfo},
          new String[]{storageIdOf(target, blockPoolId)},
          new StorageType[]{StorageType.DISK},
          new byte[]{1, 2},
          new byte[0],
          policy);
      ExtendedBlock rebuilt =
          StripedBlockUtil.constructInternalBlock(blockGroup, policy, 0);
      final DataNode reconstructionTarget = target;
      long request = CausynthMessagePropagation.beginRequest(
          worker.getDatanodeId(), "ec-reconstruct");
      try {
        worker.getErasureCodingWorker().processErasureCodingTasks(
            Collections.singletonList(task));
        GenericTestUtils.waitFor(
            () -> reconstructionTarget.getFSDataset().isValidBlock(rebuilt),
            50, 60000);
        assertTrue(target.getFSDataset().isValidBlock(rebuilt),
            "the target must receive the rebuilt internal block; the recorded"
                + " reconstruction has to SUCCEED");
      } finally {
        CausynthMessagePropagation.endRequest(request, "ec-reconstruct");
      }
    }
  }

  private static String storageIdOf(DataNode node, String blockPoolId)
      throws IOException {
    return node.getFSDataset().getStorageReports(blockPoolId)[0]
        .getStorage().getStorageID();
  }

  /**
   * Makes every block-key id the recorded window sees identical across
   * GraphChecker replay sessions: the NameNode manager seeds serialNo from
   * SecureRandom once per JVM and each replay task is its own JVM.  Two
   * rotations here retire the constructor-time keys, and those are then
   * dropped everywhere so every manager starts the window holding exactly the
   * pinned pair.  Taken from hdfs-17899-bug3, which passes.
   */
  private static void pinBlockKeys(MiniDFSCluster cluster) throws Exception {
    BlockTokenSecretManager master = cluster.getNamesystem()
        .getBlockManager().getBlockTokenSecretManager();
    master.setSerialNo(SERIAL_NO);
    for (int rotation = 0; rotation < 2; rotation++) { // causynth-d3-rotation-scope
      long causynthRotation = CausynthMessagePropagation.beginTick(
          master, "KEY_MANAGER_TICK");
      try {
        master.updateKeys(Long.MAX_VALUE);
      } finally {
        CausynthMessagePropagation.endTick(causynthRotation);
      }
    }
    assertEquals(SERIAL_NO + 1, master.getCurrentKey().getKeyId());
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
      assertTrue(manager.hasKey(SERIAL_NO + 1)
          && manager.hasKey(SERIAL_NO + 2));
    }
  }

  /**
   * Points the manager's current key back at {@code keyId}, which it must
   * still hold.
   *
   * <p>{@code addKeys} moves allKeys and currentKey together, and this case
   * needs them apart: the node has to VERIFY tokens signed with the master's
   * newest key while still DERIVING its own data-encryption key from an older
   * one.  BlockTokenSecretManager exposes no setter, so the field is set the
   * same way {@link #allKeys} is read.</p>
   */
  private static void rollCurrentKeyBackTo(BlockTokenSecretManager manager,
      int keyId) throws ReflectiveOperationException {
    synchronized (manager) {
      BlockKey key = allKeys(manager).get(keyId);
      assertTrue(key != null, "the manager no longer holds key " + keyId);
      Field field =
          BlockTokenSecretManager.class.getDeclaredField("currentKey");
      field.setAccessible(true);
      field.set(manager, key);
    }
  }

  /** One key-refresh heartbeat: the one shared helper's (CausynthCluster). */
  private static void refreshKeysFromNameNode(DataNode datanode, String api)
      throws IOException {
    CausynthCluster.refreshKeysFromNameNode(datanode, api);
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

  private static DataNode find(List<DataNode> nodes, DatanodeInfo location) {
    List<DataNode> matches = new ArrayList<>();
    for (DataNode node : nodes) {
      if (node.getDatanodeUuid().equals(location.getDatanodeUuid())) {
        matches.add(node);
      }
    }
    assertEquals(1, matches.size(), "no DataNode for " + location);
    return matches.get(0);
  }

  private static void registerSources(MiniDFSCluster cluster, DataNode worker,
      DataNode helper, DataNode lost, DataNode target) {
    // Every node, registered whole, before any traffic the recording
    // depends on (CausynthCluster).
    CausynthCluster.registerNameNode(cluster, 0, "hdfs-17967/nn0");
    CausynthCluster.registerDataNode(worker, "hdfs-17967/worker-dn");
    CausynthCluster.registerDataNode(helper, "hdfs-17967/helper-dn");
    CausynthCluster.registerDataNode(lost, "hdfs-17967/lost-dn");
    CausynthCluster.registerDataNode(target, "hdfs-17967/target-dn");
    CausynthCluster.registerOtherDataNodes(cluster, "hdfs-17967");
  }
}
