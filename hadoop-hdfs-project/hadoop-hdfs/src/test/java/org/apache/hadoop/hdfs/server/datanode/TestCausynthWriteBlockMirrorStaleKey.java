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
import java.util.concurrent.TimeUnit;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo.DatanodeInfoBuilder;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import java.util.List;
import java.util.ArrayList;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.fs.permission.FsPermission;
import java.net.InetSocketAddress;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.security.token.block.BlockKey;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.server.namenode.NameNodeRpcServer;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.HeartbeatResponse;
import org.apache.hadoop.ipc.CausynthMessagePropagation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Real client -&gt; pipeline-head DataNode -&gt; mirror DataNode stale-key
 * workload for HDFS-17967 path A ({@code DataXceiver.writeBlock}).
 *
 * <p>The head of a write pipeline opens its own SASL-encrypted connection to
 * the mirror and presents a data-encryption key derived from ITS current
 * block key.  When the NameNode has rotated block keys and the head has not
 * picked the new ones up, the mirror can no longer resolve that key id and
 * answers ERROR_UNKNOWN_KEY; the head rebuilds the exception at {@code
 * DataTransferSaslUtil.readSaslMessage} -- the annotated site -- and
 * {@code writeBlock} gives the whole write up instead of clearing the cached
 * key and retrying once, which is what PR 8698 adds.</p>
 *
 * <p>THE KEY FLOWS THE RPC'S WAY (experiments/hadoop/lib/README.md).  The
 * master rotates twice inside the window, and every DataNode takes the new
 * keys over ONE heartbeat of its own -- the NameNode answers with the
 * KeyUpdateCommand.  The head then derives the key it presents to the mirror
 * from its CURRENT key, the master's newest, and the mirror verifies it with
 * the keys its own heartbeat delivered: the recording is healthy.  Lose the
 * mirror's heartbeat answer (the NameNode has already cleared its
 * needKeyUpdate) or its request, and the mirror holds only the pinned pair
 * while the head presents a serial two ahead: ERROR_UNKNOWN_KEY at the
 * annotated site.  No key expires; the clocks stay at the recording.</p>
 */
public class TestCausynthWriteBlockMirrorStaleKey {
  /** Block-key serial number pinned by {@link #pinBlockKeys}. */
  private static final int SERIAL_NO = Integer.MAX_VALUE / 17967 + 2;

  @Test
  @Timeout(120)
  public void testWritePipelineAfterKeyRotations() throws Exception {
    // Nothing set up before the window expires on the wall clock.
    Configuration conf = CausynthCluster.configure(new HdfsConfiguration());
    conf.setBoolean(DFSConfigKeys.DFS_ENCRYPT_DATA_TRANSFER_KEY, true);
    conf.setBoolean(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
    conf.setInt(DFSConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY, 10 * 60 * 1000);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY,
        10 * 60 * 1000);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_ACCESS_KEY_UPDATE_INTERVAL_KEY, 60);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_LIFETIME_KEY, 1);
    // A concolic replay is slow, and one held to its schedule prefix can
    // hold a heartbeat until its turn: placement must not read a slow node
    // as a stale one.
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_KEY,
        TimeUnit.HOURS.toMillis(6));
    conf.setBoolean(
        DFSConfigKeys.DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_WRITE_KEY, false);
    conf.setBoolean(
        DFSConfigKeys.DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_READ_KEY, false);
    // Each DataNode has IPC connections of its own, as a DataNode process
    // does (CausynthCluster.dataNodeOverlays).
    try (MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(3)
        .dataNodeConfOverlays(CausynthCluster.dataNodeOverlays(3)).build();
         DistributedFileSystem fs = cluster.getFileSystem()) {
      cluster.waitActive();

      // The block is written BEFORE anything is recorded, at replication 1,
      // purely so a DataNode has something to transfer.
      Path path = new Path("/causynth-hdfs-17967-a");
      // Pinned to the first DataNode: the case's roles are read off this
      // block, and random placement would make a different started node play
      // them in every run (CausynthCluster.createOnNode).
      try (FSDataOutputStream out = CausynthCluster.createOnNode(fs, path,
          cluster.getDataNodes().get(0))) {
        out.write(new byte[]{1, 2, 3, 4});
      }
      LocatedBlock located = DFSTestUtil.getAllBlocks(fs, path).get(0);
      ExtendedBlock block = located.getBlock();
      List<DataNode> nodes = cluster.getDataNodes();
      DataNode source = find(nodes, located.getLocations()[0]);
      assertEquals(nodes.get(0), source,
          "the block must be on the DataNode the workload pinned it to");
      List<DataNode> rest = new ArrayList<>();
      for (DataNode node : nodes) {
        if (node != source) rest.add(node);
      }
      DataNode head = rest.get(0);
      DataNode mirror = rest.get(1);

      pinBlockKeys(cluster);
      registerSources(cluster, source, head, mirror);

      BlockTokenSecretManager master = cluster.getNamesystem()
          .getBlockManager().getBlockTokenSecretManager();
      String blockPoolId = block.getBlockPoolId();
      NameNodeRpcServer namenode =
          (NameNodeRpcServer) cluster.getNameNodeRpc();
      CausynthCluster.startRecording();

      // NO CLIENT IN THE RECORDED WINDOW.  An earlier version drove this with
      // a client write, and the client's own SASL exchange dragged its whole
      // anchor chain into the target's identity.  A DataNode transfer to TWO
      // targets produces the same mirror pipeline -- Sender.writeBlock sends
      // to targets[0] and passes targets[1..] downstream, so the head opens
      // the mirror connection itself -- in the shape of hdfs-17899-bug3,
      // which passes.
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
      CausynthCluster.recordingPrecondition(
          () -> currentKeyId(master) == initialKeyId + 2,
          "the recorded window must rotate the master twice");
      for (DataNode node : nodes) {
        cluster.getNamesystem().getBlockManager().getDatanodeManager()
            .getDatanode(node.getDatanodeId()).setNeedKeyUpdate(true);
      }

      // Every DataNode takes the new keys over one heartbeat of its own, in
      // the order of the nodes' names, not the cluster's.
      for (DataNode node : CausynthCluster.dataNodes()) {
        refreshKeysFromNameNode(node, "heartbeat");
      }
      BlockTokenSecretManager headKeys =
          head.getBlockPoolTokenSecretManager().get(blockPoolId);
      BlockTokenSecretManager mirrorKeys =
          mirror.getBlockPoolTokenSecretManager().get(blockPoolId);
      CausynthCluster.recordingPrecondition(
          () -> currentKeyId(headKeys) == currentKeyId(master),
          "the head must present the master's current key");
      CausynthCluster.recordingPrecondition(
          () -> mirrorKeys.hasKey(currentKeyId(master)),
          "the mirror must hold the key its heartbeat delivered");

      long request = CausynthMessagePropagation.beginRequest(
          source.getDatanodeId(), "transfer-block");
      try {
        source.transferBlock(block,
            new DatanodeInfo[]{
                new DatanodeInfoBuilder().setNodeID(
                    head.getDatanodeId()).build(),
                new DatanodeInfoBuilder().setNodeID(
                    mirror.getDatanodeId()).build()},
            new StorageType[]{StorageType.DISK, StorageType.DISK},
            new String[0]);
        GenericTestUtils.waitFor(
            () -> mirror.getFSDataset().isValidBlock(block), 20, CausynthCluster.WINDOW_WAIT_MS);
        assertTrue(mirror.getFSDataset().isValidBlock(block),
            "the mirror must receive the block through the head");
      } finally {
        CausynthMessagePropagation.endRequest(request, "transfer-block");
      }
    }
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
    return nodes.stream().filter(node -> node.getDatanodeUuid()
        .equals(location.getDatanodeUuid())).findFirst()
        .orElseThrow(IllegalStateException::new);
  }

  private static void registerSources(MiniDFSCluster cluster,
      DataNode source, DataNode head, DataNode mirror) {
    // Every node, registered whole, before any traffic the recording
    // depends on (CausynthCluster).
    CausynthCluster.registerNameNode(cluster, 0, "hdfs-17967/nn0");
    CausynthCluster.registerDataNode(source, "hdfs-17967/source-dn");
    CausynthCluster.registerDataNode(head, "hdfs-17967/head-dn");
    CausynthCluster.registerDataNode(mirror, "hdfs-17967/mirror-dn");
    CausynthCluster.registerOtherDataNodes(cluster, "hdfs-17967");
  }
}
