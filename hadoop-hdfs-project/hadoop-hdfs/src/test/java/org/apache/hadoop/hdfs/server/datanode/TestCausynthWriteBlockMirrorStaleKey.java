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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
 * <p>THE RECORDING MUST LEAVE A ROTATION GAP.  Neither DataNode is refreshed
 * inside the recorded window, so both keep the pinned pair while the master
 * moves two serials ahead.  The recording stays HEALTHY -- the mirror still
 * holds the key the head presents, because a DataNode retains its older keys
 * until they expire -- and that expiry is exactly what a witness has to buy
 * with a clock move.  Refreshing either node here would put the master's
 * CURRENT serial on the wire, and with a zero gap no clock assignment can
 * make it expire: on hdfs-17899 that left every composed path either
 * self-contradictory or satisfied at the recorded valuation, and no witness.
 * Which of the two nodes the NameNode picks as the head does not matter
 * precisely because neither is refreshed.</p>
 */
public class TestCausynthWriteBlockMirrorStaleKey {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestCausynthWriteBlockMirrorStaleKey.class);

  /** Block-key serial number pinned by {@link #pinBlockKeys}. */
  private static final int SERIAL_NO = Integer.MAX_VALUE / 17967 + 2;

  @Test
  @Timeout(120)
  public void testWritePipelineAfterKeyRotations() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(DFSConfigKeys.DFS_ENCRYPT_DATA_TRANSFER_KEY, true);
    conf.setBoolean(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
    conf.setInt(DFSConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY, 10 * 60 * 1000);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY,
        10 * 60 * 1000);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_ACCESS_KEY_UPDATE_INTERVAL_KEY, 60);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_LIFETIME_KEY, 1);
    // No automatic heartbeat inside the recorded window: a refresh the
    // workload did not ask for would close the rotation gap behind its back.
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 3600);
    // ...but suppressed heartbeats make every DataNode look STALE after the
    // default 30s and placement then avoids them.  The native run is too
    // quick to notice; the concolic replay is not.
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_KEY,
        TimeUnit.HOURS.toMillis(6));
    conf.setBoolean(
        DFSConfigKeys.DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_WRITE_KEY, false);
    conf.setBoolean(
        DFSConfigKeys.DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_READ_KEY, false);
    try (MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(3).build();
         DistributedFileSystem fs = cluster.getFileSystem()) {
      cluster.waitActive();

      // The block is written BEFORE anything is recorded, at replication 1,
      // purely so a DataNode has something to transfer.
      Path path = new Path("/causynth-hdfs-17967-a");
      try (FSDataOutputStream out = fs.create(path, (short) 1)) {
        out.write(new byte[]{1, 2, 3, 4});
      }
      LocatedBlock located = DFSTestUtil.getAllBlocks(fs, path).get(0);
      ExtendedBlock block = located.getBlock();
      List<DataNode> nodes = cluster.getDataNodes();
      DataNode source = find(nodes, located.getLocations()[0]);
      List<DataNode> rest = new ArrayList<>();
      for (DataNode node : nodes) {
        if (node != source) rest.add(node);
      }
      DataNode head = rest.get(0);
      DataNode mirror = rest.get(1);

      pinBlockKeys(cluster);
      stageRefreshGap(cluster, head);
      registerSources(cluster, source, head, mirror);

      BlockTokenSecretManager master = cluster.getNamesystem()
          .getBlockManager().getBlockTokenSecretManager();
      String blockPoolId = block.getBlockPoolId();
      NameNodeRpcServer namenode =
          (NameNodeRpcServer) cluster.getNameNodeRpc();
      CausynthMessagePropagation.registerSourceAlias(master,
          namenode.getClientRpcServer());
      for (DataNode node : nodes) {
        CausynthMessagePropagation.registerSourceAlias(
            node.getBlockPoolTokenSecretManager().get(blockPoolId),
            node.getDatanodeId());
      }
      CausynthMessagePropagation.startRecording();

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
      assertEquals(initialKeyId + 2, currentKeyId(master),
          "the recorded window must rotate the master twice");
      // The MIRROR refreshes normally.  The SOURCE deliberately does NOT: it
      // is the head's upstream partner, and if it moved forward the head
      // would have to resolve a key it only gets from its own refresh --
      // which is the very RPC a witness flips, so the flip would break the
      // source->head hop BEFORE the mirror is ever contacted.  Leaving the
      // source on the staged key keeps that hop healthy in both worlds.
      cluster.getNamesystem().getBlockManager().getDatanodeManager()
          .getDatanode(mirror.getDatanodeId()).setNeedKeyUpdate(true);
      refreshKeysFromNameNode(mirror, "heartbeat");
      cluster.getNamesystem().getBlockManager().getDatanodeManager()
          .getDatanode(head.getDatanodeId()).setNeedKeyUpdate(true);

      // THE TRIGGER.  This is the head's own key-refresh RPC, and its
      // failure is SWALLOWED -- which is not a convenience, it is the bug:
      // the production code ignores a failed refresh and keeps using the
      // keys it already holds.  Left uncaught, a flipped
      // hadoopIpcRequestFails throws straight out of the workload and the
      // declared occurrence is never reached at all, so no MARKER_FLIPPED
      // witness could ever be asked for.
      try {
        refreshKeysFromNameNode(head, "heartbeat");
      } catch (IOException refreshFailed) {
        LOG.info("the head's key refresh failed; it keeps the keys it has",
            refreshFailed);
      }

      BlockTokenSecretManager headKeys =
          head.getBlockPoolTokenSecretManager().get(blockPoolId);
      BlockTokenSecretManager mirrorKeys =
          mirror.getBlockPoolTokenSecretManager().get(blockPoolId);
      BlockTokenSecretManager sourceKeys =
          source.getBlockPoolTokenSecretManager().get(blockPoolId);
      // The RECORDING is healthy in every respect: the refresh succeeded, so
      // the head stands on the master's current key and the mirror holds it.
      // The whole difference between this run and the failing one is the one
      // RPC above.
      assertEquals(currentKeyId(master), currentKeyId(headKeys),
          "the head's refresh must SUCCEED in the recording");
      assertTrue(mirrorKeys.hasKey(currentKeyId(headKeys)),
          "the mirror must resolve what the head presents, or the recording"
              + " is already the failure");
      // ...and the failing world is reachable by that flip alone: the key
      // the head would fall back on is one the mirror has never held.
      assertTrue(!mirrorKeys.hasKey(SERIAL_NO + 1),
          "the mirror must NEVER have held the head's fallback key, or a"
              + " failed refresh changes nothing and only a clock can");
      assertTrue(headKeys.hasKey(currentKeyId(sourceKeys)),
          "the head must hold the SOURCE's key whatever happens to its own"
              + " refresh, or the flip breaks the upstream hop first");

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
            () -> mirror.getFSDataset().isValidBlock(block), 20, 20000);
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

  /**
   * Stages the ONE asymmetry a failed key-refresh RPC needs in order to be
   * the trigger, and nothing else.
   *
   * <p>{@code addKeys} MERGES -- {@code allKeys.put} per received key -- and
   * only {@code removeExpiredKeys()} ever removes, driven by a clock.  So if
   * the head and the mirror start from the same key set, a failed refresh
   * only makes the head present an OLDER key the mirror still holds, the
   * flip cannot reach the fatal, and the only witness left is a clock move.
   * That is exactly why this family kept reporting CLOCK_SKEW while carrying
   * the marker.</p>
   *
   * <p>So: roll the master once more and drop the pinned first key from its
   * EXPORT, then hand that export to everyone EXCEPT the head.  The mirror
   * has therefore never held {@code SERIAL_NO + 1}, while the head still
   * stands on it -- and still holds {@code SERIAL_NO + 2}, which is what its
   * upstream partner presents, so a flipped refresh cannot break that hop
   * first.  The head's in-window refresh is then the only thing between the
   * healthy run and the failing one.</p>
   */
  private static void stageRefreshGap(MiniDFSCluster cluster, DataNode head)
      throws Exception {
    BlockTokenSecretManager master = cluster.getNamesystem()
        .getBlockManager().getBlockTokenSecretManager();
    long causynthRotation = CausynthMessagePropagation.beginTick(
        master, "KEY_MANAGER_TICK");
    try {
      master.updateKeys(Long.MAX_VALUE);
    } finally {
      CausynthMessagePropagation.endTick(causynthRotation);
    }
    assertEquals(SERIAL_NO + 2, master.getCurrentKey().getKeyId());
    Set<Integer> staged =
        new TreeSet<>(Arrays.asList(SERIAL_NO + 2, SERIAL_NO + 3));
    retainKeys(master, staged);
    assertEquals(staged, keyIds(master),
        "the master must no longer EXPORT the head's fallback key");
    String blockPoolId = cluster.getNamesystem().getBlockPoolId();
    for (DataNode node : cluster.getDataNodes()) {
      BlockTokenSecretManager manager =
          node.getBlockPoolTokenSecretManager().get(blockPoolId);
      if (node == head) {
        assertEquals(SERIAL_NO + 1, currentKeyId(manager),
            "the head must stand on the fallback key");
        assertTrue(manager.hasKey(SERIAL_NO + 2),
            "the head must hold its upstream partner's key");
        continue;
      }
      manager.addKeys(master.exportKeys());
      retainKeys(manager, staged);
      assertEquals(SERIAL_NO + 2, currentKeyId(manager));
      assertTrue(!manager.hasKey(SERIAL_NO + 1),
          "DataNode " + node.getDatanodeId() + " must never have held the"
              + " head's fallback key");
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

  /** One key-refresh heartbeat, so the node can verify fresh block tokens. */
  private static void refreshKeysFromNameNode(DataNode datanode, String api)
      throws IOException {
    BPOfferService service = datanode.getAllBpOs().get(0);
    BPServiceActor actor = service.getBPServiceActors().get(0);
    long request = CausynthMessagePropagation.beginRequest(
        datanode.getDatanodeId(), api);
    try {
      HeartbeatResponse response = actor.sendHeartBeat(false);
      DatanodeCommand[] commands = response.getCommands();
      if (commands != null) {
        for (DatanodeCommand command : commands) {
          service.processCommandFromActor(command, actor);
        }
      }
    } finally {
      CausynthMessagePropagation.endRequest(request, api);
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
    NameNodeRpcServer namenode = (NameNodeRpcServer) cluster.getNameNodeRpc();
    CausynthMessagePropagation.registerSource(
        namenode.getClientRpcServer(), "CLUSTER_NODE", "NAMENODE",
        "hdfs-17967/nn0", 0);
    CausynthMessagePropagation.registerSource(source.getDatanodeId(),
        "CLUSTER_NODE", "DATANODE", "hdfs-17967/source-dn", 0);
    CausynthMessagePropagation.registerSource(head.getDatanodeId(),
        "CLUSTER_NODE", "DATANODE", "hdfs-17967/head-dn", 0);
    CausynthMessagePropagation.registerSource(mirror.getDatanodeId(),
        "CLUSTER_NODE", "DATANODE", "hdfs-17967/mirror-dn", 0);
  }
}
