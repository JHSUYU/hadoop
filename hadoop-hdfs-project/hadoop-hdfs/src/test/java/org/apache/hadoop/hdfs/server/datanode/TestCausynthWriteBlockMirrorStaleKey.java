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
      // No DAEMON heartbeat may fire inside the recorded window.  The
      // interval is already 3600 s, but a node still has stray turns left
      // over from start-up, and their NameNode-side registration
      // conversions land on an IPC handler with no inbound context: two of
      // them then share ONE address -- a BLOCKING OCCURRENCE_ADDRESS_SHARED
      // over REGION.16ba41cf that withholds the candidate.  It is timing
      // dependent (the recorded root calls differ every run), which is why
      // it comes and goes.  Every heartbeat this case needs is driven
      // explicitly below, so the daemon has nothing left to do.
      //
      // Handing the node its source anchor instead -- what hdfs-11741 does
      // -- was tried and is worse HERE: it turns those stray turns into
      // ANCHORED offer-service occurrences that a focused replay then has
      // to reproduce, and path A came back with 18 REPLAY
      // TOPOLOGY_DIVERGENCE rows.  11741 tolerates that because its
      // heartbeats fire once a second and the corpus is dense with them;
      // this workload has a handful of strays, which is the worst case.
      for (DataNode node : cluster.getDataNodes()) {
        DataNodeTestUtils.setHeartbeatsDisabledForTests(node, true);
        // And no incremental block report either.  When the transfer lands,
        // the head and the mirror each call notifyNamenodeReceivedBlock,
        // which sends blockReceivedAndDeleted(registration, ...) on the
        // block-pool actor thread with no request scope open.  The
        // NameNode then converts that DatanodeRegistration on an IPC
        // handler with a LOST context, and the TWO of them -- one per node
        // -- share ONE address: the BLOCKING OCCURRENCE_ADDRESS_SHARED over
        // REGION.16ba41cf that has withheld this candidate through three
        // earlier attempts (source anchor, which made it 18 divergences;
        // daemon heartbeats off, which changed nothing).  The workload's
        // own assertion reads mirror.getFSDataset() directly, so nothing
        // here needs the NameNode to learn about the new replica.
        DataNodeTestUtils.pauseIBR(node);
      }
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
      for (DataNode node : nodes) {
        cluster.getNamesystem().getBlockManager().getDatanodeManager()
            .getDatanode(node.getDatanodeId()).setNeedKeyUpdate(true);
      }

      // Only the HEAD is skewed: it derives the key it presents to the mirror
      // from its current key, and the mirror still RETAINS that key, so the
      // recording is healthy and a witness has to move the clock past its
      // expiry.  The mirror stays current, so the two are not symmetric.
      for (DataNode node : nodes) {
        refreshKeysFromNameNode(node, "heartbeat");
      }
      BlockTokenSecretManager headKeys =
          head.getBlockPoolTokenSecretManager().get(blockPoolId);
      BlockTokenSecretManager mirrorKeys =
          mirror.getBlockPoolTokenSecretManager().get(blockPoolId);
      rollCurrentKeyBackTo(headKeys, SERIAL_NO + 1);
      assertEquals(SERIAL_NO + 1, currentKeyId(headKeys),
          "the head must present the pinned key, or there is no gap");
      assertEquals(currentKeyId(master), currentKeyId(mirrorKeys),
          "the mirror must be current, or the two are symmetric");
      assertTrue(mirrorKeys.hasKey(SERIAL_NO + 1),
          "the mirror must still RETAIN the head's key, or the recording is"
              + " already the failure");

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
