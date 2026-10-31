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
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
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
 * hdfs-inj-02 in an HA cluster: a DataNode takes the standby NameNode's
 * KeyUpdateCommand as its current key too, signs the block token of a
 * transfer it sources with it, and a target DataNode that did not receive
 * the standby's update rejects that token
 * (BlockTokenSecretManager#retrievePassword, hdfs-adhoc-12).
 * hdfs-buggy-17383's workload (graphchecker/hdfs-bug-17383), its node names
 * the case's.
 *
 * <p>Based on HDFS-17383's TestUpdateDataNodeCurrentKey (two NameNodes,
 * block tokens on), in the shape of hdfs-adhoc-12's workload
 * (TestCausynthDataTransferRpcFailure: data transfer unencrypted, so the
 * target checks the token with no SASL key check before it).  nn0 is
 * active.  Inside the window the standby nn1 rotates its block keys twice,
 * and each DataNode takes nn1's new keys over ONE heartbeat of its own to
 * nn1, the target first: the source's current key is then nn1's newest,
 * which the target holds only because its own heartbeat delivered it.  The
 * source replicates its block to the target with a token under that key:
 * the recording is healthy.  Lose the target's heartbeat to nn1 -- its
 * request, or nn1's answer after nn1 cleared the node's key-update mark --
 * and the target never holds the key the source signs with.  With the
 * standby's keys taken as not current (the unmodified tree) the source signs
 * with nn0's key whatever nn1 sends, and the same loss is harmless.</p>
 */
public class TestCausynthStandbyCurrentKey {
  /** Block-key serial number pinned by {@link #pinBlockKeys}. */
  private static final int SERIAL_NO = Integer.MAX_VALUE / 17383 + 2;
  /** Longer than any case runs. */
  private static final long NEVER_S = TimeUnit.HOURS.toSeconds(6);

  @Test
  @Timeout(180)
  public void testTransferAfterStandbyKeyUpdate() throws Exception {
    // Nothing set up before the window expires on the wall clock.
    Configuration conf = CausynthCluster.configure(new HdfsConfiguration());
    conf.setBoolean(DFSConfigKeys.DFS_ENCRYPT_DATA_TRANSFER_KEY, false);
    conf.setBoolean(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
    conf.setInt(DFSConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY, 10 * 60 * 1000);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY,
        10 * 60 * 1000);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_ACCESS_KEY_UPDATE_INTERVAL_KEY, 60);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_LIFETIME_KEY, 1);
    // The window's heartbeats are the workload's own, one per DataNode to
    // the standby: four daemon heartbeat streams (two DataNodes, two
    // NameNodes) would race them for the key-update mark, and nothing in
    // the window needs one.  The standby tails no edits in the window.
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, NEVER_S);
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_KEY,
        TimeUnit.SECONDS.toMillis(3 * NEVER_S));
    conf.setLong(DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY, NEVER_S);
    // Each DataNode has IPC connections of its own, as a DataNode process
    // does (CausynthCluster.dataNodeOverlays).
    try (MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .nnTopology(MiniDFSNNTopology.simpleHATopology())
        .numDataNodes(2)
        .dataNodeConfOverlays(CausynthCluster.dataNodeOverlays(2)).build()) {
      cluster.waitActive();
      cluster.transitionToActive(0);
      List<DataNode> nodes = cluster.getDataNodes();
      BlockTokenSecretManager active = master(cluster, 0);
      BlockTokenSecretManager standby = master(cluster, 1);
      // Each DataNode hears that nn0 is active, and takes nn0's keys: the
      // new active marks every DataNode for a key update.
      for (DataNode node : nodes) {
        DataNodeTestUtils.triggerHeartbeat(node);
      }
      GenericTestUtils.waitFor(() -> nodes.stream().allMatch(node ->
          activeIs(node, actor(cluster, node, 0))
              && currentKeyId(keys(node, cluster)) == currentKeyId(active)),
          100, CausynthCluster.WINDOW_WAIT_MS);

      DistributedFileSystem fs = cluster.getFileSystem(0);
      Path path = new Path("/causynth-hdfs-inj-02");
      // Pinned to the first DataNode: the case's roles are read off this
      // block (CausynthCluster.createOnNode).
      try (FSDataOutputStream out = CausynthCluster.createOnNode(fs, path,
          nodes.get(0))) {
        out.write(1);
      }
      LocatedBlock located = DFSTestUtil.getAllBlocks(fs, path).get(0);
      ExtendedBlock block = located.getBlock();
      DataNode source = find(nodes, located.getLocations()[0]);
      assertEquals(nodes.get(0), source,
          "the block must be on the DataNode the workload pinned it to");
      DataNode target = nodes.get(1);
      pinBlockKeys(cluster);
      registerSources(cluster, source, target);

      BlockTokenSecretManager sourceKeys = keys(source, cluster);
      BlockTokenSecretManager targetKeys = keys(target, cluster);
      BPServiceActor sourceToStandby = actor(cluster, source, 1);
      BPServiceActor targetToStandby = actor(cluster, target, 1);
      NameNodeRpcServer standbyRpc =
          (NameNodeRpcServer) cluster.getNameNodeRpc(1);
      CausynthCluster.startRecording();

      // The standby rotates twice, as its own request, as its heartbeat
      // manager does each key-update interval and marks every DataNode
      // for a key update; an explicit rotation marks none, so the workload
      // marks the two it drives.
      int initialKeyId = currentKeyId(standby);
      long rotations = CausynthMessagePropagation.beginRequest(
          standbyRpc.getClientRpcServer(), "rotate-block-keys");
      try {
        for (int rotation = 0; rotation < 2; rotation++) {
          long causynthRotation = CausynthMessagePropagation.beginTick(
              standby, "KEY_MANAGER_TICK");
          try {
            standby.updateKeys(Long.MAX_VALUE);
          } finally {
            CausynthMessagePropagation.endTick(causynthRotation);
          }
        }
      } finally {
        CausynthMessagePropagation.endRequest(rotations, "rotate-block-keys");
      }
      CausynthCluster.recordingPrecondition(
          () -> currentKeyId(standby) == initialKeyId + 2,
          "the recorded window must rotate the standby twice");
      int standbyKeyId = currentKeyId(standby);
      for (DataNode node : new DataNode[]{target, source}) {
        cluster.getNamesystem(1).getBlockManager().getDatanodeManager()
            .getDatanode(node.getDatanodeId()).setNeedKeyUpdate(true);
      }

      // The verifier first, then the signer: each takes the standby's new
      // keys over its own heartbeat to the standby, the KeyUpdateCommand in
      // the answer.  The target's is the delivery a witness fails, and
      // nothing else in the window delivers the key again.
      refreshKeysFromNameNode(target, targetToStandby);
      refreshKeysFromNameNode(source, sourceToStandby);
      CausynthCluster.recordingPrecondition(
          () -> targetKeys.hasKey(currentKeyId(sourceKeys)),
          "the target must hold the key the source signs with, or the"
              + " recording is already the failure");
      CausynthCluster.recordingPrecondition(
          () -> targetKeys.hasKey(standbyKeyId),
          "the target must hold the standby's current key");

      long request = CausynthMessagePropagation.beginRequest(
          source.getDatanodeId(), "replicate-block");
      try {
        source.transferBlock(block,
            new DatanodeInfo[]{new DatanodeInfoBuilder()
                .setNodeID(target.getDatanodeId()).build()},
            new StorageType[]{StorageType.DISK}, new String[0]);
        GenericTestUtils.waitFor(
            () -> target.getFSDataset().isValidBlock(block), 20,
            CausynthCluster.WINDOW_WAIT_MS);
        assertTrue(target.getFSDataset().isValidBlock(block));
      } finally {
        CausynthMessagePropagation.endRequest(request, "replicate-block");
      }
    }
  }

  /**
   * Makes every block-key id the recorded window sees identical across
   * GraphChecker replay sessions, on both NameNodes: each master seeds its
   * serial from SecureRandom once per JVM, in its own half of the id range.
   * Two rotations retire each master's constructor-time keys, which are
   * then dropped everywhere; every DataNode takes the standby's pinned keys
   * and then the active's, so it starts the window holding both pinned
   * pairs, nn0's current key current.  Nothing is recorded yet.
   */
  private static void pinBlockKeys(MiniDFSCluster cluster) throws Exception {
    Set<Integer> pinned = new TreeSet<>();
    for (int index : new int[]{1, 0}) {
      BlockTokenSecretManager master = master(cluster, index);
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
      int current = currentKeyId(master);
      Set<Integer> pair = new TreeSet<>(Arrays.asList(current, current + 1));
      retainKeys(master, pair);
      assertEquals(pair, keyIds(master));
      pinned.addAll(pair);
    }
    for (DataNode node : cluster.getDataNodes()) {
      BlockTokenSecretManager manager = keys(node, cluster);
      manager.addKeys(master(cluster, 1).exportKeys());
      manager.addKeys(master(cluster, 0).exportKeys());
      retainKeys(manager, pinned);
      assertEquals(pinned, keyIds(manager),
          "DataNode " + node.getDatanodeId() + " holds unpinned keys");
      assertEquals(currentKeyId(master(cluster, 0)), currentKeyId(manager));
    }
  }

  /**
   * One key-refresh heartbeat of {@code datanode} to the NameNode {@code
   * actor} serves, as the workload's own request on that node: the
   * heartbeat is sent and every command it returns -- a KeyUpdateCommand
   * among them -- is applied before this returns
   * (CausynthCluster#refreshKeysFromNameNode, to a NameNode of choice).  A
   * modeled transport failure leaves the node's keys as they were.
   */
  private static void refreshKeysFromNameNode(DataNode datanode,
      BPServiceActor actor) {
    BPOfferService service = datanode.getAllBpOs().get(0);
    long request = CausynthMessagePropagation.beginRequest(
        datanode.getDatanodeId(), "heartbeat");
    try {
      HeartbeatResponse response = actor.sendHeartBeat(false);
      DatanodeCommand[] commands = response.getCommands();
      if (commands != null) {
        for (DatanodeCommand command : commands) {
          service.processCommandFromActor(command, actor);
        }
      }
    } catch (IOException transport) {
      return;
    } finally {
      CausynthMessagePropagation.endRequest(request, "heartbeat");
    }
  }

  private static BlockTokenSecretManager master(MiniDFSCluster cluster,
      int index) {
    return cluster.getNamesystem(index).getBlockManager()
        .getBlockTokenSecretManager();
  }

  private static BlockTokenSecretManager keys(DataNode node,
      MiniDFSCluster cluster) {
    return node.getBlockPoolTokenSecretManager()
        .get(cluster.getNamesystem(0).getBlockPoolId());
  }

  /** The actor of {@code node} that serves NameNode {@code index}. */
  private static BPServiceActor actor(MiniDFSCluster cluster, DataNode node,
      int index) {
    int port = cluster.getNameNode(index).getServiceRpcAddress().getPort();
    return node.getAllBpOs().get(0).getBPServiceActors().stream()
        .filter(actor -> actor.getNNSocketAddress().getPort() == port)
        .findFirst().orElseThrow(IllegalStateException::new);
  }

  private static boolean activeIs(DataNode node, BPServiceActor actor) {
    return node.getAllBpOs().get(0).getActiveNN() == actor.bpNamenode;
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
      DataNode source, DataNode target) {
    // Every node, registered whole, before any traffic the recording
    // depends on (CausynthCluster).
    CausynthCluster.registerNameNode(cluster, 0, "hdfs-inj-02/nn0");
    CausynthCluster.registerNameNode(cluster, 1, "hdfs-inj-02/nn1");
    CausynthCluster.registerDataNode(source, "hdfs-inj-02/source-dn");
    CausynthCluster.registerDataNode(target, "hdfs-inj-02/target-dn");
    CausynthCluster.registerOtherDataNodes(cluster, "hdfs-inj-02");
  }
}
