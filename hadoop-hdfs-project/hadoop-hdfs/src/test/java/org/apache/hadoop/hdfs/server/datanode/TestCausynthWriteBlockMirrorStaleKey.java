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
    conf.setInt(
        DFSConfigKeys.DFS_CLIENT_BLOCK_WRITE_LOCATEFOLLOWINGBLOCK_RETRIES_KEY,
        12);
    conf.setInt(DFSConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY, 10 * 60 * 1000);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY,
        10 * 60 * 1000);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_ACCESS_KEY_UPDATE_INTERVAL_KEY, 60);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_LIFETIME_KEY, 1);
    // No automatic heartbeat inside the recorded window: a refresh the
    // workload did not ask for would close the rotation gap behind its back.
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 3600);
    // ...but suppressing heartbeats makes every DataNode look STALE after
    // the default 30s, and block placement then avoids stale nodes.  The
    // native run is quick enough never to notice; the concolic replay is not,
    // and it placed ONE replica instead of two, so there was no mirror, no
    // handshake and no target ("the write needs a two-node pipeline").  The
    // staleness window is therefore pushed out past any replay, and
    // stale-node avoidance is turned off for writes as well.
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_KEY,
        TimeUnit.HOURS.toMillis(6));
    conf.setBoolean(
        DFSConfigKeys.DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_WRITE_KEY, false);
    conf.setBoolean(
        DFSConfigKeys.DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_READ_KEY, false);
    try (MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(2).build();
         DistributedFileSystem fs = cluster.getFileSystem()) {
      cluster.waitActive();

      pinBlockKeys(cluster);

      // WARM THE CLIENT'S OWN ENCRYPTION KEY, before the rotations and after
      // the pinning.  A DFSClient fetches its data-encryption key lazily, on
      // its first encrypted connection, and caches it until it expires.
      // Without this the first encrypted write in the recorded window is also
      // the client's first fetch, so the client gets the master's CURRENT key
      // while both DataNodes are deliberately two serials behind -- the gap
      // runs the wrong way, neither node can resolve the client's key, the
      // client excludes both and the write fails outright with "could only be
      // written to 0 of the 1 minReplication nodes".  Warmed here the client
      // holds a key the DataNodes have, so the recording is healthy and the
      // only stale key in it is the pipeline head's, which is what this case
      // is about.  The token lifetime and key-update interval above put the
      // cached key's expiry about an hour out, so it is not refetched.
      Path warmup = new Path("/causynth-hdfs-17967-a-warmup");
      try (FSDataOutputStream out = fs.create(warmup, (short) 2)) {
        out.write(new byte[]{0});
      }

      registerSources(cluster, fs.getClient());

      BlockTokenSecretManager master = cluster.getNamesystem()
          .getBlockManager().getBlockTokenSecretManager();
      String blockPoolId = cluster.getNamesystem().getBlockPoolId();
      NameNodeRpcServer namenode =
          (NameNodeRpcServer) cluster.getNameNodeRpc();
      CausynthMessagePropagation.registerSourceAlias(master,
          namenode.getClientRpcServer());
      for (DataNode node : cluster.getDataNodes()) {
        CausynthMessagePropagation.registerSourceAlias(
            node.getBlockPoolTokenSecretManager().get(blockPoolId),
            node.getDatanodeId());
      }
      CausynthMessagePropagation.startRecording();

      // THE ROTATIONS ARE DRIVEN HERE, NOT WAITED FOR.  Shortening the key
      // update interval and waiting for the NameNode's own key-updater daemon
      // to notice leaves the rotation on a thread with no root context, and
      // the first-pass trace then refuses to build a skeleton for it:
      // "region skeleton has no authoritative execution context root:
      // BlockTokenSecretManager.updateKeys()".  Calling it inside a tick
      // scope -- the same scope pinBlockKeys uses for the two rotations it
      // makes before recording -- gives each rotation an owner, and it is
      // deterministic besides, which matters under the concolic VM where a
      // daemon's timing is not the recording's.
      // ...and inside a REQUEST scope on the NameNode, not only a tick.  A
      // tick names the rotation; the request names the node it happened on.
      // Driven from the test's main thread, which sits inside no request, the
      // rotation has no root and the first-pass trace refuses to build a
      // skeleton for it -- "region skeleton has no authoritative execution
      // context root".  In the cases that pass, the equivalent rotation runs
      // on the NameNode's own instrumented heartbeat thread, which carries
      // one; this supplies the same thing explicitly.
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

      // Rotating the master directly does not, by itself, tell the DataNodes
      // to come and get the new keys: the NameNode normally sets that flag
      // from its own HeartbeatManager when the update interval elapses, and
      // driving the rotation here bypasses it.  Ask for the KeyUpdateCommand
      // explicitly, so the keys still arrive the way the case declares them
      // -- on a heartbeat response, over the block-key ports -- rather than
      // being installed in process.
      for (DataNode node : cluster.getDataNodes()) {
        cluster.getNamesystem().getBlockManager().getDatanodeManager()
            .getDatanode(node.getDatanodeId()).setNeedKeyUpdate(true);
      }

      // TOKENS AND HANDSHAKES NEED DIFFERENT KEYS HERE, so the two are
      // separated rather than left to one refresh.
      //
      // Leaving both DataNodes unrefreshed does give a rotation gap, but it
      // breaks the write before any mirror handshake happens: the NameNode
      // signs the client's BLOCK TOKEN with its current key, a DataNode that
      // does not hold that key cannot verify it, and the client excludes both
      // nodes ("Got access token error ... could only be written to 0 of the
      // 1 minReplication nodes").  That is a different failure from the one
      // this case is about.
      //
      // So both nodes take the rotated keys -- tokens verify -- and each then
      // has its CURRENT key rolled back to the pinned one, which every node
      // still retains because it has not expired.  The pipeline head derives
      // the data-encryption key it presents to the mirror from that current
      // key, so the handshake carries a key two serials behind the master
      // while everything else in the write is up to date.  The recording
      // stays healthy because the mirror still holds it; a witness has to
      // move the clock past its expiry.  Both nodes are rolled back because
      // the NameNode, not this test, chooses which one heads the pipeline.
      for (DataNode node : cluster.getDataNodes()) {
        BlockTokenSecretManager manager =
            node.getBlockPoolTokenSecretManager().get(blockPoolId);
        refreshKeysFromNameNode(node);
        rollCurrentKeyBackTo(manager, SERIAL_NO + 1);
        assertEquals(SERIAL_NO + 1, currentKeyId(manager),
            "DataNode " + node.getDatanodeId() + " must present the pinned"
                + " key, or the recording carries no rotation gap");
        assertTrue(manager.hasKey(currentKeyId(master)),
            "DataNode " + node.getDatanodeId() + " must still hold the"
                + " master's current key, or block tokens cannot verify");
      }

      Path path = new Path("/causynth-hdfs-17967-a");
      long request = CausynthMessagePropagation.beginRequest(
          fs.getClient(), "write-block");
      try {
        try (FSDataOutputStream out = fs.create(path, (short) 2)) {
          out.write(new byte[]{1, 2, 3, 4});
        }
        // Observed, not asserted beyond liveness: the recording takes the
        // healthy arm by construction, and what the campaign is about is the
        // counterfactual in which the mirror can no longer resolve the head's
        // key.
        LocatedBlock located = DFSTestUtil.getAllBlocks(fs, path).get(0);
        assertEquals(2, located.getLocations().length,
            "the write needs a two-node pipeline for a mirror handshake");
      } finally {
        CausynthMessagePropagation.endRequest(request, "write-block");
      }
      assertTrue(fs.exists(path));
    }
  }

  /**
   * Makes every block-key id the recorded window sees identical across
   * GraphChecker replay sessions; see the same helper in
   * {@code TestCausynthDataTransferRpcFailure}, from which this is taken
   * unchanged but for the serial.
   */
  private static void pinBlockKeys(MiniDFSCluster cluster) throws Exception {
    BlockTokenSecretManager master = cluster.getNamesystem()
        .getBlockManager().getBlockTokenSecretManager();
    master.setSerialNo(SERIAL_NO);
    { // causynth-d3-rotation-scope
      long causynthRotation = CausynthMessagePropagation.beginTick(
          master, "KEY_MANAGER_TICK");
      try {
        master.updateKeys(Long.MAX_VALUE);
      } finally {
        CausynthMessagePropagation.endTick(causynthRotation);
      }
    }
    { // causynth-d3-rotation-scope
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
  private static void refreshKeysFromNameNode(DataNode datanode)
      throws IOException {
    BPOfferService service = datanode.getAllBpOs().get(0);
    BPServiceActor actor = service.getBPServiceActors().get(0);
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
    } finally {
      CausynthMessagePropagation.endRequest(request, "heartbeat");
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

  private static void registerSources(MiniDFSCluster cluster,
      DFSClient client) throws IOException {
    NameNodeRpcServer namenode = (NameNodeRpcServer) cluster.getNameNodeRpc();
    CausynthMessagePropagation.registerSource(
        client, "EXTERNAL_APP", "DFS_CLIENT", "hdfs-17967/client", 0);
    CausynthMessagePropagation.registerSource(
        namenode.getClientRpcServer(), "CLUSTER_NODE", "NAMENODE",
        "hdfs-17967/nn0", 0);
    int index = 0;
    for (DataNode node : cluster.getDataNodes()) {
      CausynthMessagePropagation.registerSource(node.getDatanodeId(),
          "CLUSTER_NODE", "DATANODE", "hdfs-17967/dn" + index++, 0);
    }
  }
}
