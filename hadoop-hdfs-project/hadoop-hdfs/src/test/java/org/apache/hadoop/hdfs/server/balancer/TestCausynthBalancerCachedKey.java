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
package org.apache.hadoop.hdfs.server.balancer;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.net.Socket;
import java.util.ArrayList;
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
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.NameNodeProxies;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo.DatanodeInfoBuilder;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;
import org.apache.hadoop.hdfs.protocol.datatransfer.Sender;
import org.apache.hadoop.hdfs.protocol.datatransfer.TrustedChannelResolver;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.DataTransferSaslUtil;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.SaslDataTransferClient;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BlockOpResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.hdfs.security.token.block.BlockKey;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.apache.hadoop.hdfs.server.datanode.CausynthCluster;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeRpcServer;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.ipc.CausynthMessagePropagation;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * HDFS-17899 bug1: the Balancer's {@link KeyManager} caches one
 * {@link DataEncryptionKey} and hands it to every block move until the cache
 * expires, so a key refresh that does NOT replace the cached key -- because
 * the refresh RPC failed, or because the cache has not expired -- leaves the
 * Balancer presenting a key the destination DataNode may no longer resolve.
 * The destination answers ERROR_UNKNOWN_KEY, the move dies, and nothing
 * clears the cache and retries, which is what PR 8364 adds.
 *
 * <p>This drives the defect directly instead of through
 * {@code TestBalancerWithEncryptedTransfer}.  The causal chain that test
 * produces is seventeen links deep and only its root survives into a replay,
 * and an occurrence IS its causal address, so the plan and the replay minted
 * different anchors for the same exchange and every path was refused on
 * identity (OCCURRENCE_ANCHOR_UNMATCHED) -- never on the mechanism.  The same
 * rewrite is what moved hdfs-17967 path A from refused to composing, and
 * hdfs-17899 bug3, which passes, was written this way from the start.
 *
 * <p>Nothing about the defect is given up.  The mover here IS a real
 * {@link KeyManager}: it takes its keys from the NameNode over the recorded
 * RPC (where the {@code FAULT:HADOOP_IPC:REQUEST} fault point lives), caches its
 * encryption key in {@link KeyManager#newDataEncryptionKey()}, and signs the
 * move's access token with {@link KeyManager#getAccessToken}, exactly as
 * {@code Dispatcher.PendingMove.dispatch} does.  What is left out is the
 * balancer's block-selection machinery, which no part of the mechanism
 * touches.
 *
 * <p>THE KEY FLOWS THE RPC'S WAY (experiments/hadoop/lib/README.md).  Inside
 * the window the master rotates twice; every DataNode takes the new keys over
 * its own heartbeat, one each, whose answer carries the KeyUpdateCommand; the
 * Balancer then takes the NameNode's keys over its own RPC and presents the
 * master's CURRENT key in the move.  The recording succeeds.  A world whose
 * destination did not get that heartbeat's answer -- the request failed, or
 * the NameNode applied it and the reply was lost -- has the destination
 * without the presented key, which is the failure: no key has to expire.
 */
public class TestCausynthBalancerCachedKey {
  /** Block-key serial number pinned by {@link #pinBlockKeys}. */
  private static final int SERIAL_NO = Integer.MAX_VALUE / 17899;

  @Test
  @Timeout(180)
  public void testBlockMoveWithCachedKeyAfterRotations() throws Exception {
    // Nothing set up before the window expires on the wall clock.
    Configuration conf = CausynthCluster.configure(new HdfsConfiguration());
    conf.setBoolean(DFSConfigKeys.DFS_ENCRYPT_DATA_TRANSFER_KEY, true);
    conf.setBoolean(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
    conf.setInt(DFSConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY, 10 * 60 * 1000);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY,
        10 * 60 * 1000);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_ACCESS_KEY_UPDATE_INTERVAL_KEY, 60);
    // No key and no cached encryption key may lapse on the wall clock inside
    // the window: a replay on an interpreter-only JVM runs far longer than
    // the native run.
    conf.setLong(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_LIFETIME_KEY, 600);
    // A concolic replay is slow, and one held to its schedule prefix can
    // hold a heartbeat until its turn: placement must not read a slow node
    // as a stale one.  Daemon heartbeats themselves run as in production:
    // the rotations below are explicit and mark no node for a key update,
    // so only the nodes this workload marks refresh.
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
      // purely so one DataNode holds a replica another can be told to take.
      Path path = new Path("/causynth-hdfs-17899-bug1");
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
      DataNode proxy = find(nodes, located.getLocations()[0]);
      assertEquals(nodes.get(0), proxy,
          "the block must be on the DataNode the workload pinned it to");
      List<DataNode> rest = new ArrayList<>();
      for (DataNode node : nodes) {
        if (node != proxy) {
          rest.add(node);
        }
      }
      DataNode target = rest.get(0);

      pinBlockKeys(cluster);
      BlockTokenSecretManager master = cluster.getNamesystem()
          .getBlockManager().getBlockTokenSecretManager();
      String blockPoolId = block.getBlockPoolId();
      NameNodeRpcServer namenode =
          (NameNodeRpcServer) cluster.getNameNodeRpc();
      // Every node, registered whole, before any traffic the recording
      // depends on (CausynthCluster); the balancer is this case's own.
      CausynthCluster.registerNameNode(cluster, 0, "hdfs-17899/nn0");
      CausynthCluster.registerDataNode(proxy, "hdfs-17899/proxy-dn");
      CausynthCluster.registerDataNode(target, "hdfs-17899/target-dn");
      CausynthCluster.registerDataNode(rest.get(1), "hdfs-17899/spare-dn");
      CausynthCluster.registerOtherDataNodes(cluster, "hdfs-17899");
      CausynthMessagePropagation.registerSource(this, "EXTERNAL_APP",
          "BALANCER", "hdfs-17899/balancer", 0);

      NamenodeProtocol rpc = NameNodeProxies.createProxy(conf, fs.getUri(),
          NamenodeProtocol.class).getProxy();
      KeyManager keyManager = null;
      long balance = 0L;
      try {
        // Every heartbeat of the window is the workload's (CausynthCluster).
        CausynthCluster.driveHeartbeats();
        CausynthCluster.startRecording();

        // 1. The master rotates twice: its current key is two serials past
        //    what every node holds.
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
          CausynthMessagePropagation.endRequest(rotations,
              "rotate-block-keys");
        }
        CausynthCluster.recordingPrecondition(
            () -> currentKeyId(master) == initialKeyId + 2,
            "the recorded window must rotate the master twice");

        // 2. Every DataNode takes the new keys over its OWN heartbeat, one
        //    each, in the order of the nodes' names: the answer carries the
        //    KeyUpdateCommand, and the IPC doors' fault points on this RPC
        //    are what a witness flips.
        for (DataNode node : nodes) {
          cluster.getNamesystem().getBlockManager().getDatanodeManager()
              .getDatanode(node.getDatanodeId()).setNeedKeyUpdate(true);
        }
        for (DataNode node : CausynthCluster.dataNodes()) {
          refreshKeysFromNameNode(node, "heartbeat");
        }

        // 3. The Balancer takes the NameNode's keys over its own RPC.  Its
        //    own request, not part of the move: every request is rooted
        //    where it happens, one scope each.
        long fetch = CausynthMessagePropagation.beginRequest(this,
            "fetch-block-keys");
        try {
          keyManager = new KeyManager(blockPoolId, rpc, true, conf);
          CausynthMessagePropagation.registerSourceAlias(keyManager, this);
        } finally {
          CausynthMessagePropagation.endRequest(fetch, "fetch-block-keys");
        }

        // 4. The move: the Balancer presents the master's current key.
        balance = CausynthMessagePropagation.beginRequest(this,
            "balance-block");
        int presented = keyManager.newDataEncryptionKey().keyId;
        CausynthCluster.recordingPrecondition(
            () -> presented == currentKeyId(master),
            "the balancer must present the master's CURRENT key");
        BlockTokenSecretManager targetKeys =
            target.getBlockPoolTokenSecretManager().get(blockPoolId);
        CausynthCluster.recordingPrecondition(
            () -> targetKeys.hasKey(presented),
            "the destination must hold the presented key, delivered by its"
                + " heartbeat, or the recording is already the failure");

        DatanodeInfo proxyInfo = new DatanodeInfoBuilder()
            .setNodeID(proxy.getDatanodeId()).build();
        Token<BlockTokenIdentifier> accessToken = keyManager.getAccessToken(
            block, new StorageType[]{StorageType.DISK}, new String[]{""});
        Status status = sendReplaceBlock(conf, keyManager, target, block,
            proxy.getDatanodeUuid(), proxyInfo, accessToken);
        assertEquals(Status.SUCCESS, status,
            "the recorded move must SUCCEED: the destination holds the key its"
                + " heartbeat delivered");
        final DataNode destination = target;
        GenericTestUtils.waitFor(
            () -> destination.getFSDataset().isValidBlock(block), 20, CausynthCluster.WINDOW_WAIT_MS);
        assertTrue(target.getFSDataset().isValidBlock(block),
            "the destination must receive the block from the proxy");
      } finally {
        if (balance != 0L) {
          CausynthMessagePropagation.endRequest(balance, "balance-block");
        }
        if (keyManager != null) {
          keyManager.close();
        }
        RPC.stopProxy(rpc);
      }
    }
  }

  /**
   * Sends one REPLACE_BLOCK over an encrypted connection, the way
   * {@code Dispatcher.PendingMove.dispatch} does, with the Balancer's
   * {@link KeyManager} as the encryption-key factory.  {@code
   * DFSTestUtil.replaceBlock} cannot be used: it writes a raw socket with
   * {@code DUMMY_TOKEN} and no SASL, which a cluster with
   * {@code dfs.encrypt.data.transfer} refuses before the op is read.
   */
  private static Status sendReplaceBlock(Configuration conf,
      KeyManager keyManager, DataNode target, ExtendedBlock block,
      String delHint, DatanodeInfo proxyInfo,
      Token<BlockTokenIdentifier> accessToken) throws IOException {
    SaslDataTransferClient saslClient = new SaslDataTransferClient(conf,
        DataTransferSaslUtil.getSaslPropertiesResolver(conf),
        TrustedChannelResolver.getInstance(conf));
    int bufferSize = DFSUtilClient.getIoFileBufferSize(conf);
    Socket sock = new Socket();
    DataOutputStream out = null;
    DataInputStream in = null;
    try {
      sock.connect(
          NetUtils.createSocketAddr(target.getDatanodeId().getXferAddr()),
          HdfsConstants.READ_TIMEOUT);
      sock.setSoTimeout(HdfsConstants.READ_TIMEOUT * 5);
      sock.setKeepAlive(true);
      OutputStream unbufOut = sock.getOutputStream();
      InputStream unbufIn = sock.getInputStream();
      IOStreamPair saslStreams = saslClient.socketSend(sock, unbufOut,
          unbufIn, keyManager, accessToken, target.getDatanodeId());
      out = new DataOutputStream(
          new BufferedOutputStream(saslStreams.out, bufferSize));
      in = new DataInputStream(
          new BufferedInputStream(saslStreams.in, bufferSize));
      new Sender(out).replaceBlock(block, StorageType.DISK, accessToken,
          delHint, proxyInfo, null);
      out.flush();
      BlockOpResponseProto response =
          BlockOpResponseProto.parseDelimitedFrom(in);
      while (response.getStatus() == Status.IN_PROGRESS) {
        response = BlockOpResponseProto.parseDelimitedFrom(in);
      }
      return response.getStatus();
    } finally {
      closeQuietly(out);
      closeQuietly(in);
      sock.close();
    }
  }

  private static void closeQuietly(java.io.Closeable stream) {
    if (stream != null) {
      try {
        stream.close();
      } catch (IOException ignored) {
        // the socket close below is what matters
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
    for (DataNode node : nodes) {
      if (node.getDatanodeUuid().equals(location.getDatanodeUuid())) {
        return node;
      }
    }
    throw new IllegalStateException("no DataNode for " + location);
  }
}
