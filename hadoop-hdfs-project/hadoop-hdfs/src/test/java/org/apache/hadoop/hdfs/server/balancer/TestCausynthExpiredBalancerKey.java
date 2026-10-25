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
 * HDFS-11741: the Balancer's {@link KeyManager} mints one
 * {@code DataEncryptionKey} and hands it to every block move without ever
 * checking its expiry, so a Balancer that runs long enough presents a key
 * derived from a block key the NameNode has retired and expired and every
 * DataNode has removed: the destination's {@code retrieveDataEncryptionKey}
 * finds no block key for the id and the move fails.
 *
 * <p>The shape is {@link TestCausynthBalancerCachedKey}'s (hdfs-17899 bug1):
 * the mover is the test holding a real {@link KeyManager}, which takes its
 * keys from the NameNode over the recorded RPC, caches its encryption key in
 * {@link KeyManager#newDataEncryptionKey()} and signs the move's access token
 * with {@link KeyManager#getAccessToken}, as
 * {@code Dispatcher.PendingMove.dispatch} does; every DataNode takes the
 * NameNode's keys over its own heartbeat.  What differs is the order: the
 * Balancer caches its key BEFORE two rotations, so the key it presents is the
 * one the NameNode retired, and the window holds the two removals that decide
 * whether that key still exists: the NameNode's in its third rotation (what
 * it exports) and the destination's in the second key update (what it
 * holds).</p>
 *
 * <p>The recording succeeds: no key expires inside it (the token lifetime is
 * ten hours).  A world whose NameNode clock at its third rotation and whose
 * destination clock at its key update are both past the retired key's expiry
 * -- the clocks of a Balancer that has run for half a day -- has the
 * destination without the presented key, which is the failure.  The block
 * keys' serial numbers are pinned before the window, so only the clocks
 * decide it.</p>
 */
public class TestCausynthExpiredBalancerKey {
  /** Block-key serial number pinned by {@link #pinBlockKeys}. */
  private static final int SERIAL_NO = Integer.MAX_VALUE / 11741;

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
    // In minutes: the NameNode's own monitor never rotates inside the window,
    // and no key lapses on the wall clock inside it -- a replay on an
    // interpreter-only JVM runs far longer than the native run.
    conf.setLong(DFSConfigKeys.DFS_BLOCK_ACCESS_KEY_UPDATE_INTERVAL_KEY, 60);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_LIFETIME_KEY, 600);
    // A concolic replay is slow, and one held to its schedule prefix can
    // hold a heartbeat until its turn: placement must not read a slow node
    // as a stale one.
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_KEY,
        TimeUnit.HOURS.toMillis(6));
    conf.setBoolean(
        DFSConfigKeys.DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_WRITE_KEY, false);
    conf.setBoolean(
        DFSConfigKeys.DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_READ_KEY, false);
    try (MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(2)
        .dataNodeConfOverlays(CausynthCluster.dataNodeOverlays(2)).build();
         DistributedFileSystem fs = cluster.getFileSystem()) {
      cluster.waitActive();

      // The block is written BEFORE anything is recorded, at replication 1,
      // on the first DataNode, so the other one can be told to take it.
      Path path = new Path("/causynth-hdfs-11741");
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
      DataNode target = nodes.get(1);

      pinBlockKeys(cluster);
      BlockTokenSecretManager master = cluster.getNamesystem()
          .getBlockManager().getBlockTokenSecretManager();
      String blockPoolId = block.getBlockPoolId();
      NameNodeRpcServer namenode =
          (NameNodeRpcServer) cluster.getNameNodeRpc();
      CausynthCluster.registerNameNode(cluster, 0, "hdfs-11741/nn0");
      CausynthCluster.registerDataNode(proxy, "hdfs-11741/proxy-dn");
      CausynthCluster.registerDataNode(target, "hdfs-11741/target-dn");
      CausynthMessagePropagation.registerSource(this, "EXTERNAL_APP",
          "BALANCER", "hdfs-11741/balancer", 0);

      NamenodeProtocol rpc = NameNodeProxies.createProxy(conf, fs.getUri(),
          NamenodeProtocol.class).getProxy();
      KeyManager keyManager = null;
      long balance = 0L;
      try {
        CausynthCluster.startRecording();

        // 1. The master rotates once, and every DataNode takes the keys over
        //    its own heartbeat: the cluster's keys are in step, the current
        //    key's expiry written by a recorded NameNode clock.
        rotate(namenode, master, 1);
        CausynthCluster.recordingPrecondition(
            () -> currentKeyId(master) == SERIAL_NO + 2,
            "the first rotation must make the second pinned key current");
        refreshDataNodes(cluster);

        // 2. The Balancer starts: its KeyManager takes the NameNode's keys
        //    over its own RPC and mints the encryption key it caches, from
        //    the master's current block key.
        long fetch = CausynthMessagePropagation.beginRequest(this,
            "fetch-block-keys");
        int cached;
        try {
          keyManager = new KeyManager(blockPoolId, rpc, true, conf);
          CausynthMessagePropagation.registerSourceAlias(keyManager, this);
          cached = keyManager.newDataEncryptionKey().keyId;
        } finally {
          CausynthMessagePropagation.endRequest(fetch, "fetch-block-keys");
        }
        CausynthCluster.recordingPrecondition(
            () -> cached == SERIAL_NO + 2,
            "the balancer must cache a key of the master's current block key");

        // 3. The master rotates twice: the cached key's block key is retired
        //    by the first, and the second's removeExpiredKeys decides whether
        //    the master still exports it.  Every DataNode takes the keys over
        //    its own heartbeat, whose addKeys first runs removeExpiredKeys on
        //    the node's clock.
        rotate(namenode, master, 2);
        CausynthCluster.recordingPrecondition(
            () -> currentKeyId(master) == SERIAL_NO + 4,
            "the master must be two rotations past the cached key");
        refreshDataNodes(cluster);

        // 4. The Balancer's key updater turn: its block keys follow the
        //    master, its cached encryption key does not.
        long refresh = CausynthMessagePropagation.beginRequest(this,
            "refresh-block-keys");
        try {
          keyManager.updateBlockKeys();
        } finally {
          CausynthMessagePropagation.endRequest(refresh, "refresh-block-keys");
        }

        // 5. The move: the Balancer presents its cached key.
        balance = CausynthMessagePropagation.beginRequest(this,
            "balance-block");
        int presented = keyManager.newDataEncryptionKey().keyId;
        CausynthCluster.recordingPrecondition(
            () -> presented == cached && presented != currentKeyId(master),
            "the balancer must present its cached key, behind the master's");
        BlockTokenSecretManager targetKeys =
            target.getBlockPoolTokenSecretManager().get(blockPoolId);
        CausynthCluster.recordingPrecondition(
            () -> targetKeys.hasKey(presented),
            "the destination must still hold the retired key, or the"
                + " recording is already the failure");

        DatanodeInfo proxyInfo = new DatanodeInfoBuilder()
            .setNodeID(proxy.getDatanodeId()).build();
        Token<BlockTokenIdentifier> accessToken = keyManager.getAccessToken(
            block, new StorageType[]{StorageType.DISK}, new String[]{""});
        Status status = sendReplaceBlock(conf, keyManager, target, block,
            proxy.getDatanodeUuid(), proxyInfo, accessToken);
        assertEquals(Status.SUCCESS, status,
            "the recorded move must SUCCEED: the destination holds the key");
        final DataNode destination = target;
        GenericTestUtils.waitFor(
            () -> destination.getFSDataset().isValidBlock(block), 20,
            CausynthCluster.WINDOW_WAIT_MS);
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

  /** {@code rotations} rotations of the master, one key-manager tick each. */
  private static void rotate(NameNodeRpcServer namenode,
      BlockTokenSecretManager master, int rotations) throws IOException {
    long request = CausynthMessagePropagation.beginRequest(
        namenode.getClientRpcServer(), "rotate-block-keys");
    try {
      for (int rotation = 0; rotation < rotations; rotation++) {
        long tick = CausynthMessagePropagation.beginTick(master,
            "KEY_MANAGER_TICK");
        try {
          master.updateKeys(Long.MAX_VALUE);
        } finally {
          CausynthMessagePropagation.endTick(tick);
        }
      }
    } finally {
      CausynthMessagePropagation.endRequest(request, "rotate-block-keys");
    }
  }

  /**
   * Each DataNode, in the order of the nodes' names (CausynthCluster), marked
   * for a key update as the NameNode marks it after a rotation and at once
   * sent one key-refresh heartbeat: the mark is taken by this heartbeat, not
   * by a daemon heartbeat whose command another thread applies later.
   */
  private static void refreshDataNodes(MiniDFSCluster cluster)
      throws IOException {
    for (DataNode node : CausynthCluster.dataNodes()) {
      cluster.getNamesystem().getBlockManager().getDatanodeManager()
          .getDatanode(node.getDatanodeId()).setNeedKeyUpdate(true);
      CausynthCluster.refreshKeysFromNameNode(node, "heartbeat");
    }
  }

  /**
   * Sends one REPLACE_BLOCK over an encrypted connection, the way
   * {@code Dispatcher.PendingMove.dispatch} does, with the Balancer's
   * {@link KeyManager} as the encryption-key factory
   * (TestCausynthBalancerCachedKey).
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
   * pinned pair (TestCausynthBalancerCachedKey).
   */
  private static void pinBlockKeys(MiniDFSCluster cluster) throws Exception {
    BlockTokenSecretManager master = cluster.getNamesystem()
        .getBlockManager().getBlockTokenSecretManager();
    master.setSerialNo(SERIAL_NO);
    for (int rotation = 0; rotation < 2; rotation++) {
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
    for (DataNode node : nodes) {
      if (node.getDatanodeUuid().equals(location.getDatanodeUuid())) {
        return node;
      }
    }
    throw new IllegalStateException("no DataNode for " + location);
  }
}
