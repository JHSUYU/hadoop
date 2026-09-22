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
import java.util.EnumSet;
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
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo.DatanodeInfoBuilder;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;
import org.apache.hadoop.hdfs.protocol.datatransfer.Sender;
import org.apache.hadoop.hdfs.protocol.datatransfer.TrustedChannelResolver;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.DataEncryptionKeyFactory;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.DataTransferSaslUtil;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.SaslDataTransferClient;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BlockOpResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.hdfs.security.token.block.BlockKey;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.apache.hadoop.hdfs.server.namenode.NameNodeRpcServer;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.HeartbeatResponse;
import org.apache.hadoop.ipc.CausynthMessagePropagation;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * REPLACE_BLOCK target -&gt; proxy stale-key workload for HDFS-17967 path B
 * ({@code DataXceiver.replaceBlock}).
 *
 * <p>A block move is three parties.  A mover (here the test, standing in for
 * the Balancer) sends REPLACE_BLOCK to the TARGET DataNode naming a PROXY
 * DataNode that holds the replica.  The target then opens its OWN SASL
 * connection to the proxy ({@code DataXceiver.java:1241},
 * {@code datanode.saslClient.socketSend(proxySock, ...)}) and presents a
 * data-encryption key derived from its current block key.  When the target's
 * key is one the proxy can no longer resolve, the proxy answers
 * ERROR_UNKNOWN_KEY, the target rebuilds the exception at the annotated site,
 * and {@code replaceBlock} abandons the move at {@code DataXceiver.java:1285}
 * instead of clearing the cached key and retrying once -- what PR 8698 adds.
 *
 * <p>This is a DIFFERENT failure from HDFS-11741, which is the FIRST hop: the
 * mover's own cached key going stale against the target.  Here the mover is
 * healthy throughout and the skew is entirely between the two DataNodes, so
 * the rejection happens on the PROXY and the {@code declared_target_occurrence}
 * names it.  Exactly one proxy connection exists per REPLACE_BLOCK, so one
 * rejection is the whole failure -- no conjunction over peers.
 *
 * <p>THE RECORDING MUST LEAVE A ROTATION GAP, and it must be ASYMMETRIC.  Both
 * DataNodes take the master's rotated keys so both can still verify the block
 * token; only the TARGET's currentKey is then rolled back to the pinned one.
 * The proxy still RETAINS that key, so the recorded move SUCCEEDS, and a
 * witness has to buy the proxy's forgetting with a clock move.  A zero gap
 * leaves every composed path either self-contradictory or satisfied at the
 * recorded valuation -- measured on hdfs-17899 -- and no witness at all.
 */
public class TestCausynthReplaceBlockProxyStaleKey {
  /** Block-key serial number pinned by {@link #pinBlockKeys}. */
  private static final int SERIAL_NO = Integer.MAX_VALUE / 17967 + 4;

  @Test
  @Timeout(180)
  public void testBlockMoveAfterKeyRotations() throws Exception {
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
    // default 30s, and placement then avoids them.  The native run is too
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
      // purely so one DataNode holds a replica the others can be told to take.
      Path path = new Path("/causynth-hdfs-17967-b");
      try (FSDataOutputStream out = fs.create(path, (short) 1)) {
        out.write(new byte[]{1, 2, 3, 4});
      }
      LocatedBlock located = DFSTestUtil.getAllBlocks(fs, path).get(0);
      ExtendedBlock block = located.getBlock();
      List<DataNode> nodes = cluster.getDataNodes();
      DataNode proxy = find(nodes, located.getLocations()[0]);
      List<DataNode> rest = new ArrayList<>();
      for (DataNode node : nodes) {
        if (node != proxy) {
          rest.add(node);
        }
      }
      DataNode target = rest.get(0);

      pinBlockKeys(cluster);
      registerSources(cluster, proxy, target, rest.get(1));

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

      BlockTokenSecretManager targetKeys =
          target.getBlockPoolTokenSecretManager().get(blockPoolId);
      BlockTokenSecretManager proxyKeys =
          proxy.getBlockPoolTokenSecretManager().get(blockPoolId);
      rollCurrentKeyBackTo(targetKeys, SERIAL_NO + 1);
      assertEquals(SERIAL_NO + 1, currentKeyId(targetKeys),
          "the target must present the pinned key, or there is no gap");
      assertEquals(currentKeyId(master), currentKeyId(proxyKeys),
          "the proxy must be current, or the two are symmetric");
      assertTrue(proxyKeys.hasKey(SERIAL_NO + 1),
          "the proxy must still RETAIN the target's key, or the recording is"
              + " already the failure");

      // The mover is healthy: it takes the master's CURRENT key, which the
      // target holds, so the FIRST hop succeeds and the only skew in the
      // recording is between the two DataNodes.  This is what separates
      // path B from HDFS-11741.
      CausynthMessagePropagation.registerSource(this, "EXTERNAL_APP",
          "BALANCER", "hdfs-17967/mover", 0);
      DatanodeInfo proxyInfo = new DatanodeInfoBuilder()
          .setNodeID(proxy.getDatanodeId()).build();
      Token<BlockTokenIdentifier> accessToken = master.generateToken(block,
          EnumSet.of(BlockTokenIdentifier.AccessMode.REPLACE,
              BlockTokenIdentifier.AccessMode.COPY),
          new StorageType[]{StorageType.DISK}, new String[]{""});
      long request =
          CausynthMessagePropagation.beginRequest(this, "replace-block");
      try {
        Status status = sendReplaceBlock(conf, master, target, block,
            proxy.getDatanodeUuid(), proxyInfo, accessToken);
        assertEquals(Status.SUCCESS, status,
            "the recorded move must SUCCEED: the proxy still retains the"
                + " target's key, and the failure is what a witness buys");
        GenericTestUtils.waitFor(
            () -> target.getFSDataset().isValidBlock(block), 20, 20000);
        assertTrue(target.getFSDataset().isValidBlock(block),
            "the target must receive the block through the proxy");
      } finally {
        CausynthMessagePropagation.endRequest(request, "replace-block");
      }
    }
  }

  /**
   * Sends one REPLACE_BLOCK over an encrypted connection, the way
   * {@code Dispatcher.PendingMove.dispatch} does.  {@code
   * DFSTestUtil.replaceBlock} cannot be used: it writes a raw socket with
   * {@code DUMMY_TOKEN} and no SASL, which a cluster with
   * {@code dfs.encrypt.data.transfer} refuses before the op is read.
   */
  private static Status sendReplaceBlock(Configuration conf,
      BlockTokenSecretManager master, DataNode target, ExtendedBlock block,
      String delHint, DatanodeInfo proxyInfo,
      Token<BlockTokenIdentifier> accessToken) throws IOException {
    SaslDataTransferClient saslClient = new SaslDataTransferClient(conf,
        DataTransferSaslUtil.getSaslPropertiesResolver(conf),
        TrustedChannelResolver.getInstance(conf));
    DataEncryptionKeyFactory keyFactory = new DataEncryptionKeyFactory() {
      @Override
      public DataEncryptionKey newDataEncryptionKey() {
        return master.generateDataEncryptionKey();
      }
    };
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
          unbufIn, keyFactory, accessToken, target.getDatanodeId());
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
      DataNode proxy, DataNode target, DataNode spare) {
    NameNodeRpcServer namenode = (NameNodeRpcServer) cluster.getNameNodeRpc();
    CausynthMessagePropagation.registerSource(
        namenode.getClientRpcServer(), "CLUSTER_NODE", "NAMENODE",
        "hdfs-17967/nn0", 0);
    CausynthMessagePropagation.registerSource(proxy.getDatanodeId(),
        "CLUSTER_NODE", "DATANODE", "hdfs-17967/proxy-dn", 0);
    CausynthMessagePropagation.registerSource(target.getDatanodeId(),
        "CLUSTER_NODE", "DATANODE", "hdfs-17967/target-dn", 0);
    CausynthMessagePropagation.registerSource(spare.getDatanodeId(),
        "CLUSTER_NODE", "DATANODE", "hdfs-17967/spare-dn", 0);
  }
}
