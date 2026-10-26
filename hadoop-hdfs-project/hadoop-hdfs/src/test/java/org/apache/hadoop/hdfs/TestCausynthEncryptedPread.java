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
package org.apache.hadoop.hdfs;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.security.token.block.BlockKey;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.server.datanode.CausynthCluster;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.ipc.CausynthMessagePropagation;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * HDFS-5766: a positional read whose DataNode cannot resolve the encryption
 * key the client presents.  The DataNode's SASL handshake throws {@code
 * InvalidEncryptionKeyException}; the client's positional read
 * (DFSInputStream.fetchBlockByteRange, actualGetFromOneDataNode) clears its
 * key but also takes the DataNode for a dead one, so with no other replica
 * the read ends in {@code BlockMissingException} instead of retrying the
 * same DataNode with a new key.
 *
 * <p>The shape is {@link TestCausynthReplicatedChecksum}'s (HDFS-12931) and
 * {@link TestCausynthStripedChecksum}'s (hdfs-17897): a file of one replica
 * on the one DataNode; inside the window the master rotates twice, the
 * DataNode takes the new keys over exactly one heartbeat of its own, the
 * client fetches the master's current encryption key over its own RPC
 * ({@code cache-encryption-key}), and {@code encrypted-pread} presents it to
 * the DataNode in a positional read.  The recording succeeds; a world in
 * which the DataNode's key-update heartbeat fails leaves the DataNode
 * without the key the client presents.</p>
 */
public class TestCausynthEncryptedPread {
  /** Block-key serial number pinned by {@link #pinBlockKeys}. */
  private static final int SERIAL_NO = Integer.MAX_VALUE / 5766;

  @Test
  public void testStaleEncryptionKeyFailsPread() throws Exception {
    // Nothing set up before the window expires on the wall clock.
    Configuration conf = CausynthCluster.configure(new HdfsConfiguration());
    conf.setBoolean(DFSConfigKeys.DFS_ENCRYPT_DATA_TRANSFER_KEY, true);
    conf.setBoolean(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 1);
    // A read that finds no live replica gives up at once instead of
    // sleeping and refetching the block's locations: what follows a failed
    // read is not what the case is about (TestEncryptedTransfer's client,
    // 899c200cc8e).
    conf.setInt(HdfsClientConfigKeys.DFS_CLIENT_MAX_BLOCK_ACQUIRE_FAILURES_KEY,
        0);
    // The DataNode's daemon heartbeat runs once, at its registration, and
    // not again inside the window: the key update is delivered only by the
    // workload's own heartbeat (hdfs-buggy-11741 B11741a: a slow replay's
    // daemon heartbeat took the mark).
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY,
        TimeUnit.HOURS.toSeconds(6));
    try (MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(1)
        .dataNodeConfOverlays(CausynthCluster.dataNodeOverlays(1)).build();
         DistributedFileSystem fs = cluster.getFileSystem()) {
      cluster.waitActive();
      DFSClient client = DFSClientAdapter.getDFSClient(fs);

      Path file = new Path("/hdfs-5766/file");
      byte[] data = new byte[1024];
      for (int index = 0; index < data.length; index++) {
        data[index] = (byte) index;
      }
      try (FSDataOutputStream out = fs.create(file)) {
        out.write(data);
      }
      DataNode holder = cluster.getDataNodes().get(0);
      assertEquals(holder.getDatanodeUuid(), client.getLocatedBlocks(
          file.toString(), 0).get(0).getLocations()[0].getDatanodeUuid(),
          "the block's one replica must be on the one DataNode");
      CausynthMessagePropagation.registerSource(
          client, "EXTERNAL_APP", "DFS_CLIENT", "hdfs-5766/client", 0);
      CausynthCluster.registerNameNode(cluster, 0, "hdfs-5766/nn0");
      CausynthCluster.registerDataNode(holder, "hdfs-5766/dn0");
      BlockTokenSecretManager master = cluster.getNamesystem()
          .getBlockManager().getBlockTokenSecretManager();
      pinBlockKeys(cluster, master);
      CausynthCluster.startRecording();

      // THE KEY FLOWS THE RPC'S WAY (experiments/hadoop/lib/README.md).  The
      // master rotates twice; an explicit rotation marks no DataNode for a key
      // update, so the workload marks it.
      int initialKeyId = master.getCurrentKey().getKeyId();
      for (int rotation = 0; rotation < 2; rotation++) {
        long causynthRotation = CausynthMessagePropagation.beginTick(
            master, "KEY_MANAGER_TICK");
        try {
          master.updateKeys(Long.MAX_VALUE);
        } finally {
          CausynthMessagePropagation.endTick(causynthRotation);
        }
      }
      CausynthCluster.recordingPrecondition(
          () -> master.getCurrentKey().getKeyId() == initialKeyId + 2,
          "the recorded window must rotate the master twice");
      int currentKeyId = master.getCurrentKey().getKeyId();
      String blockPoolId = cluster.getNamesystem().getBlockPoolId();
      cluster.getNamesystem().getBlockManager().getDatanodeManager()
          .getDatanode(holder.getDatanodeId()).setNeedKeyUpdate(true);
      // The DataNode takes the master's new keys over ITS OWN heartbeat,
      // once.  A delivery a witness fails is not repeated in the window.
      CausynthCluster.refreshKeysFromNameNode(holder, "heartbeat");

      // The client then takes the master's CURRENT key over its own RPC and
      // presents it to the block's DataNode.
      client.clearDataEncryptionKey();
      long cacheRequest = CausynthMessagePropagation.beginRequest(
          client, "cache-encryption-key");
      try {
        client.newDataEncryptionKey();
      } finally {
        CausynthMessagePropagation.endRequest(
            cacheRequest, "cache-encryption-key");
      }
      CausynthCluster.recordingPrecondition(
          () -> client.getEncryptionKey().keyId == currentKeyId,
          "the client must present the master's current key");
      BlockTokenSecretManager keys =
          holder.getBlockPoolTokenSecretManager().get(blockPoolId);
      CausynthCluster.recordingPrecondition(
          () -> keys.hasKey(currentKeyId),
          "the DataNode must hold the key the client presents, or the"
              + " recording is already the failure");

      byte[] read = new byte[512];
      long preadRequest = CausynthMessagePropagation.beginRequest(
          client, "encrypted-pread");
      try (FSDataInputStream in = fs.open(file)) {
        in.readFully(256, read);
      } finally {
        CausynthMessagePropagation.endRequest(preadRequest, "encrypted-pread");
      }
      assertArrayEquals(Arrays.copyOfRange(data, 256, 768), read);
    }
  }

  /**
   * Makes every block-key id the recorded window sees identical across
   * GraphChecker replay sessions, as {@link TestCausynthStripedChecksum}
   * does: the manager seeds serialNo from SecureRandom once per JVM and each
   * replay task is its own JVM.  Two rotations retire the two
   * constructor-time keys, the retired ones are dropped everywhere, and the
   * DataNode takes the master's export in-process, out of the trace because
   * nothing is recorded yet.
   */
  private static void pinBlockKeys(MiniDFSCluster cluster,
      BlockTokenSecretManager master) throws Exception {
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
      assertTrue(manager.hasKey(SERIAL_NO + 1) && manager.hasKey(SERIAL_NO + 2));
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
}
