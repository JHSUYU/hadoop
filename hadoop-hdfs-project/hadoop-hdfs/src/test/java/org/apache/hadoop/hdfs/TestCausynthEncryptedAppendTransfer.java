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
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.security.token.block.BlockKey;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.server.datanode.CausynthCluster;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.ipc.CausynthMessagePropagation;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * HDFS-10609: an append that adds a DataNode to its pipeline, whose source
 * DataNode cannot resolve the encryption key the client presents.  The
 * pipeline set-up asks the source to copy the replica to the added DataNode
 * (DataStreamer.addDatanode2ExistingPipeline, transfer); the source's SASL
 * handshake throws {@code InvalidEncryptionKeyException}, transfer does not
 * clear the client's key and try again, and the writer aborts.
 *
 * <p>The shape is TestEncryptedTransfer#testEncryptedAppendRequiringBlockTransfer's
 * (an append whose block has fewer replicas than the file wants, so the
 * client adds a DataNode and has the replica transferred to it) with the
 * key window of {@link TestCausynthStripedChecksum} (hdfs-17897): the block
 * has its one replica on dn0 and the file wants three; inside the window the
 * master rotates twice, every DataNode takes the new keys over exactly one
 * heartbeat of its own, the client fetches the master's current encryption
 * key over its own RPC ({@code cache-encryption-key}), and {@code
 * append-block} appends: the pipeline set-up adds dn1 and asks dn0 for the
 * transfer before it connects to anything else.  The recording succeeds; a
 * world in which dn0's key-update heartbeat fails leaves dn0 without the key
 * the client presents.</p>
 */
public class TestCausynthEncryptedAppendTransfer {
  /** Block-key serial number pinned by {@link #pinBlockKeys}. */
  private static final int SERIAL_NO = Integer.MAX_VALUE / 10609;

  @Test
  public void testStaleEncryptionKeyFailsAppendTransfer() throws Exception {
    // Nothing set up before the window expires on the wall clock.
    Configuration conf = CausynthCluster.configure(new HdfsConfiguration());
    conf.setBoolean(DFSConfigKeys.DFS_ENCRYPT_DATA_TRANSFER_KEY, true);
    conf.setBoolean(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
    // A DataNode's daemon heartbeat runs once, at its registration, and not
    // again inside the window: a key update is delivered only by the
    // workload's own heartbeat of that node (hdfs-buggy-11741 B11741a: a slow
    // replay's daemon heartbeat took the mark).
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY,
        TimeUnit.HOURS.toSeconds(6));
    // The NameNode's redundancy monitor computes its work at start-up and
    // not again in the run: the under-replicated block keeps its one
    // replica until the append, and no heartbeat answer of the window
    // carries a replication command.
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_INTERVAL_SECONDS_KEY,
        (int) TimeUnit.HOURS.toSeconds(6));
    try (MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(2)
        .dataNodeConfOverlays(CausynthCluster.dataNodeOverlays(2)).build();
         DistributedFileSystem fs = cluster.getFileSystem()) {
      cluster.waitActive();
      DFSClient client = DFSClientAdapter.getDFSClient(fs);

      // The block is written BEFORE anything is recorded, at replication 1,
      // on the first DataNode; the file then wants three.
      DataNode source = cluster.getDataNodes().get(0);
      DataNode added = cluster.getDataNodes().get(1);
      Path file = new Path("/hdfs-10609/file");
      try (FSDataOutputStream out =
               CausynthCluster.createOnNode(fs, file, source)) {
        out.write(new byte[1024]);
      }
      assertTrue(fs.setReplication(file, (short) 3));
      LocatedBlock located = client.getLocatedBlocks(file.toString(), 0)
          .get(0);
      assertEquals(1, located.getLocations().length,
          "the block must keep its one replica until the append");
      assertEquals(source.getDatanodeUuid(),
          located.getLocations()[0].getDatanodeUuid(),
          "the block must be on the DataNode the workload pinned it to");
      CausynthMessagePropagation.registerSource(
          client, "EXTERNAL_APP", "DFS_CLIENT", "hdfs-10609/client", 0);
      CausynthCluster.registerNameNode(cluster, 0, "hdfs-10609/nn0");
      CausynthCluster.registerDataNode(source, "hdfs-10609/dn0");
      CausynthCluster.registerDataNode(added, "hdfs-10609/dn1");
      BlockTokenSecretManager master = cluster.getNamesystem()
          .getBlockManager().getBlockTokenSecretManager();
      pinBlockKeys(cluster, master);
      CausynthCluster.startRecording();

      // THE KEY FLOWS THE RPC'S WAY (experiments/hadoop/lib/README.md).  The
      // master rotates twice; an explicit rotation marks no DataNode for a key
      // update, so the workload marks every one.
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
      for (DataNode node : cluster.getDataNodes()) {
        cluster.getNamesystem().getBlockManager().getDatanodeManager()
            .getDatanode(node.getDatanodeId()).setNeedKeyUpdate(true);
      }
      // Every DataNode takes the master's new keys over ITS OWN heartbeat,
      // one each, in the order of the nodes' names.  A delivery a witness
      // fails is not repeated in the window.
      for (DataNode node : CausynthCluster.dataNodes()) {
        CausynthCluster.refreshKeysFromNameNode(node, "heartbeat");
      }

      // The client then takes the master's CURRENT key over its own RPC and
      // presents it to the DataNodes of the append's pipeline.
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
      for (DataNode node : cluster.getDataNodes()) {
        BlockTokenSecretManager keys =
            node.getBlockPoolTokenSecretManager().get(blockPoolId);
        CausynthCluster.recordingPrecondition(
            () -> keys.hasKey(currentKeyId),
            "every DataNode must hold the key the client presents, or the"
                + " recording is already the failure");
      }

      // The append's pipeline has one DataNode where the file wants three:
      // its set-up adds dn1 and asks dn0 to transfer the replica to it.
      long appendRequest = CausynthMessagePropagation.beginRequest(
          client, "append-block");
      try (FSDataOutputStream out = fs.append(file)) {
        out.write(new byte[512]);
        out.hflush();
        DFSOutputStream stream = (DFSOutputStream) out.getWrappedStream();
        CausynthCluster.recordingPrecondition(
            () -> stream.getPipeline().length == 2,
            "the append must have added dn1 to the block's pipeline");
      } finally {
        CausynthMessagePropagation.endRequest(appendRequest, "append-block");
      }
    }
  }

  /**
   * Makes every block-key id the recorded window sees identical across
   * GraphChecker replay sessions, as {@link TestCausynthStripedChecksum}
   * does: the manager seeds serialNo from SecureRandom once per JVM and each
   * replay task is its own JVM.  Two rotations retire the two
   * constructor-time keys, the retired ones are dropped everywhere, and the
   * DataNodes take the master's export in-process, out of the trace because
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
