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

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.NameNodeProxies;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.security.token.block.BlockKey;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.server.datanode.CausynthCluster;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeRpcServer;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.ipc.CausynthMessagePropagation;
import org.apache.hadoop.ipc.RPC;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** HDFS-11741's non-refreshing encryption-key cache, using current tracing. */
public class TestCausynthExpiredBalancerKey {
  /** Block-key serial number pinned by {@link #pinBlockKeys}. */
  private static final int SERIAL_NO = Integer.MAX_VALUE / 11741;

  @Test
  public void testEncryptedBalancerAfterKeyRotations() throws Exception {
    // Nothing set up before the window expires on the wall clock.
    Configuration conf = CausynthCluster.configure(new HdfsConfiguration());
    conf.setBoolean(DFSConfigKeys.DFS_ENCRYPT_DATA_TRANSFER_KEY, true);
    conf.setBoolean(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
    conf.setInt(HdfsClientConfigKeys.BlockWrite.LOCATEFOLLOWINGBLOCK_RETRIES_KEY, 30);
    conf.setInt(HdfsClientConfigKeys.BlockWrite.LOCATEFOLLOWINGBLOCK_MAX_DELAY_MS_KEY, 2000);
    // This case is replayed on an interpreter-only JVM under load, where a
    // DataNode can miss TestBalancer's 11s dead-node window while it is still
    // completing its first block report.  The NameNode then prunes it and the
    // setup write fails with "There are 0 datanode(s) running".  Widen the
    // dead-node and stale-node windows and let each cluster finish its first
    // block report before anything writes to it.  None of this touches the
    // recorded window, which opens later inside KeyManager.testWait.
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_KEY, 300000);
    TestBalancer.tolerateSlowDatanodes = true;
    TestBalancer workload = new TestBalancer();
    CausynthMessagePropagation.registerSource(workload, "EXTERNAL_APP",
        "BALANCER", "hdfs-11741/balancer", 0);
    long[] request = {0L};
    workload.beforeBalancer = () -> {
      // testUnevenDistribution restarts the cluster right before this hook and
      // runBalancer creates the KeyManager right after it, so this is where
      // the live NameNode's keys become session-stable before the Balancer
      // takes its constructor snapshot of them.
      try {
        pinBlockKeys(workload.getCluster());
      } catch (Exception e) {
        throw new IllegalStateException("pinning block keys failed", e);
      }
      request[0] = CausynthMessagePropagation.beginRequest(
          workload, "balance-block");
    };
    KeyManager.testWait = keyManager -> {
      KeyManager.testWait = ignored -> { };
      CausynthMessagePropagation.registerSourceAlias(keyManager, workload);
      MiniDFSCluster cluster = workload.getCluster();
      // Every node, registered whole, before any traffic the recording
      // depends on: the attribution rule names each daemon thread's node
      // from what it finds registered (CausynthCluster).
      CausynthCluster.registerNameNode(cluster, 0, "hdfs-11741/nn0");
      CausynthCluster.registerOtherDataNodes(cluster, "hdfs-11741");
      BlockTokenSecretManager master = cluster.getNamesystem()
          .getBlockManager().getBlockTokenSecretManager();
      String blockPoolId = cluster.getNamesystem().getBlockPoolId();
      NamenodeProtocol rpc = NameNodeProxies.createProxy(conf,
          cluster.getFileSystem().getUri(), NamenodeProtocol.class).getProxy();
      CausynthCluster.startRecording();
      // Refresh once inside the recorded interval. The Balancer cache below
      // then reads the NameNode-delivered key expression instead of the
      // constructor's pre-recording concrete snapshot.
      keyManager.updateBlockKeys();
      // Cache G before the NameNode publishes later versions. The seed keeps
      // G valid; symbolic expiry and the production removal loop determine
      // whether a DataNode still has it when the Balancer uses its cache.
      keyManager.newDataEncryptionKey();
      { // causynth-d3-rotation-scope
        long causynthRotation = org.apache.hadoop.ipc.CausynthMessagePropagation.beginTick(
            master, "KEY_MANAGER_TICK");
        try {
          master.updateKeys(Long.MAX_VALUE);
        } finally {
          org.apache.hadoop.ipc.CausynthMessagePropagation.endTick(
              causynthRotation);
        }
      }
      { // causynth-d3-rotation-scope
        long causynthRotation = org.apache.hadoop.ipc.CausynthMessagePropagation.beginTick(
            master, "KEY_MANAGER_TICK");
        try {
          master.updateKeys(Long.MAX_VALUE);
        } finally {
          org.apache.hadoop.ipc.CausynthMessagePropagation.endTick(
              causynthRotation);
        }
      }
      try {
        // In the order of the nodes' names, not the cluster's.
        for (DataNode node : CausynthCluster.dataNodes()) {
          BlockTokenSecretManager manager = node.getBlockPoolTokenSecretManager()
              .get(blockPoolId);
          long refresh = CausynthMessagePropagation.beginRequest(
              node.getDatanodeId(), "refresh-block-keys");
          try {
            manager.addKeys(rpc.getBlockKeys());
          } finally {
            CausynthMessagePropagation.endRequest(refresh, "refresh-block-keys");
          }
        }
      } finally {
        RPC.stopProxy(rpc);
      }
      keyManager.updateBlockKeys();
    };
    try {
      workload.testBalancer1Internal(conf, 1000L);
    } finally {
      TestBalancer.tolerateSlowDatanodes = false;
      KeyManager.testWait = ignored -> { };
      if (request[0] != 0L) {
        CausynthMessagePropagation.endRequest(request[0], "balance-block");
      }
    }
  }

  /**
   * Makes every block-key id the recorded window sees identical across
   * GraphChecker replay sessions.  The NameNode manager seeds serialNo from
   * SecureRandom once per JVM and each replay task is its own JVM.
   * setSerialNo only moves the counter, so the two constructor-time keys keep
   * their random ids; two rotations retire them: the first mints
   * SERIAL_NO + 1 as nextKey, the second makes it currentKey (the key the
   * Balancer's recorded newDataEncryptionKey is minted from) and mints
   * SERIAL_NO + 2.  DataNodes and the Balancer mint nothing themselves (they
   * only addKeys what the NameNode exports), so delivering the master's export
   * in-process here is the same state the recorded RPC refresh delivers later;
   * it stays out of the trace because nothing is recorded yet.  The recorded
   * window's own two rotations in testWait are unchanged.
   */
  private static void pinBlockKeys(MiniDFSCluster cluster) throws Exception {
    BlockTokenSecretManager master = cluster.getNamesystem()
        .getBlockManager().getBlockTokenSecretManager();
    master.setSerialNo(SERIAL_NO);
    { // causynth-d3-rotation-scope
      long causynthRotation = org.apache.hadoop.ipc.CausynthMessagePropagation.beginTick(
          master, "KEY_MANAGER_TICK");
      try {
        master.updateKeys(Long.MAX_VALUE);
      } finally {
        org.apache.hadoop.ipc.CausynthMessagePropagation.endTick(
            causynthRotation);
      }
    }
    { // causynth-d3-rotation-scope
      long causynthRotation = org.apache.hadoop.ipc.CausynthMessagePropagation.beginTick(
          master, "KEY_MANAGER_TICK");
      try {
        master.updateKeys(Long.MAX_VALUE);
      } finally {
        org.apache.hadoop.ipc.CausynthMessagePropagation.endTick(
            causynthRotation);
      }
    }
    assertEquals(SERIAL_NO + 1, master.getCurrentKey().getKeyId());
    // The two retired constructor-time keys still sit in every allKeys map
    // (they expire long after the run) and would reach the target as a
    // second, unpinned key family.  Drop them everywhere before recording, so
    // every manager starts the recorded window holding exactly the pinned
    // pair; managers created later (a Balancer/SPS KeyManager) copy the
    // pruned NameNode export and never see them.
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
