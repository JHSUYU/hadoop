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

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.security.token.block.BlockKey;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeRpcServer;
import org.apache.hadoop.ipc.CausynthMessagePropagation;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestBalancerWithEncryptedTransfer {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestBalancerWithEncryptedTransfer.class);
  /** Block-key serial number pinned by {@link #pinBlockKeys}. */
  private static final int SERIAL_NO = Integer.MAX_VALUE / 17899;
  
  private final Configuration conf = new HdfsConfiguration();
  
  @BeforeEach
  public void setUpConf() {
    conf.setBoolean(DFSConfigKeys.DFS_ENCRYPT_DATA_TRANSFER_KEY, true);
    conf.setBoolean(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
    conf.setInt(
        DFSConfigKeys.DFS_CLIENT_BLOCK_WRITE_LOCATEFOLLOWINGBLOCK_RETRIES_KEY,
        12);
    conf.setInt(DFSConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY, 10 * 60 * 1000);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY,
        10 * 60 * 1000);
  }
  
  @Test
  @Timeout(value = 60)
  public void testEncryptedBalancer0() throws Exception {
    new TestBalancer().testBalancer0Internal(conf);
  }
  
  @Test
  @Timeout(value = 60)
  public void testEncryptedBalancer1() throws Exception {
    TestBalancer workload = new TestBalancer();
    CausynthMessagePropagation.registerSource(
        workload, "EXTERNAL_APP", "BALANCER", "hdfs-17899/balancer", 0);
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
      prepareStaleKeyBoundary(workload.getCluster(), keyManager);
    };
    try {
      workload.testBalancer1Internal(conf, 1000L);
    } finally {
      KeyManager.testWait = ignored -> { };
      if (request[0] != 0L) {
        CausynthMessagePropagation.endRequest(request[0], "balance-block");
      }
    }
  }

  /**
   * Makes every block-key id the recorded window sees identical across
   * GraphChecker replay sessions.  The NameNode manager seeds serialNo from
   * SecureRandom once per JVM and each replay task is its own JVM, so without
   * this the replay groups of one campaign mint unrelated ids.  setSerialNo
   * only moves the counter, so the two constructor-time keys keep their
   * random ids; two rotations retire them: the first mints SERIAL_NO + 1 as
   * nextKey, the second makes it currentKey (the stale key of this workload)
   * and mints SERIAL_NO + 2.  DataNodes and the Balancer mint nothing themselves (they only
   * addKeys what the NameNode exports), so delivering the master's export
   * in-process here is the same state a KeyUpdateCommand delivers; it stays
   * out of the trace because nothing is recorded yet.  The recorded window's
   * own rotations and refreshes are unchanged.
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

  private static void prepareStaleKeyBoundary(MiniDFSCluster cluster,
      KeyManager keyManager) throws IOException {
    NameNodeRpcServer namenode = (NameNodeRpcServer) cluster.getNameNodeRpc();
    CausynthMessagePropagation.registerSource(
        namenode.getClientRpcServer(), "CLUSTER_NODE", "NAMENODE",
        "hdfs-17899/nn0", 0);
    int index = 0;
    for (DataNode node : cluster.getDataNodes()) {
      CausynthMessagePropagation.registerSource(
          node.getDatanodeId(), "CLUSTER_NODE", "DATANODE",
          "hdfs-17899/dn" + index++, 0);
    }

    BlockTokenSecretManager master = cluster.getNamesystem()
        .getBlockManager().getBlockTokenSecretManager();
    CausynthMessagePropagation.registerSourceAlias(master,
        namenode.getClientRpcServer());
    String blockPoolId = cluster.getNamesystem().getBlockPoolId();
    for (DataNode node : cluster.getDataNodes()) {
      CausynthMessagePropagation.registerSourceAlias(
          node.getBlockPoolTokenSecretManager().get(blockPoolId),
          node.getDatanodeId());
    }

    CausynthMessagePropagation.startRecording();
    ExportedBlockKeys initial = master.exportKeys();
    int currentKeyId;
    synchronized (master) {
      master.setKeyUpdateIntervalForTesting(-initial.getTokenLifetime() - 1);
      try {
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
        currentKeyId = master.getCurrentKey().getKeyId();
      } finally {
        master.setKeyUpdateIntervalForTesting(initial.getKeyUpdateInterval());
      }
    }
    boolean delivered =
        pushKeyUpdate(cluster, blockPoolId, currentKeyId, true);
    synchronized (master) {
      master.setKeyUpdateIntervalForTesting(-initial.getTokenLifetime() - 1);
      try {
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
        currentKeyId = master.getCurrentKey().getKeyId();
      } finally {
        master.setKeyUpdateIntervalForTesting(initial.getKeyUpdateInterval());
      }
    }
    pushKeyUpdate(cluster, blockPoolId, currentKeyId, delivered);
    keyManager.updateBlockKeys();
  }

  /**
   * Pushes the rotated block keys to every DataNode and returns true when every
   * DataNode adopted currentKeyId. The refresh runs on the DataNode's own
   * BPServiceActor thread, so a modeled transport failure on that heartbeat is
   * never thrown here; it surfaces only as a convergence timeout. Report that
   * as false and continue instead of aborting the workload, so the Balancer
   * still dispatches against a target that kept its prior keys. Pass
   * awaitDelivery=false to skip the wait outright once an earlier round has
   * already reported a failure.
   */
  private static boolean pushKeyUpdate(MiniDFSCluster cluster,
      String blockPoolId, int currentKeyId, boolean awaitDelivery)
      throws IOException {
    for (DatanodeDescriptor node : cluster.getNamesystem().getBlockManager()
        .getDatanodeManager().getDatanodes()) {
      node.setNeedKeyUpdate(true);
    }
    cluster.triggerHeartbeats();
    if (!awaitDelivery) {
      return false;
    }
    try {
      GenericTestUtils.waitFor(() -> {
        for (DataNode node : cluster.getDataNodes()) {
          BlockTokenSecretManager keys = node.getBlockPoolTokenSecretManager()
              .get(blockPoolId);
          if (keys.getCurrentKey().getKeyId() != currentKeyId) {
            return false;
          }
        }
        return true;
      }, 10, 120000);
      return true;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException(e);
    } catch (TimeoutException e) {
      LOG.info("DataNodes retained their prior block keys after the key-update"
          + " heartbeat failed", e);
      return false;
    }
  }

  @Test
  @Timeout(value = 60)
  public void testEncryptedBalancer2() throws Exception {
    new TestBalancer().testBalancer2Internal(conf);
  }

}
