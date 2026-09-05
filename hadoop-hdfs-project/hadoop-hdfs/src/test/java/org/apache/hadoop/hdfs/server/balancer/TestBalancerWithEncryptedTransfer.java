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
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
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

public class TestBalancerWithEncryptedTransfer {
  
  private final Configuration conf = new HdfsConfiguration();
  
  @BeforeEach
  public void setUpConf() {
    conf.setBoolean(DFSConfigKeys.DFS_ENCRYPT_DATA_TRANSFER_KEY, true);
    conf.setBoolean(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
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
    workload.beforeBalancer = () -> request[0] =
        CausynthMessagePropagation.beginRequest(workload, "balance-block");
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
    int staleKeyId = initial.getCurrentKey().getKeyId();
    master.setKeyUpdateIntervalForTesting(-initial.getTokenLifetime() - 1);
    try {
      master.updateKeys(Long.MAX_VALUE);
      pushKeyUpdate(cluster, blockPoolId,
          master.getCurrentKey().getKeyId(), null);
      master.updateKeys(Long.MAX_VALUE);
      pushKeyUpdate(cluster, blockPoolId,
          master.getCurrentKey().getKeyId(), staleKeyId);
    } finally {
      master.setKeyUpdateIntervalForTesting(initial.getKeyUpdateInterval());
    }
    keyManager.updateBlockKeys();
  }

  private static void pushKeyUpdate(MiniDFSCluster cluster,
      String blockPoolId, int currentKeyId, Integer absentKeyId)
      throws IOException {
    for (DatanodeDescriptor node : cluster.getNamesystem().getBlockManager()
        .getDatanodeManager().getDatanodes()) {
      node.setNeedKeyUpdate(true);
    }
    cluster.triggerHeartbeats();
    try {
      GenericTestUtils.waitFor(() -> {
        for (DataNode node : cluster.getDataNodes()) {
          BlockTokenSecretManager keys = node.getBlockPoolTokenSecretManager()
              .get(blockPoolId);
          if (!keys.hasKey(currentKeyId)
              || keys.getCurrentKey().getKeyId() != currentKeyId
              || absentKeyId != null && keys.hasKey(absentKeyId)) {
            return false;
          }
        }
        return true;
      }, 10, 30000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException(e);
    } catch (TimeoutException e) {
      throw new IOException(e);
    }
  }

  @Test
  @Timeout(value = 60)
  public void testEncryptedBalancer2() throws Exception {
    new TestBalancer().testBalancer2Internal(conf);
  }

}
