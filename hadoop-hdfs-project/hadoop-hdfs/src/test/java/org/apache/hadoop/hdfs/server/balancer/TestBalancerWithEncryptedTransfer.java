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

import edu.uva.liftlab.graphchecker.annotation.Debug;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.security.token.block.BlockKey;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeRpcServer;
import org.apache.hadoop.ipc.CausynthMessagePropagation;
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
    KeyManager.testWait = keyManager -> {
      KeyManager.testWait = ignored -> { };
      prepareStaleKeyBoundary(workload.getCluster(), keyManager);
    };
    long request = CausynthMessagePropagation.beginRequest(
        workload, "balance-block");
    try {
      workload.testBalancer1Internal(conf);
    } finally {
      KeyManager.testWait = ignored -> { };
      CausynthMessagePropagation.endRequest(request, "balance-block");
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
    master.setKeyUpdateIntervalForTesting(0);
    master.updateKeys(1);
    master.updateKeys(1);
    ExportedBlockKeys fresh = master.exportKeys();
    installOnlyCurrentKey(cluster, fresh);

    boolean rpcFails = Debug.makeSymbolicBoolean("rpcFails");
    try {
      if (rpcFails) {
        throw new IOException("symbolic RPC failure");
      }
      keyManager.updateBlockKeys();
    } catch (IOException expected) {
      // A failed refresh deliberately retains the Balancer's S0.
    }
  }

  private static void installOnlyCurrentKey(MiniDFSCluster cluster,
      ExportedBlockKeys keys) throws IOException {
    BlockKey current = keys.getCurrentKey();
    ExportedBlockKeys currentOnly = new ExportedBlockKeys(true,
        keys.getKeyUpdateInterval(), keys.getTokenLifetime(), current,
        new BlockKey[]{current});
    for (DataNode node : cluster.getDataNodes()) {
      node.getBlockPoolTokenSecretManager().clearAllKeysForTesting();
      node.getBlockPoolTokenSecretManager().addKeys(
          cluster.getNamesystem().getBlockPoolId(), currentOnly, true);
    }
  }
  
  @Test
  @Timeout(value = 60)
  public void testEncryptedBalancer2() throws Exception {
    new TestBalancer().testBalancer2Internal(conf);
  }

}
