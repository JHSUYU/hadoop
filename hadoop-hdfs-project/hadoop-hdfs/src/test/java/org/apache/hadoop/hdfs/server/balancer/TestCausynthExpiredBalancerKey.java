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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.NameNodeProxies;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeRpcServer;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.ipc.CausynthMessagePropagation;
import org.apache.hadoop.ipc.RPC;
import org.junit.jupiter.api.Test;

/** HDFS-11741's non-refreshing encryption-key cache, using current tracing. */
public class TestCausynthExpiredBalancerKey {
  @Test
  public void testEncryptedBalancerAfterKeyRotations() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(DFSConfigKeys.DFS_ENCRYPT_DATA_TRANSFER_KEY, true);
    conf.setBoolean(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
    TestBalancer workload = new TestBalancer();
    CausynthMessagePropagation.registerSource(workload, "EXTERNAL_APP",
        "BALANCER", "hdfs-11741/balancer", 0);
    long[] request = {0L};
    workload.beforeBalancer = () -> request[0] =
        CausynthMessagePropagation.beginRequest(workload, "balance-block");
    KeyManager.testWait = keyManager -> {
      KeyManager.testWait = ignored -> { };
      CausynthMessagePropagation.registerSourceAlias(keyManager, workload);
      MiniDFSCluster cluster = workload.getCluster();
      NameNodeRpcServer namenode = (NameNodeRpcServer) cluster.getNameNodeRpc();
      CausynthMessagePropagation.registerSource(namenode.getClientRpcServer(),
          "CLUSTER_NODE", "NAMENODE", "hdfs-11741/nn0", 0);
      BlockTokenSecretManager master = cluster.getNamesystem()
          .getBlockManager().getBlockTokenSecretManager();
      CausynthMessagePropagation.registerSourceAlias(master,
          namenode.getClientRpcServer());
      String blockPoolId = cluster.getNamesystem().getBlockPoolId();
      int index = 0;
      for (DataNode node : cluster.getDataNodes()) {
        CausynthMessagePropagation.registerSource(node.getDatanodeId(),
            "CLUSTER_NODE", "DATANODE", "hdfs-11741/dn" + index++, 0);
        CausynthMessagePropagation.registerSourceAlias(
            node.getBlockPoolTokenSecretManager().get(blockPoolId),
            node.getDatanodeId());
      }
      NamenodeProtocol rpc = NameNodeProxies.createProxy(conf,
          cluster.getFileSystem().getUri(), NamenodeProtocol.class).getProxy();
      CausynthMessagePropagation.startRecording();
      // Cache G before the NameNode publishes later versions. The seed keeps
      // G valid; symbolic expiry and the production removal loop determine
      // whether a DataNode still has it when the Balancer uses its cache.
      keyManager.newDataEncryptionKey();
      master.updateKeys(Long.MAX_VALUE);
      master.updateKeys(Long.MAX_VALUE);
      try {
        for (DataNode node : cluster.getDataNodes()) {
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
      KeyManager.testWait = ignored -> { };
      if (request[0] != 0L) {
        CausynthMessagePropagation.endRequest(request[0], "balance-block");
      }
    }
  }
}
