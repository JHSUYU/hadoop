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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.SystemErasureCodingPolicies;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeRpcServer;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.ipc.CausynthMessagePropagation;
import org.apache.hadoop.ipc.RPC;
import org.junit.jupiter.api.Test;

/** Causal workload for the stale-key striped-checksum failure in HDFS-17897. */
public class TestCausynthStripedChecksum {
  @Test
  public void testStaleEncryptionKeyFailsStripedChecksum() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(DFSConfigKeys.DFS_ENCRYPT_DATA_TRANSFER_KEY, true);
    conf.setBoolean(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
    conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 3600);
    try (MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(3).build();
         DistributedFileSystem fs = cluster.getFileSystem()) {
      cluster.waitActive();
      DFSClient client = DFSClientAdapter.getDFSClient(fs);

      ErasureCodingPolicy policy = SystemErasureCodingPolicies.getByID(
          SystemErasureCodingPolicies.XOR_2_1_POLICY_ID);
      fs.enableErasureCodingPolicy(policy.getName());
      Path directory = new Path("/hdfs-17897");
      Path file = new Path(directory, "file");
      fs.mkdirs(directory);
      fs.setErasureCodingPolicy(directory, policy.getName());

      try (FSDataOutputStream out = fs.create(file)) {
        out.write(new byte[1024]);
      }
      registerSources(cluster, client, file);
      BlockTokenSecretManager master = cluster.getNamesystem()
          .getBlockManager().getBlockTokenSecretManager();
      CausynthMessagePropagation.registerSourceAlias(master,
          ((NameNodeRpcServer) cluster.getNameNodeRpc()).getClientRpcServer());
      CausynthMessagePropagation.startRecording();
      client.clearDataEncryptionKey();
      long cacheRequest = CausynthMessagePropagation.beginRequest(
          client, "cache-encryption-key");
      try {
        client.newDataEncryptionKey();
      } finally {
        CausynthMessagePropagation.endRequest(
            cacheRequest, "cache-encryption-key");
      }

      master.updateKeys(Long.MAX_VALUE);
      master.updateKeys(Long.MAX_VALUE);
      refreshDataNodes(cluster, conf, fs);

      long checksumRequest = CausynthMessagePropagation.beginRequest(
          client, "striped-file-checksum");
      try {
        fs.getFileChecksum(file);
      } finally {
        CausynthMessagePropagation.endRequest(
            checksumRequest, "striped-file-checksum");
      }
    }
  }

  private static void refreshDataNodes(MiniDFSCluster cluster,
      Configuration conf, DistributedFileSystem fs) throws Exception {
    NamenodeProtocol namenode = NameNodeProxies.createProxy(
        conf, fs.getUri(), NamenodeProtocol.class).getProxy();
    try {
      String blockPoolId = cluster.getNamesystem().getBlockPoolId();
      for (DataNode node : cluster.getDataNodes()) {
        BlockTokenSecretManager manager =
            node.getBlockPoolTokenSecretManager().get(blockPoolId);
        CausynthMessagePropagation.registerSourceAlias(
            manager, node.getDatanodeId());
        long request = CausynthMessagePropagation.beginRequest(
            node.getDatanodeId(), "refresh-block-keys");
        try {
          ExportedBlockKeys fresh = namenode.getBlockKeys();
          manager.addKeys(fresh);
        } finally {
          CausynthMessagePropagation.endRequest(request, "refresh-block-keys");
        }
      }
    } finally {
      RPC.stopProxy(namenode);
    }
  }

  private static void registerSources(MiniDFSCluster cluster,
      DFSClient client, Path file) throws Exception {
    NameNodeRpcServer namenode = (NameNodeRpcServer) cluster.getNameNodeRpc();
    CausynthMessagePropagation.registerSource(
        client, "EXTERNAL_APP", "DFS_CLIENT", "hdfs-17897/client", 0);
    CausynthMessagePropagation.registerSource(
        namenode.getClientRpcServer(), "CLUSTER_NODE", "NAMENODE",
        "hdfs-17897/nn0", 0);
    DatanodeInfo[] locations = client.getLocatedBlocks(
        file.toString(), 0).get(0).getLocations();
    int nextIndex = locations.length;
    for (DataNode node : cluster.getDataNodes()) {
      int index = -1;
      for (int locationIndex = 0; locationIndex < locations.length;
           locationIndex++) {
        if (node.getDatanodeUuid().equals(
            locations[locationIndex].getDatanodeUuid())) {
          index = locationIndex;
          break;
        }
      }
      if (index < 0) {
        index = nextIndex++;
      }
      CausynthMessagePropagation.registerSource(
          node.getDatanodeId(), "CLUSTER_NODE", "DATANODE",
          "hdfs-17897/dn" + index, 0);
    }
  }
}
