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

import java.io.IOException;
import java.util.List;

import edu.uva.liftlab.graphchecker.annotation.Debug;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.NameNodeProxies;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo.DatanodeInfoBuilder;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.security.token.block.BlockKey;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.namenode.NameNodeRpcServer;
import org.apache.hadoop.hdfs.server.protocol.KeyUpdateCommand;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.ipc.CausynthMessagePropagation;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertTrue;

/** Real NN -> source DN -> target DN stale-key workload for HDFS-17899. */
public class TestCausynthDataTransferRpcFailure {
  @Test
  @Timeout(120)
  public void testReplicationAfterModeledKeyRefreshRpcFailure()
      throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(DFSConfigKeys.DFS_ENCRYPT_DATA_TRANSFER_KEY, true);
    conf.setBoolean(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
    try (MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(2).build();
         DistributedFileSystem fs = cluster.getFileSystem()) {
      cluster.waitActive();
      Path path = new Path("/causynth-hdfs-17899-bug3");
      try (FSDataOutputStream out = fs.create(path, (short) 1)) {
        out.write(1);
      }

      LocatedBlock located = DFSTestUtil.getAllBlocks(fs, path).get(0);
      ExtendedBlock block = located.getBlock();
      List<DataNode> nodes = cluster.getDataNodes();
      DataNode source = find(nodes, located.getLocations()[0]);
      DataNode target = nodes.get(0) == source ? nodes.get(1) : nodes.get(0);
      registerSources(cluster, source, target);

      BlockTokenSecretManager master = cluster.getNamesystem()
          .getBlockManager().getBlockTokenSecretManager();
      master.setKeyUpdateIntervalForTesting(0);
      master.updateKeys(1);
      master.updateKeys(1);
      ExportedBlockKeys fresh = master.exportKeys();
      installOnlyCurrentKey(target, block.getBlockPoolId(), fresh);

      long request = CausynthMessagePropagation.beginRequest(
          source.getDatanodeId(), "replicate-block");
      try {
        NamenodeProtocol namenode = NameNodeProxies.createProxy(conf,
            fs.getUri(), NamenodeProtocol.class).getProxy();
        ExportedBlockKeys refreshed = null;
        boolean rpcFails = Debug.makeSymbolicBoolean("rpcFails");
        try {
          if (rpcFails) {
            throw new IOException("symbolic RPC failure");
          }
          refreshed = namenode.getBlockKeys();
        } catch (IOException expected) {
          // A failed refresh deliberately retains the source DataNode's S0.
        }
        if (refreshed != null) {
          source.getAllBpOs().get(0).applyKeyUpdateCommand(
              new KeyUpdateCommand(refreshed));
        }

        source.transferBlock(block,
            new DatanodeInfo[]{new DatanodeInfoBuilder()
                .setNodeID(target.getDatanodeId()).build()},
            new StorageType[]{StorageType.DISK}, new String[0]);
        GenericTestUtils.waitFor(
            () -> target.getFSDataset().isValidBlock(block), 20, 20000);
        assertTrue(target.getFSDataset().isValidBlock(block));
      } finally {
        CausynthMessagePropagation.endRequest(request, "replicate-block");
      }
    }
  }

  private static DataNode find(List<DataNode> nodes, DatanodeInfo location) {
    return nodes.stream().filter(node -> node.getDatanodeUuid()
        .equals(location.getDatanodeUuid())).findFirst().orElseThrow();
  }

  private static void installOnlyCurrentKey(DataNode target, String blockPool,
      ExportedBlockKeys keys) throws IOException {
    BlockKey current = keys.getCurrentKey();
    ExportedBlockKeys currentOnly = new ExportedBlockKeys(true,
        keys.getKeyUpdateInterval(), keys.getTokenLifetime(), current,
        new BlockKey[]{current});
    target.getBlockPoolTokenSecretManager().clearAllKeysForTesting();
    target.getBlockPoolTokenSecretManager().addKeys(
        blockPool, currentOnly, true);
  }

  private static void registerSources(MiniDFSCluster cluster,
      DataNode source, DataNode target) {
    NameNodeRpcServer namenode =
        (NameNodeRpcServer) cluster.getNameNodeRpc();
    CausynthMessagePropagation.registerSource(
        namenode.getClientRpcServer(), "CLUSTER_NODE", "NAMENODE",
        "hdfs-17899/nn0", 0);
    CausynthMessagePropagation.registerSource(source.getDatanodeId(),
        "CLUSTER_NODE", "DATANODE", "hdfs-17899/source-dn", 0);
    CausynthMessagePropagation.registerSource(target.getDatanodeId(),
        "CLUSTER_NODE", "DATANODE", "hdfs-17899/target-dn", 0);
  }
}
