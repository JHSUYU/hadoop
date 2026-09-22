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
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.security.token.block.BlockPoolTokenSecretManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNodeRpcServer;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.HeartbeatResponse;
import org.apache.hadoop.ipc.CausynthMessagePropagation;

/**
 * THE registration helper every GraphChecker workload uses: one node, one
 * source, and every object the node runs its work through registered as
 * that node -- then the recording boundary.
 *
 * <p>Two properties of a workload are real and stated here once: every node
 * is registered BEFORE any traffic the recording depends on, and each node
 * is registered whole.  "Whole" is what GraphChecker's attribution rule
 * needs: a handler serving an RPC is the registered RPC server's, a
 * connection's handshake is the registered SASL server's, a daemon turn is
 * the registered DataNode's -- the engine finds those objects on the
 * thread's frames or as a tick's owner, and names the node without any
 * hand-woven scope in the tree.</p>
 *
 * <p>What this deliberately does NOT do: suppress daemon activity.
 * Heartbeats and block reports run as they do in production; the schedule
 * prefix makes them reproducible and the attribution rule makes them
 * attributable.</p>
 *
 * <p>Two more properties are about the world the window starts from, which
 * the schedule prefix cannot reach because it only orders the window
 * itself: nothing set up before the window may EXPIRE on the wall clock
 * ({@link #configure}), because the recording runs on a JIT and every replay
 * on an interpreter many times slower, so an idle connection that is still
 * open when one opens its window is gone when the other does; and a per-node
 * loop inside the window visits the nodes in the order of their NAMES
 * ({@link #dataNodes}), because which physical node plays which role is the
 * cluster's random choice.</p>
 */
public final class CausynthCluster {
  private static final String NODE = "CLUSTER_NODE";
  /** The DataNodes this run has registered, by identity, and their names. */
  private static final Map<DataNode, String> REGISTERED =
      new IdentityHashMap<>();
  /** Longer than any case runs: an expiry that cannot fire. */
  private static final int NEVER_MS = (int) TimeUnit.HOURS.toMillis(6);

  private CausynthCluster() {
  }

  /**
   * Makes the set-up the window starts from the same in every run: nothing
   * the workload opens before its window expires on the wall clock.
   *
   * <p>An IPC client connection closes after {@code
   * ipc.client.connection.maxidletime} without a call (the server side
   * after twice that), and the client keeps a DataNode peer for {@code
   * dfs.client.socketcache.expiryMsec}.  The recording reaches its window
   * in a few seconds; a replay on the concolic VM takes many times longer,
   * so a connection the recording's window still used was gone when the
   * replay's opened -- hdfs-17899-bug3's first held event was that
   * connection's idle wait, which the replay never made -- or the other way
   * round.  Called on the configuration the cluster is built from.</p>
   */
  public static Configuration configure(Configuration conf) {
    conf.setInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECTION_MAXIDLETIME_KEY,
        NEVER_MS);
    conf.setLong(HdfsClientConfigKeys.DFS_CLIENT_SOCKET_CACHE_EXPIRY_MSEC_KEY,
        NEVER_MS);
    return conf;
  }

  /**
   * Every DataNode this run registered, in the order of the names it
   * registered them by: THE order a per-node loop inside the window takes.
   *
   * <p>The cluster's own list is in start-up order, and which of those
   * nodes plays which role is the cluster's choice -- a replication-1 block
   * lands on a random node, an erasure-coded group's locations come out in
   * a random order -- so a loop over it refreshes the ROLES in a different
   * order from run to run, and a replay held to the recording's schedule
   * waits for a node that is not next.  The names are the case's.</p>
   */
  public static List<DataNode> dataNodes() {
    List<DataNode> nodes = new ArrayList<>(REGISTERED.keySet());
    nodes.sort(Comparator.comparing(REGISTERED::get));
    return nodes;
  }

  /**
   * Registers NameNode {@code index} as {@code sourceId}: its client RPC
   * server is the anchor, and its other RPC servers, the NameNode, the
   * namesystem, the block manager and the master block-token manager are
   * the same node.
   */
  public static void registerNameNode(MiniDFSCluster cluster, int index,
      String sourceId) {
    NameNodeRpcServer rpc =
        (NameNodeRpcServer) cluster.getNameNodeRpc(index);
    FSNamesystem namesystem = cluster.getNamesystem(index);
    BlockManager blocks = namesystem.getBlockManager();
    List<Object> aliases = new ArrayList<>();
    aliases.add(rpc);
    aliases.add(serverOf(rpc, "getServiceRpcServer"));
    aliases.add(serverOf(rpc, "getLifelineRpcServer"));
    aliases.add(cluster.getNameNode(index));
    aliases.add(namesystem);
    aliases.add(blocks);
    aliases.add(blocks.getDatanodeManager());
    aliases.add(blocks.getBlockTokenSecretManager());
    CausynthMessagePropagation.registerNode(rpc.getClientRpcServer(), NODE,
        "NAMENODE", sourceId, aliases.toArray());
  }

  /**
   * Registers {@code datanode} as {@code sourceId}: its DatanodeID is the
   * anchor, and the DataNode itself, its SASL server and client, its
   * transfer server, its block-token managers and its block-pool services
   * are the same node.
   */
  public static void registerDataNode(DataNode datanode, String sourceId) {
    List<Object> aliases = new ArrayList<>();
    aliases.add(datanode);
    aliases.add(datanode.getSaslServer());
    aliases.add(datanode.getSaslClient());
    aliases.add(datanode.getXferServer());
    BlockPoolTokenSecretManager keys =
        datanode.getBlockPoolTokenSecretManager();
    aliases.add(keys);
    for (BPOfferService pool : datanode.getAllBpOs()) {
      aliases.add(pool);
      aliases.addAll(pool.getBPServiceActors());
      if (keys != null && keys.isBlockPoolRegistered(pool.getBlockPoolId())) {
        aliases.add(keys.get(pool.getBlockPoolId()));
      }
    }
    CausynthMessagePropagation.registerNode(datanode.getDatanodeId(), NODE,
        "DATANODE", sourceId, dedupe(aliases).toArray());
    REGISTERED.put(datanode, sourceId);
  }

  /**
   * Registers every DataNode of {@code cluster} the workload did not name,
   * as {@code <prefix>/dn<index>}: no node's traffic runs unattributed.
   * Called after the named ones, so the names a case is about are its own.
   */
  public static void registerOtherDataNodes(MiniDFSCluster cluster,
      String prefix) {
    List<DataNode> nodes = cluster.getDataNodes();
    for (int index = 0; index < nodes.size(); index++) {
      DataNode node = nodes.get(index);
      if (!REGISTERED.containsKey(node)) {
        registerDataNode(node, prefix + "/dn" + index);
      }
    }
  }

  /**
   * One key-refresh heartbeat of {@code datanode}'s, as the workload's own
   * request on that node ({@code api}): the heartbeat is sent and every
   * command it returns -- a KeyUpdateCommand among them -- is applied before
   * this returns.  One request, one scope.
   *
   * <p>Which nodes refresh is the workload's to say: a node refreshes only
   * if the NameNode has it marked for a key update, which the workload's
   * explicit rotations never do on their own.  So daemon heartbeats running
   * as in production cannot close a rotation gap the case keeps.</p>
   *
   * @return false when the heartbeat's transport failed -- a modeled failure
   *         leaves the node's keys as they were
   */
  public static boolean refreshKeysFromNameNode(DataNode datanode,
      String api) throws IOException {
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
      return true;
    } catch (IOException transport) {
      return false;
    } finally {
      CausynthMessagePropagation.endRequest(request, api);
    }
  }

  /** Opens the recorded window: the one recording boundary. */
  public static void startRecording() {
    CausynthMessagePropagation.startRecording();
  }

  /** A NameNode RPC server the class keeps package-private, or null. */
  private static Object serverOf(NameNodeRpcServer rpc, String getter) {
    try {
      Method method = NameNodeRpcServer.class.getDeclaredMethod(getter);
      method.setAccessible(true);
      Object server = method.invoke(rpc);
      return server == rpc.getClientRpcServer() ? null : server;
    } catch (ReflectiveOperationException | RuntimeException absent) {
      return null;
    }
  }

  private static List<Object> dedupe(List<Object> objects) {
    List<Object> distinct = new ArrayList<>();
    for (Object object : objects) {
      if (object == null) {
        continue;
      }
      boolean seen = false;
      for (Object kept : distinct) {
        if (kept == object) {
          seen = true;
          break;
        }
      }
      if (!seen) {
        distinct.add(object);
      }
    }
    return distinct;
  }
}
