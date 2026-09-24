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
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.security.token.block.BlockPoolTokenSecretManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNodeRpcServer;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.HeartbeatResponse;
import org.apache.hadoop.ipc.CausynthMessagePropagation;

import static org.junit.jupiter.api.Assertions.assertTrue;

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
 * cluster's random choice; and each DataNode has IPC connections of its
 * own ({@link #dataNodeOverlays}), because in one JVM they would share one,
 * whose threads belong to whichever node opened it first; and a block a case
 * reads its roles off is written to a node the workload names ({@link
 * #createOnNode}), because random placement would make a different started
 * node play each role in every run.</p>
 */
public final class CausynthCluster {
  private static final String NODE = "CLUSTER_NODE";
  /** The DataNodes this run has registered, by identity, and their names. */
  private static final Map<DataNode, String> REGISTERED =
      new IdentityHashMap<>();
  /** Longer than any case runs: an expiry that cannot fire. */
  private static final int NEVER_MS = (int) TimeUnit.HOURS.toMillis(6);

  /**
   * The bound of every wait a workload makes inside its window for its own
   * results.  A replay that cannot hold its schedule prefix holds every
   * thread for the watchdog's timeout (15 s) before it lets the prefix go
   * and runs free; a workload that gives up first fails for the engine's
   * reason and not the case's -- hdfs-17899-bug2's 15 s wait for a key
   * rotation timed out in the very stall that would have released it.
   */
  public static final int WINDOW_WAIT_MS =
      (int) TimeUnit.SECONDS.toMillis(60);

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
   * One configuration overlay per DataNode, for {@link
   * MiniDFSCluster.Builder#dataNodeConfOverlays}: each DataNode gets IPC
   * connections of its own, as a DataNode process does.
   *
   * <p>In one JVM the IPC client shares a connection among every caller
   * whose connection id is equal, so the DataNodes of a MiniDFSCluster
   * heartbeat over ONE connection to the NameNode, and that connection's
   * threads -- the reader and the request sender -- are attributed to
   * whichever DataNode opened it first: a race decided before the window,
   * so a different node from run to run, and every WAKE and RECV those
   * threads make with it.  hdfs-17899-bug3's focused replays waited for
   * target-dn's connection wake where the run's connection was
   * source-dn's.  The connection id includes the idle time, so a DataNode's
   * own idle time -- never, plus its index -- is its own connection.</p>
   */
  public static Configuration[] dataNodeOverlays(int numDataNodes) {
    Configuration[] overlays = new Configuration[numDataNodes];
    for (int index = 0; index < numDataNodes; index++) {
      Configuration overlay = new Configuration(false);
      overlay.setInt(
          CommonConfigurationKeysPublic.IPC_CLIENT_CONNECTION_MAXIDLETIME_KEY,
          NEVER_MS + 1 + index);
      overlays[index] = overlay;
    }
    return overlays;
  }

  /**
   * Creates {@code path} with its one replica on {@code holder}: the node a
   * case reads its roles off is then the same started node in every run.
   *
   * <p>A replication-1 block lands on a random DataNode, and the roles a
   * block-key case names -- source, proxy, head -- are read off where it
   * landed.  Names keep the WINDOW's events in order ({@link #dataNodes}),
   * but everything physical about the node still differs between the
   * recording and a replay: its start-up frames, its connections, its place
   * in the NameNode's lists.  hdfs-17899-bug1's seed replays held or broke
   * their prefix depending on where the block went.  A favored node makes
   * it the node the workload names, and the caller asserts it did.</p>
   */
  public static FSDataOutputStream createOnNode(DistributedFileSystem fs,
      Path path, DataNode holder) throws IOException {
    return fs.create(path, FsPermission.getFileDefault(), true, 4096,
        (short) 1, fs.getDefaultBlockSize(path), null,
        new InetSocketAddress[] {holder.getXferAddress()});
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
   * <p>A node refreshes only if the NameNode has it marked for a key
   * update, and whichever heartbeat of the node arrives first takes the
   * mark: this one, or the node's own daemon heartbeat, whose command the
   * actor hands to its command processor with the answer's scope carried
   * ({@code BPServiceActor.CommandProcessingThread#carried}).  Either way
   * the delivery is a recorded heartbeat answer, and its IPC doors are what
   * a witness fails.</p>
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

  /**
   * A check the workload makes inside its window of the RECORDING's world:
   * asserted in the recording and in a native run, and not at all in a
   * concolic replay.
   *
   * <p>A replay runs a world the engine chose -- a clock moved, a branch
   * flipped -- and the state a witness exists to change is exactly what such
   * a check states: the key a node must still retain, the rotation gap that
   * must still be open.  Asserted in a replay, it ends the witness before the
   * workload reaches its anchor: hdfs-17967 path B's "the proxy must still
   * RETAIN" check killed all 22 admitted replays that way.  The condition is
   * a supplier, so a replay does not even read the state.  Only reads belong
   * in it: a call that acts on the system -- mints or presents a key,
   * refreshes, sends -- is recorded behaviour, and stays in the workload in
   * every run with only its result checked here.</p>
   */
  public static void recordingPrecondition(BooleanSupplier holds,
      String message) {
    if (!CausynthMessagePropagation.replaying()) {
      assertTrue(holds.getAsBoolean(), message);
    }
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
