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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.qjournal.MiniJournalCluster;
import org.apache.hadoop.hdfs.qjournal.server.JournalNode;
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

  /**
   * Set on a workload JVM whose test was written without GraphChecker: every
   * MiniDFSCluster it builds is made reproducible ({@link #configure}, {@link
   * #dataNodeOverlays}), every node it builds or restarts is built in a node
   * startup under its name ({@link #nodeStartup}, {@link #nodeRestart}) and
   * registered whole once it serves, each NameNode, DataNode and JournalNode
   * as {@code auto<n>/nn<k>}, {@code auto<n>/dn<k>} and {@code
   * auto<n>/jn<k>}, and the first cluster opens the window with one request
   * of the test on its client {@code auto<n>/client0}: once it is up, or,
   * for a case that declares its window at startup ({@code
   * recording_window=startup}), before it starts.  A MiniJournalCluster is
   * the same.  So any cluster test of the module is a workload as it is
   * written.
   */
  public static final String AUTOMATIC = "causynth.cluster.automatic";
  /** Each automatic cluster, by identity, in the order it began. */
  private static final Map<Object, Automatic> AUTOMATIC_CLUSTERS =
      new IdentityHashMap<>();
  /**
   * The slot of each node an automatic cluster built, by the node (a
   * NameNode by its cluster's NameNodeInfo, which outlives a restart).
   */
  private static final Map<Object, Slot> SLOTS = new IdentityHashMap<>();
  /** The JournalNodes this run registered. */
  private static final Set<JournalNode> JOURNALS =
      Collections.newSetFromMap(new IdentityHashMap<>());
  private static Object windowCluster;
  private static long windowRequest;

  /** One automatic cluster: its name prefix and the nodes it has built. */
  private static final class Automatic {
    private final String prefix;
    /** How many nodes of each kind ({@code nn}, {@code dn}, {@code jn}). */
    private final Map<String, Integer> built = new HashMap<>();
    /** Whether its JournalNodes served once (MiniJournalCluster#waitActive). */
    private boolean up;

    private Automatic(String prefix) {
      this.prefix = prefix;
    }
  }

  /**
   * One node of an automatic cluster across its restarts: the name it is
   * registered by and the life it is in.  The slot is registered with each
   * life, so a restart moves it to the next epoch before the new life is
   * built ({@link #nodeRestart}).
   */
  private static final class Slot {
    private final String role;
    private final String name;
    private long epoch;

    private Slot(String role, String name) {
      this.role = role;
      this.name = name;
    }
  }

  /**
   * One node's startup ({@link #nodeStartup}, {@link #nodeRestart}): what
   * the test's thread starts or creates until it is closed belongs to the
   * node.  Not automatic: nothing.
   */
  public static final class NodeStartup implements AutoCloseable {
    private static final NodeStartup NONE = new NodeStartup(null, 0L);
    private final Slot slot;
    private final long token;

    private NodeStartup(Slot slot, long token) {
      this.slot = slot;
      this.token = token;
    }

    /** The node is built: {@code key} (the node, or its slot) finds it. */
    public void built(Object key) {
      if (slot != null && key != null) {
        synchronized (CausynthCluster.class) {
          SLOTS.put(key, slot);
        }
      }
    }

    @Override
    public void close() {
      if (slot != null) {
        CausynthMessagePropagation.endNodeStartup(token);
      }
    }
  }

  private CausynthCluster() {
  }

  public static boolean automatic() {
    return Boolean.getBoolean(AUTOMATIC);
  }

  /**
   * An automatic MiniDFSCluster's construction begins ({@link #setUp}); the
   * overlays it builds its DataNodes with.
   */
  public static Configuration[] automaticSetUp(MiniDFSCluster cluster,
      Configuration conf, int numDataNodes, Configuration[] dataNodeOverlays) {
    setUp(cluster, conf, "DFS_CLIENT");
    return dataNodeOverlays != null ? dataNodeOverlays
        : dataNodeOverlays(numDataNodes);
  }

  /**
   * An automatic MiniJournalCluster's construction begins ({@link #setUp}):
   * the test's own configuration, which its managers are built from too.
   */
  public static void journalSetUp(MiniJournalCluster cluster,
      Configuration conf) {
    setUp(cluster, conf, "QJM_CLIENT");
  }

  /**
   * A cluster's construction begins: it is configured ({@link #configure})
   * and named {@code auto<n>}, n counting the clusters begun before it; for
   * a case that declares its window at startup, the first opens the window
   * now, its client the test's thread registered on the cluster.
   */
  private static synchronized void setUp(Object cluster, Configuration conf,
      String clientRole) {
    configure(conf);
    AUTOMATIC_CLUSTERS.put(cluster,
        new Automatic("auto" + AUTOMATIC_CLUSTERS.size()));
    if (windowCluster == null && CausynthMessagePropagation.startupWindow()) {
      openWindow(cluster, clientRole);
    }
  }

  /**
   * The construction of a node of {@code role} that {@code cluster} builds,
   * in a node startup under the name it is then registered by: {@code
   * <prefix>/<nn|dn|jn><k>}, k counting the nodes of that kind the cluster
   * built before.  What the construction starts or creates is the node's
   * from its first moment, though the test's request stands where it runs:
   * round HS1's nodes built inside the window ran their start-up as the
   * test's client.  None for a cluster not set up as an automatic one (a
   * deprecated MiniDFSCluster constructor's).
   */
  public static NodeStartup nodeStartup(Object cluster, String role) {
    if (!automatic()) {
      return NodeStartup.NONE;
    }
    Slot slot;
    synchronized (CausynthCluster.class) {
      Automatic owner = AUTOMATIC_CLUSTERS.get(cluster);
      if (owner == null) {
        return NodeStartup.NONE;
      }
      String kind = kind(role);
      int k = owner.built.merge(kind, 1, Integer::sum) - 1;
      slot = new Slot(role, owner.prefix + "/" + kind + k);
    }
    return new NodeStartup(slot, CausynthMessagePropagation.beginNodeStartup(
        NODE, role, slot.name));
  }

  /**
   * A restart of the node {@code previous} keys, built anew in its slot:
   * the same name at its next epoch, the slot moved first, so the new life
   * is named from its first moment.  None for a node no automatic cluster
   * built.
   */
  public static NodeStartup nodeRestart(Object previous) {
    if (!automatic()) {
      return NodeStartup.NONE;
    }
    Slot slot;
    synchronized (CausynthCluster.class) {
      slot = SLOTS.get(previous);
      if (slot == null) {
        return NodeStartup.NONE;
      }
      slot.epoch++;
    }
    CausynthMessagePropagation.restartSource(slot, NODE, slot.role,
        slot.name, slot.epoch);
    return new NodeStartup(slot,
        CausynthMessagePropagation.beginNodeStartup(slot));
  }

  private static String kind(String role) {
    switch (role) {
    case "NAMENODE":
      return "nn";
    case "DATANODE":
      return "dn";
    case "JOURNALNODE":
      return "jn";
    default:
      throw new IllegalArgumentException("no node kind: " + role);
    }
  }

  /**
   * An automatic cluster is up: its NameNodes and DataNodes registered,
   * each under its slot, and the first cluster with a NameNode opens the
   * window unless the case opened it at startup; the window cluster's
   * client is the test's file system client too.
   */
  public static synchronized void registerAutomatically(
      MiniDFSCluster cluster) throws IOException {
    MiniDFSCluster.NameNodeInfo[] nameNodes = cluster.getNameNodeInfos();
    for (int index = 0; index < nameNodes.length; index++) {
      if (nameNodes[index].nameNode != null) {
        registerNameNode(cluster, index, SLOTS.get(nameNodes[index]));
      }
    }
    registerStartedDataNodes(cluster);
    if (cluster.getNameNode(0) != null) {
      if (windowCluster == null) {
        openWindow(cluster, "DFS_CLIENT");
      }
      if (windowCluster == cluster) {
        CausynthMessagePropagation.registerSourceAlias(
            cluster.getFileSystem(0).getClient(), cluster);
      }
    }
  }

  /** The NameNode an automatic cluster restarted in slot {@code index}. */
  public static synchronized void restartedNameNode(MiniDFSCluster cluster,
      int index) {
    Slot slot = SLOTS.get(cluster.getNameNodeInfos()[index]);
    if (slot != null) {
      registerNameNode(cluster, index, slot);
    }
  }

  /**
   * An automatic journal cluster built its JournalNodes: each registered
   * under its slot as soon as it serves.
   */
  public static synchronized void registerJournalNodes(
      MiniJournalCluster cluster) {
    for (int index = 0; index < cluster.getNumNodes(); index++) {
      registerJournalNode(cluster.getJournalNode(index));
    }
  }

  /**
   * An automatic journal cluster's JournalNodes serve
   * (MiniJournalCluster#waitActive): the first cluster of the run opens the
   * window unless the case opened it at startup.
   *
   * <p>A QJM test builds its QuorumJournalManagers on its own thread after
   * the cluster is up, so none exists when the window opens: the window's
   * client is the test's thread, registered on the cluster it waited for as
   * {@code auto<n>/client0}, and every manager it builds inside the window
   * starts its logger threads in that request.</p>
   */
  public static synchronized void registerJournalsAutomatically(
      MiniJournalCluster cluster) {
    Automatic owner = AUTOMATIC_CLUSTERS.get(cluster);
    if (owner.up) {
      return;
    }
    owner.up = true;
    if (windowCluster == null) {
      openWindow(cluster, "QJM_CLIENT");
    }
  }

  /** The JournalNode an automatic journal cluster restarted in a slot. */
  public static synchronized void restartedJournalNode(
      MiniJournalCluster cluster, int index) {
    registerJournalNode(cluster.getJournalNode(index));
  }

  /**
   * Registers JournalNode {@code node} under its slot: its RPC server is the
   * anchor, and the JournalNode and its protocol server are the same node.
   * A handler serving a journal's request has both on its frames.
   */
  private static void registerJournalNode(JournalNode node) {
    if (!JOURNALS.add(node)) {
      return;
    }
    Slot slot = SLOTS.get(node);
    Object rpc = node.getRpcServer();
    register(hidden(rpc, "getRpcServer"), "JOURNALNODE", slot.name,
        slot.epoch, rpc, node, slot);
  }

  /**
   * The window opens: {@code cluster} is the test's client {@code
   * <prefix>/client0} and the test's thread stands in its request.
   */
  private static void openWindow(Object cluster, String clientRole) {
    CausynthMessagePropagation.registerSource(cluster, "EXTERNAL_APP",
        clientRole, AUTOMATIC_CLUSTERS.get(cluster).prefix + "/client0", 0);
    windowCluster = cluster;
    startRecording();
    windowRequest = CausynthMessagePropagation.beginRequest(cluster, "test");
  }

  /**
   * The DataNodes a registered automatic cluster started or restarted since,
   * each under its slot.
   */
  public static synchronized void registerStartedDataNodes(
      MiniDFSCluster cluster) {
    if (!AUTOMATIC_CLUSTERS.containsKey(cluster)) {
      return;
    }
    for (DataNode node : cluster.getDataNodes()) {
      if (!REGISTERED.containsKey(node)) {
        Slot slot = SLOTS.get(node);
        registerDataNode(node, slot.name, slot.epoch, slot);
      }
    }
  }

  /** The window's request ends when its cluster shuts down. */
  public static synchronized void shutdownAutomatically(Object cluster) {
    if (cluster == windowCluster) {
      CausynthMessagePropagation.endRequest(windowRequest, "test");
      windowCluster = null;
    }
  }

  /**
   * Makes the set-up the window starts from the same in every run: nothing
   * the workload opens before its window expires on the wall clock, no socket
   * times out while a replay waits for a turn, and an HA client tries the
   * NameNodes in their configured order.
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
    // A data-transfer socket's read and write time out after a minute, and
    // so does an RPC: a replay held to its order waits longer than that for
    // a turn (the ad-hoc sweep's upstream workloads failed on 65 s and 75 s
    // reads).
    conf.setInt(HdfsClientConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY, NEVER_MS);
    conf.setInt(HdfsClientConfigKeys.DFS_DATANODE_SOCKET_WRITE_TIMEOUT_KEY,
        NEVER_MS);
    conf.setInt(CommonConfigurationKeys.IPC_CLIENT_RPC_TIMEOUT_KEY, NEVER_MS);
    // So does a QuorumJournalManager's wait for a quorum of its loggers.
    for (String quorumWait : new String[] {
        DFSConfigKeys.DFS_QJOURNAL_START_SEGMENT_TIMEOUT_KEY,
        DFSConfigKeys.DFS_QJOURNAL_PREPARE_RECOVERY_TIMEOUT_KEY,
        DFSConfigKeys.DFS_QJOURNAL_ACCEPT_RECOVERY_TIMEOUT_KEY,
        DFSConfigKeys.DFS_QJOURNAL_FINALIZE_SEGMENT_TIMEOUT_KEY,
        DFSConfigKeys.DFS_QJOURNAL_SELECT_INPUT_STREAMS_TIMEOUT_KEY,
        DFSConfigKeys.DFS_QJOURNAL_GET_JOURNAL_STATE_TIMEOUT_KEY,
        DFSConfigKeys.DFS_QJOURNAL_NEW_EPOCH_TIMEOUT_KEY,
        DFSConfigKeys.DFS_QJOURNAL_WRITE_TXNS_TIMEOUT_KEY,
        DFSConfigKeys.DFS_QJM_OPERATIONS_TIMEOUT}) {
      conf.setInt(quorumWait, NEVER_MS);
    }
    // An HA client tries the NameNodes in a random order by default, so the
    // recording's first call went to nn2 and a replay's to nn1.
    conf.setBoolean(HdfsClientConfigKeys.Failover.RANDOM_ORDER, false);
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
    registerNameNode(cluster, index, sourceId, 0L, null);
  }

  /** {@link #registerNameNode} of an automatic cluster's slot. */
  private static void registerNameNode(MiniDFSCluster cluster, int index,
      Slot slot) {
    registerNameNode(cluster, index, slot.name, slot.epoch, slot);
  }

  private static void registerNameNode(MiniDFSCluster cluster, int index,
      String sourceId, long epoch, Slot slot) {
    NameNodeRpcServer rpc =
        (NameNodeRpcServer) cluster.getNameNodeRpc(index);
    FSNamesystem namesystem = cluster.getNamesystem(index);
    BlockManager blocks = namesystem.getBlockManager();
    List<Object> aliases = new ArrayList<>();
    aliases.add(rpc);
    aliases.add(hidden(rpc, "getServiceRpcServer"));
    aliases.add(hidden(rpc, "getLifelineRpcServer"));
    aliases.add(cluster.getNameNode(index));
    aliases.add(namesystem);
    aliases.add(blocks);
    aliases.add(blocks.getDatanodeManager());
    aliases.add(blocks.getBlockTokenSecretManager());
    aliases.add(slot);
    register(rpc.getClientRpcServer(), "NAMENODE", sourceId, epoch,
        aliases.toArray());
  }

  /**
   * Registers {@code datanode} as {@code sourceId}: its DatanodeID is the
   * anchor, and the DataNode itself, its SASL server and client, its
   * transfer server, its RPC server, its block-token managers and its
   * block-pool services are the same node.
   */
  public static void registerDataNode(DataNode datanode, String sourceId) {
    registerDataNode(datanode, sourceId, 0L, null);
  }

  private static void registerDataNode(DataNode datanode, String sourceId,
      long epoch, Slot slot) {
    List<Object> aliases = new ArrayList<>();
    aliases.add(datanode);
    aliases.add(datanode.getSaslServer());
    aliases.add(datanode.getSaslClient());
    aliases.add(datanode.getXferServer());
    // A handler serving a client's ClientDatanodeProtocol call (a replica's
    // visible length) is the RPC server's.
    aliases.add(datanode.ipcServer);
    BlockPoolTokenSecretManager keys =
        datanode.getBlockPoolTokenSecretManager();
    aliases.add(keys);
    for (BPOfferService pool : datanode.getAllBpOs()) {
      aliases.add(pool);
      aliases.addAll(pool.getBPServiceActors());
      // A DataNode registered as soon as it is built (a restart) knows no
      // block pool until its handshake.
      String blockPool = pool.getBlockPoolId(true);
      if (keys != null && blockPool != null
          && keys.isBlockPoolRegistered(blockPool)) {
        aliases.add(keys.get(blockPool));
      }
    }
    // A DataNode that never registered with a NameNode (a test's failed
    // start) has no DatanodeID: it is still a node, anchored by itself.
    Object anchor = datanode.getDatanodeId() != null
        ? datanode.getDatanodeId() : datanode;
    aliases.add(slot);
    register(anchor, "DATANODE", sourceId, epoch, dedupe(aliases).toArray());
    REGISTERED.put(datanode, sourceId);
  }

  /**
   * Registers one node of {@code role} as {@code sourceId} in life {@code
   * epoch}: its anchor as the source and every object it runs its work
   * through as the same node (a null alias, or the anchor, skipped).
   */
  private static void register(Object anchor, String role, String sourceId,
      long epoch, Object... aliases) {
    CausynthMessagePropagation.registerSource(anchor, NODE, role, sourceId,
        epoch);
    for (Object alias : aliases) {
      if (alias != null && alias != anchor) {
        CausynthMessagePropagation.registerSourceAlias(alias, anchor);
      }
    }
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
   * An upstream test's whole registration in one call: NameNode 0 as {@code
   * <prefix>/nn0}, each client as {@code <prefix>/client<i>} and every
   * DataNode as {@code <prefix>/dn<index>}.
   */
  public static void registerCluster(MiniDFSCluster cluster, String prefix,
      DFSClient... clients) {
    for (int index = 0; index < clients.length; index++) {
      CausynthMessagePropagation.registerSource(clients[index],
          "EXTERNAL_APP", "DFS_CLIENT", prefix + "/client" + index, 0);
    }
    registerNameNode(cluster, 0, prefix + "/nn0");
    registerOtherDataNodes(cluster, prefix);
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

  /** What a getter its class keeps package-private returns, or null. */
  private static Object hidden(Object owner, String getter) {
    try {
      Method method = owner.getClass().getDeclaredMethod(getter);
      method.setAccessible(true);
      return method.invoke(owner);
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
