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
package org.apache.hadoop.hdfs.server.sps;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;

import java.io.EOFException;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.StoragePolicySatisfierMode;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.server.balancer.NameNodeConnector;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.io.retry.FailoverProxyProvider.ProxyInfo;
import org.apache.hadoop.io.retry.RetryInvocationHandler;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.GenericTestUtils.LogCapturer;
import org.apache.hadoop.test.Whitebox;
import org.apache.hadoop.util.ExitUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An external SPS (and a Balancer service) marks itself running by holding
 * the lease of an id file: {@code NameNodeConnector#checkAndMarkRunning}.
 * If the NameNode applies the {@code create} of that file but its reply never
 * reaches the client, the connector's retry runs on the same cached
 * DFSClient, i.e. the same lease holder. Its fail-fast {@code append} then
 * gets "already the current lease holder" (AlreadyBeingCreatedException),
 * which the connector reads as "another instance is running": the external
 * SPS terminates although no other SPS exists.
 *
 * The lost reply is injected between the RPC layer and the retry proxy of
 * the client's own ClientProtocol proxy: the real create RPC runs and the
 * NameNode applies it, then the reply is replaced by an EOFException, exactly
 * where a broken connection surfaces. The retry proxy above it keeps its
 * production policy, so the test also shows what a client retry would do.
 */
@Timeout(value = 300)
public class TestExternalSpsLostCreateResponse {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestExternalSpsLostCreateResponse.class);
  private static final String SPS_NAME =
      ExternalStoragePolicySatisfier.class.getSimpleName();
  private static final Path SPS_ID_PATH = HdfsServerConstants.MOVER_ID_PATH;
  /** Balancer.BALANCER_ID_PATH is package private. */
  private static final Path BALANCER_ID_PATH = new Path("/system/balancer.id");

  private MiniDFSCluster cluster;
  private DistributedFileSystem connectorFs;
  private NameNodeConnector connector;

  private Configuration startCluster(Configuration conf) throws IOException {
    conf.set(DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_MODE_KEY,
        StoragePolicySatisfierMode.EXTERNAL.toString());
    NameNodeConnector.checkOtherInstanceRunning(true);
    NameNodeConnector.setWrite2IdFile(true);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    cluster.waitActive();
    // The file system every NameNodeConnector of this JVM gets from
    // FileSystem.get(nameNodeUri, conf): one DFSClient, one lease holder.
    connectorFs = (DistributedFileSystem) FileSystem.get(nameNodeUri(conf), conf);
    return conf;
  }

  private static URI nameNodeUri(Configuration conf) {
    Collection<URI> uris = DFSUtil.getInternalNsRpcUris(conf);
    assertEquals(1, uris.size());
    return uris.iterator().next();
  }

  private String leaseHolder(Path path) {
    return NameNodeAdapter.getLeaseHolderForPath(cluster.getNameNode(),
        path.toString());
  }

  @AfterEach
  public void tearDown() throws IOException {
    if (connector != null) {
      connector.close();
    }
    if (connectorFs != null) {
      connectorFs.close();
    }
    ExitUtil.resetFirstExitException();
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * The recording: two SPS starts in a row, the first one stopped properly,
   * both mark themselves running.
   */
  @Test
  public void testConsecutiveStartsSucceed() throws Exception {
    Configuration conf = startCluster(new HdfsConfiguration());
    String clientName = connectorFs.getClient().getClientName();

    NameNodeConnector first =
        ExternalStoragePolicySatisfier.getNameNodeConnector(conf);
    assertNotNull(first);
    assertEquals(clientName, leaseHolder(SPS_ID_PATH));
    first.close();
    assertFalse(connectorFs.exists(SPS_ID_PATH));

    connector = ExternalStoragePolicySatisfier.getNameNodeConnector(conf);
    assertNotNull(connector);
    assertNotSame(first, connector);
    assertEquals(clientName, leaseHolder(SPS_ID_PATH));
  }

  /**
   * The create of the SPS id file is applied by the NameNode, its reply is
   * lost. The SPS retries on the same DFSClient, finds its own lease on the
   * id file and terminates: "another ExternalStoragePolicySatisfier is
   * running", though it is the only one. This test passes while the bug is
   * present.
   */
  @Test
  public void testLostCreateResponseTerminatesSps() throws Exception {
    Configuration conf = startCluster(new HdfsConfiguration());
    String clientName = connectorFs.getClient().getClientName();
    LostCreateReply wire = LostCreateReply.inject(connectorFs, SPS_ID_PATH);
    LogCapturer spsLog =
        LogCapturer.captureLogs(ExternalStoragePolicySatisfier.LOG);
    ExitUtil.disableSystemExit();

    ExitUtil.ExitException exit;
    try {
      exit = assertThrows(ExitUtil.ExitException.class,
          () -> ExternalStoragePolicySatisfier.getNameNodeConnector(conf));
    } finally {
      spsLog.stopCapturing();
    }
    LOG.info("SPS terminated, status {}", exit.status, exit);

    assertEquals(-1, exit.status);
    assertEquals("Exit immediately because another " + SPS_NAME
        + " is running", exit.getMessage());
    // Attempt 1: the create reached the NameNode, the reply did not.
    assertEquals(1, wire.creates.size());
    assertTrue(spsLog.getOutput().contains(LostCreateReply.MESSAGE));
    // Attempt 2: the fail-fast append of the same holder was refused.
    assertEquals(1, wire.appendRefusals.size());
    RemoteException refusal = wire.appendRefusals.get(0);
    assertEquals(AlreadyBeingCreatedException.class.getName(),
        refusal.getClassName());
    assertTrue(refusal.getMessage().contains(
        clientName + " is already the current lease holder"),
        refusal.getMessage());
    assertTrue(spsLog.getOutput().contains(
        "Another " + SPS_NAME + " is running."));
    // The "other" SPS is this SPS: the only lease on the id file is its own.
    assertEquals(clientName, leaseHolder(SPS_ID_PATH));
    assertEquals(wire.creates.get(0).getFileId(),
        connectorFs.getClient().getFileInfo(SPS_ID_PATH.toString())
            .getFileId());
  }

  /**
   * dfs.client.retry.policy.enabled lets the client proxy re-send the lost
   * create with the same call id; the NameNode retry cache answers it with
   * the reply of the applied call, so the SPS starts.
   */
  @Test
  public void testClientRetryPolicyAbsorbsLostCreateResponse()
      throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(HdfsClientConfigKeys.Retry.POLICY_ENABLED_KEY, true);
    conf.set(HdfsClientConfigKeys.Retry.POLICY_SPEC_KEY, "100,3");
    startCluster(conf);
    String clientName = connectorFs.getClient().getClientName();
    LostCreateReply wire = LostCreateReply.inject(connectorFs, SPS_ID_PATH);
    FSNamesystem fsn = cluster.getNamesystem();
    long hitsBefore = fsn.getRetryCache().getMetricsForTests().getCacheHit();

    connector = ExternalStoragePolicySatisfier.getNameNodeConnector(conf);

    assertNotNull(connector);
    assertEquals(2, wire.creates.size());
    assertEquals(wire.creates.get(0).getFileId(),
        wire.creates.get(1).getFileId());
    assertEquals(hitsBefore + 1,
        fsn.getRetryCache().getMetricsForTests().getCacheHit());
    assertTrue(wire.appendRefusals.isEmpty());
    assertEquals(clientName, leaseHolder(SPS_ID_PATH));
  }

  /**
   * The Balancer id file, same lost reply: every later attempt of the same
   * process (a Balancer -asService round) fails with "Another Balancer is
   * running." until the NameNode reclaims the lease at its hard limit.
   */
  @Test
  public void testLostCreateResponseStallsBalancerUntilHardLimit()
      throws Exception {
    Configuration conf = startCluster(new HdfsConfiguration());
    String clientName = connectorFs.getClient().getClientName();
    Collection<URI> namenodes = DFSUtil.getInternalNsRpcUris(conf);
    LostCreateReply wire =
        LostCreateReply.inject(connectorFs, BALANCER_ID_PATH);

    IOException lost = assertThrows(IOException.class,
        () -> newBalancerConnector(namenodes, conf));
    assertTrue(lost instanceof EOFException, lost.toString());
    for (int round = 1; round <= 3; round++) {
      IOException refused = assertThrows(IOException.class,
          () -> newBalancerConnector(namenodes, conf));
      assertEquals("Another Balancer is running.", refused.getMessage());
      assertEquals(clientName, leaseHolder(BALANCER_ID_PATH));
    }
    assertEquals(3, wire.appendRefusals.size());

    // Only the lease hard limit (20 min by default) releases the id file.
    NameNodeAdapter.setLeasePeriod(cluster.getNamesystem(), 1000, 2000);
    GenericTestUtils.waitFor(() -> leaseHolder(BALANCER_ID_PATH) == null,
        200, 30000);
    connector = newBalancerConnector(namenodes, conf);
    assertEquals(clientName, leaseHolder(BALANCER_ID_PATH));
  }

  /** The connector a Balancer round builds (Balancer#doBalance). */
  private static NameNodeConnector newBalancerConnector(
      Collection<URI> namenodes, Configuration conf) throws IOException {
    List<NameNodeConnector> connectors =
        NameNodeConnector.newNameNodeConnectors(namenodes, null, "Balancer",
            BALANCER_ID_PATH, conf,
            NameNodeConnector.DEFAULT_MAX_IDLE_ITERATIONS);
    return connectors.get(0);
  }

  /**
   * Wraps the RPC translator under the client's retry proxy. The first
   * create of the id file runs for real, then its reply is dropped.
   */
  private static final class LostCreateReply implements Answer<Object> {
    static final String MESSAGE = "simulated lost create response";
    private final String idPath;
    /** What the NameNode replied to each create of the id file. */
    final List<HdfsFileStatus> creates = new CopyOnWriteArrayList<>();
    /** What the NameNode replied to each refused append of the id file. */
    final List<RemoteException> appendRefusals = new CopyOnWriteArrayList<>();

    private LostCreateReply(Path idPath) {
      this.idPath = idPath.toString();
    }

    static LostCreateReply inject(DistributedFileSystem fs, Path idPath) {
      ClientProtocol namenode = fs.getClient().getNamenode();
      Object handler = Proxy.getInvocationHandler(namenode);
      assertTrue(handler instanceof RetryInvocationHandler, handler.toString());
      Object descriptor = Whitebox.getInternalState(handler, "proxyDescriptor");
      @SuppressWarnings("unchecked")
      ProxyInfo<ClientProtocol> translator = (ProxyInfo<ClientProtocol>)
          Whitebox.getInternalState(descriptor, "proxyInfo");
      LostCreateReply reply = new LostCreateReply(idPath);
      ClientProtocol lossy = mock(translator.proxy.getClass(),
          withSettings().spiedInstance(translator.proxy).defaultAnswer(reply));
      Whitebox.setInternalState(descriptor, "proxyInfo",
          new ProxyInfo<>(lossy, translator.proxyInfo));
      return reply;
    }

    @Override
    public Object answer(InvocationOnMock invocation) throws Throwable {
      String method = invocation.getMethod().getName();
      boolean onIdFile = invocation.getArguments().length > 0
          && idPath.equals(invocation.getArgument(0));
      Object result;
      try {
        result = invocation.callRealMethod();
      } catch (RemoteException e) {
        if (onIdFile && "append".equals(method)) {
          appendRefusals.add(e);
        }
        throw e;
      }
      if (onIdFile && "create".equals(method)) {
        creates.add((HdfsFileStatus) result);
        if (creates.size() == 1) {
          LOG.info("NameNode applied create of {} (fileId {}); dropping reply",
              idPath, ((HdfsFileStatus) result).getFileId());
          throw new EOFException(MESSAGE);
        }
      }
      return result;
    }
  }
}
