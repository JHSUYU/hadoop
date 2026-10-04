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
package org.apache.hadoop.hdfs.server.namenode;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.JavaKeyStoreProvider;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.ReencryptAction;
import org.apache.hadoop.hdfs.protocol.ZoneReencryptionStatus;
import org.apache.hadoop.hdfs.server.datanode.CausynthCluster;
import org.apache.hadoop.ipc.CausynthMessagePropagation;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * HDFS bug study HDFS-2 #09: the re-encryption throttle can compute a
 * negative sleep, and a zone it was re-encrypting stays submitted for good.
 *
 * <p>{@link ReencryptionHandler}'s throttle, below a handler ratio of 1.0,
 * reads its {@code throttleTimerAll} stopwatch once for the time it expects
 * under the lock and again for how long to sleep: {@code sleepMs =
 * locked / ratio - all}.  Nothing keeps the second reading below twice the
 * locked time, so the sleep can be negative; {@code Thread.sleep} then
 * throws {@code IllegalArgumentException}, the handler's {@code run} returns
 * on it, and the zone it was processing keeps its {@code Processing} status
 * with no thread left to finish it.  Every later START of that zone is
 * refused by {@code EncryptionZoneManager#reencryptEncryptionZone}'s "already
 * submitted" check until the NameNode restarts.</p>
 *
 * <p>The workload re-encrypts a ten-file zone in batches of five under a
 * ratio of 0.5, waits for it to complete, and starts it again, which
 * succeeds.  Every cluster test of {@code TestReencryption} keeps the ratio
 * at 1.0, which returns before the clock code, and
 * {@code TestReencryptionHandler#testThrottle} mocks the handler's
 * collaborators, so no existing test reaches the bug.</p>
 */
public class TestCausynthReencryptNegativeSleep {
  private static final String KEY = "test_key";
  private static final int FILES = 10;
  private static final int BATCH = 5;

  @Test
  @Timeout(300)
  public void testReencryptAgainAfterThrottledReencryption()
      throws Exception {
    // Nothing set up before the window expires on the wall clock.
    Configuration conf = CausynthCluster.configure(new HdfsConfiguration());
    File keyStore =
        new File(GenericTestUtils.getRandomizedTestDir(), "test.jks");
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_KEY_PROVIDER_PATH,
        JavaKeyStoreProvider.SCHEME_NAME + "://file"
            + new Path(keyStore.getAbsolutePath()).toUri());
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REENCRYPT_BATCH_SIZE_KEY, BATCH);
    // Below 1.0 the handler's throttle runs its clock code.
    conf.setDouble(
        DFSConfigKeys.DFS_NAMENODE_REENCRYPT_THROTTLE_LIMIT_HANDLER_RATIO_KEY,
        0.5);
    // Each DataNode has IPC connections of its own, as a DataNode process
    // does (CausynthCluster.dataNodeOverlays).
    try (MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(1)
        .dataNodeConfOverlays(CausynthCluster.dataNodeOverlays(1)).build();
         DistributedFileSystem fs = cluster.getFileSystem()) {
      cluster.waitActive();
      DFSClient client = fs.getClient();
      // A JKS file provider holds its keys in memory: the client uses the
      // NameNode's, as TestReencryption does.
      KeyProvider provider = cluster.getNamesystem().getProvider();
      client.setKeyProvider(provider);
      DFSTestUtil.createKey(KEY, cluster, conf);
      Path zone = new Path("/zone");
      fs.mkdirs(zone);
      fs.createEncryptionZone(zone, KEY);
      for (int i = 0; i < FILES; i++) {
        DFSTestUtil.createFile(fs, new Path(zone, Integer.toString(i)), 8196,
            (short) 1, 0xFEED);
      }
      // Every file's EDEK now names an older key version: a START
      // re-encrypts all ten, two batches, each followed by a throttle.
      provider.rollNewVersion(KEY);
      provider.flush();
      CausynthCluster.registerCluster(cluster, "reencrypt", client);
      CausynthCluster.startRecording();

      start(fs, zone, "start-reencryption");
      long wait = CausynthMessagePropagation.beginRequest(client,
          "await-reencryption");
      boolean completed;
      try {
        completed = awaitCompleted(fs, zone);
      } finally {
        CausynthMessagePropagation.endRequest(wait, "await-reencryption");
      }
      CausynthCluster.recordingPrecondition(() -> completed,
          "the first re-encryption must complete");
      // The zone is no longer running: a second START is accepted.
      start(fs, zone, "start-reencryption-again");
    }
  }

  private static void start(DistributedFileSystem fs, Path zone, String api)
      throws IOException {
    long request = CausynthMessagePropagation.beginRequest(fs.getClient(),
        api);
    try {
      fs.reencryptEncryptionZone(zone, ReencryptAction.START);
    } finally {
      CausynthMessagePropagation.endRequest(request, api);
    }
  }

  /**
   * Whether the zone's re-encryption completed within the window's wait,
   * as the NameNode lists it: a bounded wait that never fails, so a run
   * whose handler died still makes the workload's next request.
   */
  private static boolean awaitCompleted(DistributedFileSystem fs, Path zone)
      throws IOException, InterruptedException {
    long deadline = Time.monotonicNow() + CausynthCluster.WINDOW_WAIT_MS;
    while (true) {
      RemoteIterator<ZoneReencryptionStatus> zones =
          fs.listReencryptionStatus();
      while (zones.hasNext()) {
        ZoneReencryptionStatus status = zones.next();
        if (status.getZoneName().equals(zone.toString())
            && status.getState() == ZoneReencryptionStatus.State.Completed) {
          return true;
        }
      }
      if (Time.monotonicNow() >= deadline) {
        return false;
      }
      Thread.sleep(100);
    }
  }
}
