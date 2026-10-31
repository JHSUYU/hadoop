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
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.datanode.CausynthCluster;
import org.apache.hadoop.ipc.CausynthMessagePropagation;
import org.apache.hadoop.test.PathUtils;
import org.apache.hadoop.util.ExitUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * hdfs-inj-01: a NameNode whose failed log-segment start is swallowed keeps
 * serving in BETWEEN_LOG_SEGMENTS, and the next roll trips
 * endCurrentLogSegment's {@code isSegmentOpen()} precondition
 * (hdfs-adhoc-45).  hdfs-buggy-14075's workload, its failed roll tolerated
 * whatever it throws unless the NameNode terminated.
 *
 * <p>The NameNode has one edit journal, redundant (not required) with the
 * minimum redundant journals 1, so a failure to start a log segment on it
 * leaves {@code journalSet.startLogSegment} with too few journals and throws,
 * rather than JournalSet shutting the NameNode down for a required journal.
 * The workload rolls the edit log twice inside its window; each roll ends the
 * current segment (healthy, the previous one is open) and starts the next.
 * The first roll follows an edit; the second rolls at once, as the standby's
 * roll RPC calls rollEditLog directly.  In the recording both rolls
 * succeed.</p>
 *
 * <p>When the first roll's segment start fails, the correct NameNode
 * terminates (in a test, ExitUtil's ExitException): the workload ends there,
 * as the process would.  Any other failure of the first roll -- the segment
 * start's IOException re-thrown (HDFS-14075), or the roll's own header edit
 * meeting the null stream a swallowed start leaves -- leaves a NameNode that
 * keeps serving, so the workload goes on to the second roll, whose
 * {@code endCurrentLogSegment} then throws "Bad state: BETWEEN_LOG_SEGMENTS"
 * when the start was not completed.  The counterfactual is a local-IO fault
 * on the first roll's edits_inprogress create.</p>
 */
public class TestCausynthRollSegmentFailure {
  static final File TEST_DIR =
      PathUtils.getTestDir(TestCausynthRollSegmentFailure.class);

  @Test
  @Timeout(300)
  public void testNextRollAfterFailedStartLogSegment() throws Exception {
    Configuration conf = CausynthCluster.configure(new HdfsConfiguration());
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_EDITS_ASYNC_LOGGING, false);
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 21600L);
    conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY,
        new File(TEST_DIR, "name").getAbsolutePath());
    conf.set(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY,
        new File(TEST_DIR, "edits").getAbsolutePath());
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_MINIMUM_KEY, 1);

    try (MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(0).build()) {
      cluster.waitActive();
      DFSClient client = cluster.getFileSystem().getClient();
      FSEditLog editLog = cluster.getNamesystem().getFSImage().getEditLog();
      CausynthCluster.registerCluster(cluster, "roll", client);
      CausynthCluster.startRecording();

      roll(editLog, client, "roll-1", true);
      CausynthCluster.recordingPrecondition(editLog::isSegmentOpen,
          "the first roll must leave a segment open");
      roll(editLog, client, "roll-2", false);
      CausynthCluster.recordingPrecondition(editLog::isSegmentOpen,
          "the second roll must leave a segment open");
    }
  }

  private static void roll(FSEditLog editLog, DFSClient client, String api,
      boolean logFirst) {
    long request = CausynthMessagePropagation.beginRequest(client, api);
    try {
      if (logFirst) {
        editLog.logSetReplication("/causynth-roll", (short) 1);
        editLog.logSync();
      }
      editLog.rollEditLog(NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION);
    } catch (ExitUtil.ExitException terminated) {
      // The NameNode terminated: the workload ends, as the process would.
      throw terminated;
    } catch (IOException | RuntimeException rollFailed) {
      // A roll that failed without terminating the NameNode: the NameNode
      // keeps serving and the next roll comes.  The recording never reaches
      // this; a counterfactual replay that faults the first roll does.
    } finally {
      CausynthMessagePropagation.endRequest(request, api);
    }
  }
}
