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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.datanode.metrics.DataNodeDiskMetrics;
import org.apache.hadoop.hdfs.server.protocol.SlowDiskReports.DiskOp;
import org.apache.hadoop.ipc.CausynthMessagePropagation;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * HDFS bug study #15: a slow-disk exclusion list outlives turning exclusion
 * off.
 *
 * <p>{@link DataNodeDiskMetrics}'s detector daemon publishes the top {@code
 * maxSlowDisksToExclude} slow disks on every turn where that number is
 * positive, and on a turn where it is 0 -- the documented "exclude nothing"
 * a reconfiguration may set at run time -- it publishes nothing, so a list
 * published before stays.  {@code FsVolumeList} filters every volume choice
 * by that list.</p>
 *
 * <p>The workload flags the DataNode's only SSD slow (the class's test hook)
 * and at once reconfigures {@code dfs.datanode.max.slowdisks.to.exclude}
 * from 1 to 0, waits a few detector turns, and writes a file under the
 * ALL_SSD policy.  Every detector turn after the flag reads 0, so the write
 * lands on the SSD.  A turn that runs between the flag and the
 * reconfiguration reads 1 and publishes [SSD]; the later turns never clear
 * it, and the write finds no SSD volume to choose
 * ({@code RoundRobinVolumeChoosingPolicy#chooseVolume}'s
 * {@code DiskOutOfSpaceException}).</p>
 */
public class TestCausynthSlowDiskZeroMax {
  /** The detector's turn. */
  private static final long DETECTION_INTERVAL_MS = 1000;
  /** The DataNode's only SSD, beside one DISK. */
  private static final StorageType[] STORAGES = {StorageType.SSD,
      StorageType.DISK};

  @Test
  @Timeout(120)
  public void testAllSsdWriteAfterSlowDiskExclusionTurnedOff()
      throws Exception {
    // Nothing set up before the window expires on the wall clock.
    Configuration conf = CausynthCluster.configure(new HdfsConfiguration());
    // File-IO profiling on: the DataNode keeps its disk metrics and runs
    // the slow-disk detector.
    conf.setInt(
        DFSConfigKeys.DFS_DATANODE_FILEIO_PROFILING_SAMPLING_PERCENTAGE_KEY,
        100);
    conf.setInt(DFSConfigKeys.DFS_DATANODE_MAX_SLOWDISKS_TO_EXCLUDE_KEY, 1);
    conf.set(DFSConfigKeys.DFS_DATANODE_OUTLIERS_REPORT_INTERVAL_KEY,
        DETECTION_INTERVAL_MS + "ms");
    // Each DataNode has IPC connections of its own, as a DataNode process
    // does (CausynthCluster.dataNodeOverlays).
    try (MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(1)
        .storagesPerDatanode(STORAGES.length)
        .storageTypes(STORAGES)
        .dataNodeConfOverlays(CausynthCluster.dataNodeOverlays(1)).build();
         DistributedFileSystem fs = cluster.getFileSystem()) {
      cluster.waitActive();
      DataNode datanode = cluster.getDataNodes().get(0);
      DataNodeDiskMetrics metrics = datanode.getDiskMetrics();
      assertNotNull(metrics, "file-IO profiling must start the detector");
      String ssd = ssdPath(datanode);
      Path dir = new Path("/causynth-slowdisk");
      fs.mkdirs(dir);
      fs.setStoragePolicy(dir, HdfsConstants.ALLSSD_STORAGE_POLICY_NAME);
      CausynthCluster.registerCluster(cluster, "slowdisk", fs.getClient());
      CausynthCluster.startRecording();

      // The SSD is slow, as the detector would find it.
      long flag = CausynthMessagePropagation.beginRequest(
          datanode.getDatanodeId(), "flag-slow-disk");
      try {
        metrics.addSlowDiskForTesting(ssd, ImmutableMap.of(
            DiskOp.READ, 1.0, DiskOp.WRITE, 1.5, DiskOp.METADATA, 2.0));
      } finally {
        CausynthMessagePropagation.endRequest(flag, "flag-slow-disk");
      }
      // At once, exclusion off: 0 is "exclude nothing".
      long off = CausynthMessagePropagation.beginRequest(
          datanode.getDatanodeId(), "slow-disk-exclusion-off");
      try {
        datanode.reconfigureProperty(
            DFSConfigKeys.DFS_DATANODE_MAX_SLOWDISKS_TO_EXCLUDE_KEY, "0");
      } finally {
        CausynthMessagePropagation.endRequest(off,
            "slow-disk-exclusion-off");
      }
      // A few detector turns, each reading 0.
      Thread.sleep(3 * DETECTION_INTERVAL_MS);

      Path file = new Path(dir, "file");
      long write = CausynthMessagePropagation.beginRequest(fs.getClient(),
          "write-all-ssd");
      try {
        DFSTestUtil.createFile(fs, file, 1024, (short) 1, 0L);
      } finally {
        CausynthMessagePropagation.endRequest(write, "write-all-ssd");
      }
      LocatedBlock block = DFSTestUtil.getAllBlocks(fs, file).get(0);
      CausynthCluster.recordingPrecondition(
          () -> block.getStorageTypes()[0] == StorageType.SSD,
          "the ALL_SSD write must land on the SSD");
      assertEquals(1, block.getLocations().length);
    }
  }

  /** The base path the detector and the volume chooser name the SSD by. */
  private static String ssdPath(DataNode datanode) throws IOException {
    try (FsDatasetSpi.FsVolumeReferences volumes =
             datanode.getFSDataset().getFsVolumeReferences()) {
      for (FsVolumeSpi volume : volumes) {
        if (volume.getStorageType() == StorageType.SSD) {
          return volume.getBaseURI().getPath();
        }
      }
    }
    throw new AssertionError("no SSD volume on " + datanode);
  }
}
