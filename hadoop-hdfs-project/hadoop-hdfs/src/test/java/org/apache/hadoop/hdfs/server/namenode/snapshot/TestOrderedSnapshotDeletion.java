/*
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
package org.apache.hadoop.hdfs.server.namenode.snapshot;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.SafeModeAction;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.XAttrHelper;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeRpcServer;
import org.apache.hadoop.hdfs.server.namenode.XAttrFeature;
import org.apache.hadoop.ipc.CausynthMessagePropagation;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Map;

import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.XATTR_SNAPSHOT_DELETED;
import static org.apache.hadoop.hdfs.server.namenode.snapshot.SnapshotManager.DFS_NAMENODE_SNAPSHOT_DELETION_ORDERED;
import static org.junit.Assert.assertTrue;

/**
 * Test ordered snapshot deletion.
 */
public class TestOrderedSnapshotDeletion {
  static final String xattrName = "user.a1";
  static final byte[] xattrValue = {0x31, 0x32, 0x33};
  private final Path snapshottableDir
      = new Path("/" + getClass().getSimpleName());

  private MiniDFSCluster cluster;

  @Before
  public void setUp() throws Exception {
    final Configuration conf = new Configuration();
    conf.setBoolean(DFS_NAMENODE_SNAPSHOT_DELETION_ORDERED, true);

    // One run, one replayed edit-log range.  Every NameNode-startup
    // occurrence of this case is addressed by the k-th applyEditLogOp of
    // the single loadEditRecords the restart runs, so one edit record more
    // or fewer in [imageTxId + 1 .. lastTxId] renumbers all of them and
    // every task addressed there is refused OCCURRENCE_ANCHOR_UNMATCHED.
    // Nothing inside the window may roll the open segment, checkpoint the
    // namespace, or write an edit off the request thread:
    //  - the autoroll threshold is 0.5 * checkpoint.txns and the roller
    //    checks once before its first sleep, so both are pinned;
    //  - checkpoint.period and checkpoint.txns are the two
    //    needsResaveBasedOnStaleCheckpoint conditions (image age and
    //    transactions loaded) that make startup save a namespace, which
    //    would move the image txid the next load starts from -- the age one
    //    is wall-clock, so a slow run alone could flip it;
    //  - async edit logging writes the records from the FSEditLogAsync
    //    daemon instead of the thread that made them, which is one more
    //    thread between a request and its edits for no gain here.
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_TXNS_KEY, 1L << 40);
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_PERIOD_KEY,
        365L * 24 * 60 * 60);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_EDIT_LOG_AUTOROLL_CHECK_INTERVAL_MS,
        24 * 60 * 60 * 1000);
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_EDITS_ASYNC_LOGGING, false);

    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0).build();
    cluster.waitActive();
    CausynthMessagePropagation.registerSource(
        this, "EXTERNAL_APP", "HDFS_CLIENT", "hdfs-17960/client", 0);
    CausynthMessagePropagation.registerSource(
        cluster, "CLUSTER_NODE", "NAMENODE", "hdfs-17960/nn0", 0);
    bindNameNodeAliases();
  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  @Test(timeout = 60000)
  public void testOrderedSnapshotDeletion() throws Exception {
    DistributedFileSystem hdfs = cluster.getFileSystem();
    hdfs.mkdirs(snapshottableDir);
    hdfs.allowSnapshot(snapshottableDir);

    final Path sub0 = new Path(snapshottableDir, "sub0");
    hdfs.mkdirs(sub0);
    hdfs.createSnapshot(snapshottableDir, "s0");

    final Path sub1 = new Path(snapshottableDir, "sub1");
    hdfs.mkdirs(sub1);
    hdfs.createSnapshot(snapshottableDir, "s1");

    final Path sub2 = new Path(snapshottableDir, "sub2");
    hdfs.mkdirs(sub2);
    hdfs.createSnapshot(snapshottableDir, "s2");

    assertXAttrSet("s1", hdfs, null);
    assertXAttrSet("s2", hdfs, null);
    hdfs.deleteSnapshot(snapshottableDir, "s0");
    assertXAttrSet("s2", hdfs, null);
    hdfs.deleteSnapshot(snapshottableDir,
        getDeletedSnapshotName(hdfs, snapshottableDir, "s1"));
    hdfs.deleteSnapshot(snapshottableDir,
        getDeletedSnapshotName(hdfs, snapshottableDir, "s2"));
  }

  static void assertMarkedAsDeleted(Path snapshotRoot, Path snapshottableDir,
      MiniDFSCluster cluster) throws IOException {
    final String snapName =
        getDeletedSnapshotName(cluster.getFileSystem(), snapshottableDir,
            snapshotRoot.getName());
    final Path snapPathNew =
        SnapshotTestHelper.getSnapshotRoot(snapshottableDir, snapName);
    // Check if the path exists
    Assert.assertNotNull(cluster.getFileSystem().getFileStatus(snapPathNew));

    // Check xAttr for snapshotRoot
    final INode inode = cluster.getNamesystem().getFSDirectory()
        .getINode(snapPathNew.toString());
    final XAttrFeature f = inode.getXAttrFeature();
    final XAttr xAttr = f.getXAttr(XATTR_SNAPSHOT_DELETED);
    Assert.assertNotNull(xAttr);
    Assert.assertEquals(XATTR_SNAPSHOT_DELETED.substring("system.".length()),
        xAttr.getName());
    Assert.assertEquals(XAttr.NameSpace.SYSTEM, xAttr.getNameSpace());
    Assert.assertNull(xAttr.getValue());

    // Check inode
    Assert.assertTrue(inode instanceof Snapshot.Root);
    Assert.assertTrue(((Snapshot.Root) inode).isMarkedAsDeleted());
  }

  static void assertNotMarkedAsDeleted(Path snapshotRoot,
      MiniDFSCluster cluster) throws IOException {
    // Check if the path exists
    Assert.assertNotNull(cluster.getFileSystem().getFileStatus(snapshotRoot));

    // Check xAttr for snapshotRoot
    final INode inode = cluster.getNamesystem().getFSDirectory()
        .getINode(snapshotRoot.toString());
    final XAttrFeature f = inode.getXAttrFeature();
    if (f != null) {
      final XAttr xAttr = f.getXAttr(XATTR_SNAPSHOT_DELETED);
      Assert.assertNull(xAttr);
    }

    // Check inode
    Assert.assertTrue(inode instanceof Snapshot.Root);
    Assert.assertFalse(((Snapshot.Root)inode).isMarkedAsDeleted());
  }

  void assertXAttrSet(String snapshot,
                      DistributedFileSystem hdfs, XAttr newXattr)
      throws IOException {
    String snapName = getDeletedSnapshotName(hdfs, snapshottableDir, snapshot);
    hdfs.deleteSnapshot(snapshottableDir, snapName);
    // Check xAttr for parent directory
    Path snapshotRoot =
        SnapshotTestHelper.getSnapshotRoot(snapshottableDir, snapshot);
    assertMarkedAsDeleted(snapshotRoot, snapshottableDir, cluster);
    // Check xAttr for parent directory
    snapName = getDeletedSnapshotName(hdfs, snapshottableDir, snapshot);
    snapshotRoot =
        SnapshotTestHelper.getSnapshotRoot(snapshottableDir, snapName);
    // Make sure its not user visible
    if (cluster.getNameNode().getConf().getBoolean(DFSConfigKeys.
            DFS_NAMENODE_XATTRS_ENABLED_KEY,
        DFSConfigKeys.DFS_NAMENODE_XATTRS_ENABLED_DEFAULT)) {
      Map<String, byte[]> xattrMap = hdfs.getXAttrs(snapshotRoot);
      assertTrue(newXattr == null ? xattrMap.isEmpty() :
          Arrays.equals(newXattr.getValue(), xattrMap.get(xattrName)));
    }
  }

  @Test(timeout = 60000)
  public void testSnapshotXattrPersistence() throws Exception {
    DistributedFileSystem hdfs = cluster.getFileSystem();
    hdfs.mkdirs(snapshottableDir);
    hdfs.allowSnapshot(snapshottableDir);

    final Path sub0 = new Path(snapshottableDir, "sub0");
    hdfs.mkdirs(sub0);
    hdfs.createSnapshot(snapshottableDir, "s0");

    final Path sub1 = new Path(snapshottableDir, "sub1");
    hdfs.mkdirs(sub1);
    hdfs.createSnapshot(snapshottableDir, "s1");
    assertXAttrSet("s1", hdfs, null);
    assertXAttrSet("s1", hdfs, null);
    cluster.restartNameNodes();
    assertXAttrSet("s1", hdfs, null);
  }

  @Test(timeout = 60000)
  public void testSnapshotXattrWithSaveNameSpace() throws Exception {
    DistributedFileSystem hdfs = cluster.getFileSystem();
    hdfs.mkdirs(snapshottableDir);
    hdfs.allowSnapshot(snapshottableDir);

    final Path sub0 = new Path(snapshottableDir, "sub0");
    hdfs.mkdirs(sub0);
    hdfs.createSnapshot(snapshottableDir, "s0");

    final Path sub1 = new Path(snapshottableDir, "sub1");
    hdfs.mkdirs(sub1);
    hdfs.createSnapshot(snapshottableDir, "s1");
    assertXAttrSet("s1", hdfs, null);
    hdfs.setSafeMode(SafeModeAction.ENTER);
    hdfs.saveNamespace();
    hdfs.setSafeMode(SafeModeAction.LEAVE);
    cluster.restartNameNodes();
    assertXAttrSet("s1", hdfs, null);
  }

  @Test(timeout = 6000000)
  public void testOrderedDeletionWithRestart() throws Exception {
    DistributedFileSystem hdfs = cluster.getFileSystem();
    CausynthMessagePropagation.startRecording();

    long createRequest = CausynthMessagePropagation.beginRequest(
        this, "create-snapshots");
    try {
      hdfs.mkdirs(snapshottableDir);
      hdfs.allowSnapshot(snapshottableDir);

      final Path sub0 = new Path(snapshottableDir, "sub0");
      hdfs.mkdirs(sub0);
      hdfs.createSnapshot(snapshottableDir, "s0");

      final Path sub1 = new Path(snapshottableDir, "sub1");
      hdfs.mkdirs(sub1);
      hdfs.createSnapshot(snapshottableDir, "s1");
    } finally {
      CausynthMessagePropagation.endRequest(
          createRequest, "create-snapshots");
    }

    long deleteRequest = CausynthMessagePropagation.beginRequest(
        this, "record-ordered-deletes");
    try {
      assertXAttrSet("s1", hdfs, null);
      assertXAttrSet("s1", hdfs, null);
    } finally {
      CausynthMessagePropagation.endRequest(
          deleteRequest, "record-ordered-deletes");
    }

    // Record the HEALTHY epoch-1 value.  The restarted NameNode re-reads
    // this configuration into SnapshotManager.snapshotDeletionOrdered, and
    // that field is the campaign's one symbolic root.  Recording it at the
    // value the program normally runs with -- true -- keeps the recording
    // free of the fault: ordered deletion marks-and-renames both duplicate
    // OP_DELETE_SNAPSHOT records on replay, nothing is removed from
    // snapshotsByNames, numSnapshots is never decremented, and the
    // checkpoint's snapshot-count check agrees.  The count mismatch of
    // HDFS-17960 is then reachable only by FLIPPING the root to false,
    // which is the shape the concolic search is supposed to find.
    cluster.getNameNode().getConf().setBoolean(
        DFS_NAMENODE_SNAPSHOT_DELETION_ORDERED, true);
    CausynthMessagePropagation.restartSource(
        cluster, "CLUSTER_NODE", "NAMENODE", "hdfs-17960/nn0", 1);
    long restartRequest = CausynthMessagePropagation.beginRequest(
        cluster, "restart-namenode");
    try {
      cluster.restartNameNodes();
      bindNameNodeAliases();
    } finally {
      CausynthMessagePropagation.endRequest(
          restartRequest, "restart-namenode");
    }
    // Healthy replay: both snapshots survive the two duplicate deletion
    // records, and the counter still matches the list.  Under ordered=false
    // these would be 1 and 0 -- the divergence the annotated check catches.
    //
    // Observed, not asserted.  A replay that flips the root to false is
    // SUPPOSED to see 1 and 0 here, and that is the whole point of the
    // search; asserting the healthy values ends the test at this line and
    // the checkpoint below -- which contains the annotated check -- never
    // runs, so the flipped path never reaches the target site at all.  The
    // calls stay, because the reads are part of the behaviour being
    // recorded.  The failure this campaign is about is the count mismatch
    // inside FSImageFormatPBSnapshot.Saver.serializeSnapshotSection, and
    // nothing here weakens it.
    System.out.println("[causynth] post-restart snapshotListing="
        + hdfs.getSnapshotListing(snapshottableDir).length
        + " numSnapshots="
        + cluster.getNamesystem().getSnapshotManager().getNumSnapshots());

    long checkpointRequest = CausynthMessagePropagation.beginRequest(
        this, "checkpoint-namespace");
    try {
      hdfs.setSafeMode(SafeModeAction.ENTER);
      // The checkpoint reaches the annotated snapshot-count check in
      // FSImageFormatPBSnapshot.Saver.serializeSnapshotSection and finds the
      // counts equal, so it completes.  The target site is still executed --
      // that is what lets the planner select the target occurrence -- and the
      // exception is the counterfactual the solver has to find.
      hdfs.saveNamespace();
      hdfs.setSafeMode(SafeModeAction.LEAVE);
    } finally {
      CausynthMessagePropagation.endRequest(
          checkpointRequest, "checkpoint-namespace");
    }
  }

  private void bindNameNodeAliases() {
    CausynthMessagePropagation.registerSourceAlias(
        cluster.getNameNode(), cluster);
    NameNodeRpcServer rpc = (NameNodeRpcServer) cluster.getNameNodeRpc();
    CausynthMessagePropagation.registerSourceAlias(
        rpc.getClientRpcServer(), cluster);
  }

  @Test(timeout = 60000)
  public void testSnapshotXattrWithDisablingXattr() throws Exception {
    DistributedFileSystem hdfs = cluster.getFileSystem();
    hdfs.mkdirs(snapshottableDir);
    hdfs.allowSnapshot(snapshottableDir);

    final Path sub0 = new Path(snapshottableDir, "sub0");
    hdfs.mkdirs(sub0);
    hdfs.createSnapshot(snapshottableDir, "s0");

    final Path sub1 = new Path(snapshottableDir, "sub1");
    hdfs.mkdirs(sub1);
    hdfs.createSnapshot(snapshottableDir, "s1");
    assertXAttrSet("s1", hdfs, null);
    cluster.getNameNode().getConf().setBoolean(
        DFSConfigKeys.DFS_NAMENODE_XATTRS_ENABLED_KEY, false);
    cluster.restartNameNodes();
    // ensure xAttr feature is disabled
    try {
      hdfs.getXAttrs(snapshottableDir);
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("The XAttr operation has been " +
          "rejected.  Support for XAttrs has been disabled by " +
          "setting dfs.namenode.xattrs.enabled to false"));
    }
    // try deleting snapshot and verify it still sets the snapshot XAttr
    assertXAttrSet("s1", hdfs, null);
  }

  @Test(timeout = 60000)
  public void testSnapshotXAttrWithPreExistingXattrs() throws Exception {
    DistributedFileSystem hdfs = cluster.getFileSystem();
    hdfs.mkdirs(snapshottableDir);
    hdfs.allowSnapshot(snapshottableDir);
    hdfs.setXAttr(snapshottableDir, xattrName, xattrValue,
        EnumSet.of(XAttrSetFlag.CREATE));
    XAttr newXAttr = XAttrHelper.buildXAttr(xattrName, xattrValue);
    final Path sub0 = new Path(snapshottableDir, "sub0");
    hdfs.mkdirs(sub0);
    hdfs.createSnapshot(snapshottableDir, "s0");

    final Path sub1 = new Path(snapshottableDir, "sub1");
    hdfs.mkdirs(sub1);
    hdfs.createSnapshot(snapshottableDir, "s1");
    assertXAttrSet("s1", hdfs, newXAttr);
  }

  public static String getDeletedSnapshotName(DistributedFileSystem hdfs,
      Path snapshottableDir, String snapshot) throws IOException {
    return Arrays.stream(hdfs.getSnapshotListing(snapshottableDir))
        .filter(p -> p.getFullPath().getName().startsWith(snapshot)).findFirst()
        .get().getFullPath().getName();
  }
}
