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
package org.apache.hadoop.hdfs.security.token.block;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.ipc.CausynthSymbolicSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class TestBlockTokenSecretManagerCausynthSource {
  private static final String EXPIRY_SIGNATURE =
      "<org.apache.hadoop.security.token.delegation.DelegationKey: "
          + "long expiryDate>";

  private static volatile Integer modeledKeyId;
  private static volatile Long modeledExpiry;
  private static volatile Object symbolizedKeyIdOwner;
  private static volatile int concreteKeyIdBeforeModel;
  private static volatile int keyIdSymbolizeCalls;
  /** Mints the VM actually performed, i.e. first mint of one owner+field. */
  private static volatile int expirySymbolizeCalls;
  /** Every entry into the hook, including a guard-suppressed repeat. */
  private static volatile int expiryInvocations;
  private static volatile Object lastExpiryOwner;
  private static volatile String lastExpirySource;
  private static volatile String lastExpiryFieldSignature;
  private static final List<Object> EXPIRY_OWNERS = new ArrayList<>();
  /** Models the VM guard: one symbol per (owner, field) occurrence. */
  private static final List<Object[]> MINTED_PAIRS = new ArrayList<>();

  @Before
  public void installBridge() throws Exception {
    resetBridge();
    installTestBridge();
    clearObservations();
  }

  @After
  public void removeBridge() throws Exception {
    resetBridge();
  }

  @Test
  public void bothLoopsMintEveryMasterKeyExactlyOnceInUpdateKeys()
      throws Exception {
    BlockTokenSecretManager master = master();
    Map<Integer, BlockKey> allKeys = allKeys(master);
    List<BlockKey> before = new ArrayList<>(allKeys.values());
    clearObservations();
    modeledExpiry = Long.MAX_VALUE;

    assertTrue(master.updateKeys());

    assertTrue("master rotated no keys", allKeys.size() >= 2);
    assertBothLoopsMintedExactlyOnce(before, allKeys.values());
    for (BlockKey key : allKeys.values()) {
      assertEquals(Long.MAX_VALUE, key.getExpiryDate());
    }
    assertEquals("HDFS11741.KEY_EXPIRY", lastExpirySource);
    assertEquals(EXPIRY_SIGNATURE, lastExpiryFieldSignature);
    assertTrue(mintedExpiryOn(lastExpiryOwner));
  }

  @Test
  public void bothLoopsMintEveryWorkerKeyExactlyOnceInAddKeys()
      throws Exception {
    BlockTokenSecretManager master = master();
    ExportedBlockKeys exported = roundTrip(master.exportKeys());
    BlockTokenSecretManager worker = worker();
    Map<Integer, BlockKey> workerKeys = allKeys(worker);
    List<BlockKey> before = new ArrayList<>(workerKeys.values());
    clearObservations();
    modeledExpiry = Long.MAX_VALUE;

    worker.addKeys(exported);

    assertTrue("worker installed no keys", workerKeys.size() >= 2);
    assertBothLoopsMintedExactlyOnce(before, workerKeys.values());
    for (BlockKey key : workerKeys.values()) {
      assertEquals(Long.MAX_VALUE, key.getExpiryDate());
    }
    assertEquals("HDFS11741.KEY_EXPIRY", lastExpirySource);
    assertEquals(EXPIRY_SIGNATURE, lastExpiryFieldSignature);
  }

  /**
   * A member that survives the install is handed to both loops in the same
   * call.  The VM guard makes the second hand-off a no-op, so the occurrence
   * still owns exactly one symbol.
   */
  @Test
  public void guardMakesTheSecondMintOfOneOccurrenceANoOp() throws Exception {
    BlockTokenSecretManager master = master();
    ExportedBlockKeys exported = roundTrip(master.exportKeys());
    BlockTokenSecretManager worker = worker();
    modeledExpiry = Long.MAX_VALUE;
    worker.addKeys(exported);
    Map<Integer, BlockKey> workerKeys = allKeys(worker);
    assertTrue("worker installed too few keys", workerKeys.size() >= 2);

    // Refresh only one installed key; the rest survive the install and are
    // therefore reached by both loops of the same call.
    BlockKey refreshed = exported.getAllKeys()[0];
    BlockKey survivor = null;
    for (BlockKey key : workerKeys.values()) {
      if (key.getKeyId() != refreshed.getKeyId()) {
        survivor = key;
      }
    }
    assertNotNull("no surviving worker key", survivor);
    ExportedBlockKeys partial = roundTrip(new ExportedBlockKeys(true, 1000L,
        1000L, exported.getCurrentKey(), new BlockKey[]{refreshed}));
    List<BlockKey> before = new ArrayList<>(workerKeys.values());
    clearObservations();
    modeledExpiry = Long.MAX_VALUE;

    worker.addKeys(partial);

    BlockKey installed = workerKeys.get(refreshed.getKeyId());
    assertNotSame("the refreshed key was not replaced", refreshed, installed);
    assertSame("the survivor was replaced", survivor,
        workerKeys.get(survivor.getKeyId()));
    int survivors = intersectionSize(before, workerKeys.values());
    assertTrue("no member survived the install", survivors > 0);
    // Each survivor was handed to the hook twice and the guard suppressed the
    // repeat, so the hook entries exceed the performed mints by exactly the
    // number of survivors.
    assertEquals(survivors, expiryInvocations - expirySymbolizeCalls);
    assertBothLoopsMintedExactlyOnce(before, workerKeys.values());
  }

  /**
   * The pre-install loop runs before {@code removeExpiredKeys()}, so a member
   * that never carried a symbol is symbolized in time for the removal branch
   * of the very same call.  Without that loop the stale key below would
   * survive on its concrete far-future expiry.
   */
  @Test
  public void removalBranchSeesASymbolMintedInTheSameCall() throws Exception {
    BlockTokenSecretManager master = master();
    ExportedBlockKeys exported = roundTrip(master.exportKeys());
    BlockTokenSecretManager worker = worker();
    Map<Integer, BlockKey> workerKeys = allKeys(worker);
    int staleId = 0;
    while (containsId(exported, staleId)) {
      staleId++;
    }
    workerKeys.put(staleId,
        new BlockKey(staleId, Long.MAX_VALUE, (byte[]) null));
    clearObservations();
    modeledExpiry = Long.MIN_VALUE;

    worker.addKeys(exported);

    assertFalse("the stale key outlived a symbol minted in this call",
        workerKeys.containsKey(staleId));
    assertEquals(exported.getAllKeys().length, workerKeys.size());
    // One hook entry for the stale member, then one per installed member.
    assertEquals(1 + workerKeys.size(), expiryInvocations);
    assertEquals(expiryInvocations, expirySymbolizeCalls);
    assertEquals(expirySymbolizeCalls, distinctExpiryOwners());
  }

  /**
   * K is published on the export path, on the exact key the NameNode hands
   * out.  The owning map is re-keyed before the export reads it, so the wire
   * image the Balancer and every DataNode receive carries the modeled id.
   */
  @Test
  public void exportPublishesKOnTheKeyItHandsOut() throws Exception {
    BlockTokenSecretManager master = master();
    Map<Integer, BlockKey> masterKeys = allKeys(master);
    BlockKey exportedKey = keyField(master, "currentKey");
    int modeledK = 0;
    while (masterKeys.containsKey(modeledK)) {
      modeledK++;
    }
    int concreteId = exportedKey.getKeyId();
    clearObservations();
    modeledKeyId = modeledK;

    ExportedBlockKeys exported = master.exportKeys();

    assertEquals(1, keyIdSymbolizeCalls);
    assertSame(exportedKey, symbolizedKeyIdOwner);
    assertEquals(concreteId, concreteKeyIdBeforeModel);
    assertEquals(modeledK, exportedKey.getKeyId());
    assertSame(exportedKey, masterKeys.get(modeledK));
    assertFalse(masterKeys.containsKey(concreteKeyIdBeforeModel));
    // The wire image carries the published K: the exported currentKey is this
    // same object, and the exported array was read off the re-keyed map.
    assertSame(exportedKey, exported.getCurrentKey());
    assertEquals(modeledK, exported.getCurrentKey().getKeyId());
    assertTrue(containsId(exported, modeledK));
    assertFalse(containsId(exported, concreteId));

    // A wire round trip gives the worker fresh objects; each of those is its
    // own occurrence and gets its own expiry mint.
    ExportedBlockKeys wire = roundTrip(exported);
    BlockTokenSecretManager worker = worker();
    Map<Integer, BlockKey> workerKeys = allKeys(worker);
    List<BlockKey> workerBefore = new ArrayList<>(workerKeys.values());
    clearObservations();
    modeledExpiry = Long.MAX_VALUE;
    worker.addKeys(wire);

    BlockKey workerExportedKey = workerKeys.get(modeledK);
    assertNotNull(workerExportedKey);
    assertNotSame(exportedKey, workerExportedKey);
    assertTrue(mintedExpiryOn(workerExportedKey));
    assertBothLoopsMintedExactlyOnce(workerBefore, workerKeys.values());
    assertNotNull(worker.retrieveDataEncryptionKey(
        modeledK, new byte[]{1, 2, 3, 4}));
  }

  /**
   * The NameNode's selected-key expiry is published on the export path too,
   * on the same key K is published on.  Without this the only NameNode-side
   * mint of the expiry sits in the interval-guarded once-only rotation, which
   * a replayed cluster never re-enters, so the source materializes nothing.
   */
  @Test
  public void exportPublishesTheSelectedKeyExpiry() throws Exception {
    BlockTokenSecretManager master = master();
    BlockKey exportedKey = keyField(master, "currentKey");
    clearObservations();
    modeledExpiry = 4242424242L;

    ExportedBlockKeys exported = master.exportKeys();

    assertEquals(1, expiryInvocations);
    assertEquals(1, expirySymbolizeCalls);
    assertSame(exportedKey, lastExpiryOwner);
    assertEquals("HDFS11741.KEY_EXPIRY", lastExpirySource);
    assertEquals(EXPIRY_SIGNATURE, lastExpiryFieldSignature);
    assertTrue(mintedExpiryOn(exportedKey));
    // The wire image carries the modeled expiry: the exported currentKey is
    // this same object and the exported array was read off the same map.
    assertSame(exportedKey, exported.getCurrentKey());
    assertEquals(4242424242L, exported.getCurrentKey().getExpiryDate());
    assertTrue(containsExpiry(exported, 4242424242L));
  }

  /**
   * A second export re-enters the hook and re-mints nothing: the occurrence
   * already carries an expression, so the guard makes the repeat a no-op.
   */
  @Test
  public void aSecondExportRemintsNoExpiry() throws Exception {
    BlockTokenSecretManager master = master();
    BlockKey exportedKey = keyField(master, "currentKey");
    clearObservations();
    modeledExpiry = 4242424242L;

    master.exportKeys();
    master.exportKeys();

    assertEquals(2, expiryInvocations);
    assertEquals(1, expirySymbolizeCalls);
    assertEquals(1, distinctExpiryOwners());
    assertSame(exportedKey, lastExpiryOwner);
  }

  /**
   * Worker mode exports nothing, so it enters neither hook: the role guard
   * above both mints is not a selector on the published occurrence.
   */
  @Test
  public void workerExportEntersNoHook() throws Exception {
    BlockTokenSecretManager worker = worker();
    clearObservations();
    modeledExpiry = 4242424242L;
    modeledKeyId = 7;

    assertNull(worker.exportKeys());

    assertEquals(0, expiryInvocations);
    assertEquals(0, expirySymbolizeCalls);
    assertEquals(0, keyIdSymbolizeCalls);
  }

  /**
   * The rotation no longer publishes K.  It is interval-guarded and once-only,
   * so a mint site there is planned from a tracing pass and then never
   * executed by a replayed cluster.
   */
  @Test
  public void rotationPublishesNoK() throws Exception {
    BlockTokenSecretManager master = master();
    Map<Integer, BlockKey> masterKeys = allKeys(master);
    int modeledK = 0;
    while (masterKeys.containsKey(modeledK)) {
      modeledK++;
    }
    List<BlockKey> before = new ArrayList<>(masterKeys.values());
    clearObservations();
    modeledKeyId = modeledK;
    modeledExpiry = Long.MAX_VALUE;

    assertTrue(master.updateKeys());

    assertEquals(0, keyIdSymbolizeCalls);
    assertNull(symbolizedKeyIdOwner);
    assertFalse(masterKeys.containsKey(modeledK));
    // The expiry mints are untouched by the move.
    assertBothLoopsMintedExactlyOnce(before, masterKeys.values());
  }

  @Test
  public void managerExposesNoSelectedKeyMachinery() {
    for (Method method
        : BlockTokenSecretManager.class.getDeclaredMethods()) {
      String name = method.getName();
      assertFalse("selector survives: " + method,
          "causynthHdfs11741SelectedKey".equals(name));
      assertFalse("selected NN expiry hook survives: " + method,
          "symbolizeCausynthHdfs11741NameNodeExpiry".equals(name));
      assertFalse("selected DN expiry hook survives: " + method,
          "symbolizeCausynthHdfs11741DataNodeExpiry".equals(name));
    }
    for (Method method : CausynthSymbolicSource.class.getDeclaredMethods()) {
      assertFalse("selector survives on the bridge: " + method,
          "selectedIntValue".equals(method.getName()));
      assertFalse("reject survives on the bridge: " + method,
          "reject".equals(method.getName()));
    }
  }

  /**
   * Both loops covered the whole map at their own moment, and no occurrence
   * owns more than one symbol.
   */
  private static void assertBothLoopsMintedExactlyOnce(
      List<BlockKey> before, Collection<BlockKey> after) {
    assertEquals("both loops must walk the whole map",
        before.size() + after.size(), expiryInvocations);
    List<Object> union = new ArrayList<>();
    for (BlockKey key : before) {
      addDistinct(union, key);
    }
    for (BlockKey key : after) {
      addDistinct(union, key);
    }
    assertEquals("one mint per distinct occurrence",
        union.size(), expirySymbolizeCalls);
    assertEquals("one mint per distinct occurrence",
        union.size(), distinctExpiryOwners());
    for (Object owner : union) {
      assertTrue("no expiry mint for " + owner, mintedExpiryOn(owner));
    }
  }

  private static int intersectionSize(
      List<BlockKey> before, Collection<BlockKey> after) {
    List<Object> shared = new ArrayList<>();
    for (BlockKey key : before) {
      for (BlockKey other : after) {
        if (key == other) {
          addDistinct(shared, key);
        }
      }
    }
    return shared.size();
  }

  public static boolean symbolizeForTest(String sourceId, Object owner,
      String fieldSignature) {
    try {
      if (fieldSignature.endsWith(" int keyId>")) {
        if (modeledKeyId == null || alreadyMinted(owner, fieldSignature)) {
          return false;
        }
        Field keyId = owner.getClass().getSuperclass()
            .getDeclaredField("keyId");
        keyId.setAccessible(true);
        concreteKeyIdBeforeModel = keyId.getInt(owner);
        keyId.setInt(owner, modeledKeyId);
        keyIdSymbolizeCalls++;
        symbolizedKeyIdOwner = owner;
        MINTED_PAIRS.add(new Object[]{owner, fieldSignature});
        return true;
      }
      if (fieldSignature.endsWith(" long expiryDate>")) {
        expiryInvocations++;
        lastExpiryOwner = owner;
        lastExpirySource = sourceId;
        lastExpiryFieldSignature = fieldSignature;
        // The VM keeps a propagated expression, so a repeat is a no-op that
        // still reports success to the application hook.
        if (alreadyMinted(owner, fieldSignature)) {
          return true;
        }
        if (modeledExpiry == null) {
          return false;
        }
        Field expiry = owner.getClass().getSuperclass()
            .getDeclaredField("expiryDate");
        expiry.setAccessible(true);
        expiry.setLong(owner, modeledExpiry);
        expirySymbolizeCalls++;
        EXPIRY_OWNERS.add(owner);
        MINTED_PAIRS.add(new Object[]{owner, fieldSignature});
        return true;
      }
      return false;
    } catch (ReflectiveOperationException failure) {
      return false;
    }
  }

  private static boolean alreadyMinted(Object owner, String fieldSignature) {
    for (Object[] pair : MINTED_PAIRS) {
      if (pair[0] == owner && fieldSignature.equals(pair[1])) {
        return true;
      }
    }
    return false;
  }

  private static boolean containsExpiry(ExportedBlockKeys keys,
      long expiryDate) {
    for (BlockKey key : keys.getAllKeys()) {
      if (key != null && key.getExpiryDate() == expiryDate) {
        return true;
      }
    }
    return keys.getCurrentKey() != null
        && keys.getCurrentKey().getExpiryDate() == expiryDate;
  }

  private static boolean containsId(ExportedBlockKeys keys, int keyId) {
    for (BlockKey key : keys.getAllKeys()) {
      if (key != null && key.getKeyId() == keyId) {
        return true;
      }
    }
    return keys.getCurrentKey() != null
        && keys.getCurrentKey().getKeyId() == keyId;
  }

  private static boolean mintedExpiryOn(Object owner) {
    for (Object seen : EXPIRY_OWNERS) {
      if (seen == owner) {
        return true;
      }
    }
    return false;
  }

  private static void addDistinct(List<Object> distinct, Object candidate) {
    for (Object known : distinct) {
      if (known == candidate) {
        return;
      }
    }
    distinct.add(candidate);
  }

  private static int distinctExpiryOwners() {
    List<Object> distinct = new ArrayList<>();
    for (Object seen : EXPIRY_OWNERS) {
      addDistinct(distinct, seen);
    }
    return distinct.size();
  }

  private static BlockTokenSecretManager master() {
    return new BlockTokenSecretManager(1000L, 1000L, 0, 1, "bp", null,
        false);
  }

  private static BlockTokenSecretManager worker() {
    return new BlockTokenSecretManager(1000L, 1000L, "bp", null, false);
  }

  private static BlockKey keyField(BlockTokenSecretManager manager,
      String fieldName) throws Exception {
    Field field = BlockTokenSecretManager.class.getDeclaredField(fieldName);
    field.setAccessible(true);
    return (BlockKey) field.get(manager);
  }

  private static ExportedBlockKeys roundTrip(ExportedBlockKeys keys)
      throws IOException {
    DataOutputBuffer output = new DataOutputBuffer();
    keys.write(output);
    DataInputBuffer input = new DataInputBuffer();
    input.reset(output.getData(), output.getLength());
    ExportedBlockKeys copy = new ExportedBlockKeys();
    copy.readFields(input);
    return copy;
  }

  @SuppressWarnings("unchecked")
  private static Map<Integer, BlockKey> allKeys(
      BlockTokenSecretManager manager) throws Exception {
    Field field = BlockTokenSecretManager.class.getDeclaredField("allKeys");
    field.setAccessible(true);
    return (Map<Integer, BlockKey>) field.get(manager);
  }

  private static void clearObservations() {
    modeledKeyId = null;
    modeledExpiry = null;
    symbolizedKeyIdOwner = null;
    concreteKeyIdBeforeModel = 0;
    keyIdSymbolizeCalls = 0;
    expirySymbolizeCalls = 0;
    expiryInvocations = 0;
    lastExpiryOwner = null;
    lastExpirySource = null;
    lastExpiryFieldSignature = null;
    EXPIRY_OWNERS.clear();
    MINTED_PAIRS.clear();
  }

  private static void installTestBridge() throws Exception {
    Method method = TestBlockTokenSecretManagerCausynthSource.class
        .getDeclaredMethod("symbolizeForTest", String.class, Object.class,
            String.class);
    Field field = CausynthSymbolicSource.class.getDeclaredField("symbolizer");
    field.setAccessible(true);
    field.set(null, method);
    Field resolved = CausynthSymbolicSource.class.getDeclaredField("resolved");
    resolved.setAccessible(true);
    resolved.setBoolean(null, true);
  }

  private static void resetBridge() throws Exception {
    Field field = CausynthSymbolicSource.class.getDeclaredField("symbolizer");
    field.setAccessible(true);
    field.set(null, null);
    Field resolved = CausynthSymbolicSource.class.getDeclaredField("resolved");
    resolved.setAccessible(true);
    resolved.setBoolean(null, true);
  }
}
