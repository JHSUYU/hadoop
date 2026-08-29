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
  /** Existing propagated expiry expressions resumed before a later scan. */
  private static volatile int expiryResumeSuccesses;
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
  public void rotationMintsOnlyTheNameNodeRetiringKeyExpiry()
      throws Exception {
    BlockTokenSecretManager master = master();
    Map<Integer, BlockKey> allKeys = allKeys(master);
    BlockKey oldCurrent = keyField(master, "currentKey");
    int retiringId = oldCurrent.getKeyId();
    clearObservations();
    modeledExpiry = 4242424242L;

    assertTrue(master.updateKeys());

    BlockKey retiringKey = allKeys.get(retiringId);
    assertNotNull(retiringKey);
    assertNotSame(oldCurrent, retiringKey);
    assertEquals(4242424242L, retiringKey.getExpiryDate());
    assertEquals(1, expiryInvocations);
    assertEquals(1, expirySymbolizeCalls);
    assertEquals(1, distinctExpiryOwners());
    assertSame(retiringKey, lastExpiryOwner);
    assertEquals("HDFS11741.NAMENODE.KEY_EXPIRY", lastExpirySource);
    assertEquals(EXPIRY_SIGNATURE, lastExpiryFieldSignature);
    assertTrue(mintedExpiryOn(retiringKey));
    assertEquals(0, keyIdSymbolizeCalls);
  }

  @Test
  public void nextRotationReusesOnlyTheExistingRetiringExpiryBeforeRemoval()
      throws Exception {
    BlockTokenSecretManager master = master();
    Map<Integer, BlockKey> allKeys = allKeys(master);
    int firstRetiringId = keyField(master, "currentKey").getKeyId();
    modeledExpiry = Long.MAX_VALUE;

    assertTrue(master.updateKeys());
    BlockKey firstRetiring = allKeys.get(firstRetiringId);
    assertNotNull(firstRetiring);

    // The second rotation models the already-carried E as expired. Its hook
    // must run before removeExpiredKeys, while currentKey and nextKey remain
    // unminted; the rotation then creates exactly one new NN expiry root.
    modeledExpiry = Long.MIN_VALUE;
    assertTrue(master.updateKeys());

    assertFalse(allKeys.containsKey(firstRetiringId));
    assertEquals(2, expiryInvocations);
    assertEquals(2, expirySymbolizeCalls);
    assertEquals(2, distinctExpiryOwners());
    assertEquals(1, expiryResumeSuccesses);
    assertTrue(mintedExpiryOn(firstRetiring));
    assertEquals(0, keyIdSymbolizeCalls);
  }

  @Test
  public void workerInstallsNameNodeExpiryWithoutMintingALocalRoot()
      throws Exception {
    BlockTokenSecretManager master = master();
    int retiringId = keyField(master, "currentKey").getKeyId();
    modeledExpiry = 4242424242L;
    assertTrue(master.updateKeys());
    ExportedBlockKeys exported = roundTrip(master.exportKeys());
    BlockTokenSecretManager worker = worker();
    Map<Integer, BlockKey> workerKeys = allKeys(worker);
    clearObservations();

    worker.addKeys(exported);

    assertTrue("worker installed no keys", workerKeys.size() >= 2);
    assertEquals(0, expiryInvocations);
    assertEquals(0, expirySymbolizeCalls);
    BlockKey installed = workerKeys.get(retiringId);
    assertNotNull(installed);
    assertEquals(4242424242L, installed.getExpiryDate());
  }

  /**
   * H1 installs K with the NameNode-owned expiry. H2 omits K, so addKeys must
   * remove the old local entry using that propagated expiry before folding the
   * received array. Missing K from H2 alone is not a map deletion.
   */
  @Test
  public void secondHeartbeatRemovesKUsingTheNameNodeExpiry() throws Exception {
    BlockTokenSecretManager master = master();
    ExportedBlockKeys beforeRotation = roundTrip(master.exportKeys());
    int k = beforeRotation.getCurrentKey().getKeyId();
    BlockTokenSecretManager worker = worker();
    Map<Integer, BlockKey> workerKeys = allKeys(worker);
    worker.addKeys(beforeRotation);
    assertTrue("pre-rotation export did not install K",
        workerKeys.containsKey(k));

    clearObservations();
    modeledExpiry = Long.MIN_VALUE;
    assertTrue(master.updateKeys());
    ExportedBlockKeys h1 = roundTrip(master.exportKeys());
    clearObservations();
    worker.addKeys(h1);
    assertTrue("H1 did not install K", workerKeys.containsKey(k));
    assertEquals(Long.MIN_VALUE, workerKeys.get(k).getExpiryDate());

    // R2 removes the expired retiring version at the NameNode.  H2 then
    // omits K; the worker consumes the already propagated E_ret before it
    // folds that new array.
    assertTrue(master.updateKeys());
    ExportedBlockKeys h2 = roundTrip(master.exportKeys());
    assertFalse(containsId(h2, k));
    clearObservations();

    worker.addKeys(h2);

    assertFalse("H2 left the expired propagated K installed",
        workerKeys.containsKey(k));
    assertEquals(0, expiryInvocations);
    assertEquals(0, expirySymbolizeCalls);
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
    assertEquals(0, expiryInvocations);
    assertEquals(0, expirySymbolizeCalls);

    // A wire round trip gives the worker fresh objects, but their expressions
    // still originate at the NameNode; worker addKeys must not mint a root.
    ExportedBlockKeys wire = roundTrip(exported);
    BlockTokenSecretManager worker = worker();
    Map<Integer, BlockKey> workerKeys = allKeys(worker);
    clearObservations();
    worker.addKeys(wire);

    BlockKey workerExportedKey = workerKeys.get(modeledK);
    assertNotNull(workerExportedKey);
    assertNotSame(exportedKey, workerExportedKey);
    assertEquals(wire.getCurrentKey().getExpiryDate(),
        workerExportedKey.getExpiryDate());
    assertEquals(0, expiryInvocations);
    assertEquals(0, expirySymbolizeCalls);
    assertNotNull(worker.retrieveDataEncryptionKey(
        modeledK, new byte[]{1, 2, 3, 4}));
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
        // The VM keeps a propagated expression, while the replay assignment
        // may give that same root a new concrete model in this activation.
        if (alreadyMinted(owner, fieldSignature)) {
          if (modeledExpiry != null) {
            Field expiry = owner.getClass().getSuperclass()
                .getDeclaredField("expiryDate");
            expiry.setAccessible(true);
            expiry.setLong(owner, modeledExpiry);
          }
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

  public static boolean resumeForTest(Object owner, String fieldSignature) {
    try {
      if (!fieldSignature.endsWith(" long expiryDate>")
          || !alreadyMinted(owner, fieldSignature)) {
        return false;
      }
      if (modeledExpiry != null) {
        Field expiry = owner.getClass().getSuperclass()
            .getDeclaredField("expiryDate");
        expiry.setAccessible(true);
        expiry.setLong(owner, modeledExpiry);
      }
      expiryResumeSuccesses++;
      return true;
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
    expiryResumeSuccesses = 0;
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
    Method resume = TestBlockTokenSecretManagerCausynthSource.class
        .getDeclaredMethod("resumeForTest", Object.class, String.class);
    Field resumer = CausynthSymbolicSource.class.getDeclaredField("resumer");
    resumer.setAccessible(true);
    resumer.set(null, resume);
    Field resolved = CausynthSymbolicSource.class.getDeclaredField("resolved");
    resolved.setAccessible(true);
    resolved.setBoolean(null, true);
  }

  private static void resetBridge() throws Exception {
    Field field = CausynthSymbolicSource.class.getDeclaredField("symbolizer");
    field.setAccessible(true);
    field.set(null, null);
    Field resumer = CausynthSymbolicSource.class.getDeclaredField("resumer");
    resumer.setAccessible(true);
    resumer.set(null, null);
    Field resolved = CausynthSymbolicSource.class.getDeclaredField("resolved");
    resolved.setAccessible(true);
    resolved.setBoolean(null, true);
  }
}
