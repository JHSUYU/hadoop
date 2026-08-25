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
  private static volatile Integer modeledKeyId;
  private static volatile Integer selectedKeyId;
  private static volatile Long modeledExpiry;
  private static volatile Object symbolizedKeyIdOwner;
  private static volatile int concreteKeyIdBeforeModel;
  private static volatile int keyIdSymbolizeCalls;
  private static volatile Object symbolizedOwner;
  private static volatile String symbolizedSource;
  private static volatile int symbolizeCalls;
  private static volatile int rejectCalls;

  @Before
  public void installBridge() throws Exception {
    resetBridge();
    installTestBridge();
    clearObservations();
  }

  @Test
  public void futureNextKeyDefinesKBeforeItsConsumersAndTarget()
      throws Exception {
    BlockTokenSecretManager master = master();
    Map<Integer, BlockKey> masterKeys = allKeys(master);
    int modeledK = 0;
    while (masterKeys.containsKey(modeledK)) {
      modeledK++;
    }

    // Rotation N creates the exact future key and publishes its keyId only
    // after allKeys owns that same object.
    modeledKeyId = modeledK;
    assertTrue(master.updateKeys());
    BlockKey futureKey = keyField(master, "nextKey");
    assertEquals(1, keyIdSymbolizeCalls);
    assertSame(futureKey, symbolizedKeyIdOwner);
    assertEquals(modeledK, futureKey.getKeyId());
    assertSame(futureKey, masterKeys.get(modeledK));
    assertFalse(masterKeys.containsKey(concreteKeyIdBeforeModel));

    // Export while K is still nextKey, and deserialize it so the worker owns
    // a fresh object.  The lookup below is by the exact K map entry, not by
    // scanning for an equal field value or a BlockKey runtime type.
    ExportedBlockKeys beforePromotion = roundTrip(master.exportKeys());
    BlockTokenSecretManager worker = new BlockTokenSecretManager(
        1000L, 1000L, "bp", null, false);
    worker.addKeys(beforePromotion);
    BlockKey workerFutureKey = allKeys(worker).get(modeledK);
    assertNotNull(workerFutureKey);
    assertNotSame(futureKey, workerFutureKey);

    // Rotation N+1 consumes the already-defined K for the exact NN member,
    // then promotes it to currentKey.  Disable a new keyId model so this test
    // continues to follow K rather than the following generation.
    modeledKeyId = null;
    selectedKeyId = modeledK;
    modeledExpiry = Long.MAX_VALUE;
    clearExpiryObservations();
    assertTrue(master.updateKeys());
    assertSame(futureKey, symbolizedOwner);
    assertEquals("HDFS11741.NAMENODE.SELECTED_KEY_EXPIRY",
        symbolizedSource);
    assertEquals(modeledK, keyField(master, "currentKey").getKeyId());

    // A wire-equivalent fresh export carries the promoted K.  DN addKeys
    // consults its pre-existing exact K member before installing this copy,
    // and the same K reaches the target lookup successfully.
    ExportedBlockKeys promoted = roundTrip(master.exportKeys());
    assertEquals(modeledK, promoted.getCurrentKey().getKeyId());
    clearExpiryObservations();
    worker.addKeys(promoted);
    assertSame(workerFutureKey, symbolizedOwner);
    assertEquals("HDFS11741.DATANODE.SELECTED_KEY_EXPIRY",
        symbolizedSource);
    assertNotNull(worker.retrieveDataEncryptionKey(
        modeledK, new byte[]{1, 2, 3, 4}));
    assertEquals(0, rejectCalls);
  }

  @After
  public void removeBridge() throws Exception {
    resetBridge();
  }

  @Test
  public void nameNodeExpiryHookUsesOnlyTheMapMemberSelectedByK()
      throws Exception {
    BlockTokenSecretManager manager = master();
    ExportedBlockKeys exported = manager.exportKeys();
    BlockKey selected = exported.getCurrentKey();
    BlockKey other = anotherKey(exported, selected.getKeyId());
    long otherExpiry = other.getExpiryDate();
    clearObservations();
    selectedKeyId = selected.getKeyId();
    modeledExpiry = Long.MIN_VALUE;

    assertTrue(manager.updateKeys());

    assertEquals(1, symbolizeCalls);
    assertSame(selected, symbolizedOwner);
    assertEquals("HDFS11741.NAMENODE.SELECTED_KEY_EXPIRY",
        symbolizedSource);
    assertEquals(otherExpiry, other.getExpiryDate());
    assertEquals(0, rejectCalls);
  }

  @Test
  public void dataNodeExpiryHookUsesTheSameExactKLookup() throws Exception {
    BlockTokenSecretManager master = master();
    ExportedBlockKeys exported = master.exportKeys();
    BlockTokenSecretManager worker = new BlockTokenSecretManager(
        1000L, 1000L, "bp", null, false);
    worker.addKeys(exported);
    BlockKey selected = exported.getCurrentKey();
    clearObservations();
    selectedKeyId = selected.getKeyId();
    modeledExpiry = Long.MIN_VALUE;

    worker.addKeys(exported);

    assertEquals(1, symbolizeCalls);
    assertSame(selected, symbolizedOwner);
    assertEquals("HDFS11741.DATANODE.SELECTED_KEY_EXPIRY",
        symbolizedSource);
    assertEquals(0, rejectCalls);
  }

  @Test
  public void absentSelectedKRejectsWithoutTryingAnotherElement()
      throws Exception {
    BlockTokenSecretManager manager = master();
    Map<Integer, BlockKey> allKeys = allKeys(manager);
    int absent = 0;
    while (allKeys.containsKey(absent)) {
      absent++;
    }
    clearObservations();
    selectedKeyId = absent;
    modeledExpiry = Long.MIN_VALUE;

    assertTrue(manager.updateKeys());

    assertEquals(0, symbolizeCalls);
    assertEquals(1, rejectCalls);
    assertNull(symbolizedOwner);
  }

  @Test
  public void inconsistentMapEntryRejectsInsteadOfGuessingByType()
      throws Exception {
    BlockTokenSecretManager manager = master();
    Map<Integer, BlockKey> allKeys = allKeys(manager);
    int selected = 0;
    while (allKeys.containsKey(selected)
        || allKeys.containsKey(selected + 1)) {
      selected += 2;
    }
    allKeys.put(selected, new BlockKey(selected + 1, Long.MAX_VALUE,
        (byte[]) null));
    clearObservations();
    selectedKeyId = selected;
    modeledExpiry = Long.MIN_VALUE;

    assertTrue(manager.updateKeys());

    assertEquals(0, symbolizeCalls);
    assertEquals(1, rejectCalls);
  }

  public static boolean symbolizeForTest(String sourceId, Object owner,
      String fieldSignature) {
    try {
      if (fieldSignature.endsWith(" int keyId>")) {
        if (modeledKeyId == null) {
          return false;
        }
        Field keyId = owner.getClass().getSuperclass()
            .getDeclaredField("keyId");
        keyId.setAccessible(true);
        concreteKeyIdBeforeModel = keyId.getInt(owner);
        keyId.setInt(owner, modeledKeyId);
        keyIdSymbolizeCalls++;
        symbolizedKeyIdOwner = owner;
        return true;
      }
      if (fieldSignature.endsWith(" long expiryDate>")
          && modeledExpiry != null) {
        Field expiry = owner.getClass().getSuperclass()
            .getDeclaredField("expiryDate");
        expiry.setAccessible(true);
        expiry.setLong(owner, modeledExpiry);
        symbolizeCalls++;
        symbolizedOwner = owner;
        symbolizedSource = sourceId;
        return true;
      }
      return false;
    } catch (ReflectiveOperationException failure) {
      return false;
    }
  }

  public static Integer selectIntForTest(String sourceId) {
    return selectedKeyId;
  }

  public static void rejectForTest(String sourceId, String detail) {
    rejectCalls++;
  }

  private static BlockTokenSecretManager master() {
    return new BlockTokenSecretManager(1000L, 1000L, 0, 1, "bp", null,
        false);
  }

  private static BlockKey anotherKey(ExportedBlockKeys keys, int selected) {
    for (BlockKey key : keys.getAllKeys()) {
      if (key != null && key.getKeyId() != selected) {
        return key;
      }
    }
    throw new AssertionError("master exported no second key");
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
    selectedKeyId = null;
    modeledExpiry = null;
    symbolizedKeyIdOwner = null;
    concreteKeyIdBeforeModel = 0;
    keyIdSymbolizeCalls = 0;
    clearExpiryObservations();
    rejectCalls = 0;
  }

  private static void clearExpiryObservations() {
    symbolizedOwner = null;
    symbolizedSource = null;
    symbolizeCalls = 0;
  }

  private static void installTestBridge() throws Exception {
    setBridgeMethod("symbolizer", "symbolizeForTest", String.class,
        Object.class, String.class);
    setBridgeMethod("rejecter", "rejectForTest", String.class,
        String.class);
    setBridgeMethod("intSelector", "selectIntForTest", String.class);
    Field resolved = CausynthSymbolicSource.class.getDeclaredField("resolved");
    resolved.setAccessible(true);
    resolved.setBoolean(null, true);
  }

  private static void setBridgeMethod(String fieldName, String methodName,
      Class<?>... parameters) throws Exception {
    Method method = TestBlockTokenSecretManagerCausynthSource.class
        .getDeclaredMethod(methodName, parameters);
    Field field = CausynthSymbolicSource.class.getDeclaredField(fieldName);
    field.setAccessible(true);
    field.set(null, method);
  }

  private static void resetBridge() throws Exception {
    for (String fieldName : new String[]{"symbolizer", "rejecter",
        "intSelector"}) {
      Field field = CausynthSymbolicSource.class.getDeclaredField(fieldName);
      field.setAccessible(true);
      field.set(null, null);
    }
    Field resolved = CausynthSymbolicSource.class.getDeclaredField("resolved");
    resolved.setAccessible(true);
    resolved.setBoolean(null, false);
  }
}
