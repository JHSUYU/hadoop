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
package org.apache.hadoop.security.token.delegation;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.ipc.CausynthSymbolicSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class TestDelegationKeyCausynthSource {
  private static final String EXPIRY_SIGNATURE =
      "<org.apache.hadoop.security.token.delegation.DelegationKey: "
          + "long expiryDate>";

  private static volatile Integer modeledKeyId;
  private static volatile Long modeledExpiryDate;
  private static volatile boolean runtimeResult;
  private static volatile int calls;
  private static volatile String lastSourceId;
  private static volatile String lastFieldSignature;
  private static volatile Object lastOwner;

  @Before
  public void resetBridge() throws Exception {
    resetCausynthSymbolizer();
    modeledKeyId = null;
    modeledExpiryDate = null;
    runtimeResult = true;
    calls = 0;
    lastSourceId = null;
    lastFieldSignature = null;
    lastOwner = null;
    installTestSymbolizer();
  }

  @After
  public void clearBridge() throws Exception {
    modeledKeyId = null;
    modeledExpiryDate = null;
    resetCausynthSymbolizer();
  }

  @Test
  public void modeledKeyIdRekeysTheSameObjectInTheOwningMap() {
    DelegationKey key = new DelegationKey(7, 100L, (byte[]) null);
    Map<Integer, DelegationKey> keys = new HashMap<>();
    keys.put(7, key);
    modeledKeyId = 9;

    assertTrue(key.symbolizeCausynthHdfs11741CurrentKeyId(keys));
    assertEquals(1, calls);
    assertEquals(9, key.getKeyId());
    assertFalse(keys.containsKey(7));
    assertSame(key, keys.get(9));
  }

  @Test
  public void mismatchedMapOwnerRefusesBeforeCallingTheRuntime() {
    DelegationKey key = new DelegationKey(7, 100L, (byte[]) null);
    Map<Integer, DelegationKey> keys = new HashMap<>();
    keys.put(7, new DelegationKey(7, 100L, (byte[]) null));
    modeledKeyId = 9;

    assertFalse(key.symbolizeCausynthHdfs11741CurrentKeyId(keys));
    assertEquals(0, calls);
    assertEquals(7, key.getKeyId());
  }

  @Test
  public void occupiedModeledIdRestoresSilentlyWithoutARejectCall() {
    DelegationKey key = new DelegationKey(7, 100L, (byte[]) null);
    DelegationKey occupied = new DelegationKey(9, 100L, (byte[]) null);
    Map<Integer, DelegationKey> keys = new HashMap<>();
    keys.put(7, key);
    keys.put(9, occupied);
    modeledKeyId = 9;

    assertFalse(key.symbolizeCausynthHdfs11741CurrentKeyId(keys));
    assertEquals(7, key.getKeyId());
    assertSame(key, keys.get(7));
    assertSame(occupied, keys.get(9));
    assertEquals(1, calls);
  }

  @Test
  public void failedRuntimeRestoresAFieldMutatedBeforeFailure() {
    DelegationKey key = new DelegationKey(7, 100L, (byte[]) null);
    Map<Integer, DelegationKey> keys = new HashMap<>();
    keys.put(7, key);
    modeledKeyId = 9;
    runtimeResult = false;

    assertFalse(key.symbolizeCausynthHdfs11741CurrentKeyId(keys));
    assertEquals(7, key.getKeyId());
    assertSame(key, keys.get(7));
    assertFalse(keys.containsKey(9));
  }

  @Test
  public void expiryMintsOnThisAndDeclaresTheExactField() {
    DelegationKey key = new DelegationKey(7, 100L, (byte[]) null);
    DelegationKey other = new DelegationKey(8, 200L, (byte[]) null);
    modeledExpiryDate = 55L;

    assertTrue(key.symbolizeCausynthHdfs11741ExpiryDate());

    assertEquals(1, calls);
    assertEquals(55L, key.getExpiryDate());
    assertEquals(200L, other.getExpiryDate());
    assertSame(key, lastOwner);
    assertEquals("HDFS11741.KEY_EXPIRY", lastSourceId);
    assertEquals(EXPIRY_SIGNATURE, lastFieldSignature);
  }

  @Test
  public void everyOccurrenceOfTheFieldIsItsOwnMint() {
    DelegationKey first = new DelegationKey(7, 100L, (byte[]) null);
    DelegationKey second = new DelegationKey(8, 200L, (byte[]) null);
    modeledExpiryDate = 55L;

    assertTrue(first.symbolizeCausynthHdfs11741ExpiryDate());
    assertSame(first, lastOwner);
    assertTrue(second.symbolizeCausynthHdfs11741ExpiryDate());
    assertSame(second, lastOwner);

    assertEquals(2, calls);
    assertEquals(55L, first.getExpiryDate());
    assertEquals(55L, second.getExpiryDate());
  }

  @Test
  public void failedExpiryRuntimeRestoresTheConcreteField() {
    DelegationKey key = new DelegationKey(7, 100L, (byte[]) null);
    modeledExpiryDate = 55L;
    runtimeResult = false;

    assertFalse(key.symbolizeCausynthHdfs11741ExpiryDate());

    assertEquals(1, calls);
    assertEquals(100L, key.getExpiryDate());
  }

  @Test
  public void absentBridgeLeavesTheFieldConcreteAndFailsClosed()
      throws Exception {
    resetCausynthSymbolizer();
    DelegationKey key = new DelegationKey(7, 100L, (byte[]) null);

    assertFalse(key.symbolizeCausynthHdfs11741ExpiryDate());

    assertEquals(0, calls);
    assertEquals(100L, key.getExpiryDate());
  }

  @Test
  public void bridgeExposesNoSelectorOrRejectEntryPoint() {
    for (Method method : CausynthSymbolicSource.class.getDeclaredMethods()) {
      assertFalse("selector survives on the bridge: " + method,
          "selectedIntValue".equals(method.getName()));
      assertFalse("reject survives on the bridge: " + method,
          "reject".equals(method.getName()));
    }
    for (Field field : CausynthSymbolicSource.class.getDeclaredFields()) {
      assertFalse("selector field survives: " + field,
          "intSelector".equals(field.getName()));
      assertFalse("rejecter field survives: " + field,
          "rejecter".equals(field.getName()));
    }
    for (Method method : DelegationKey.class.getDeclaredMethods()) {
      assertFalse("selected-key expiry hook survives: " + method,
          method.getName().contains("NameNodeExpiryDate"));
      assertFalse("selected-key expiry hook survives: " + method,
          method.getName().contains("DataNodeExpiryDate"));
    }
  }

  public static boolean symbolizeForTest(String sourceId, Object owner,
      String fieldSignature) {
    calls++;
    lastSourceId = sourceId;
    lastOwner = owner;
    lastFieldSignature = fieldSignature;
    try {
      if (fieldSignature.endsWith(" int keyId>")) {
        if (modeledKeyId == null) {
          return false;
        }
        Field keyId = DelegationKey.class.getDeclaredField("keyId");
        keyId.setAccessible(true);
        keyId.setInt(owner, modeledKeyId);
      } else if (fieldSignature.endsWith(" long expiryDate>")) {
        if (modeledExpiryDate == null) {
          return false;
        }
        Field expiry = DelegationKey.class.getDeclaredField("expiryDate");
        expiry.setAccessible(true);
        expiry.setLong(owner, modeledExpiryDate);
      } else {
        return false;
      }
      return runtimeResult;
    } catch (ReflectiveOperationException failure) {
      return false;
    }
  }

  private static void installTestSymbolizer() throws Exception {
    Method method = TestDelegationKeyCausynthSource.class.getDeclaredMethod(
        "symbolizeForTest", String.class, Object.class, String.class);
    Field symbolizer = CausynthSymbolicSource.class.getDeclaredField(
        "symbolizer");
    symbolizer.setAccessible(true);
    symbolizer.set(null, method);
    Field resolved = CausynthSymbolicSource.class.getDeclaredField("resolved");
    resolved.setAccessible(true);
    resolved.setBoolean(null, true);
  }

  private static void resetCausynthSymbolizer() throws Exception {
    Field symbolizer = CausynthSymbolicSource.class.getDeclaredField(
        "symbolizer");
    symbolizer.setAccessible(true);
    symbolizer.set(null, null);
    Field resolved = CausynthSymbolicSource.class.getDeclaredField("resolved");
    resolved.setAccessible(true);
    resolved.setBoolean(null, true);
  }
}
