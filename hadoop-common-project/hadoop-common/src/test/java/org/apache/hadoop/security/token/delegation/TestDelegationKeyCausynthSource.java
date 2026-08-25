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
  private static volatile Integer modeledKeyId;
  private static volatile Long modeledExpiryDate;
  private static volatile Integer selectedIntValue;
  private static volatile boolean runtimeResult;
  private static volatile int calls;
  private static volatile int rejectCalls;
  private static volatile String lastSourceId;
  private static volatile String lastFieldSignature;
  private static volatile Object lastOwner;

  @Before
  public void resetBridge() throws Exception {
    resetCausynthSymbolizer();
    modeledKeyId = null;
    modeledExpiryDate = null;
    selectedIntValue = null;
    runtimeResult = true;
    calls = 0;
    rejectCalls = 0;
    lastSourceId = null;
    lastFieldSignature = null;
    lastOwner = null;
    installTestSymbolizer();
  }

  @After
  public void clearBridge() throws Exception {
    modeledKeyId = null;
    modeledExpiryDate = null;
    selectedIntValue = null;
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
  public void occupiedModeledIdRefusesWithoutOverwritingEitherEntry() {
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
    assertEquals(1, rejectCalls);
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
  public void nameNodeExpiryUsesTheExactInstanceAndDeclaredSource() {
    DelegationKey selected = new DelegationKey(7, 100L, (byte[]) null);
    DelegationKey other = new DelegationKey(8, 200L, (byte[]) null);
    modeledExpiryDate = 55L;

    assertTrue(selected.symbolizeCausynthHdfs11741NameNodeExpiryDate());
    assertEquals(55L, selected.getExpiryDate());
    assertEquals(200L, other.getExpiryDate());
    assertSame(selected, lastOwner);
    assertEquals("HDFS11741.NAMENODE.SELECTED_KEY_EXPIRY", lastSourceId);
    assertEquals(
        "<org.apache.hadoop.security.token.delegation.DelegationKey: long expiryDate>",
        lastFieldSignature);
  }

  @Test
  public void dataNodeExpiryHasAnIndependentSourceIdentity() {
    DelegationKey selected = new DelegationKey(7, 100L, (byte[]) null);
    modeledExpiryDate = 66L;

    assertTrue(selected.symbolizeCausynthHdfs11741DataNodeExpiryDate());
    assertEquals(66L, selected.getExpiryDate());
    assertEquals("HDFS11741.DATANODE.SELECTED_KEY_EXPIRY", lastSourceId);
  }

  @Test
  public void failedExpiryRuntimeRestoresTheConcreteField() {
    DelegationKey selected = new DelegationKey(7, 100L, (byte[]) null);
    modeledExpiryDate = 55L;
    runtimeResult = false;

    assertFalse(selected.symbolizeCausynthHdfs11741NameNodeExpiryDate());
    assertEquals(100L, selected.getExpiryDate());
  }

  @Test
  public void selectorOnlyBridgeReturnsExactIntegerOrNull() {
    selectedIntValue = 17;
    assertEquals(Integer.valueOf(17), CausynthSymbolicSource.selectedIntValue(
        "HDFS11741.NAMENODE.CURRENT_KEY_ID"));
    selectedIntValue = null;
    assertEquals(null, CausynthSymbolicSource.selectedIntValue(
        "HDFS11741.NAMENODE.CURRENT_KEY_ID"));
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

  public static Integer selectIntForTest(String sourceId) {
    return selectedIntValue;
  }

  public static void rejectForTest(String sourceId, String detail) {
    rejectCalls++;
  }

  private static void installTestSymbolizer() throws Exception {
    Method method = TestDelegationKeyCausynthSource.class.getDeclaredMethod(
        "symbolizeForTest", String.class, Object.class, String.class);
    Field symbolizer = CausynthSymbolicSource.class.getDeclaredField(
        "symbolizer");
    symbolizer.setAccessible(true);
    symbolizer.set(null, method);
    Method reject = TestDelegationKeyCausynthSource.class.getDeclaredMethod(
        "rejectForTest", String.class, String.class);
    Field rejecter = CausynthSymbolicSource.class.getDeclaredField("rejecter");
    rejecter.setAccessible(true);
    rejecter.set(null, reject);
    Method select = TestDelegationKeyCausynthSource.class.getDeclaredMethod(
        "selectIntForTest", String.class);
    Field selector = CausynthSymbolicSource.class.getDeclaredField(
        "intSelector");
    selector.setAccessible(true);
    selector.set(null, select);
    Field resolved = CausynthSymbolicSource.class.getDeclaredField("resolved");
    resolved.setAccessible(true);
    resolved.setBoolean(null, true);
  }

  private static void resetCausynthSymbolizer() throws Exception {
    Field resolved = CausynthSymbolicSource.class.getDeclaredField("resolved");
    resolved.setAccessible(true);
    resolved.setBoolean(null, false);
    Field symbolizer = CausynthSymbolicSource.class.getDeclaredField(
        "symbolizer");
    symbolizer.setAccessible(true);
    symbolizer.set(null, null);
    Field rejecter = CausynthSymbolicSource.class.getDeclaredField("rejecter");
    rejecter.setAccessible(true);
    rejecter.set(null, null);
    Field selector = CausynthSymbolicSource.class.getDeclaredField(
        "intSelector");
    selector.setAccessible(true);
    selector.set(null, null);
  }
}
