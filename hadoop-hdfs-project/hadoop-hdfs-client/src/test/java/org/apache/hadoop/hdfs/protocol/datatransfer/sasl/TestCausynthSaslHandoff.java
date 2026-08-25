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
package org.apache.hadoop.hdfs.protocol.datatransfer.sasl;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hdfs.net.BasicInetPeer;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.apache.hadoop.ipc.CausynthSymbolicHandoff;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Focused coverage for the single-JVM encrypted-SASL manual carrier. */
public class TestCausynthSaslHandoff {
  private static final String TRACE_PROPERTY = "causynth.trace.output.dir";
  private static final String RESULTS_PROPERTY = "causynth.concolic.results";

  @Before
  public void enableBridge() throws Exception {
    System.setProperty(TRACE_PROPERTY, "target/causynth-sasl-trace");
    System.setProperty(RESULTS_PROPERTY, "target/causynth-sasl-results");
    TestRuntime.reset();
    installTestBinding();
  }

  @After
  public void clearBridge() throws Exception {
    CausynthSaslHandoff.leaveConnection();
    System.clearProperty(TRACE_PROPERTY);
    System.clearProperty(RESULTS_PROPERTY);
    TestRuntime.reset();
    setBinding(null);
  }

  @Test
  public void exactSenderHandlesAttachToFreshParsedFields() throws Exception {
    try (Connection connection = new Connection()) {
      DataEncryptionKey key = key();
      Object sender = new Object();
      String userName = "17 bp " +
          new String(org.apache.commons.codec.binary.Base64.encodeBase64(
              key.nonce, false), com.google.common.base.Charsets.UTF_8);

      CausynthSaslHandoff.enterClientConnection(connection.client);
      CausynthSaslHandoff.Attempt attempt =
          CausynthSaslHandoff.exportEncryptionKey(sender, key);
      attempt.registerUserName(userName);
      CausynthSaslHandoff.sendHandshake();

      List<Call> exports = TestRuntime.exports();
      assertEquals(3, exports.size());
      assertSame(key, exports.get(0).owner);
      assertCall(exports.get(0),
          "org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey",
          "keyId", "I", 101L);
      assertCall(exports.get(1),
          "org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey",
          "blockPoolId", "Ljava/lang/String;", 102L);
      assertCall(exports.get(2),
          "org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey",
          "nonce", "[B", 103L);

      Peer serverPeer = connection.serverPeer();
      Object receiver = new Object();
      CausynthSaslHandoff.enterServerConnection(serverPeer);
      CausynthSaslHandoff.deliverUserName(receiver, userName);
      CausynthSaslHandoff.Parsed parsed =
          CausynthSaslHandoff.receiveUserName(receiver, userName,
              key.keyId, key.blockPoolId, key.nonce.clone());

      assertEquals(key.keyId, parsed.keyId);
      assertEquals(key.blockPoolId, parsed.blockPoolId);
      assertArrayEquals(key.nonce, parsed.nonce);
      List<Call> receives = TestRuntime.receives();
      assertEquals(3, receives.size());
      assertSame(parsed, receives.get(0).owner);
      assertCall(receives.get(0), CausynthSaslHandoff.Parsed.class.getName(),
          "keyId", "I", 101L);
      assertCall(receives.get(1), CausynthSaslHandoff.Parsed.class.getName(),
          "blockPoolId", "Ljava/lang/String;", 102L);
      assertCall(receives.get(2), CausynthSaslHandoff.Parsed.class.getName(),
          "nonce", "[B", 103L);
      assertEquals(0, pendingConnectionCount());
    }
  }

  @Test
  public void connectionExitRetractsAnAttemptThatWasNeverSent()
      throws Exception {
    try (Connection connection = new Connection()) {
      DataEncryptionKey key = key();
      String userName = "17 bp nonce";
      CausynthSaslHandoff.enterClientConnection(connection.client);
      CausynthSaslHandoff.Attempt attempt =
          CausynthSaslHandoff.exportEncryptionKey(this, key);
      attempt.registerUserName(userName);
      CausynthSaslHandoff.leaveConnection();

      CausynthSaslHandoff.enterServerConnection(connection.serverPeer());
      CausynthSaslHandoff.deliverUserName(this, userName);
      CausynthSaslHandoff.receiveUserName(this, userName, key.keyId,
          key.blockPoolId, key.nonce.clone());

      assertTrue(TestRuntime.receives().isEmpty());
      assertEquals(0, pendingConnectionCount());
    }
  }

  @Test
  public void connectionExitRetractsAnAttemptAfterSendFailure()
      throws Exception {
    try (Connection connection = new Connection()) {
      DataEncryptionKey key = key();
      String userName = userName(key);
      CausynthSaslHandoff.enterClientConnection(connection.client);
      CausynthSaslHandoff.Attempt attempt =
          CausynthSaslHandoff.exportEncryptionKey(this, key);
      attempt.registerUserName(userName);
      CausynthSaslHandoff.sendHandshake();

      // Models the client finally block after the wire write throws.
      CausynthSaslHandoff.leaveConnection();

      CausynthSaslHandoff.enterServerConnection(connection.serverPeer());
      CausynthSaslHandoff.deliverUserName(this, userName);
      CausynthSaslHandoff.receiveUserName(this, userName, key.keyId,
          key.blockPoolId, key.nonce.clone());

      assertTrue(TestRuntime.receives().isEmpty());
      assertEquals(0, pendingConnectionCount());
    }
  }

  @Test
  public void reusedConnectionAttachesOnlyTheNewAttempt() throws Exception {
    try (Connection connection = new Connection()) {
      DataEncryptionKey first = key();
      String firstUserName = userName(first);
      CausynthSaslHandoff.enterClientConnection(connection.client);
      CausynthSaslHandoff.Attempt firstAttempt =
          CausynthSaslHandoff.exportEncryptionKey(this, first);
      firstAttempt.registerUserName(firstUserName);
      CausynthSaslHandoff.sendHandshake();
      CausynthSaslHandoff.leaveConnection();

      DataEncryptionKey second = new DataEncryptionKey(18, "bp2",
          new byte[]{7, 8, 9}, new byte[]{10, 11, 12}, Long.MAX_VALUE, "AES");
      String secondUserName = userName(second);
      CausynthSaslHandoff.enterClientConnection(connection.client);
      CausynthSaslHandoff.Attempt secondAttempt =
          CausynthSaslHandoff.exportEncryptionKey(this, second);
      secondAttempt.registerUserName(secondUserName);
      CausynthSaslHandoff.sendHandshake();

      CausynthSaslHandoff.enterServerConnection(connection.serverPeer());
      CausynthSaslHandoff.deliverUserName(this, secondUserName);
      CausynthSaslHandoff.receiveUserName(this, secondUserName, second.keyId,
          second.blockPoolId, second.nonce.clone());

      List<Call> receives = TestRuntime.receives();
      assertEquals(3, receives.size());
      assertEquals(104L, receives.get(0).handle);
      assertEquals(105L, receives.get(1).handle);
      assertEquals(106L, receives.get(2).handle);
      assertEquals(0, pendingConnectionCount());
    }
  }

  @Test
  public void duplicateLiveConnectionFailsClosed() throws Exception {
    try (Connection connection = new Connection()) {
      DataEncryptionKey first = key();
      String firstUserName = userName(first);
      CausynthSaslHandoff.enterClientConnection(connection.client);
      CausynthSaslHandoff.Attempt firstAttempt =
          CausynthSaslHandoff.exportEncryptionKey(this, first);
      firstAttempt.registerUserName(firstUserName);
      CausynthSaslHandoff.sendHandshake();

      DataEncryptionKey second = new DataEncryptionKey(18, "bp2",
          new byte[]{7, 8, 9}, new byte[]{10, 11, 12}, Long.MAX_VALUE, "AES");
      String secondUserName = userName(second);
      CausynthSaslHandoff.Attempt secondAttempt =
          CausynthSaslHandoff.exportEncryptionKey(this, second);
      secondAttempt.registerUserName(secondUserName);
      CausynthSaslHandoff.sendHandshake();

      assertEquals(0, pendingConnectionCount());
      CausynthSaslHandoff.enterServerConnection(connection.serverPeer());
      CausynthSaslHandoff.deliverUserName(this, firstUserName);
      CausynthSaslHandoff.receiveUserName(this, firstUserName, first.keyId,
          first.blockPoolId, first.nonce.clone());
      CausynthSaslHandoff.deliverUserName(this, secondUserName);
      CausynthSaslHandoff.receiveUserName(this, secondUserName, second.keyId,
          second.blockPoolId, second.nonce.clone());

      assertTrue(TestRuntime.receives().isEmpty());
      assertEquals(0, pendingConnectionCount());
    }
  }

  @Test
  public void mismatchedUserNameCannotClaimOrConsumeAttempt()
      throws Exception {
    try (Connection connection = new Connection()) {
      DataEncryptionKey key = key();
      String userName = userName(key);
      String mismatched = "999 wrong bm9uY2U=";
      CausynthSaslHandoff.enterClientConnection(connection.client);
      CausynthSaslHandoff.Attempt attempt =
          CausynthSaslHandoff.exportEncryptionKey(this, key);
      attempt.registerUserName(userName);
      CausynthSaslHandoff.sendHandshake();

      CausynthSaslHandoff.enterServerConnection(connection.serverPeer());
      CausynthSaslHandoff.deliverUserName(this, mismatched);
      CausynthSaslHandoff.receiveUserName(this, mismatched, 999, "wrong",
          new byte[]{1});
      assertTrue(TestRuntime.receives().isEmpty());
      assertEquals(1, pendingConnectionCount());

      // The mismatch did not consume the exact claim.  The real username may
      // claim it once, and a repeated delivery cannot attach it again.
      CausynthSaslHandoff.deliverUserName(this, userName);
      CausynthSaslHandoff.receiveUserName(this, userName, key.keyId,
          key.blockPoolId, key.nonce.clone());
      assertEquals(3, TestRuntime.receives().size());
      CausynthSaslHandoff.deliverUserName(this, userName);
      CausynthSaslHandoff.receiveUserName(this, userName, key.keyId,
          key.blockPoolId, key.nonce.clone());
      assertEquals(3, TestRuntime.receives().size());
      assertEquals(0, pendingConnectionCount());
    }
  }

  private static DataEncryptionKey key() {
    return new DataEncryptionKey(17, "bp", new byte[]{1, 2, 3},
        new byte[]{4, 5, 6}, Long.MAX_VALUE, "AES");
  }

  private static String userName(DataEncryptionKey key) {
    return key.keyId + " " + key.blockPoolId + " "
        + new String(org.apache.commons.codec.binary.Base64.encodeBase64(
            key.nonce, false), com.google.common.base.Charsets.UTF_8);
  }

  @SuppressWarnings("unchecked")
  private static int pendingConnectionCount() throws Exception {
    Field field = CausynthSaslHandoff.class.getDeclaredField(
        "PENDING_CONNECTIONS");
    field.setAccessible(true);
    Map<String, CausynthSaslHandoff.Attempt> pending =
        (Map<String, CausynthSaslHandoff.Attempt>) field.get(null);
    synchronized (pending) {
      return pending.size();
    }
  }

  private static void assertCall(Call call, String ownerClass, String field,
      String descriptor, long handle) {
    assertEquals(ownerClass, call.declaringClass);
    assertEquals(field, call.fieldName);
    assertEquals(descriptor, call.descriptor);
    assertEquals(handle, call.handle);
  }

  private static void installTestBinding() throws Exception {
    Class<?> bindingClass = Class.forName(
        CausynthSymbolicHandoff.class.getName() + "$Binding");
    Constructor<?> constructor = bindingClass.getDeclaredConstructor(
        Method.class, Method.class, Method.class, Method.class, Method.class,
        Method.class);
    constructor.setAccessible(true);
    Object binding = constructor.newInstance(
        TestRuntime.class.getMethod("exportSymbolicLeaf", Object.class,
            String.class, String.class, String.class),
        TestRuntime.class.getMethod("receiveSymbolicLeaf", Object.class,
            String.class, String.class, String.class, Long.TYPE),
        HandoffResult.class.getMethod("status"),
        HandoffResult.class.getMethod("handle"),
        HandoffResult.class.getMethod("reason"),
        TestRuntime.class.getMethod("completeResponseHandoff"));
    setBinding(binding);
  }

  private static void setBinding(Object value) throws Exception {
    Field binding = CausynthSymbolicHandoff.class.getDeclaredField("binding");
    binding.setAccessible(true);
    binding.set(null, value);
  }

  /** Test-only implementation of GraphChecker's reflection ABI. */
  public static final class TestRuntime {
    private static final List<Call> EXPORTS = new ArrayList<Call>();
    private static final List<Call> RECEIVES = new ArrayList<Call>();
    private static long nextHandle = 101L;

    private TestRuntime() {
    }

    public static HandoffResult exportSymbolicLeaf(Object owner,
        String declaringClass, String fieldName, String descriptor) {
      if (!"keyId".equals(fieldName) && !"blockPoolId".equals(fieldName)
          && !"nonce".equals(fieldName)) {
        return new HandoffResult(-1, 0L, "UNEXPECTED_FIELD");
      }
      long handle = nextHandle++;
      EXPORTS.add(new Call(owner, declaringClass, fieldName, descriptor,
          handle));
      return new HandoffResult(0, handle, "");
    }

    public static HandoffResult receiveSymbolicLeaf(Object owner,
        String declaringClass, String fieldName, String descriptor,
        long handle) {
      RECEIVES.add(new Call(owner, declaringClass, fieldName, descriptor,
          handle));
      return new HandoffResult(0, handle, "");
    }

    public static boolean completeResponseHandoff() {
      return true;
    }

    static void reset() {
      EXPORTS.clear();
      RECEIVES.clear();
      nextHandle = 101L;
    }

    static List<Call> exports() {
      return Collections.unmodifiableList(EXPORTS);
    }

    static List<Call> receives() {
      return Collections.unmodifiableList(RECEIVES);
    }
  }

  /** Reflection result shape used by the production bridge. */
  public static final class HandoffResult {
    private final int status;
    private final long handle;
    private final String reason;

    HandoffResult(int status, long handle, String reason) {
      this.status = status;
      this.handle = handle;
      this.reason = reason;
    }

    public int status() {
      return status;
    }

    public long handle() {
      return handle;
    }

    public String reason() {
      return reason;
    }
  }

  private static final class Call {
    private final Object owner;
    private final String declaringClass;
    private final String fieldName;
    private final String descriptor;
    private final long handle;

    private Call(Object owner, String declaringClass, String fieldName,
        String descriptor, long handle) {
      this.owner = owner;
      this.declaringClass = declaringClass;
      this.fieldName = fieldName;
      this.descriptor = descriptor;
      this.handle = handle;
    }
  }

  private static final class Connection implements AutoCloseable {
    private final ServerSocket listener;
    private final Socket client;
    private final Socket server;

    private Connection() throws Exception {
      listener = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
      client = new Socket(listener.getInetAddress(), listener.getLocalPort());
      server = listener.accept();
    }

    private Peer serverPeer() throws Exception {
      return new BasicInetPeer(server);
    }

    @Override
    public void close() throws Exception {
      server.close();
      client.close();
      listener.close();
    }
  }
}
