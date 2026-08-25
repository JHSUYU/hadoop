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

import java.net.Socket;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.apache.hadoop.ipc.CausynthRpcTrace;
import org.apache.hadoop.ipc.CausynthSymbolicHandoff;

/**
 * Single-JVM control plane for symbolic leaves encoded in a SASL user name.
 *
 * <p>The real SASL wire format remains the three existing concrete components.
 * MiniDFSCluster runs both endpoints in one JVM, so an opaque VM handle is
 * correlated by the concrete connection while the username is transmitted
 * normally.  The bounded map carries an exact username claim plus opaque
 * handles; Hadoop never dereferences a handle.  GraphChecker's VM validates
 * every attach.</p>
 */
@InterfaceAudience.Private
public final class CausynthSaslHandoff {
  public static final String TRANSPORT = "SASL_HANDSHAKE";

  private static final String HALF = CausynthRpcTrace.REQUEST;
  private static final String LEAF_PREFIX = "HANDSHAKE/";
  private static final String KEY_CLASS =
      "org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey";
  private static final String PARSED_CLASS =
      CausynthSaslHandoff.class.getName() + "$Parsed";
  private static final String LEAF_KEY_ID = "keyId";
  private static final String LEAF_BLOCK_POOL_ID = "blockPoolId";
  private static final String LEAF_NONCE = "nonce";
  private static final String DESCRIPTOR_KEY_ID = "I";
  private static final String DESCRIPTOR_BLOCK_POOL_ID = "Ljava/lang/String;";
  private static final String DESCRIPTOR_NONCE = "[B";
  private static final String GAP_MALFORMED_SHAPE =
      "SASL_HANDOFF_MALFORMED_NAME_SHAPE";
  private static final String GAP_UNMATCHED =
      "SASL_HANDOFF_UNMATCHED_ATTEMPT";
  private static final int MAX_PENDING_CONNECTIONS = 256;

  private static final AtomicLong EXCHANGES = new AtomicLong();
  private static final ThreadLocal<String> CONNECTION =
      new ThreadLocal<String>();
  private static final ThreadLocal<Attempt> EXPORTED_ATTEMPT =
      new ThreadLocal<Attempt>();
  private static final ThreadLocal<Attempt> DELIVERED_ATTEMPT =
      new ThreadLocal<Attempt>();
  private static final Map<String, Attempt> PENDING_CONNECTIONS =
      new LinkedHashMap<String, Attempt>(32, 0.75f, false) {
        private static final long serialVersionUID = 1L;

        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Attempt> eldest) {
          return size() > MAX_PENDING_CONNECTIONS;
        }
      };
  private static final Attempt DISABLED =
      new Attempt("", "0", "", null, 0L, 0L, 0L);

  private CausynthSaslHandoff() {
  }

  public static void enterClientConnection(Socket socket) {
    if (socket == null) {
      CONNECTION.remove();
      return;
    }
    CONNECTION.set(address(socket.getLocalSocketAddress()) + "->"
        + address(socket.getRemoteSocketAddress()));
  }

  /** Client-side variant for the Peer entry point. */
  public static void enterClientConnection(Peer peer) {
    if (peer == null) {
      CONNECTION.remove();
      return;
    }
    CONNECTION.set(address(peer.getLocalAddressString()) + "->"
        + address(peer.getRemoteAddressString()));
  }

  public static void enterServerConnection(Peer peer) {
    if (peer == null) {
      CONNECTION.remove();
      return;
    }
    CONNECTION.set(address(peer.getRemoteAddressString()) + "->"
        + address(peer.getLocalAddressString()));
  }

  /**
   * Clears thread state and retracts this thread's attempt if the server did
   * not claim it.  Identity checking is important when a later attempt has
   * already reused the same concrete connection key.
   */
  public static void leaveConnection() {
    Attempt attempt = EXPORTED_ATTEMPT.get();
    if (attempt != null && !attempt.connectionId.isEmpty()) {
      synchronized (PENDING_CONNECTIONS) {
        if (PENDING_CONNECTIONS.get(attempt.connectionId) == attempt) {
          PENDING_CONNECTIONS.remove(attempt.connectionId);
        }
        attempt.closed = true;
      }
    }
    CONNECTION.remove();
    EXPORTED_ATTEMPT.remove();
    DELIVERED_ATTEMPT.remove();
  }

  private static String address(Object value) {
    String text = value == null ? "" : String.valueOf(value).trim();
    int slash = text.lastIndexOf('/');
    if (slash >= 0) {
      text = text.substring(slash + 1);
    }
    while (text.startsWith("/")) {
      text = text.substring(1);
    }
    return text;
  }

  public static Attempt exportEncryptionKey(Object localOwner,
      DataEncryptionKey key) {
    if (!CausynthRpcTrace.isEnabled() || key == null) {
      return DISABLED;
    }
    String exchangeId = "SASL:" + EXCHANGES.incrementAndGet();
    String attemptId = "0";
    Attempt attempt;
    if (symbolicEnabled()) {
      attempt = new Attempt(exchangeId, attemptId,
          address(CONNECTION.get()), localOwner,
          export(key, LEAF_KEY_ID, DESCRIPTOR_KEY_ID, exchangeId, attemptId,
              LEAF_PREFIX + LEAF_KEY_ID, localOwner),
          export(key, LEAF_BLOCK_POOL_ID, DESCRIPTOR_BLOCK_POOL_ID,
              exchangeId, attemptId, LEAF_PREFIX + LEAF_BLOCK_POOL_ID,
              localOwner),
          export(key, LEAF_NONCE, DESCRIPTOR_NONCE, exchangeId, attemptId,
              LEAF_PREFIX + LEAF_NONCE, localOwner));
    } else {
      attempt = new Attempt(exchangeId, attemptId,
          address(CONNECTION.get()), localOwner, 0L, 0L, 0L);
    }
    EXPORTED_ATTEMPT.set(attempt);
    return attempt;
  }

  /** Records the point at which the encoded username enters the handshake. */
  public static void sendHandshake() {
    Attempt attempt = EXPORTED_ATTEMPT.get();
    if (attempt == null || attempt.exchangeId.isEmpty()) {
      return;
    }
    synchronized (PENDING_CONNECTIONS) {
      if (!attempt.closed && attempt.registeredUserName != null
          && PENDING_CONNECTIONS.get(attempt.connectionId) == attempt) {
        attempt.sent = true;
      }
    }
    CausynthRpcTrace.emitBoundary(CausynthRpcTrace.SEND, TRANSPORT,
        attempt.exchangeId, attempt.attemptId, HALF, attempt.localOwner);
  }

  /** Matches the username delivered by SASL to its exact connection attempt. */
  public static void deliverUserName(Object localOwner, String userName) {
    if (!CausynthRpcTrace.isEnabled()) {
      return;
    }
    DELIVERED_ATTEMPT.remove();
    Attempt attempt = take(userName);
    if (attempt == null) {
      CausynthRpcTrace.emitGap(TRANSPORT, "SASL:unmatched", "0", HALF,
          CausynthRpcTrace.RECEIVE, GAP_UNMATCHED, 1, "HANDSHAKE",
          localOwner);
      return;
    }
    DELIVERED_ATTEMPT.set(attempt);
    CausynthRpcTrace.emitBoundary(CausynthRpcTrace.RECEIVE, TRANSPORT,
        attempt.exchangeId, attempt.attemptId, HALF, localOwner);
  }

  public static void malformedUserName(Object localOwner, String userName,
      int componentCount) {
    if (!CausynthRpcTrace.isEnabled()) {
      return;
    }
    CausynthRpcTrace.emitGap(TRANSPORT, "SASL:unparsed", "0", HALF,
        CausynthRpcTrace.RECEIVE, GAP_MALFORMED_SHAPE, 1,
        "HANDSHAKE/components=" + componentCount, localOwner);
  }

  /** Creates the exact receiver carrier and attaches the sender's handles. */
  public static Parsed receiveUserName(Object localOwner, String userName,
      int keyId, String blockPoolId, byte[] nonce) {
    Parsed parsed = new Parsed(keyId, blockPoolId, nonce);
    if (!CausynthRpcTrace.isEnabled()) {
      return parsed;
    }
    Attempt attempt = DELIVERED_ATTEMPT.get();
    if (attempt == null) {
      return parsed;
    }
    DELIVERED_ATTEMPT.remove();
    if (!symbolicEnabled()) {
      return parsed;
    }
    receive(parsed, LEAF_KEY_ID, DESCRIPTOR_KEY_ID, attempt.keyIdHandle,
        attempt, LEAF_PREFIX + LEAF_KEY_ID, localOwner);
    receive(parsed, LEAF_BLOCK_POOL_ID, DESCRIPTOR_BLOCK_POOL_ID,
        attempt.blockPoolIdHandle, attempt,
        LEAF_PREFIX + LEAF_BLOCK_POOL_ID, localOwner);
    receive(parsed, LEAF_NONCE, DESCRIPTOR_NONCE, attempt.nonceHandle,
        attempt, LEAF_PREFIX + LEAF_NONCE, localOwner);
    return parsed;
  }

  private static long export(DataEncryptionKey key, String field,
      String descriptor, String exchangeId, String attemptId, String leaf,
      Object localOwner) {
    CausynthSymbolicHandoff.Outcome outcome =
        CausynthSymbolicHandoff.export(key, KEY_CLASS, field, descriptor);
    if (outcome.ok()) {
      return outcome.handle();
    }
    if (!"RPC_HANDOFF_NOT_SYMBOLIC".equals(outcome.reason())) {
      CausynthRpcTrace.emitGap(TRANSPORT, exchangeId, attemptId, HALF,
          CausynthSymbolicHandoff.STAGE_EXPORT, outcome.reason(), 1, leaf,
          localOwner);
    }
    return 0L;
  }

  private static boolean symbolicEnabled() {
    String results = System.getProperty("causynth.concolic.results");
    return results != null && !results.trim().isEmpty()
        && CausynthSymbolicHandoff.available();
  }

  private static void receive(Parsed parsed, String field, String descriptor,
      long handle, Attempt attempt, String leaf, Object localOwner) {
    if (handle <= 0L) {
      return;
    }
    CausynthSymbolicHandoff.Outcome outcome =
        CausynthSymbolicHandoff.receive(parsed, PARSED_CLASS, field,
            descriptor, handle);
    if (!outcome.ok()) {
      CausynthRpcTrace.emitGap(TRANSPORT, attempt.exchangeId,
          attempt.attemptId, HALF, CausynthRpcTrace.RECEIVE,
          outcome.reason(), 1, leaf, localOwner);
    }
  }

  private static Attempt take(String userName) {
    if (userName == null || userName.isEmpty()) {
      return null;
    }
    String connection = address(CONNECTION.get());
    if (connection.isEmpty()) {
      return null;
    }
    synchronized (PENDING_CONNECTIONS) {
      Attempt attempt = PENDING_CONNECTIONS.get(connection);
      if (attempt == null || !attempt.sent || attempt.closed
          || !userName.equals(attempt.registeredUserName)) {
        return null;
      }
      PENDING_CONNECTIONS.remove(connection);
      attempt.closed = true;
      return attempt;
    }
  }

  /** Exact receiver storage declared by HadoopCommunicationAdapter. */
  public static final class Parsed {
    public int keyId;
    public String blockPoolId;
    public byte[] nonce;

    Parsed(int keyId, String blockPoolId, byte[] nonce) {
      this.keyId = keyId;
      this.blockPoolId = blockPoolId;
      this.nonce = nonce;
    }
  }

  /** One exported encrypted-handshake attempt. */
  public static final class Attempt {
    private final String exchangeId;
    private final String attemptId;
    private final String connectionId;
    private final Object localOwner;
    private final long keyIdHandle;
    private final long blockPoolIdHandle;
    private final long nonceHandle;
    private String registeredUserName;
    private boolean sent;
    private boolean closed;

    private Attempt(String exchangeId, String attemptId, String connectionId,
        Object localOwner, long keyIdHandle, long blockPoolIdHandle,
        long nonceHandle) {
      this.exchangeId = exchangeId;
      this.attemptId = attemptId;
      this.connectionId = connectionId;
      this.localOwner = localOwner;
      this.keyIdHandle = keyIdHandle;
      this.blockPoolIdHandle = blockPoolIdHandle;
      this.nonceHandle = nonceHandle;
    }

    public String exchangeId() {
      return exchangeId;
    }

    public void registerUserName(String userName) {
      if (exchangeId.isEmpty() || userName == null || userName.isEmpty()) {
        return;
      }
      if (!connectionId.isEmpty()) {
        synchronized (PENDING_CONNECTIONS) {
          if (closed) {
            return;
          }
          if (registeredUserName != null
              && !registeredUserName.equals(userName)) {
            if (PENDING_CONNECTIONS.get(connectionId) == this) {
              PENDING_CONNECTIONS.remove(connectionId);
            }
            closed = true;
            return;
          }
          Attempt live = PENDING_CONNECTIONS.get(connectionId);
          if (live != null && live != this) {
            PENDING_CONNECTIONS.remove(connectionId);
            live.closed = true;
            closed = true;
            return;
          }
          registeredUserName = userName;
          PENDING_CONNECTIONS.put(connectionId, this);
        }
      }
    }
  }
}
