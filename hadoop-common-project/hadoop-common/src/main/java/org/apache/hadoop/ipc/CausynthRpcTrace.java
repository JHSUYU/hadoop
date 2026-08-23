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
package org.apache.hadoop.ipc;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.io.Writable;

/**
 * The application-side control plane for GraphChecker's RPC boundary trace.
 *
 * <p>This class does two things and nothing else:</p>
 *
 * <ol>
 *   <li>allocates the exchange identity - an {@code exchangeId} for one
 *   logical call and an {@code attemptId} for one retry of it - and returns
 *   it to the call site, so a send and its matching receive name the same
 *   exchange without comparing payload bytes;</li>
 *   <li>emits one boundary event per direction so the trace can order the
 *   exchange against everything else the workload did.</li>
 * </ol>
 *
 * <p>Symbolic propagation is not implemented here.  The selected production
 * call sites are instrumented from GraphChecker's typed communication-port
 * catalog; those hooks pass the live VM broker handle with the real exchange
 * and the VM records the sole {@code VALUE_HANDOFF} authority.  Walking a
 * runtime payload reflectively and pairing its leaves again duplicated that
 * authority, imposed arbitrary depth/cardinality caps, and rejected valid
 * replay alternatives when object shape or ordinals drifted.</p>
 *
 * <p>Nothing here fails a Hadoop call.  With GraphChecker absent every bridge
 * resolves to a no-op and vanilla Hadoop runs unchanged.  With GraphChecker
 * present, boundary recording remains observational and never changes the
 * RPC's behavior.</p>
 */
public final class CausynthRpcTrace {

  /** Transport name of Hadoop's own IPC. */
  public static final String TRANSPORT_HADOOP_IPC = "HADOOP_IPC";

  /** Boundary directions.  Paired by the trace recorder. */
  public static final String SEND = "SEND";
  public static final String RECEIVE = "RECEIVE";

  /**
   * Direction of a named handoff failure.  It is deliberately neither
   * {@link #SEND} nor {@link #RECEIVE} so a gap can never be mistaken for a
   * boundary, or consume a boundary's pending correlation.
   */
  public static final String GAP = "RPC_HANDOFF_GAP";

  /** The two halves of one exchange. */
  public static final String REQUEST = "REQUEST";
  public static final String RESPONSE = "RESPONSE";

  private static final String TRACE_MODE_KEY = "causynth.trace.output.dir";
  private static final String RECORDER_CLASS =
      "edu.uva.liftlab.graphchecker.runtime.CausynthTraceRecorder";

  /** Bounded number of live exchanges remembered for correlation. */
  private static final int MAX_LIVE_EXCHANGES = 512;

  /**
   * The reply envelope a synchronous call is waiting on, mapped to the
   * exchange that produced it.
   *
   * <p>Correlating the client return by object identity, rather than by a
   * thread-local left over from the send, is what makes a failed or abandoned
   * call harmless: a reply that was never awaited simply has no entry, and no
   * later reply can inherit a stale one.  Identity is the key, so an
   * {@code IdentityHashMap} is required and insertion order is tracked
   * separately to keep the map bounded.</p>
   */
  private static final Map<Object, Pending> AWAITED_REPLIES =
      new IdentityHashMap<Object, Pending>();
  private static final Deque<Object> AWAITED_ORDER =
      new ArrayDeque<Object>();

  private static final AtomicBoolean RECORDER_UNAVAILABLE_REPORTED =
      new AtomicBoolean();
  private static volatile Method recordRpc;
  private static volatile String recorderFailure;
  private static volatile boolean recorderResolved;

  private CausynthRpcTrace() {
  }

  // -------------------------------------------------------------- gating --

  /** Whether boundary events and correlation should be recorded. */
  public static boolean tracingEnabled() {
    return present(TRACE_MODE_KEY);
  }

  /** Whether any GraphChecker bridge work is requested at all. */
  public static boolean isEnabled() {
    return tracingEnabled();
  }

  private static boolean present(String key) {
    String value = System.getProperty(key);
    return value != null && !value.trim().isEmpty();
  }

  // ------------------------------------------------------------ identity --

  /**
   * The stable identity of one logical Hadoop IPC call.  Hadoop's own
   * {@code (clientId, callId)} pair already names exactly one call on one
   * client; the retry counter names the attempt and is therefore kept
   * separate rather than folded into the same opaque string.
   */
  public static String ipcExchangeId(byte[] clientId, int callId) {
    final char[] hex = "0123456789abcdef".toCharArray();
    StringBuilder result = new StringBuilder();
    if (clientId != null) {
      for (byte value : clientId) {
        int unsigned = value & 0xff;
        result.append(hex[unsigned >>> 4]).append(hex[unsigned & 0x0f]);
      }
    }
    return result.append(':').append(callId).toString();
  }

  /** One attempt of one exchange. */
  public static String attemptId(int retryCount) {
    return Integer.toString(retryCount);
  }

  /**
   * The annotation a non-boundary row carries in place of a plain exchange
   * name: the exchange it is about, then what it says about it.
   *
   * <p>A gap and a lifecycle note are not deliveries.  Naming them after the
   * exchange alone would let one be read as a boundary of it - or consume a
   * boundary's pending correlation - so the annotation is deliberately part
   * of the recorded string.  It is diagnostic text, not identity: the
   * exchange, the attempt and the half of these rows are stated as their own
   * arguments, exactly as a boundary states them.</p>
   */
  private static String annotated(String exchangeId, String detail) {
    return safe(exchangeId) + "#" + detail;
  }

  // -------------------------------------------------------------- events --

  /**
   * Records one traced communication boundary.
   *
   * <p>The exchange is named by {@code exchangeId} and by nothing else.  The
   * attempt and the half are identities in their own right - README section 2
   * lists them as such - and are stated as their own arguments, so the
   * recorded leg and the expression attestation of the value it carried name
   * one exchange under one string.  Folding all three into one opaque key
   * produced an exchange name no attestation ever carried, and the two could
   * then never be joined.</p>
   */
  public static void emitBoundary(String direction, String transport,
      String exchangeId, String attemptId, String half, Object localOwner) {
    record(direction, transport, safe(exchangeId), attemptId, half,
        localOwner);
  }

  /**
   * Records one named coverage gap.  {@code reason} is the checker's own
   * status name wherever the checker produced one, so an unmatched exchange,
   * an incompatible shape, a stale handle and an absent VM stay
   * distinguishable downstream.
   */
  public static void emitGap(String transport, String exchangeId,
      String attemptId, String half, String stage, String reason,
      int count, String leafPath, Object localOwner) {
    record(GAP, transport,
        annotated(exchangeId, "stage=" + safe(stage)
            + "#reason=" + safe(reason)
            + "#count=" + count
            + "#leaf=" + safe(leafPath)),
        attemptId, half, localOwner);
  }

  /**
   * Records one named lifecycle event of an exchange - a timeout, a
   * cancellation, a late arrival, a fan-out membership.  The event name
   * travels inside the correlation string, next to the exchange it is about,
   * so an exchange that ended is never merely an exchange that is missing a
   * receive.
   */
  public static void emitEvent(String transport, String exchangeId,
      String attemptId, String half, String event, String detail,
      Object localOwner) {
    record(CausynthExchangeLifecycle.EVENT, transport,
        annotated(exchangeId, "event=" + safe(event)
            + "#detail=" + safe(detail)),
        attemptId, half, localOwner);
  }

  // ------------------------------------------------------------ IPC hooks --

  /**
   * The IPC send hook.  It runs immediately before the request is handed to
   * the connection.
   *
   * @return the allocated exchange id, or the empty string when nothing is on
   */
  public static String sendIpcRequest(byte[] clientId, int callId,
      int retryCount, Writable rpcRequest) {
    if (!isEnabled()) {
      return "";
    }
    String exchangeId = ipcExchangeId(clientId, callId);
    String attempt = attemptId(retryCount);
    emitBoundary(SEND, TRANSPORT_HADOOP_IPC, exchangeId, attempt, REQUEST,
        null);
    // A send issued inside a fan-out branch belongs to that branch because of
    // where it was issued, never because of what it carries.
    CausynthExchangeLifecycle.joinBranch(TRANSPORT_HADOOP_IPC, exchangeId,
        attempt);
    return exchangeId;
  }

  /**
   * Binds the reply envelope of a completed call to its exchange, so the
   * client return hook above this layer can name the exchange whose response
   * it is about to deserialize.
   *
   * <p>Both the synchronous return and the asynchronous {@code get} pass
   * through here.  The envelope is keyed by identity, so it does not matter
   * which thread completes an asynchronous call, and a completion nobody
   * awaited still cannot inherit another call's exchange.</p>
   *
   * @return {@code response}, unchanged
   */
  public static Writable completeIpcRequest(byte[] clientId, int callId,
      int retryCount, Writable response) {
    if (!isEnabled() || response == null) {
      return response;
    }
    CausynthExchangeLifecycle.noteBranchResponse(TRANSPORT_HADOOP_IPC,
        ipcExchangeId(clientId, callId), attemptId(retryCount));
    Pending pending = new Pending(ipcExchangeId(clientId, callId),
        attemptId(retryCount));
    synchronized (AWAITED_REPLIES) {
      if (AWAITED_REPLIES.put(response, pending) == null) {
        AWAITED_ORDER.addLast(response);
      }
      while (AWAITED_ORDER.size() > MAX_LIVE_EXCHANGES) {
        AWAITED_REPLIES.remove(AWAITED_ORDER.removeFirst());
      }
    }
    return response;
  }

  /**
   * The IPC handler-entry hook.
   */
  public static void receiveIpcRequest(byte[] clientId, int callId,
      int retryCount, Object param, Object localOwner) {
    if (!isEnabled()) {
      return;
    }
    String exchangeId = ipcExchangeId(clientId, callId);
    String attempt = attemptId(retryCount);
    emitBoundary(RECEIVE, TRANSPORT_HADOOP_IPC, exchangeId, attempt, REQUEST,
        localOwner);
  }

  /**
   * The IPC response hook.
   */
  public static void sendIpcResponse(byte[] clientId, int callId,
      int retryCount, Object result, Object localOwner) {
    if (!isEnabled()) {
      return;
    }
    String exchangeId = ipcExchangeId(clientId, callId);
    String attempt = attemptId(retryCount);
    emitBoundary(SEND, TRANSPORT_HADOOP_IPC, exchangeId, attempt, RESPONSE,
        localOwner);
  }

  /**
   * The client return hook.
   */
  public static void receiveIpcResponse(Object replyEnvelope,
      Object returnMessage) {
    if (!isEnabled() || replyEnvelope == null) {
      return;
    }
    Pending pending;
    synchronized (AWAITED_REPLIES) {
      // The order deque is pruned by the bound in completeIpcRequest, never
      // by value equality: an envelope must only ever be matched by identity.
      pending = AWAITED_REPLIES.remove(replyEnvelope);
    }
    if (pending == null) {
      // Nothing awaited this envelope: an asynchronous completion, a replayed
      // buffer or a reply this bridge never saw sent.  Leaving it unbound is
      // the point; guessing an exchange for it is exactly what must not
      // happen.
      return;
    }
    emitBoundary(RECEIVE, TRANSPORT_HADOOP_IPC, pending.exchangeId,
        pending.attemptId, RESPONSE, null);
  }

  // ------------------------------------------------- IPC attempt outcomes --

  /**
   * A bounded wait on an attempt expired.  The attempt is <em>not</em> over:
   * an asynchronous caller may poll again and still be answered, so this is
   * recorded as a distinct, non-terminal event rather than as a timeout.
   */
  public static void expireIpcAttempt(byte[] clientId, int callId,
      int retryCount, long timeout, Object unit) {
    if (!isEnabled()) {
      return;
    }
    CausynthExchangeLifecycle.note(TRANSPORT_HADOOP_IPC,
        ipcExchangeId(clientId, callId), attemptId(retryCount), RESPONSE,
        CausynthExchangeLifecycle.POLL_EXPIRED,
        "waited=" + timeout + String.valueOf(unit), null);
  }

  /**
   * The thread waiting for an attempt was interrupted and gave the attempt up.
   * The request was really sent, so only the response half ends here: a server
   * that is still handling this request may legitimately bind its arguments.
   */
  public static void cancelIpcAttempt(byte[] clientId, int callId,
      int retryCount) {
    if (!isEnabled()) {
      return;
    }
    CausynthExchangeLifecycle.terminate(TRANSPORT_HADOOP_IPC,
        ipcExchangeId(clientId, callId), attemptId(retryCount), RESPONSE,
        CausynthExchangeLifecycle.CANCELLED, "waiter=interrupted", null);
  }

  /**
   * An attempt completed with an error and therefore produced no response
   * value.  A remote error, a socket timeout and a broken connection are three
   * different endings and are named as three different events.
   */
  public static void failIpcAttempt(byte[] clientId, int callId,
      int retryCount, IOException error) {
    if (!isEnabled()) {
      return;
    }
    CausynthExchangeLifecycle.terminate(TRANSPORT_HADOOP_IPC,
        ipcExchangeId(clientId, callId), attemptId(retryCount), RESPONSE,
        CausynthExchangeLifecycle.classify(error),
        "error=" + (error == null ? "" : error.getClass().getName()), null);
  }

  /**
   * The request never reached the connection, so neither half of this attempt
   * can ever bind and both are retired.
   */
  public static void abortIpcRequest(byte[] clientId, int callId,
      int retryCount, Throwable cause) {
    if (!isEnabled()) {
      return;
    }
    CausynthExchangeLifecycle.terminate(TRANSPORT_HADOOP_IPC,
        ipcExchangeId(clientId, callId), attemptId(retryCount),
        CausynthExchangeLifecycle.BOTH_HALVES,
        CausynthExchangeLifecycle.SEND_FAILED,
        "error=" + (cause == null ? "" : cause.getClass().getName()), null);
  }

  /**
   * A response has been read off a connection for {@code callId}.
   *
   * <p>Three situations reach this point and they must not be confused.  An
   * ordinary arrival needs nothing: its receive boundary follows.  An arrival
   * whose attempt already terminated is a late response, recorded against that
   * exchange and bound to nothing.  An arrival with no live call at all cannot
   * even be given an attempt ordinal - this bridge reports it as an orphan
   * rather than picking one of the exchange's attempts for it.</p>
   */
  public static void arriveIpcResponse(byte[] clientId, int callId,
      Map<Integer, Client.Call> live) {
    if (!isEnabled()) {
      return;
    }
    String exchangeId = ipcExchangeId(clientId, callId);
    Client.Call call = live == null ? null : live.get(Integer.valueOf(callId));
    if (call == null) {
      CausynthExchangeLifecycle.orphanResponse(TRANSPORT_HADOOP_IPC,
          exchangeId, null);
      return;
    }
    String attempt = attemptId(call.retry);
    String terminal = CausynthExchangeLifecycle.terminalOf(
        TRANSPORT_HADOOP_IPC, exchangeId, attempt, RESPONSE);
    if (terminal != null) {
      CausynthExchangeLifecycle.lateResponse(TRANSPORT_HADOOP_IPC, exchangeId,
          attempt, terminal, null);
    }
  }

  // ------------------------------------------------------ deferred replies --

  /**
   * The handler returned no result because it deferred its response.  The
   * exchange is open, not failed, and its response will be exported later from
   * whichever thread produces it.
   */
  public static void deferIpcResponse(byte[] clientId, int callId,
      int retryCount, Object localOwner) {
    if (!isEnabled()) {
      return;
    }
    CausynthExchangeLifecycle.note(TRANSPORT_HADOOP_IPC,
        ipcExchangeId(clientId, callId), attemptId(retryCount), RESPONSE,
        CausynthExchangeLifecycle.DEFERRED, "handler=returned-no-result",
        localOwner);
  }

  /**
   * The deferred response of {@code call} is ready.  The call carries its own
   * identity, so the exchange is named from the call rather than from a
   * handler thread-local that has long since moved on.
   */
  public static void resumeIpcResponse(Server.Call call, Object result,
      Object localOwner) {
    if (!isEnabled() || call == null) {
      return;
    }
    String exchangeId = ipcExchangeId(call.clientId, call.callId);
    String attempt = attemptId(call.retryCount);
    CausynthExchangeLifecycle.note(TRANSPORT_HADOOP_IPC, exchangeId, attempt,
        RESPONSE, CausynthExchangeLifecycle.DEFERRED_RESUMED, "", localOwner);
    emitBoundary(SEND, TRANSPORT_HADOOP_IPC, exchangeId, attempt, RESPONSE,
        localOwner);
  }

  /**
   * The deferred response of {@code call} ended in an error, so this exchange
   * has no response value at all.
   */
  public static void abortIpcResponse(Server.Call call, Throwable cause,
      Object localOwner) {
    if (!isEnabled() || call == null) {
      return;
    }
    CausynthExchangeLifecycle.terminate(TRANSPORT_HADOOP_IPC,
        ipcExchangeId(call.clientId, call.callId), attemptId(call.retryCount),
        RESPONSE, CausynthExchangeLifecycle.DEFERRED_FAILED,
        "error=" + (cause == null ? "" : cause.getClass().getName()),
        localOwner);
  }

  // ------------------------------------------------------------ internals --

  /**
   * Sends one row to the checker's trace recorder.  The recorder is resolved
   * once; its absence is the ordinary vanilla case, while a present but
   * incompatible recorder is named once instead of vanishing into a permanent
   * silent latch.
   */
  private static void record(String direction, String transport,
      String correlationId, String attemptId, String half,
      Object localOwner) {
    if (!isEnabled()) {
      return;
    }
    Method recorder = resolveRecorder();
    if (recorder == null) {
      String failure = recorderFailure;
      if (failure != null && !failure.isEmpty()
          && RECORDER_UNAVAILABLE_REPORTED.compareAndSet(false, true)) {
        // There is no trace to write the gap to; the checker's own runtime
        // still files a handoff gap, and this makes the bridge state visible
        // to a plain run.
        System.err.println("CausynthRpcTrace: trace recorder unusable: "
            + failure);
      }
      return;
    }
    try {
      recorder.invoke(null, direction, transport, correlationId,
          safe(attemptId), safe(half), localOwner);
    } catch (Throwable failure) {
      if (RECORDER_UNAVAILABLE_REPORTED.compareAndSet(false, true)) {
        System.err.println("CausynthRpcTrace: trace recorder call failed: "
            + failure);
      }
    }
  }

  private static Method resolveRecorder() {
    if (recorderResolved) {
      return recordRpc;
    }
    synchronized (CausynthRpcTrace.class) {
      if (recorderResolved) {
        return recordRpc;
      }
      try {
        Class<?> recorder = Class.forName(RECORDER_CLASS);
        recordRpc = recorder.getMethod("recordRpc", String.class,
            String.class, String.class, String.class, String.class,
            Object.class);
        recorderFailure = "";
      } catch (ClassNotFoundException absent) {
        recordRpc = null;
        recorderFailure = "";
      } catch (ReflectiveOperationException | RuntimeException
          | LinkageError drift) {
        // Instrumentation must never turn a working RPC into a failed one,
        // so even a resolution error the JVM raises stays on this side of the
        // bridge - named, and not thrown at the caller.
        recordRpc = null;
        recorderFailure = "trace recorder unusable: " + drift;
      }
      recorderResolved = true;
      return recordRpc;
    }
  }

  private static String safe(String value) {
    return value == null ? "" : value;
  }

  /** The synchronous continuation on which a reply will be deserialized. */
  private static final class Pending {
    private final String exchangeId;
    private final String attemptId;

    Pending(String exchangeId, String attemptId) {
      this.exchangeId = exchangeId;
      this.attemptId = attemptId;
    }
  }

}
