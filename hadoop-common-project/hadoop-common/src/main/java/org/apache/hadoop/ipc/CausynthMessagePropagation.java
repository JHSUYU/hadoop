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

import java.util.UUID;

import edu.uva.liftlab.graphchecker.runtime.CausynthTraceRecorder;
import edu.uva.liftlab.graphchecker.runtime.SymbolicMessageEnvelope;
import org.apache.hadoop.util.StringUtils;

/** Strict message-carried GraphChecker context for Hadoop IPC and SASL. */
public final class CausynthMessagePropagation {
  public static final String IPC = "HADOOP_IPC";
  public static final String SASL = "HDFS_SASL";

  private static final ThreadLocal<Inbound> CURRENT = new ThreadLocal<>();
  private static final ThreadLocal<String> PENDING_SASL = new ThreadLocal<>();
  private static final ThreadLocal<Object> LOCAL_OWNER = new ThreadLocal<>();

  private CausynthMessagePropagation() {
  }

  public static Outbound outbound(String transport, String correlationId,
      String attemptId, String half, Object localOwner) {
    String trace = CausynthTraceRecorder.outboundExchange(transport,
        correlationId, attemptId, half, localOwner);
    String symbolic = SymbolicMessageEnvelope.takeOutbound();
    return trace.isEmpty() ? Outbound.EMPTY
        : new Outbound(correlationId, attemptId, trace, symbolic);
  }

  public static String rpcCorrelation(byte[] clientId, int callId) {
    return "hadoop-ipc-" + StringUtils.byteToHexString(clientId)
        + "-" + callId;
  }

  public static Inbound inbound(String trace, String symbolic,
      String transport, String correlationId, String attemptId, String half,
      Object localOwner) {
    endInbound();
    SymbolicMessageEnvelope.clearInbound();
    if (trace == null || trace.isEmpty()) {
      return Inbound.EMPTY;
    }
    long token = CausynthTraceRecorder.inboundExchange(trace, transport,
        correlationId, attemptId, half, localOwner);
    if (symbolic != null && !symbolic.isEmpty()) {
      SymbolicMessageEnvelope.installInbound(symbolic);
    }
    Inbound scope = new Inbound(token, correlationId, attemptId, half);
    CURRENT.set(scope);
    return scope;
  }

  public static void endInbound(Inbound scope) {
    if (scope == null || scope == Inbound.EMPTY) {
      return;
    }
    if (CURRENT.get() != scope) {
      throw new IllegalStateException("inbound message scope mismatch");
    }
    endInbound();
  }

  public static void endInbound() {
    Inbound scope = CURRENT.get();
    if (scope == null) {
      return;
    }
    CURRENT.remove();
    SymbolicMessageEnvelope.clearInbound();
    CausynthTraceRecorder.endInboundExchange(scope.token);
  }

  /** Emits the next alternating SASL request or response message. */
  public static Outbound outboundSasl() {
    Inbound current = CURRENT.get();
    if (current != null && "REQUEST".equals(current.half)) {
      return outbound(SASL, current.correlationId, current.attemptId,
          "RESPONSE", LOCAL_OWNER.get());
    }
    endInbound();
    String correlationId = "hdfs-sasl-" + UUID.randomUUID();
    PENDING_SASL.set(correlationId);
    return outbound(SASL, correlationId, "1", "REQUEST", LOCAL_OWNER.get());
  }

  /** Installs one received SASL message and infers its alternating half. */
  public static Inbound inboundSasl(String trace, String symbolic,
      String correlationId, String attemptId) {
    boolean response = correlationId != null
        && correlationId.equals(PENDING_SASL.get());
    if (response) {
      PENDING_SASL.remove();
    }
    return inbound(trace, symbolic, SASL, correlationId, attemptId,
        response ? "RESPONSE" : "REQUEST", LOCAL_OWNER.get());
  }

  public static void setLocalOwner(Object owner) {
    LOCAL_OWNER.set(owner);
  }

  public static void clearLocalOwner() {
    LOCAL_OWNER.remove();
    endInbound();
  }

  public static void registerSource(Object anchor, String kind, String role,
      String sourceId, long epoch) {
    CausynthTraceRecorder.registerSource(anchor, kind, role, sourceId, epoch);
  }

  public static void registerSourceAlias(Object alias, Object anchor) {
    CausynthTraceRecorder.registerSourceAlias(alias, anchor);
  }

  public static long beginRequest(Object source, String api) {
    return CausynthTraceRecorder.beginSourceRequest(source, api);
  }

  public static void endRequest(long request, String api) {
    endInbound();
    CausynthTraceRecorder.endSourceRequest(request, api);
  }

  public static final class Outbound {
    private static final Outbound EMPTY = new Outbound("", "", "", "");
    public final String correlationId;
    public final String attemptId;
    public final String trace;
    public final String symbolic;

    private Outbound(String correlationId, String attemptId, String trace,
        String symbolic) {
      this.correlationId = correlationId;
      this.attemptId = attemptId;
      this.trace = trace;
      this.symbolic = symbolic == null ? "" : symbolic;
    }

    public boolean active() {
      return !trace.isEmpty();
    }
  }

  public static final class Inbound {
    private static final Inbound EMPTY = new Inbound(0L, "", "", "");
    private final long token;
    private final String correlationId;
    private final String attemptId;
    private final String half;

    private Inbound(long token, String correlationId, String attemptId,
        String half) {
      this.token = token;
      this.correlationId = correlationId;
      this.attemptId = attemptId;
      this.half = half;
    }
  }
}
