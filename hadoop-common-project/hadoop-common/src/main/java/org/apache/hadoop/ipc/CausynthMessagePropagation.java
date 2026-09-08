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

import edu.uva.liftlab.graphchecker.runtime.CausynthTraceRecorder;
import edu.uva.liftlab.graphchecker.runtime.SymbolicMessageEnvelope;
import org.apache.hadoop.util.StringUtils;

/** Strict message-carried GraphChecker context for Hadoop IPC. */
public final class CausynthMessagePropagation {
  public static final String IPC = "HADOOP_IPC";

  private static final ThreadLocal<Inbound> CURRENT = new ThreadLocal<>();

  private CausynthMessagePropagation() {
  }

  public static Outbound outbound(String correlationId, String attemptId,
      String half, Object localOwner) {
    String trace = CausynthTraceRecorder.outboundExchange(IPC, correlationId,
        attemptId, half, localOwner);
    String symbolic = SymbolicMessageEnvelope.takeOutbound();
    return trace.isEmpty() ? Outbound.EMPTY
        : new Outbound(trace, symbolic);
  }

  public static String rpcCorrelation(byte[] clientId, int callId) {
    return "hadoop-ipc-" + StringUtils.byteToHexString(clientId)
        + "-" + callId;
  }

  public static Inbound inbound(String trace, String symbolic,
      String correlationId, String attemptId, String half,
      Object localOwner) {
    endInbound();
    SymbolicMessageEnvelope.clearInbound();
    if (trace == null || trace.isEmpty()) {
      return Inbound.EMPTY;
    }
    long token = CausynthTraceRecorder.inboundExchange(trace, IPC,
        correlationId, attemptId, half, localOwner);
    if (symbolic != null && !symbolic.isEmpty()) {
      SymbolicMessageEnvelope.installInbound(symbolic);
    }
    Inbound scope = new Inbound(token);
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

  public static void startRecording() {
    CausynthTraceRecorder.startRecording();
  }

  public static void registerSource(Object anchor, String kind, String role,
      String sourceId, long epoch) {
    CausynthTraceRecorder.registerSource(anchor, kind, role, sourceId, epoch);
  }

  public static void registerSourceAlias(Object alias, Object anchor) {
    CausynthTraceRecorder.registerSourceAlias(alias, anchor);
  }

  public static void restartSource(Object anchor, String kind, String role,
      String sourceId, long epoch) {
    CausynthTraceRecorder.restartSource(anchor, kind, role, sourceId, epoch);
  }

  public static long beginRequest(Object source, String api) {
    endInbound();
    return CausynthTraceRecorder.beginSourceRequest(source, api);
  }

  public static void endRequest(long request, String api) {
    endInbound();
    CausynthTraceRecorder.endSourceRequest(request, api);
  }

  public static final class Outbound {
    private static final Outbound EMPTY = new Outbound("", "");
    public final String trace;
    public final String symbolic;

    private Outbound(String trace, String symbolic) {
      this.trace = trace;
      this.symbolic = symbolic == null ? "" : symbolic;
    }

    public boolean active() {
      return !trace.isEmpty();
    }
  }

  public static final class Inbound {
    private static final Inbound EMPTY = new Inbound(0L);
    private final long token;

    private Inbound(long token) {
      this.token = token;
    }
  }
}
