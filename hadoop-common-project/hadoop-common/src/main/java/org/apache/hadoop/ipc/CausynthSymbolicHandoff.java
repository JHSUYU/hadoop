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

import java.lang.reflect.Method;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * Optional reflection bridge to GraphChecker's live expression broker.
 *
 * <p>Hadoop deliberately has no compile-time dependency on GraphChecker.  A
 * normal Hadoop JVM therefore resolves this bridge to a no-op.  In a
 * concolic JVM, the application-side transport adapters use the opaque handle
 * returned here; only GraphChecker and its VM may interpret that handle.</p>
 */
@InterfaceAudience.Private
public final class CausynthSymbolicHandoff {
  public static final String STAGE_EXPORT = "EXPORT";
  public static final String STAGE_RECEIVE = "RECEIVE";

  public static final int OK = 0;
  public static final int ABSENT = -2000;
  public static final int INCOMPATIBLE = -2001;
  public static final int INVOCATION_FAILED = -2002;
  public static final int MALFORMED_RESULT = -2003;

  public static final String REASON_ABSENT = "HANDOFF_BRIDGE_ABSENT";
  public static final String REASON_INCOMPATIBLE =
      "HANDOFF_BRIDGE_INCOMPATIBLE";
  public static final String REASON_INVOCATION_FAILED =
      "HANDOFF_BRIDGE_INVOCATION_FAILED";
  public static final String REASON_MALFORMED_RESULT =
      "HANDOFF_BRIDGE_MALFORMED_RESULT";

  private static final String RUNTIME_CLASS =
      "edu.uva.liftlab.graphchecker.runtime.ConcolicRegionRuntime";
  private static final String RESULT_CLASS = RUNTIME_CLASS + "$HandoffResult";

  private static volatile Binding binding;

  private CausynthSymbolicHandoff() {
  }

  public static boolean available() {
    return bind().usable();
  }

  public static String unavailableReason() {
    Binding current = bind();
    return current.usable() ? "" : current.unusableReason;
  }

  public static Outcome export(Object owner, String declaringClass,
      String fieldName, String descriptor) {
    Binding current = bind();
    if (!current.usable()) {
      return new Outcome(STAGE_EXPORT, current.unusableStatus,
          current.unusableReason, 0L, "");
    }
    try {
      Object result = current.export.invoke(null, owner, declaringClass,
          fieldName, descriptor);
      return read(current, STAGE_EXPORT, result);
    } catch (Throwable failure) {
      return new Outcome(STAGE_EXPORT, INVOCATION_FAILED,
          REASON_INVOCATION_FAILED, 0L, describe(failure));
    }
  }

  public static Outcome receive(Object owner, String declaringClass,
      String fieldName, String descriptor, long handle) {
    Binding current = bind();
    if (!current.usable()) {
      return new Outcome(STAGE_RECEIVE, current.unusableStatus,
          current.unusableReason, handle, "");
    }
    try {
      Object result = current.receive.invoke(null, owner, declaringClass,
          fieldName, descriptor, handle);
      return read(current, STAGE_RECEIVE, result);
    } catch (Throwable failure) {
      return new Outcome(STAGE_RECEIVE, INVOCATION_FAILED,
          REASON_INVOCATION_FAILED, handle, describe(failure));
    }
  }

  /** Completes a deferred response handoff when the runtime supports it. */
  public static void completeResponse() {
    Binding current = bind();
    if (!current.usable()) {
      return;
    }
    try {
      current.completeResponse.invoke(null);
    } catch (Throwable ignored) {
      // Optional bookkeeping must never change Hadoop behavior.
    }
  }

  private static Outcome read(Binding current, String stage, Object result) {
    if (result == null) {
      return new Outcome(stage, MALFORMED_RESULT,
          REASON_MALFORMED_RESULT, 0L, "handoff returned no result");
    }
    try {
      int status = ((Integer) current.resultStatus.invoke(result)).intValue();
      long handle = ((Long) current.resultHandle.invoke(result)).longValue();
      if (status == OK) {
        return new Outcome(stage, OK, "", handle, "");
      }
      Object reason = current.resultReason.invoke(result);
      String text = reason == null ? "" : reason.toString();
      return new Outcome(stage, status,
          text.isEmpty() ? REASON_MALFORMED_RESULT : text, handle, "");
    } catch (Throwable failure) {
      return new Outcome(stage, MALFORMED_RESULT,
          REASON_MALFORMED_RESULT, 0L, describe(failure));
    }
  }

  private static Binding bind() {
    Binding current = binding;
    if (current == null) {
      synchronized (CausynthSymbolicHandoff.class) {
        current = binding;
        if (current == null) {
          current = resolve();
          binding = current;
        }
      }
    }
    return current;
  }

  private static Binding resolve() {
    final Class<?> runtime;
    try {
      runtime = Class.forName(RUNTIME_CLASS);
    } catch (ClassNotFoundException | LinkageError absent) {
      return new Binding(ABSENT, REASON_ABSENT);
    }
    try {
      Class<?> result = Class.forName(RESULT_CLASS);
      Method export = runtime.getMethod("exportSymbolicLeaf", Object.class,
          String.class, String.class, String.class);
      Method receive = runtime.getMethod("receiveSymbolicLeaf", Object.class,
          String.class, String.class, String.class, Long.TYPE);
      Method complete = runtime.getMethod("completeResponseHandoff");
      return new Binding(export, receive, result.getMethod("status"),
          result.getMethod("handle"), result.getMethod("reason"), complete);
    } catch (ReflectiveOperationException | RuntimeException
        | LinkageError incompatible) {
      return new Binding(INCOMPATIBLE,
          REASON_INCOMPATIBLE + "(" + describe(incompatible) + ")");
    }
  }

  private static String describe(Throwable failure) {
    Throwable root = failure;
    while (root.getCause() != null && root.getCause() != root) {
      root = root.getCause();
    }
    String message = root.getMessage();
    return root.getClass().getName()
        + (message == null || message.isEmpty() ? "" : ":" + message);
  }

  /** Result returned to transport-specific adapters. */
  public static final class Outcome {
    private final String stage;
    private final int status;
    private final String reason;
    private final long handle;
    private final String detail;

    private Outcome(String stage, int status, String reason, long handle,
        String detail) {
      this.stage = stage;
      this.status = status;
      this.reason = reason == null ? "" : reason;
      this.handle = handle;
      this.detail = detail == null ? "" : detail;
    }

    public boolean ok() {
      return status == OK;
    }

    public String stage() {
      return stage;
    }

    public int status() {
      return status;
    }

    public String reason() {
      return reason;
    }

    public long handle() {
      return handle;
    }

    public String detail() {
      return detail;
    }

    @Override
    public String toString() {
      return stage + (ok() ? " OK" : " " + reason) + " handle=" + handle
          + (detail.isEmpty() ? "" : " (" + detail + ")");
    }
  }

  private static final class Binding {
    private final Method export;
    private final Method receive;
    private final Method resultStatus;
    private final Method resultHandle;
    private final Method resultReason;
    private final Method completeResponse;
    private final int unusableStatus;
    private final String unusableReason;

    private Binding(Method export, Method receive, Method resultStatus,
        Method resultHandle, Method resultReason, Method completeResponse) {
      this.export = export;
      this.receive = receive;
      this.resultStatus = resultStatus;
      this.resultHandle = resultHandle;
      this.resultReason = resultReason;
      this.completeResponse = completeResponse;
      this.unusableStatus = OK;
      this.unusableReason = "";
    }

    private Binding(int unusableStatus, String unusableReason) {
      this.export = null;
      this.receive = null;
      this.resultStatus = null;
      this.resultHandle = null;
      this.resultReason = null;
      this.completeResponse = null;
      this.unusableStatus = unusableStatus;
      this.unusableReason = unusableReason;
    }

    private boolean usable() {
      return export != null && receive != null && completeResponse != null;
    }
  }
}
