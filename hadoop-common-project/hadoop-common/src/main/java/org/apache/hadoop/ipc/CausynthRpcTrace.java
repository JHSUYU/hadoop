/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0.
 */
package org.apache.hadoop.ipc;

import java.lang.reflect.Method;

/** Optional bridge to GraphChecker's runtime tracer. */
public final class CausynthRpcTrace {
  private static volatile Method recorder;
  private static volatile boolean unavailable;

  private CausynthRpcTrace() {
  }

  public static void emitHadoopIpc(String direction, byte[] clientId,
      int callId, int retryCount, Object localOwner) {
    if (!enabled() || unavailable) {
      return;
    }
    try {
      Method method = recorder;
      if (method == null) {
        method = Class.forName(
            "edu.uva.liftlab.graphchecker.runtime.CausynthTraceRecorder")
            .getMethod("recordRpc", String.class, String.class,
                String.class, Object.class);
        recorder = method;
      }
      method.invoke(null, direction, "HADOOP_IPC",
          correlationId(clientId, callId, retryCount), localOwner);
    } catch (Throwable ignored) {
      unavailable = true;
    }
  }

  private static String correlationId(byte[] clientId, int callId,
      int retryCount) {
    StringBuilder result = new StringBuilder();
    if (clientId != null) {
      for (byte value : clientId) {
        result.append(String.format("%02x", value & 0xff));
      }
    }
    return result.append(':').append(callId).append(':')
        .append(retryCount).toString();
  }

  private static boolean enabled() {
    String output = System.getProperty("causynth.trace.output.dir");
    if (output != null && !output.trim().isEmpty()) {
      return true;
    }
    String results = System.getProperty("causynth.concolic.results");
    return results != null && !results.trim().isEmpty();
  }
}
