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

/** Optional bridge to GraphChecker's runtime tracer. */
public final class CausynthRpcTrace {
  private static volatile Method recorderWithOwner;
  private static volatile boolean recorderResolved;
  private static volatile boolean unavailable;

  private CausynthRpcTrace() {
  }

  public static String hadoopIpcId(byte[] clientId, int callId,
      int retryCount) {
    final char[] hex = "0123456789abcdef".toCharArray();
    StringBuilder result = new StringBuilder();
    if (clientId != null) {
      for (byte value : clientId) {
        int unsigned = value & 0xff;
        result.append(hex[unsigned >>> 4]).append(hex[unsigned & 0x0f]);
      }
    }
    return result.append(':').append(callId).append(':')
        .append(retryCount).toString();
  }

  public static void emitHadoopIpc(String direction, byte[] clientId,
      int callId, int retryCount) {
    emitHadoopIpc(direction, clientId, callId, retryCount, null);
  }

  public static void emitHadoopIpc(String direction, byte[] clientId,
      int callId, int retryCount, Object localOwner) {
    if (enabled()) {
      emit(direction, "HADOOP_IPC",
          hadoopIpcId(clientId, callId, retryCount), localOwner);
    }
  }

  public static boolean isEnabled() {
    return enabled();
  }

  public static void emit(String direction, String transport, String wireId) {
    emit(direction, transport, wireId, null);
  }

  public static void emit(String direction, String transport, String wireId,
      Object localOwner) {
    if (!enabled() || unavailable) {
      return;
    }
    try {
      resolveRecorder();
      recorderWithOwner.invoke(null, direction, transport, wireId, localOwner);
    } catch (Throwable ignored) {
      unavailable = true;
    }
  }

  private static synchronized void resolveRecorder() throws Exception {
    if (recorderResolved) {
      return;
    }
    Class<?> recorderClass = Class.forName(
        "edu.uva.liftlab.graphchecker.runtime.CausynthTraceRecorder");
    recorderWithOwner = recorderClass.getMethod("recordRpc",
        String.class, String.class, String.class, Object.class);
    recorderResolved = true;
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
