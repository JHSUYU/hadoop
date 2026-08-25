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

/** Optional bridge for registering stable GraphChecker trace identities. */
public final class CausynthTraceContext {
  private static final String RECORDER_CLASS =
      "edu.uva.liftlab.graphchecker.runtime.CausynthTraceRecorder";

  private static volatile Bridge bridge;
  private static volatile boolean resolved;

  private CausynthTraceContext() {
  }

  /** Returns whether the optional GraphChecker trace bridge is available. */
  public static boolean isAvailable() {
    return resolve() != null;
  }

  /** Registers one stable logical node when GraphChecker is present. */
  public static boolean registerSource(Object anchor, String kind,
      String role, String sourceId, long epoch) {
    Bridge runtime = resolve();
    if (runtime == null) {
      return false;
    }
    try {
      runtime.registerSource.invoke(null, anchor, kind, role, sourceId, epoch);
      return true;
    } catch (ReflectiveOperationException | RuntimeException failure) {
      return false;
    }
  }

  /** Advances a stable logical source to a new lifecycle epoch. */
  public static boolean restartSource(Object anchor, String kind,
      String role, String sourceId, long epoch) {
    Bridge runtime = resolve();
    if (runtime == null) {
      return false;
    }
    try {
      runtime.restartSource.invoke(null, anchor, kind, role, sourceId, epoch);
      return true;
    } catch (ReflectiveOperationException | RuntimeException failure) {
      return false;
    }
  }

  /** Binds a live service/handler object to an already registered source. */
  public static boolean registerSourceAlias(Object alias, Object anchor) {
    Bridge runtime = resolve();
    if (runtime == null) {
      return false;
    }
    try {
      runtime.registerSourceAlias.invoke(null, alias, anchor);
      return true;
    } catch (ReflectiveOperationException | RuntimeException failure) {
      return false;
    }
  }

  /** Opens one externally named request scope, or returns zero when absent. */
  public static long beginSourceRequest(Object sourceAnchor, String apiName) {
    Bridge runtime = resolve();
    if (runtime == null) {
      return 0L;
    }
    try {
      Object result = runtime.beginSourceRequest.invoke(
          null, sourceAnchor, apiName);
      return result instanceof Long ? (Long) result : 0L;
    } catch (ReflectiveOperationException | RuntimeException failure) {
      return 0L;
    }
  }

  /** Closes a request scope opened by {@link #beginSourceRequest}. */
  public static void endSourceRequest(long requestId, String apiName) {
    if (requestId <= 0L) {
      return;
    }
    Bridge runtime = resolve();
    if (runtime == null) {
      return;
    }
    try {
      runtime.endSourceRequest.invoke(null, requestId, apiName);
    } catch (ReflectiveOperationException | RuntimeException ignored) {
      // The bridge is optional and must not change ordinary Hadoop behavior.
    }
  }

  private static Bridge resolve() {
    if (!resolved) {
      synchronized (CausynthTraceContext.class) {
        if (!resolved) {
          try {
            Class<?> recorder = Class.forName(RECORDER_CLASS);
            bridge = new Bridge(
                recorder.getMethod("registerSource", Object.class,
                    String.class, String.class, String.class, long.class),
                recorder.getMethod("restartSource", Object.class,
                    String.class, String.class, String.class, long.class),
                recorder.getMethod("registerSourceAlias", Object.class,
                    Object.class),
                recorder.getMethod("beginSourceRequest", Object.class,
                    String.class),
                recorder.getMethod("endSourceRequest", long.class,
                    String.class));
          } catch (ReflectiveOperationException | LinkageError absent) {
            bridge = null;
          }
          resolved = true;
        }
      }
    }
    return bridge;
  }

  private static final class Bridge {
    private final Method registerSource;
    private final Method restartSource;
    private final Method registerSourceAlias;
    private final Method beginSourceRequest;
    private final Method endSourceRequest;

    private Bridge(Method registerSource, Method restartSource,
        Method registerSourceAlias, Method beginSourceRequest,
        Method endSourceRequest) {
      this.registerSource = registerSource;
      this.restartSource = restartSource;
      this.registerSourceAlias = registerSourceAlias;
      this.beginSourceRequest = beginSourceRequest;
      this.endSourceRequest = endSourceRequest;
    }
  }
}
