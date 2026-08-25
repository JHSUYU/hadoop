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

/** Optional bridge for declaring one exact application field symbolic. */
public final class CausynthSymbolicSource {
  private static final String RUNTIME_CLASS =
      "edu.uva.liftlab.graphchecker.runtime.ConcolicRegionRuntime";
  private static volatile Method symbolizer;
  private static volatile Method rejecter;
  private static volatile Method intSelector;
  private static volatile boolean resolved;

  private CausynthSymbolicSource() {
  }

  /**
   * Symbolizes the configured field when GraphChecker is active; otherwise it
   * is a no-op so an ordinary Hadoop process behaves identically.
   */
  public static boolean symbolize(String sourceId, Object owner,
      String fieldSignature) {
    Method method = resolve();
    if (method == null) {
      return false;
    }
    try {
      return Boolean.TRUE.equals(method.invoke(null, sourceId, owner,
          fieldSignature));
    } catch (ReflectiveOperationException | RuntimeException failure) {
      return false;
    }
  }

  /** Rejects the active source occurrence when an application invariant fails. */
  public static void reject(String sourceId, String detail) {
    resolve();
    Method method = rejecter;
    if (method == null) {
      return;
    }
    try {
      method.invoke(null, sourceId, detail);
    } catch (ReflectiveOperationException | RuntimeException ignored) {
      // The bridge is optional outside GraphChecker.
    }
  }

  /**
   * Returns the exact integer selected for an already declared source.
   *
   * <p>This is selector-only authority: it neither creates a new symbolic
   * root nor searches application objects by value. The concolic task must
   * name one source occurrence, and the runtime returns the value published
   * only after that exact producer occurrence is admitted. A missing,
   * rejected, or ambiguous selector is represented by {@code null}, so
   * application hooks fail closed instead of guessing.</p>
   */
  public static Integer selectedIntValue(String sourceId) {
    resolve();
    Method method = intSelector;
    if (method == null) {
      return null;
    }
    try {
      Object selected = method.invoke(null, sourceId);
      return selected instanceof Integer ? (Integer) selected : null;
    } catch (ReflectiveOperationException | RuntimeException failure) {
      return null;
    }
  }

  private static Method resolve() {
    if (!resolved) {
      synchronized (CausynthSymbolicSource.class) {
        if (!resolved) {
          try {
            Class<?> runtime = Class.forName(RUNTIME_CLASS);
            symbolizer = runtime.getMethod(
                "symbolizeConfiguredSourceField", String.class,
                Object.class, String.class);
            try {
              rejecter = runtime.getMethod("rejectConfiguredSourceField",
                  String.class, String.class);
            } catch (ReflectiveOperationException absent) {
              rejecter = null;
            }
            try {
              intSelector = runtime.getMethod(
                  "selectedConfiguredSourceIntValue", String.class);
            } catch (ReflectiveOperationException absent) {
              intSelector = null;
            }
          } catch (ReflectiveOperationException | LinkageError absent) {
            symbolizer = null;
            rejecter = null;
            intSelector = null;
          }
          resolved = true;
        }
      }
    }
    return symbolizer;
  }
}
