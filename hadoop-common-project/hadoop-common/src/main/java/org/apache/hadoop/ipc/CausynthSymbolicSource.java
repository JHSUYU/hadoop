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
  private static volatile Method resumer;
  private static volatile boolean resolved;

  private CausynthSymbolicSource() {
  }

  /**
   * Symbolizes the configured field when GraphChecker is active; otherwise it
   * is a no-op so an ordinary Hadoop process behaves identically.
   *
   * <p>Every occurrence of a declared field is its own symbolic variable. The
   * hook names only the field and the owning object it is standing on; it
   * never names which occurrence or which owner should be chosen. When the
   * value already carries a propagated symbolic expression that expression is
   * the authority and this call is a no-op, so an application hook may be
   * entered on every candidate owner without overwriting a live term.</p>
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

  /**
   * Reuses an expression propagated onto an exact field without minting a
   * replacement root when that field is concrete.
   */
  public static boolean resume(Object owner, String fieldSignature) {
    resolve();
    Method method = resumer;
    if (method == null) {
      return false;
    }
    try {
      return Boolean.TRUE.equals(method.invoke(null, owner, fieldSignature));
    } catch (ReflectiveOperationException | RuntimeException failure) {
      return false;
    }
  }

  private static Method resolve() {
    if (!resolved) {
      synchronized (CausynthSymbolicSource.class) {
        if (!resolved) {
          Class<?> runtime = null;
          try {
            runtime = Class.forName(RUNTIME_CLASS);
            symbolizer = runtime.getMethod(
                "symbolizeConfiguredSourceField", String.class,
                Object.class, String.class);
          } catch (ReflectiveOperationException | LinkageError absent) {
            symbolizer = null;
          }
          try {
            if (runtime != null) {
              resumer = runtime.getMethod(
                  "resumePropagatedField", Object.class, String.class);
            }
          } catch (ReflectiveOperationException | LinkageError absent) {
            resumer = null;
          }
          resolved = true;
        }
      }
    }
    return symbolizer;
  }
}
