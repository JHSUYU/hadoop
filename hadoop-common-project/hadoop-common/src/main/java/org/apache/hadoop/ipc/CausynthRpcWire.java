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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcRequestHeaderProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSymbolicExpressionProto;

/**
 * Moves opaque symbolic-expression handles in the real Hadoop RPC header.
 *
 * <p>This class deliberately has no call-id keyed relay.  A sender stages a
 * leaf only until the next header builder on that same thread serializes it.
 * A server reads request leaves from its actual {@link Server.Call}; a client
 * reads response leaves from the actual {@link Client.Call} that completed.
 * The header is therefore the sole cross-thread transport of a handle.</p>
 *
 * <p>Hadoop never dereferences a handle.  GraphChecker's VM broker remains
 * the only component that can validate it and recover the associated native
 * expression.  Zero is retained as an explicit "concrete leaf" marker; only
 * positive handles denote broker entries.</p>
 */
@InterfaceAudience.Private
public final class CausynthRpcWire {
  private static final int MAX_EXPRESSIONS = 64;
  private static final int MAX_LEAF_PATH_LENGTH = 4096;
  private static final int MAX_PROVENANCE_LENGTH = 16384;

  /** Leaves exported before the next request or response header is built. */
  private static final ThreadLocal<Pending> PENDING_HEADER =
      new ThreadLocal<Pending>() {
        @Override
        protected Pending initialValue() {
          return new Pending();
        }
      };

  /** The actual Client.Call that just returned on this caller. */
  private static final ThreadLocal<Client.Call> CURRENT_CLIENT_CALL =
      new ThreadLocal<Client.Call>();

  private CausynthRpcWire() {
  }

  /**
   * Stages one leaf for the next RPC header built by this thread.
   *
   * @return false for malformed input, overflow, or a duplicate leaf
   */
  public static boolean stageExpression(String leafPath, long handle) {
    return stageExpression(leafPath, handle, "", "");
  }

  /**
   * Stages one leaf together with the exact concolic producer execution.
   * The provenance strings are proof metadata only; Hadoop never uses them
   * to recover a value or to choose an RPC delivery.
   */
  public static boolean stageExpression(String leafPath, long handle,
      String producerTaskId, String producerOccurrenceId) {
    String path = leafPath == null ? "" : leafPath;
    String task = producerTaskId == null ? "" : producerTaskId;
    String occurrence = producerOccurrenceId == null
        ? "" : producerOccurrenceId;
    if (path.isEmpty() || path.length() > MAX_LEAF_PATH_LENGTH) {
      return false;
    }
    if (!validProvenance(task, occurrence)) {
      return false;
    }
    Pending pending = PENDING_HEADER.get();
    if (pending.poisoned.contains(path)) {
      return false;
    }
    if (pending.values.containsKey(path)) {
      pending.values.remove(path);
      pending.poisoned.add(path);
      return false;
    }
    if (pending.values.size() >= MAX_EXPRESSIONS) {
      pending.poisoned.add(path);
      return false;
    }
    pending.values.put(path, new Handle(true, handle, task, occurrence));
    return true;
  }

  /** Drops exports that never reached a header. */
  public static void discardStagedExpressions() {
    PENDING_HEADER.remove();
  }

  /** Consumes one leaf from the current call's parsed wire header. */
  public static Handle takeCurrentExpression(String leafPath) {
    String path = leafPath == null ? "" : leafPath;
    Client.Call clientCall = CURRENT_CLIENT_CALL.get();
    if (clientCall != null) {
      // A nested client response is the current half even when it carries no
      // leaves.  Falling back to the enclosing server request would attach a
      // same-named request leaf to a missing response leaf.
      return clientCall.takeResponseExpression(path);
    }
    Server.Call call = Server.getCurCall().get();
    return call == null ? Handle.missing() : call.takeRequestExpression(path);
  }

  /** Prevents a pooled handler from inheriting a nested client response. */
  static void beginServerCall() {
    CURRENT_CLIENT_CALL.remove();
  }

  /** Makes exactly this completed call's response header current. */
  static void enterClientResponse(Client.Call call) {
    if (call == null) {
      CURRENT_CLIENT_CALL.remove();
    } else {
      CURRENT_CLIENT_CALL.set(call);
    }
  }

  static void appendTo(RpcRequestHeaderProto.Builder header) {
    for (Map.Entry<String, Handle> entry : drain().entrySet()) {
      header.addSymbolicExpressions(expression(entry));
    }
  }

  static void appendTo(RpcResponseHeaderProto.Builder header) {
    for (Map.Entry<String, Handle> entry : drain().entrySet()) {
      header.addSymbolicExpressions(expression(entry));
    }
  }

  static WireValues values(RpcRequestHeaderProto header) {
    return decode(header == null
        ? Collections.<RpcSymbolicExpressionProto>emptyList()
        : header.getSymbolicExpressionsList());
  }

  static WireValues values(RpcResponseHeaderProto header) {
    return decode(header == null
        ? Collections.<RpcSymbolicExpressionProto>emptyList()
        : header.getSymbolicExpressionsList());
  }

  private static RpcSymbolicExpressionProto expression(
      Map.Entry<String, Handle> entry) {
    Handle value = entry.getValue();
    RpcSymbolicExpressionProto.Builder builder =
        RpcSymbolicExpressionProto.newBuilder()
        .setLeafPath(entry.getKey())
        .setExpressionHandle(value.handle());
    if (!value.producerTaskId().isEmpty()) {
      builder.setProducerTaskId(value.producerTaskId());
      builder.setProducerOccurrenceId(value.producerOccurrenceId());
    }
    return builder.build();
  }

  private static Map<String, Handle> drain() {
    Pending pending = PENDING_HEADER.get();
    Map<String, Handle> result =
        new LinkedHashMap<String, Handle>(pending.values);
    PENDING_HEADER.remove();
    return result;
  }

  private static WireValues decode(List<RpcSymbolicExpressionProto> entries) {
    if (entries == null || entries.size() > MAX_EXPRESSIONS) {
      return WireValues.emptyValues();
    }
    LinkedHashMap<String, Handle> values =
        new LinkedHashMap<String, Handle>();
    for (RpcSymbolicExpressionProto entry : entries) {
      String path = entry.getLeafPath();
      String task = entry.hasProducerTaskId()
          ? entry.getProducerTaskId() : "";
      String occurrence = entry.hasProducerOccurrenceId()
          ? entry.getProducerOccurrenceId() : "";
      if (path == null || path.isEmpty()
          || path.length() > MAX_LEAF_PATH_LENGTH
          || values.containsKey(path)
          || !validProvenance(task, occurrence)) {
        return WireValues.emptyValues();
      }
      values.put(path, new Handle(true, entry.getExpressionHandle(),
          task, occurrence));
    }
    return new WireValues(values);
  }

  private static boolean validProvenance(String task, String occurrence) {
    if (task.length() > MAX_PROVENANCE_LENGTH
        || occurrence.length() > MAX_PROVENANCE_LENGTH
        || !task.equals(task.trim())
        || !occurrence.equals(occurrence.trim())) {
      return false;
    }
    return task.isEmpty() == occurrence.isEmpty();
  }

  /** One exact result, including the distinction between absent and zero. */
  public static final class Handle {
    private static final Handle MISSING = new Handle(false, 0L, "", "");
    private final boolean present;
    private final long handle;
    private final String producerTaskId;
    private final String producerOccurrenceId;

    private Handle(boolean present, long handle, String producerTaskId,
        String producerOccurrenceId) {
      this.present = present;
      this.handle = handle;
      this.producerTaskId = producerTaskId;
      this.producerOccurrenceId = producerOccurrenceId;
    }

    public boolean present() {
      return present;
    }

    public long handle() {
      return handle;
    }

    public String producerTaskId() {
      return producerTaskId;
    }

    public String producerOccurrenceId() {
      return producerOccurrenceId;
    }

    static Handle missing() {
      return MISSING;
    }
  }

  /** Parsed header values owned by one concrete Hadoop Call. */
  static final class WireValues {
    private final Map<String, Handle> values;

    private WireValues(Map<String, Handle> values) {
      this.values = values;
    }

    private static WireValues emptyValues() {
      return new WireValues(new LinkedHashMap<String, Handle>());
    }

    Handle take(String leafPath) {
      if (!values.containsKey(leafPath)) {
        return Handle.missing();
      }
      return values.remove(leafPath);
    }

  }

  private static final class Pending {
    private final Map<String, Handle> values =
        new LinkedHashMap<String, Handle>();
    private final Set<String> poisoned = new LinkedHashSet<String>();
  }
}
