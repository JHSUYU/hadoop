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

import com.google.protobuf.ByteString;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcRequestHeaderProto;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Focused wire-format tests for symbolic handle provenance. */
public class TestCausynthRpcWire {
  @After
  public void clearPendingHeader() {
    CausynthRpcWire.discardStagedExpressions();
  }

  @Test
  public void producerExecutionRoundTripsInTheActualHeader()
      throws Exception {
    assertTrue(CausynthRpcWire.stageExpression("response.value", 73L,
        "TASK.REPLAY.abc", "nodeA/region/producer#1"));

    RpcRequestHeaderProto.Builder builder = RpcRequestHeaderProto.newBuilder()
        .setCallId(9)
        .setClientId(ByteString.copyFromUtf8("client"));
    CausynthRpcWire.appendTo(builder);
    RpcRequestHeaderProto parsed = RpcRequestHeaderProto.parseFrom(
        builder.build().toByteArray());

    CausynthRpcWire.Handle handle = CausynthRpcWire.values(parsed)
        .take("response.value");
    assertTrue(handle.present());
    assertEquals(73L, handle.handle());
    assertEquals("TASK.REPLAY.abc", handle.producerTaskId());
    assertEquals("nodeA/region/producer#1",
        handle.producerOccurrenceId());
  }

  @Test
  public void incompleteProducerIdentityNeverEntersAHeader() {
    assertFalse(CausynthRpcWire.stageExpression("response.value", 73L,
        "TASK.REPLAY.abc", ""));
    assertFalse(CausynthRpcWire.stageExpression("response.value", 73L,
        "", "nodeA/region/producer#1"));
  }

  @Test
  public void legacyHandleRemainsReadableButHasNoProofProvenance()
      throws Exception {
    assertTrue(CausynthRpcWire.stageExpression("request.value", 11L));
    RpcRequestHeaderProto.Builder builder = RpcRequestHeaderProto.newBuilder()
        .setCallId(10)
        .setClientId(ByteString.copyFromUtf8("client"));
    CausynthRpcWire.appendTo(builder);

    CausynthRpcWire.Handle handle = CausynthRpcWire.values(builder.build())
        .take("request.value");
    assertTrue(handle.present());
    assertEquals(11L, handle.handle());
    assertEquals("", handle.producerTaskId());
    assertEquals("", handle.producerOccurrenceId());
  }
}
