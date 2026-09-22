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
package org.apache.hadoop.hdfs.server.datanode;

import java.io.IOException;

import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.HeartbeatResponse;
import org.apache.hadoop.ipc.CausynthMessagePropagation;

/**
 * One key-refresh heartbeat, driven synchronously ON THE CALLING THREAD and
 * inside a request scope the caller names.
 *
 * <p>{@code DataNodeTestUtils.triggerHeartbeat} cannot be used for a recorded
 * window: it wakes the block-pool actor and the heartbeat then runs on that
 * daemon thread, which owns no request scope and, before the node registers
 * itself, no node either.  {@code BPOfferService} and {@code BPServiceActor}
 * are package-private, so a workload outside this package -- the balancer
 * cases are in {@code ...server.balancer} -- cannot reach them; this is the
 * one accessor they need.</p>
 */
public final class CausynthDataNodeKeys {
  private CausynthDataNodeKeys() {
  }

  /** Sends one heartbeat and applies every command it returns. */
  public static void refreshKeysFromNameNode(DataNode datanode, String api)
      throws IOException {
    BPOfferService service = datanode.getAllBpOs().get(0);
    BPServiceActor actor = service.getBPServiceActors().get(0);
    long request = CausynthMessagePropagation.beginRequest(
        datanode.getDatanodeId(), api);
    try {
      HeartbeatResponse response = actor.sendHeartBeat(false);
      DatanodeCommand[] commands = response.getCommands();
      if (commands != null) {
        for (DatanodeCommand command : commands) {
          service.processCommandFromActor(command, actor);
        }
      }
    } finally {
      CausynthMessagePropagation.endRequest(request, api);
    }
  }
}
