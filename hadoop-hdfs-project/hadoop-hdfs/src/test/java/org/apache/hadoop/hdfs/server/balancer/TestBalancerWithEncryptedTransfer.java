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
package org.apache.hadoop.hdfs.server.balancer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.ipc.CausynthTraceContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestBalancerWithEncryptedTransfer {
  
  private final Configuration conf = new HdfsConfiguration();
  private final Object traceClient = new Object();
  private long traceRequestId;
  
  @Before
  public void setUpConf() {
    conf.setBoolean(DFSConfigKeys.DFS_ENCRYPT_DATA_TRANSFER_KEY, true);
    conf.setBoolean(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
    if (CausynthTraceContext.isAvailable()) {
      if (!CausynthTraceContext.registerSource(traceClient, "EXTERNAL_APP",
          "HDFS_CLIENT", "cluster0/client0", 0L)) {
        throw new IllegalStateException(
            "GraphChecker client source registration failed");
      }
      traceRequestId = CausynthTraceContext.beginSourceRequest(
          traceClient, "encryptedBalancerWorkload");
      if (traceRequestId <= 0L) {
        throw new IllegalStateException(
            "GraphChecker request scope registration failed");
      }
    }
  }

  @After
  public void closeTraceRequest() {
    CausynthTraceContext.endSourceRequest(traceRequestId,
        "encryptedBalancerWorkload");
    traceRequestId = 0L;
  }
  
  @Test(timeout=60000)
  public void testEncryptedBalancer0() throws Exception {
    new TestBalancer().testBalancer0Internal(conf);
  }
  
  @Test(timeout=60000)
  public void testEncryptedBalancer1() throws Exception {
    new TestBalancer().testBalancer1Internal(conf);
  }
  
  @Test(timeout=60000)
  public void testEncryptedBalancer2() throws Exception {
    new TestBalancer().testBalancer2Internal(conf);
  }

}
