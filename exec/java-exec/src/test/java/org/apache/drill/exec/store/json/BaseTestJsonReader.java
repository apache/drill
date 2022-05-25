/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.json;

import static org.junit.Assert.fail;

import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.test.ClusterTest;

public class BaseTestJsonReader extends ClusterTest {

  protected void enableV2Reader(boolean enable) throws Exception {
    client.alterSession(ExecConstants.ENABLE_V2_JSON_READER_KEY, enable);
  }

  protected void resetV2Reader() throws Exception {
    client.resetSession(ExecConstants.ENABLE_V2_JSON_READER_KEY);
  }

  protected interface TestWrapper {
    void apply() throws Exception;
  }

  protected void runBoth(TestWrapper wrapper) throws Exception {
    try {
      enableV2Reader(false);
      wrapper.apply();
      enableV2Reader(true);
      wrapper.apply();
    } finally {
      resetV2Reader();
    }
  }

  protected RowSet runTest(String sql) {
    try {
      return client.queryBuilder().sql(sql).rowSet();
    } catch (RpcException e) {
      fail(e.getMessage());
      throw new IllegalStateException(e);
    }
  }
}
