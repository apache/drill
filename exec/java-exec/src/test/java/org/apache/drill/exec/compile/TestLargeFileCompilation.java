/**
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
package org.apache.drill.exec.compile;

import java.util.List;

import org.apache.drill.BaseTestQuery;
import org.apache.drill.exec.proto.UserBitShared.QueryType;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.junit.Test;

public class TestLargeFileCompilation extends BaseTestQuery {

  private static final String LARGE_QUERY;

  private static final int ITERATION_COUNT = Integer.valueOf(System.getProperty("TestLargeFileCompilation.iteration", "1"));

  static {
    StringBuilder sb = new StringBuilder("select \n");
    for (int i = 0; i < 300; i++) {
      sb.append("\temployee_id+").append(i).append(" as col").append(i).append(",\n");
    }
    sb.append("\tfull_name\nfrom cp.`employee.json` limit 1");
    LARGE_QUERY = sb.toString();
  }

  @Test
  public void testWithJDK() throws Exception {
    test(String.format("alter session set `%s`='JDK'", QueryClassLoader.JAVA_COMPILER_OPTION));
    runTest();
  }

  @Test
  public void testWithDEFAULT() throws Exception {
    test(String.format("alter session set `%s`='DEFAULT'", QueryClassLoader.JAVA_COMPILER_OPTION));
    runTest();
  }

  @Test(expected=org.apache.drill.exec.rpc.RpcException.class)
  public void testWithJanino() throws Exception {
    test(String.format("alter session set `%s`='JANINO'", QueryClassLoader.JAVA_COMPILER_OPTION));
    runTest();
  }

  private void runTest() throws Exception {
    for (int i = 0; i < ITERATION_COUNT; i++) {
      List<QueryResultBatch> results = client.runQuery(QueryType.SQL, LARGE_QUERY);
      for (QueryResultBatch queryResultBatch : results) {
        queryResultBatch.release();
      }
    }
  }

}
