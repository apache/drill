/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.pcap;

import org.apache.drill.test.BaseTestQuery;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.file.Paths;
import java.util.List;

public class TestPcapRecordReader extends BaseTestQuery {
  @BeforeClass
  public static void setupTestFiles() {
    dirTestWatcher.copyResourceToRoot(Paths.get("store", "pcap"));
  }

  @Test
  public void testStarQuery() throws Exception {
    runSQLVerifyCount("select * from dfs.`store/pcap/tcp-1.pcap`", 16);
    runSQLVerifyCount("select distinct DST_IP from dfs.`store/pcap/tcp-1.pcap`", 1);
    runSQLVerifyCount("select distinct DsT_IP from dfs.`store/pcap/tcp-1.pcap`", 1);
    runSQLVerifyCount("select distinct dst_ip from dfs.`store/pcap/tcp-1.pcap`", 1);
  }

  @Test
  public void testCountQuery() throws Exception {
    runSQLVerifyCount("select count(*) from dfs.`store/pcap/tcp-1.pcap`", 1);
    runSQLVerifyCount("select count(*) from dfs.`store/pcap/tcp-2.pcap`", 1);
  }

  @Test
  public void testDistinctQuery() throws Exception {
    runSQLVerifyCount("select distinct * from dfs.`store/pcap/tcp-1.pcap`", 1);
  }

  private void runSQLVerifyCount(String sql, int expectedRowCount) throws Exception {
    List<QueryDataBatch> results = runSQLWithResults(sql);
    printResultAndVerifyRowCount(results, expectedRowCount);
  }

  private List<QueryDataBatch> runSQLWithResults(String sql) throws Exception {
    return testSqlWithResults(sql);
  }

  private void printResultAndVerifyRowCount(List<QueryDataBatch> results,
                                            int expectedRowCount) throws SchemaChangeException {
    setColumnWidth(25);
    int rowCount = printResult(results);
    if (expectedRowCount != -1) {
      Assert.assertEquals(expectedRowCount, rowCount);
    }
  }
}
