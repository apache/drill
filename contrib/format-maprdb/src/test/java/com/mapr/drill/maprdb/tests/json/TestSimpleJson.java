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
package com.mapr.drill.maprdb.tests.json;

import java.util.List;

import org.apache.drill.BaseTestQuery;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.mapr.drill.maprdb.tests.MaprDBTestsSuite;
import com.mapr.tests.annotations.ClusterTest;

@Category(ClusterTest.class)
public class TestSimpleJson extends BaseTestQuery {

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    MaprDBTestsSuite.setupTests();
    MaprDBTestsSuite.createPluginAndGetConf(getDrillbitContext());
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    MaprDBTestsSuite.cleanupTests();
  }

  @Test
  public void testMe() throws Exception {
    setColumnWidths(new int[] {25, 40, 40, 40});
    final String sql = "SELECT\n"
        + "  _id, name, categories, full_address\n"
        + "FROM\n"
        + "  hbase.`business` business";
    runSQLAndVerifyCount(sql, 10);
  }

  protected List<QueryDataBatch> runHBaseSQLlWithResults(String sql) throws Exception {
    System.out.println("Running query:\n" + sql);
    return testSqlWithResults(sql);
  }

  protected void runSQLAndVerifyCount(String sql, int expectedRowCount) throws Exception{
    List<QueryDataBatch> results = runHBaseSQLlWithResults(sql);
    printResultAndVerifyRowCount(results, expectedRowCount);
  }

  private void printResultAndVerifyRowCount(List<QueryDataBatch> results, int expectedRowCount) throws SchemaChangeException {
    int rowCount = printResult(results);
    if (expectedRowCount != -1) {
      Assert.assertEquals(expectedRowCount, rowCount);
    }
  }

}
