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
package org.apache.drill;

import org.apache.drill.categories.SqlFunctionTest;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.test.QueryBuilder;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(SqlFunctionTest.class)
public class TestUntypedNull extends ClusterTest {

  @BeforeClass
  public static void setup() throws Exception {
    ClusterFixtureBuilder builder = ClusterFixture.builder(dirTestWatcher);
    startCluster(builder);
  }

  @Test
  public void testSplitFunction() throws Exception {
    String query = "select split(n_name, ' ') [1] from cp.`tpch/nation.parquet`\n" +
      "where n_nationkey = -1 group by n_name order by n_name limit 10";
    QueryBuilder.QuerySummary summary = queryBuilder().sql(query).run();
    assertTrue(summary.succeeded());
    assertEquals(0, summary.recordCount());
  }

  @Test
  public void testWindowFunction() throws Exception {
    String query = "select row_number() over (partition by split(n_name, ' ') [1])\n" +
      "from cp.`tpch/nation.parquet` where n_nationkey = -1";
    QueryBuilder.QuerySummary summary = queryBuilder().sql(query).run();
    assertTrue(summary.succeeded());
    assertEquals(0, summary.recordCount());
  }

  @Test
  public void testUnion() throws Exception {
    String query = "select split(n_name, ' ') [1] from cp.`tpch/nation.parquet` where n_nationkey = -1 group by n_name\n" +
      "union\n" +
      "select split(n_name, ' ') [1] from cp.`tpch/nation.parquet` where n_nationkey = -1 group by n_name";
    QueryBuilder.QuerySummary summary = queryBuilder().sql(query).run();
    assertTrue(summary.succeeded());
    assertEquals(0, summary.recordCount());
  }

  @Test
  public void testTableCreation() throws Exception {
    String tablePrefix = "table_";
    List<String> formats = Arrays.asList("parquet", "json", "csv");
    try {
      for (String format : formats) {
        client.alterSession(ExecConstants.OUTPUT_FORMAT_OPTION, format);
        String query = String.format("create table dfs.tmp.%s%s as\n" +
          "select split(n_name, ' ') [1] from cp.`tpch/nation.parquet` where n_nationkey = -1 group by n_name",
          tablePrefix, format);
        QueryBuilder.QuerySummary summary = queryBuilder().sql(query).run();
        assertTrue(summary.succeeded());
        assertEquals(1, summary.recordCount());
      }
    } finally {
      client.resetSession(ExecConstants.OUTPUT_FORMAT_OPTION);
      for (String format : formats) {
        queryBuilder().sql(String.format("drop table if exists dfs.tmp.%s%s", tablePrefix, format)).run();
      }
    }
  }

  @Test
  public void testTypeAndMode() throws Exception {
    String query = "select\n" +
      "typeof(split(n_name, ' ') [1]),\n" +
      "drilltypeof(split(n_name, ' ') [1]),\n" +
      "sqltypeof(split(n_name, ' ') [1]),\n" +
      "modeof(split(n_name, ' ') [1])\n" +
      "from cp.`tpch/nation.parquet`\n" +
      "where n_nationkey = -1";
    QueryBuilder.QuerySummary summary = queryBuilder().sql(query).run();
    assertTrue(summary.succeeded());
    assertEquals(0, summary.recordCount());
  }

}

