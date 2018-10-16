/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.sql;

import org.apache.drill.PlanTestBase;
import org.apache.drill.exec.ExecConstants;
import org.junit.Test;

public class TestAnalyze extends PlanTestBase {

  // Analyze for all columns
  @Test
  public void basic1() throws Exception {
    try {
      test("ALTER SESSION SET `planner.slice_target` = 1");
      test("CREATE TABLE dfs_test.tmp.region_basic1 AS SELECT * from cp.`region.json`");
      test("ANALYZE TABLE dfs_test.tmp.region_basic1 COMPUTE STATISTICS FOR ALL COLUMNS");
      test("SELECT * FROM dfs_test.tmp.`region_basic1/.stats.drill`");

      testBuilder()
          .sqlQuery("SELECT `column`, statcount, nonnullstatcount, ndv FROM dfs_test.tmp.`region_basic1/.stats.drill`")
          .unOrdered()
          .baselineColumns("column", "statcount", "nonnullstatcount", "ndv")
          .baselineValues("region_id", 110L, 110L, 107L)
          .baselineValues("sales_city", 110L, 110L, 111L)
          .baselineValues("sales_state_province", 110L, 110L, 13L)
          .baselineValues("sales_district", 110L, 110L, 22L)
          .baselineValues("sales_region", 110L, 110L, 8L)
          .baselineValues("sales_country", 110L, 110L, 4L)
          .baselineValues("sales_district_id", 110L, 110L, 23L)
          .go();

      // we can't compare the ndv for correctness as it is an estimate and not accurate
      testBuilder()
          .sqlQuery("SELECT statcount FROM dfs_test.tmp.`region_basic1/.stats.drill` WHERE `column` = 'region_id'")
          .unOrdered()
          .sqlBaselineQuery("SELECT count(region_id) AS statcount FROM dfs_test.tmp.region_basic1")
          .go();

    } finally {
      test("ALTER SESSION SET `planner.slice_target` = " + ExecConstants.SLICE_TARGET_DEFAULT);
    }
  }

  // Analyze for only a subset of the columns in table
  @Test
  public void basic2() throws Exception {
    try {
      test("ALTER SESSION SET `planner.slice_target` = 1");
      test("CREATE TABLE dfs_test.tmp.employee_basic2 AS SELECT * from cp.`employee.json`");
      test("ANALYZE TABLE dfs_test.tmp.employee_basic2 COMPUTE STATISTICS FOR COLUMNS (employee_id, birth_date)");
      test("SELECT * FROM dfs_test.tmp.`employee_basic2/.stats.drill`");

      testBuilder()
          .sqlQuery("SELECT `column`, statcount, nonnullstatcount, ndv FROM dfs_test.tmp.`employee_basic2/.stats.drill`")
          .unOrdered()
          .baselineColumns("column", "statcount", "nonnullstatcount", "ndv")
          .baselineValues("employee_id", 1155L, 1155L, 1144L)
          .baselineValues("birth_date", 1155L, 1155L, 53L)
          .go();

      // we can't compare the ndv for correctness as it is an estimate and not accurate
      testBuilder()
          .sqlQuery("SELECT statcount FROM dfs_test.tmp.`employee_basic2/.stats.drill` WHERE `column` = 'birth_date'")
          .unOrdered()
          .sqlBaselineQuery("SELECT count(birth_date) AS statcount FROM dfs_test.tmp.employee_basic2")
          .go();

    } finally {
      test("ALTER SESSION SET `planner.slice_target` = " + ExecConstants.SLICE_TARGET_DEFAULT);
    }
  }

  @Test
  public void join() throws Exception {
    try {
      test("ALTER SESSION SET `planner.slice_target` = 1");
      test("CREATE TABLE dfs_test.tmp.lineitem AS SELECT * FROM cp.`tpch/lineitem.parquet`");
      test("CREATE TABLE dfs_test.tmp.orders AS select * FROM cp.`tpch/orders.parquet`");
      test("ANALYZE TABLE dfs_test.tmp.lineitem COMPUTE STATISTICS FOR ALL COLUMNS");
      test("ANALYZE TABLE dfs_test.tmp.orders COMPUTE STATISTICS FOR ALL COLUMNS");
      test("SELECT * FROM dfs_test.tmp.`lineitem/.stats.drill`");
      test("SELECT * FROM dfs_test.tmp.`orders/.stats.drill`");

      test("SELECT * FROM dfs_test.tmp.`lineitem` l JOIN dfs_test.tmp.`orders` o ON l.l_orderkey = o.o_orderkey");
    } finally {
      test("ALTER SESSION SET `planner.slice_target` = " + ExecConstants.SLICE_TARGET_DEFAULT);
    }
  }
}
