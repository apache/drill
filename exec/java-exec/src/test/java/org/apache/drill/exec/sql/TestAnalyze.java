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
package org.apache.drill.exec.sql;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.apache.drill.PlanTestBase;
import org.apache.drill.exec.proto.UserBitShared.QueryType;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.test.BaseTestQuery;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class TestAnalyze extends BaseTestQuery {

  @BeforeClass
  public static void copyData() throws Exception {
    dirTestWatcher.copyResourceToRoot(Paths.get("multilevel", "parquet"));
  }

  // Analyze for all columns
  @Test
  public void basic1() throws Exception {
    try {
      test("ALTER SESSION SET `planner.slice_target` = 1");
      test("ALTER SESSION SET `store.format` = 'parquet'");
      test("CREATE TABLE dfs.tmp.region_basic1 AS SELECT * from cp.`region.json`");
      test("ANALYZE TABLE dfs.tmp.region_basic1 COMPUTE STATISTICS");
      test("SELECT * FROM dfs.tmp.`region_basic1/.stats.drill`");
      test("create table dfs.tmp.flatstats1 as select flatten(`directories`[0].`columns`) as `columns`"
              + " from dfs.tmp.`region_basic1/.stats.drill`");

      testBuilder()
          .sqlQuery("SELECT tbl.`columns`.`column` as `column`, tbl.`columns`.rowcount as rowcount,"
              + " tbl.`columns`.nonnullrowcount as nonnullrowcount, tbl.`columns`.ndv as ndv,"
              + " tbl.`columns`.avgwidth as avgwidth"
              + " FROM dfs.tmp.flatstats1 tbl")
          .unOrdered()
          .baselineColumns("column", "rowcount", "nonnullrowcount", "ndv", "avgwidth")
          .baselineValues("`region_id`", 110.0, 110.0, 110L, 8.0)
          .baselineValues("`sales_city`", 110.0, 110.0, 109L, 8.663636363636364)
          .baselineValues("`sales_state_province`", 110.0, 110.0, 13L, 2.4272727272727272)
          .baselineValues("`sales_district`", 110.0, 110.0, 23L, 9.318181818181818)
          .baselineValues("`sales_region`", 110.0, 110.0, 8L, 10.8)
          .baselineValues("`sales_country`", 110.0, 110.0, 4L, 3.909090909090909)
          .baselineValues("`sales_district_id`", 110.0, 110.0, 23L, 8.0)
          .go();
    } finally {
      resetSessionOption("planner.slice_target");
      resetSessionOption("store.format");
    }
  }

  // Analyze for only a subset of the columns in table
  @Test
  public void basic2() throws Exception {
    try {
      test("ALTER SESSION SET `planner.slice_target` = 1");
      test("ALTER SESSION SET `store.format` = 'parquet'");
      test("CREATE TABLE dfs.tmp.employee_basic2 AS SELECT * from cp.`employee.json`");
      test("ANALYZE TABLE dfs.tmp.employee_basic2 COMPUTE STATISTICS (employee_id, birth_date)");
      test("SELECT * FROM dfs.tmp.`employee_basic2/.stats.drill`");
      test("create table dfs.tmp.flatstats2 as select flatten(`directories`[0].`columns`) as `columns`"
          + " from dfs.tmp.`employee_basic2/.stats.drill`");

      testBuilder()
          .sqlQuery("SELECT tbl.`columns`.`column` as `column`, tbl.`columns`.rowcount as rowcount,"
              + " tbl.`columns`.nonnullrowcount as nonnullrowcount, tbl.`columns`.ndv as ndv,"
              + " tbl.`columns`.avgwidth as avgwidth"
              + " FROM dfs.tmp.flatstats2 tbl")
          .unOrdered()
          .baselineColumns("column", "rowcount", "nonnullrowcount", "ndv", "avgwidth")
          .baselineValues("`employee_id`", 1155.0, 1155.0, 1155L, 8.0)
          .baselineValues("`birth_date`", 1155.0, 1155.0, 52L, 10.0)
          .go();
    } finally {
      resetSessionOption("planner.slice_target");
      resetSessionOption("store.format");
    }
  }

  // Analyze with sampling percentage
  @Test
  public void basic3() throws Exception {
    try {
      test("ALTER SESSION SET `planner.slice_target` = 1");
      test("ALTER SESSION SET `store.format` = 'parquet'");
      test("ALTER SESSION SET `exec.statistics.deterministic_sampling` = true");
      test("CREATE TABLE dfs.tmp.employee_basic3 AS SELECT * from cp.`employee.json`");
      test("ANALYZE TABLE dfs.tmp.employee_basic3 COMPUTE STATISTICS (employee_id, birth_date) SAMPLE 55 PERCENT");
      test("SELECT * FROM dfs.tmp.`employee_basic3/.stats.drill`");
      test("create table dfs.tmp.flatstats3 as select flatten(`directories`[0].`columns`) as `columns`"
              + " from dfs.tmp.`employee_basic3/.stats.drill`");

      testBuilder()
              .sqlQuery("SELECT tbl.`columns`.`column` as `column`, tbl.`columns`.rowcount as rowcount,"
                      + " tbl.`columns`.nonnullrowcount as nonnullrowcount, tbl.`columns`.ndv as ndv,"
                      + " tbl.`columns`.avgwidth as avgwidth"
                      + " FROM dfs.tmp.flatstats3 tbl")
              .unOrdered()
              .baselineColumns("column", "rowcount", "nonnullrowcount", "ndv", "avgwidth")
              .baselineValues("`employee_id`", 1138.0, 1138.0, 1138L, 8.00127815945039)
              .baselineValues("`birth_date`", 1138.0, 1138.0, 38L, 10.001597699312988)
              .go();
    } finally {
      resetSessionOption("exec.statistics.deterministic_sampling");
      resetSessionOption("planner.slice_target");
      resetSessionOption("store.format");
    }
  }

  @Test
  public void join() throws Exception {
    try {
      test("ALTER SESSION SET `planner.slice_target` = 1");
      test("ALTER SESSION SET `store.format` = 'parquet'");
      test("CREATE TABLE dfs.tmp.lineitem AS SELECT * FROM cp.`tpch/lineitem.parquet`");
      test("CREATE TABLE dfs.tmp.orders AS select * FROM cp.`tpch/orders.parquet`");
      test("ANALYZE TABLE dfs.tmp.lineitem COMPUTE STATISTICS");
      test("ANALYZE TABLE dfs.tmp.orders COMPUTE STATISTICS");
      test("SELECT * FROM dfs.tmp.`lineitem/.stats.drill`");
      test("SELECT * FROM dfs.tmp.`orders/.stats.drill`");
      test("ALTER SESSION SET `planner.statistics.use` = true");
      test("SELECT * FROM dfs.tmp.`lineitem` l JOIN dfs.tmp.`orders` o ON l.l_orderkey = o.o_orderkey");
    } finally {
      resetSessionOption("planner.slice_target");
      resetSessionOption("store.format");
      resetSessionOption("planner.statistics.use");
    }
  }

  @Test
  public void testAnalyzeSupportedFormats() throws Exception {
    //Only allow computing statistics on PARQUET files.
    try {
      test("ALTER SESSION SET `planner.slice_target` = 1");
      test("ALTER SESSION SET `store.format` = 'json'");
      test("CREATE TABLE dfs.tmp.employee_basic4 AS SELECT * from cp.`employee.json`");
      //Should display not supported
      verifyAnalyzeOutput("ANALYZE TABLE dfs.tmp.employee_basic4 COMPUTE STATISTICS",
          "Table employee_basic4 is not supported by ANALYZE. "
          + "Support is currently limited to directory-based Parquet tables.");

      test("DROP TABLE dfs.tmp.employee_basic4");
      test("ALTER SESSION SET `store.format` = 'parquet'");
      test("CREATE TABLE dfs.tmp.employee_basic4 AS SELECT * from cp.`employee.json`");
      //Should complete successfully (16 columns in employee.json)
      verifyAnalyzeOutput("ANALYZE TABLE dfs.tmp.employee_basic4 COMPUTE STATISTICS",
          "16");
    } finally {
      resetSessionOption("planner.slice_target");
      resetSessionOption("store.format");
    }
  }

  @Ignore("For 1.16.0, we do not plan to support statistics on dir columns")
  @Test
  public void testAnalyzePartitionedTables() throws Exception {
    //Computing statistics on columns, dir0, dir1
    try {
      final String tmpLocation = "/multilevel/parquet";
      test("ALTER SESSION SET `planner.slice_target` = 1");
      test("ALTER SESSION SET `store.format` = 'parquet'");
      test("CREATE TABLE dfs.tmp.parquet1 AS SELECT * from dfs.`%s`", tmpLocation);
      verifyAnalyzeOutput("ANALYZE TABLE dfs.tmp.parquet1 COMPUTE STATISTICS", "11");
      test("SELECT * FROM dfs.tmp.`parquet1/.stats.drill`");
      test("create table dfs.tmp.flatstats4 as select flatten(`directories`[0].`columns`) as `columns` " +
           "from dfs.tmp.`parquet1/.stats.drill`");
      //Verify statistics
      testBuilder()
          .sqlQuery("SELECT tbl.`columns`.`column` as `column`, tbl.`columns`.rowcount as rowcount,"
              + " tbl.`columns`.nonnullrowcount as nonnullrowcount, tbl.`columns`.ndv as ndv,"
              + " tbl.`columns`.avgwidth as avgwidth"
              + " FROM dfs.tmp.flatstats4 tbl")
          .unOrdered()
          .baselineColumns("column", "rowcount", "nonnullrowcount", "ndv", "avgwidth")
          .baselineValues("`o_orderkey`", 120.0, 120.0, 119L, 4.0)
          .baselineValues("`o_custkey`", 120.0, 120.0, 113L, 4.0)
          .baselineValues("`o_orderstatus`", 120.0, 120.0, 3L, 1.0)
          .baselineValues("`o_totalprice`", 120.0, 120.0, 120L, 8.0)
          .baselineValues("`o_orderdate`", 120.0, 120.0, 111L, 4.0)
          .baselineValues("`o_orderpriority`", 120.0, 120.0, 5L, 8.458333333333334)
          .baselineValues("`o_clerk`", 120.0, 120.0, 114L, 15.0)
          .baselineValues("`o_shippriority`", 120.0, 120.0, 1L, 4.0)
          .baselineValues("`o_comment`", 120.0, 120.0, 120L, 46.333333333333336)
          .baselineValues("`dir0`", 120.0, 120.0, 3L, 4.0)
          .baselineValues("`dir1`", 120.0, 120.0, 4L, 2.0)
          .go();
    } finally {
      resetSessionOption("planner.slice_target");
      resetSessionOption("store.format");
    }
  }

  @Test
  public void testStaleness() throws Exception {
    // copy the data into the temporary location
    final String tmpLocation = "/multilevel/parquet";
    test("ALTER SESSION SET `planner.slice_target` = 1");
    test("ALTER SESSION SET `store.format` = 'parquet'");
    try {
      test("CREATE TABLE dfs.tmp.parquetStale AS SELECT o_orderkey, o_custkey, o_orderstatus, " +
           "o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment from dfs.`%s`", tmpLocation);
      verifyAnalyzeOutput("ANALYZE TABLE dfs.tmp.parquetStale COMPUTE STATISTICS", "9");
      verifyAnalyzeOutput("ANALYZE TABLE dfs.tmp.parquetStale COMPUTE STATISTICS",
          "Table parquetStale has not changed since last ANALYZE!");
      // Verify we recompute statistics once a new file/directory is added. Update the directory some
      // time after ANALYZE so that the timestamps are different.
      Thread.sleep(1000);
      final String Q4 = "/multilevel/parquet/1996/Q4";
      test("CREATE TABLE dfs.tmp.`parquetStale/1996/Q5` AS SELECT o_orderkey, o_custkey, o_orderstatus, " +
           "o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment from dfs.`%s`", Q4);
      verifyAnalyzeOutput("ANALYZE TABLE dfs.tmp.parquetStale COMPUTE STATISTICS", "9");
      Thread.sleep(1000);
      test("DROP TABLE dfs.tmp.`parquetStale/1996/Q5`");
      verifyAnalyzeOutput("ANALYZE TABLE dfs.tmp.parquetStale COMPUTE STATISTICS", "9");
    } finally {
      resetSessionOption("planner.slice_target");
      resetSessionOption("store.format");
    }
  }

  @Test
  public void testUseStatistics() throws Exception {
    //Test ndv/rowcount for scan
    test("ALTER SESSION SET `planner.slice_target` = 1");
    test("ALTER SESSION SET `store.format` = 'parquet'");
    try {
      test("CREATE TABLE dfs.tmp.employeeUseStat AS SELECT * from cp.`employee.json`");
      test("CREATE TABLE dfs.tmp.departmentUseStat AS SELECT * from cp.`department.json`");
      test("ANALYZE TABLE dfs.tmp.employeeUseStat COMPUTE STATISTICS");
      test("ANALYZE TABLE dfs.tmp.departmentUseStat COMPUTE STATISTICS");
      test("ALTER SESSION SET `planner.statistics.use` = true");
      String query = " select employee_id from dfs.tmp.employeeUseStat where department_id = 2";
      String[] expectedPlan1 = {"Filter\\(condition.*\\).*rowcount = 96.25,.*",
              "Scan.*columns=\\[`department_id`, `employee_id`\\].*rowcount = 1155.0.*"};
      PlanTestBase.testPlanWithAttributesMatchingPatterns(query, expectedPlan1, new String[]{});

      query = " select employee_id from dfs.tmp.employeeUseStat where department_id IN (2, 5)";
      String[] expectedPlan2 = {"Filter\\(condition.*\\).*rowcount = 192.5,.*",
              "Scan.*columns=\\[`department_id`, `employee_id`\\].*rowcount = 1155.0.*"};
      PlanTestBase.testPlanWithAttributesMatchingPatterns(query, expectedPlan2, new String[]{});

      query = "select employee_id from dfs.tmp.employeeUseStat where department_id IN (2, 5) and employee_id = 5";
      String[] expectedPlan3 = {"Filter\\(condition.*\\).*rowcount = 1.0,.*",
              "Scan.*columns=\\[`department_id`, `employee_id`\\].*rowcount = 1155.0.*"};
      PlanTestBase.testPlanWithAttributesMatchingPatterns(query, expectedPlan3, new String[]{});

      query = " select emp.employee_id from dfs.tmp.employeeUseStat emp join dfs.tmp.departmentUseStat dept"
          + " on emp.department_id = dept.department_id";
      String[] expectedPlan4 = {"HashJoin\\(condition.*\\).*rowcount = 1155.0,.*",
              "Scan.*columns=\\[`department_id`, `employee_id`\\].*rowcount = 1155.0.*",
              "Scan.*columns=\\[`department_id`\\].*rowcount = 12.0.*"};
      PlanTestBase.testPlanWithAttributesMatchingPatterns(query, expectedPlan4, new String[]{});

      query = " select emp.employee_id from dfs.tmp.employeeUseStat emp join dfs.tmp.departmentUseStat dept"
              + " on emp.department_id = dept.department_id where dept.department_id = 5";
      String[] expectedPlan5 = {"HashJoin\\(condition.*\\).*rowcount = 96.25,.*",
              "Scan.*columns=\\[`department_id`, `employee_id`\\].*rowcount = 1155.0.*",
              "Scan.*columns=\\[`department_id`\\].*rowcount = 12.0.*"};
      PlanTestBase.testPlanWithAttributesMatchingPatterns(query, expectedPlan5, new String[]{});

      query = " select emp.employee_id from dfs.tmp.employeeUseStat emp join dfs.tmp.departmentUseStat dept"
              + " on emp.department_id = dept.department_id"
              + " where dept.department_id = 5 and emp.employee_id = 10";
      String[] expectedPlan6 = {"MergeJoin\\(condition.*\\).*rowcount = 1.0,.*",
              "Filter\\(condition=\\[AND\\(=\\(\\$1, 10\\), =\\(\\$0, 5\\)\\)\\]\\).*rowcount = 1.0,.*",
              "Scan.*columns=\\[`department_id`, `employee_id`\\].*rowcount = 1155.0.*",
              "Filter\\(condition=\\[=\\(\\$0, 5\\)\\]\\).*rowcount = 1.0,.*",
              "Scan.*columns=\\[`department_id`\\].*rowcount = 12.0.*"};
      PlanTestBase.testPlanWithAttributesMatchingPatterns(query, expectedPlan6, new String[]{});

      query = " select emp.employee_id, count(*)"
              + " from dfs.tmp.employeeUseStat emp"
              + " group by emp.employee_id";
      String[] expectedPlan7 = {"HashAgg\\(group=\\[\\{0\\}\\], EXPR\\$1=\\[COUNT\\(\\)\\]\\).*rowcount = 1155.0,.*",
              "Scan.*columns=\\[`employee_id`\\].*rowcount = 1155.0.*"};
      PlanTestBase.testPlanWithAttributesMatchingPatterns(query, expectedPlan7, new String[]{});

      query = " select emp.employee_id from dfs.tmp.employeeUseStat emp join dfs.tmp.departmentUseStat dept"
              + " on emp.department_id = dept.department_id "
              + " group by emp.employee_id";
      String[] expectedPlan8 = {"HashAgg\\(group=\\[\\{0\\}\\]\\).*rowcount = 730.0992454469841,.*",
              "HashJoin\\(condition.*\\).*rowcount = 1155.0,.*",
              "Scan.*columns=\\[`department_id`, `employee_id`\\].*rowcount = 1155.0.*",
              "Scan.*columns=\\[`department_id`\\].*rowcount = 12.0.*"};
      PlanTestBase.testPlanWithAttributesMatchingPatterns(query, expectedPlan8, new String[]{});

      query = "select emp.employee_id, dept.department_description"
              + " from dfs.tmp.employeeUseStat emp join dfs.tmp.departmentUseStat dept"
              + " on emp.department_id = dept.department_id "
              + " group by emp.employee_id, emp.store_id, dept.department_description "
              + " having dept.department_description = 'FINANCE'";
      String[] expectedPlan9 = {"HashAgg\\(group=\\[\\{0, 1, 2\\}\\]\\).*rowcount = 60.84160378724867.*",
              "HashJoin\\(condition.*\\).*rowcount = 96.25,.*",
              "Scan.*columns=\\[`department_id`, `employee_id`, `store_id`\\].*rowcount = 1155.0.*",
              "Filter\\(condition=\\[=\\(\\$1, 'FINANCE'\\)\\]\\).*rowcount = 1.0,.*",
              "Scan.*columns=\\[`department_id`, `department_description`\\].*rowcount = 12.0.*"};
      PlanTestBase.testPlanWithAttributesMatchingPatterns(query, expectedPlan9, new String[]{});

      query = " select emp.employee_id from dfs.tmp.employeeUseStat emp join dfs.tmp.departmentUseStat dept\n"
              + " on emp.department_id = dept.department_id "
              + " group by emp.employee_id, emp.store_id "
              + " having emp.store_id = 7";
      String[] expectedPlan10 = {"HashAgg\\(group=\\[\\{0, 1\\}\\]\\).*rowcount = 29.203969817879365.*",
              "HashJoin\\(condition.*\\).*rowcount = 46.2,.*",
              "Filter\\(condition=\\[=\\(\\$2, 7\\)\\]\\).*rowcount = 46.2,.*",
              "Scan.*columns=\\[`department_id`, `employee_id`, `store_id`\\].*rowcount = 1155.0.*",
              "Scan.*columns=\\[`department_id`\\].*rowcount = 12.0.*"};
      PlanTestBase.testPlanWithAttributesMatchingPatterns(query, expectedPlan10, new String[]{});

      query = " select emp.employee_id from dfs.tmp.employeeUseStat emp join dfs.tmp.departmentUseStat dept\n"
              + " on emp.department_id = dept.department_id "
              + " group by emp.employee_id "
              + " having emp.employee_id = 7";
      String[] expectedPlan11 = {"StreamAgg\\(group=\\[\\{0\\}\\]\\).*rowcount = 1.0.*",
              "HashJoin\\(condition.*\\).*rowcount = 1.0,.*",
              "Filter\\(condition=\\[=\\(\\$1, 7\\)\\]\\).*rowcount = 1.0.*",
              "Scan.*columns=\\[`department_id`\\].*rowcount = 12.0.*",
              "Scan.*columns=\\[`department_id`, `employee_id`\\].*rowcount = 1155.0.*"};
      PlanTestBase.testPlanWithAttributesMatchingPatterns(query, expectedPlan11, new String[]{});
    } finally {
      resetSessionOption("planner.slice_target");
      resetSessionOption("store.format");
    }
  }

  @Test
  public void testWithMetadataCaching() throws Exception {
    test("ALTER SESSION SET `planner.slice_target` = 1");
    test("ALTER SESSION SET `store.format` = 'parquet'");
    test("ALTER SESSION SET `planner.statistics.use` = true");
    final String tmpLocation = "/multilevel/parquet";
    try {
      // copy the data into the temporary location
      test("DROP TABLE dfs.tmp.parquetStale");
      test("CREATE TABLE dfs.tmp.parquetStale AS SELECT o_orderkey, o_custkey, o_orderstatus, " +
              "o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment from dfs.`%s`", tmpLocation);
      String query = "select count(distinct o_orderkey) from dfs.tmp.parquetStale";
      verifyAnalyzeOutput("ANALYZE TABLE dfs.tmp.parquetStale COMPUTE STATISTICS", "9");
      test("REFRESH TABLE METADATA dfs.tmp.parquetStale");
      // Verify we recompute statistics once a new file/directory is added. Update the directory some
      // time after ANALYZE so that the timestamps are different.
      Thread.sleep(1000);
      final String Q4 = "/multilevel/parquet/1996/Q4";
      test("CREATE TABLE dfs.tmp.`parquetStale/1996/Q5` AS SELECT o_orderkey, o_custkey, o_orderstatus, " +
              "o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment from dfs.`%s`", Q4);
      // query should use STALE statistics
      String[] expectedStalePlan = {"StreamAgg\\(group=\\[\\{0\\}\\]\\).*rowcount = 119.0.*",
          "Scan.*rowcount = 130.0.*"};
      PlanTestBase.testPlanWithAttributesMatchingPatterns(query, expectedStalePlan, new String[]{});
      // Query should use Parquet Metadata, since statistics not available. In this case, NDV is computed as
      // 1/10*rowcount (Calcite default). Hence, NDV is 13.0 instead of the correct 119.0
      test("DROP TABLE dfs.tmp.`parquetStale/.stats.drill`");
      String[] expectedPlan1 = {"HashAgg\\(group=\\[\\{0\\}\\]\\).*rowcount = 13.0.*",
          "Scan.*rowcount = 130.0.*"};
      PlanTestBase.testPlanWithAttributesMatchingPatterns(query, expectedPlan1, new String[]{});
      // query should use the new statistics. NDV remains unaffected since we copy the Q4 into Q5
      verifyAnalyzeOutput("ANALYZE TABLE dfs.tmp.parquetStale COMPUTE STATISTICS", "9");
      String[] expectedPlan2 = {"StreamAgg\\(group=\\[\\{0\\}\\]\\).*rowcount = 119.0.*",
          "Scan.*rowcount = 130.0.*"};
      PlanTestBase.testPlanWithAttributesMatchingPatterns(query, expectedPlan2, new String[]{});
      test("DROP TABLE dfs.tmp.`parquetStale/1996/Q5`");
    } finally {
      resetSessionOption("planner.slice_target");
      resetSessionOption("store.format");
      resetSessionOption("planner.statistics.use");
    }
  }

  // Test basic histogram creation functionality for int, bigint, double, date, timestamp and boolean data types.
  // Test that varchar column does not fail the query but generates empty buckets.
  // Use Repeated_Count for checking number of entries, but currently we don't check actual contents of the
  // buckets since that requires enforcing a repeatable t-digest quantile that is used by histogram and is future work.
  @Test
  public void testHistogramWithDataTypes1() throws Exception {
    try {
      test("ALTER SESSION SET `planner.slice_target` = 1");
      test("ALTER SESSION SET `store.format` = 'parquet'");
      test("CREATE TABLE dfs.tmp.employee1 AS SELECT  employee_id, full_name, "
              + "case when gender = 'M' then cast(1 as boolean) else cast(0 as boolean) end as is_male, "
              + " cast(store_id as int) as store_id, cast(department_id as bigint) as department_id, "
              + " cast(birth_date as date) as birth_date, cast(hire_date as timestamp) as hire_date_and_time, "
              + " cast(salary as double) as salary from cp.`employee.json` where department_id > 10");
      test("ANALYZE TABLE dfs.tmp.employee1 COMPUTE STATISTICS");

      testBuilder()
              .sqlQuery("SELECT tbl.`columns`.`column` as `column`, "
                      + " repeated_count(tbl.`columns`.`histogram`.`buckets`) as num_bucket_entries "
                      + " from (select flatten(`directories`[0].`columns`) as `columns` "
                      + "  from dfs.tmp.`employee1/.stats.drill`) as tbl")
              .unOrdered()
              .baselineColumns("column", "num_bucket_entries")
              .baselineValues("`employee_id`", 11)
              .baselineValues("`full_name`", 0)
              .baselineValues("`is_male`", 3)
              .baselineValues("`store_id`", 11)
              .baselineValues("`department_id`", 8)
              .baselineValues("`birth_date`", 11)
              .baselineValues("`hire_date_and_time`", 7)
              .baselineValues("`salary`", 11)
              .go();

      // test the use of the just created histogram
      test("alter session set `planner.statistics.use` = true");

      // check boundary conditions: last bucket
      String query = "select 1 from dfs.tmp.employee1 where store_id > 21";
      String[] expectedPlan1 = {"Filter\\(condition.*\\).*rowcount = 112.*,.*",
              "Scan.*columns=\\[`store_id`\\].*rowcount = 1128.0.*"};
      PlanTestBase.testPlanWithAttributesMatchingPatterns(query, expectedPlan1, new String[]{});

      query = "select 1 from dfs.tmp.employee1 where store_id < 15";
      String[] expectedPlan2 = {"Filter\\(condition.*\\).*rowcount = 676.*,.*",
              "Scan.*columns=\\[`store_id`\\].*rowcount = 1128.0.*"};
      PlanTestBase.testPlanWithAttributesMatchingPatterns(query, expectedPlan2, new String[]{});

      query = "select 1 from dfs.tmp.employee1 where store_id between 1 and 23";
      String[] expectedPlan3 = {"Filter\\(condition.*\\).*rowcount = 1090.*,.*",
        "Scan.*columns=\\[`store_id`\\].*rowcount = 1128.0.*"};
      PlanTestBase.testPlanWithAttributesMatchingPatterns(query, expectedPlan3, new String[]{});

      query = "select count(*) from dfs.tmp.employee1 where store_id between 10 and 20";
      String[] expectedPlan4 = {"Filter\\(condition.*\\).*rowcount = 5??.*,.*",
        "Scan.*columns=\\[`store_id`\\].*rowcount = 1128.0.*"};
      PlanTestBase.testPlanWithAttributesMatchingPatterns(query, expectedPlan4, new String[]{});

      // col > end_point of last bucket
      query = "select 1 from dfs.tmp.employee1 where store_id > 24";
      String[] expectedPlan5 = {"Filter\\(condition.*\\).*rowcount = 1.0,.*",
        "Scan.*columns=\\[`store_id`\\].*rowcount = 1128.0.*"};
      PlanTestBase.testPlanWithAttributesMatchingPatterns(query, expectedPlan5, new String[]{});

      // col < start_point of first bucket
      query = "select 1 from dfs.tmp.employee1 where store_id < 1";
      String[] expectedPlan6 = {"Filter\\(condition.*\\).*rowcount = 1.0,.*",
        "Scan.*columns=\\[`store_id`\\].*rowcount = 1128.0.*"};
      PlanTestBase.testPlanWithAttributesMatchingPatterns(query, expectedPlan6, new String[]{});
    } finally {
      resetSessionOption("planner.slice_target");
      resetSessionOption("store.format");
      resetSessionOption("planner.statistics.use");
    }
  }

  @Test
  public void testHistogramWithSubsetColumnsAndSampling() throws Exception {
    try {
      test("ALTER SESSION SET `planner.slice_target` = 1");
      test("ALTER SESSION SET `store.format` = 'parquet'");
      test("CREATE TABLE dfs.tmp.customer1 AS SELECT  * from cp.`tpch/customer.parquet`");
      test("ANALYZE TABLE dfs.tmp.customer1 COMPUTE STATISTICS (c_custkey, c_nationkey, c_acctbal) SAMPLE 55 PERCENT");

      testBuilder()
              .sqlQuery("SELECT tbl.`columns`.`column` as `column`, "
                      + " repeated_count(tbl.`columns`.`histogram`.`buckets`) as num_bucket_entries "
                      + " from (select flatten(`directories`[0].`columns`) as `columns` "
                      + "  from dfs.tmp.`customer1/.stats.drill`) as tbl")
              .unOrdered()
              .baselineColumns("column", "num_bucket_entries")
              .baselineValues("`c_custkey`", 11)
              .baselineValues("`c_nationkey`", 11)
              .baselineValues("`c_acctbal`", 11)
              .go();
    } finally {
      resetSessionOption("planner.slice_target");
      resetSessionOption("store.format");
    }
  }

  @Test
  public void testHistogramWithColumnsWithAllNulls() throws Exception {
    try {
      test("ALTER SESSION SET `planner.slice_target` = 1");
      test("ALTER SESSION SET `store.format` = 'parquet'");
      test("CREATE TABLE dfs.tmp.all_nulls AS SELECT employee_id, cast(null as int) as null_int_col, "
              + "cast(null as bigint) as null_bigint_col, cast(null as float) as null_float_col, "
              + "cast(null as double) as null_double_col, cast(null as date) as null_date_col, "
              + "cast(null as timestamp) as null_timestamp_col, cast(null as time) as null_time_col, "
              + "cast(null as boolean) as null_boolean_col "
              + "from cp.`employee.json` ");
      test("ANALYZE TABLE dfs.tmp.all_nulls COMPUTE STATISTICS ");

      testBuilder()
              .sqlQuery("SELECT tbl.`columns`.`column` as `column`, "
                      + " repeated_count(tbl.`columns`.`histogram`.`buckets`) as num_bucket_entries "
                      + " from (select flatten(`directories`[0].`columns`) as `columns` "
                      + "  from dfs.tmp.`all_nulls/.stats.drill`) as tbl")
              .unOrdered()
              .baselineColumns("column", "num_bucket_entries")
              .baselineValues("`employee_id`", 11)
              .baselineValues("`null_int_col`", 0)
              .baselineValues("`null_bigint_col`", 0)
              .baselineValues("`null_float_col`", 0)
              .baselineValues("`null_double_col`", 0)
              .baselineValues("`null_date_col`", 0)
              .baselineValues("`null_timestamp_col`", 0)
              .baselineValues("`null_time_col`", 0)
              .baselineValues("`null_boolean_col`", 0)
              .go();

    } finally {
      resetSessionOption("planner.slice_target");
      resetSessionOption("store.format");
    }
  }

  @Test
  public void testHistogramWithIntervalPredicate() throws Exception {
    try {
      test("ALTER SESSION SET `planner.slice_target` = 1");
      test("ALTER SESSION SET `store.format` = 'parquet'");
      test("create table dfs.tmp.orders2 as select * from cp.`tpch/orders.parquet`");
      test("analyze table dfs.tmp.orders2 compute statistics");
      test("alter session set `planner.statistics.use` = true");

      String query = "select 1 from dfs.tmp.orders2 o where o.o_orderdate >= date '1996-10-01' and o.o_orderdate < date '1996-10-01' + interval '3' month";
      String[] expectedPlan1 = {"Filter\\(condition.*\\).*rowcount = 59?.*,.*", "Scan.*columns=\\[`o_orderdate`\\].*rowcount = 15000.0.*"};
      PlanTestBase.testPlanWithAttributesMatchingPatterns(query, expectedPlan1, new String[]{});
    } finally {
      resetSessionOption("planner.slice_target");
      resetSessionOption("store.format");
    }
  }

  //Helper function to verify output of ANALYZE statement
  private void verifyAnalyzeOutput(String query, String message) throws Exception {
    List<QueryDataBatch>result = testRunAndReturn(QueryType.SQL, query);
    List<List<String>> output = new ArrayList<>();
    assertTrue(result.size() == 1);
    final QueryDataBatch batch = result.get(0);
    final RecordBatchLoader loader = new RecordBatchLoader(getDrillbitContext().getAllocator());
    loader.load(batch.getHeader().getDef(), batch.getData());
    output.add(new ArrayList<String>());
    for (VectorWrapper<?> vw: loader) {
      ValueVector.Accessor accessor = vw.getValueVector().getAccessor();
      Object o = accessor.getObject(0);
      output.get(0).add(o == null ? null: o.toString());
    }
    batch.release();
    loader.clear();
    assertTrue(output.get(0).size() == 2);
    assertEquals(message, output.get(0).get(1));
  }
}
