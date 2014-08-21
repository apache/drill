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

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.drill.PlanTestBase;
import org.apache.drill.common.util.TestTools;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.proto.UserBitShared.QueryType;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.hadoop.fs.Path;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestAnalyze extends PlanTestBase {

  private static final String WORKING_PATH = TestTools.getWorkingPath();
  private static final String TEST_RES_PATH = WORKING_PATH + "/src/test/resources";

  @BeforeClass
  public static void copyData() throws Exception {
    // copy the data into the temporary location
    String tmpLocation = getDfsTestTmpSchemaLocation();
    File dataDir1 = new File(tmpLocation + Path.SEPARATOR + "parquet1");
    dataDir1.mkdir();
    FileUtils.copyDirectory(new File(String.format(String.format("%s/multilevel/parquet", TEST_RES_PATH))),
        dataDir1);
  }

  // Analyze for all columns
  @Test
  public void basic1() throws Exception {
    try {
      test("ALTER SESSION SET `planner.slice_target` = 1");
      test("ALTER SESSION SET `store.format` = 'parquet'");
      test("CREATE TABLE dfs_test.tmp.region_basic1 AS SELECT * from cp.`region.json`");
      test("ANALYZE TABLE dfs_test.tmp.region_basic1 COMPUTE STATISTICS");
      test("SELECT * FROM dfs_test.tmp.`region_basic1/.stats.drill`");
      test("create table dfs_test.tmp.flatstats1 as select flatten(`directories`[0].`columns`) as `columns`"
              + " from dfs_test.tmp.`region_basic1/.stats.drill`");

      testBuilder()
          .sqlQuery("SELECT tbl.`columns`.`column` as `column`, tbl.`columns`.statcount as statcount,"
              + " tbl.`columns`.nonnullstatcount as nonnullstatcount, tbl.`columns`.ndv as ndv,"
              + " tbl.`columns`.avgwidth as avgwidth"
              + " FROM dfs_test.tmp.flatstats1 tbl")
          .unOrdered()
          .baselineColumns("column", "statcount", "nonnullstatcount", "ndv", "avgwidth")
          .baselineValues("region_id", 110.0, 110.0, 107L, 8.0)
          .baselineValues("sales_city", 110.0, 110.0, 111L, 8.663636363636364)
          .baselineValues("sales_state_province", 110.0, 110.0, 13L, 2.4272727272727272)
          .baselineValues("sales_district", 110.0, 110.0, 22L, 9.318181818181818)
          .baselineValues("sales_region", 110.0, 110.0, 8L, 10.8)
          .baselineValues("sales_country", 110.0, 110.0, 4L, 3.909090909090909)
          .baselineValues("sales_district_id", 110.0, 110.0, 23L, 8.0)
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
      test("ALTER SESSION SET `store.format` = 'parquet'");
      test("CREATE TABLE dfs_test.tmp.employee_basic2 AS SELECT * from cp.`employee.json`");
      test("ANALYZE TABLE dfs_test.tmp.employee_basic2 COMPUTE STATISTICS (employee_id, birth_date)");
      test("SELECT * FROM dfs_test.tmp.`employee_basic2/.stats.drill`");
      test("create table dfs_test.tmp.flatstats2 as select flatten(`directories`[0].`columns`) as `columns`"
          + " from dfs_test.tmp.`employee_basic2/.stats.drill`");

      testBuilder()
          .sqlQuery("SELECT tbl.`columns`.`column` as `column`, tbl.`columns`.statcount as statcount,"
              + " tbl.`columns`.nonnullstatcount as nonnullstatcount, tbl.`columns`.ndv as ndv,"
              + " tbl.`columns`.avgwidth as avgwidth"
              + " FROM dfs_test.tmp.flatstats2 tbl")
          .unOrdered()
          .baselineColumns("column", "statcount", "nonnullstatcount", "ndv", "avgwidth")
          .baselineValues("employee_id", 1155.0, 1155.0, 1144L, 8.0)
          .baselineValues("birth_date", 1155.0, 1155.0, 53L, 10.0)
          .go();
    } finally {
      test("ALTER SESSION SET `planner.slice_target` = " + ExecConstants.SLICE_TARGET_DEFAULT);
    }
  }

  @Test
  public void join() throws Exception {
    try {
      test("ALTER SESSION SET `planner.slice_target` = 1");
      test("ALTER SESSION SET `store.format` = 'parquet'");
      test("CREATE TABLE dfs_test.tmp.lineitem AS SELECT * FROM cp.`tpch/lineitem.parquet`");
      test("CREATE TABLE dfs_test.tmp.orders AS select * FROM cp.`tpch/orders.parquet`");
      test("ANALYZE TABLE dfs_test.tmp.lineitem COMPUTE STATISTICS");
      test("ANALYZE TABLE dfs_test.tmp.orders COMPUTE STATISTICS");
      test("SELECT * FROM dfs_test.tmp.`lineitem/.stats.drill`");
      test("SELECT * FROM dfs_test.tmp.`orders/.stats.drill`");

      test("SELECT * FROM dfs_test.tmp.`lineitem` l JOIN dfs_test.tmp.`orders` o ON l.l_orderkey = o.o_orderkey");
    } finally {
      test("ALTER SESSION SET `planner.slice_target` = " + ExecConstants.SLICE_TARGET_DEFAULT);
    }
  }

  @Test
  public void testAnalyzeSupportedFormats() throws Exception {
    //Only allow computing statistics on PARQUET files.
    try {
      test("ALTER SESSION SET `planner.slice_target` = 1");
      test("ALTER SESSION SET `store.format` = 'json'");
      test("CREATE TABLE dfs_test.tmp.employee_basic3 AS SELECT * from cp.`employee.json`");
      //Should display not supported
      verifyAnalyzeOutput("ANALYZE TABLE dfs_test.tmp.employee_basic3 COMPUTE STATISTICS",
          "Table employee_basic3 is not supported by ANALYZE. "
          + "Support is currently limited to directory-based Parquet tables.");

      test("DROP TABLE dfs_test.tmp.employee_basic3");
      test("ALTER SESSION SET `store.format` = 'parquet'");
      test("CREATE TABLE dfs_test.tmp.employee_basic3 AS SELECT * from cp.`employee.json`");
      //Should complete successfully (16 columns in employee.json)
      verifyAnalyzeOutput("ANALYZE TABLE dfs_test.tmp.employee_basic3 COMPUTE STATISTICS",
          "16");
    } finally {
      test("ALTER SESSION SET `planner.slice_target` = " + ExecConstants.SLICE_TARGET_DEFAULT);
    }
  }

  @Test
  public void testAnalyzePartitionedTables() throws Exception {
    //Computing statistics on columns, dir0, dir1
    try {
      test("ALTER SESSION SET `planner.slice_target` = 1");
      test("ALTER SESSION SET `store.format` = 'parquet'");
      verifyAnalyzeOutput(String.format("ANALYZE TABLE dfs_test.`%s/%s` COMPUTE STATISTICS",
          getDfsTestTmpSchemaLocation(), "parquet1"), "11");
      test(String.format("SELECT * FROM dfs_test.`%s/%s/.stats.drill`", getDfsTestTmpSchemaLocation(), "parquet1"));
      test(String.format("create table dfs_test.tmp.flatstats3 as select flatten(`directories`[0].`columns`)"
          + " as `columns` from dfs_test.`%s/%s/.stats.drill`", getDfsTestTmpSchemaLocation(), "parquet1"));
      //Verify statistics
      testBuilder()
          .sqlQuery("SELECT tbl.`columns`.`column` as `column`, tbl.`columns`.statcount as statcount,"
              + " tbl.`columns`.nonnullstatcount as nonnullstatcount, tbl.`columns`.ndv as ndv,"
              + " tbl.`columns`.avgwidth as avgwidth"
              + " FROM dfs_test.tmp.flatstats3 tbl")
          .unOrdered()
          .baselineColumns("column", "statcount", "nonnullstatcount", "ndv", "avgwidth")
          .baselineValues("o_orderkey", 120.0, 120.0, 120L, 4.0)
          .baselineValues("o_custkey", 120.0, 120.0, 110L, 4.0)
          .baselineValues("o_orderstatus", 120.0, 120.0, 3L, 1.0)
          .baselineValues("o_totalprice", 120.0, 120.0, 121L, 8.0)
          .baselineValues("o_orderdate", 120.0, 120.0, 111L, 4.0)
          .baselineValues("o_orderpriority", 120.0, 120.0, 5L, 8.458333333333334)
          .baselineValues("o_clerk", 120.0, 120.0, 114L, 15.0)
          .baselineValues("o_shippriority", 120.0, 120.0, 1L, 4.0)
          .baselineValues("o_comment", 120.0, 120.0, 115L, 46.333333333333336)
          .baselineValues("dir0", 120.0, 120.0, 3L, 4.0)
          .baselineValues("dir1", 120.0, 120.0, 4L, 2.0)
          .go();
    } finally {
      test("ALTER SESSION SET `planner.slice_target` = " + ExecConstants.SLICE_TARGET_DEFAULT);
    }
  }

  @Test
  public void testStaleness() throws Exception {
    // copy the data into the temporary location
    String tmpLocation = getDfsTestTmpSchemaLocation();
    File dataDir1 = new File(tmpLocation + Path.SEPARATOR + "parquetStale");
    dataDir1.mkdir();
    FileUtils.copyDirectory(new File(String.format(String.format("%s/multilevel/parquet", TEST_RES_PATH))),
            dataDir1);
    test("ALTER SESSION SET `planner.slice_target` = 1");
    test("ALTER SESSION SET `store.format` = 'parquet'");
    verifyAnalyzeOutput(String.format("ANALYZE TABLE dfs_test.`%s/%s` COMPUTE STATISTICS",
        tmpLocation, "parquetStale"), "11");
    verifyAnalyzeOutput(String.format("ANALYZE TABLE dfs_test.`%s/%s` COMPUTE STATISTICS",
        tmpLocation, "parquetStale"),
        String.format("Table %s/%s has not changed since last ANALYZE!", tmpLocation, "parquetStale"));
    // Verify we recompute statistics once a new file/directory is added. Update the directory some
    // time after ANALYZE so that the timestamps are different.
    File Q4 = new File(tmpLocation + Path.SEPARATOR + "parquetStale" + Path.SEPARATOR + "1996"
        + Path.SEPARATOR + "Q4");
    File Q5 = new File(tmpLocation + Path.SEPARATOR + "parquetStale" + Path.SEPARATOR + "1996"
        + Path.SEPARATOR + "Q5");
    Thread.sleep(1000);
    FileUtils.copyDirectory(Q4, Q5);
    verifyAnalyzeOutput(String.format("ANALYZE TABLE dfs_test.`%s/%s` COMPUTE STATISTICS",
            tmpLocation, "parquetStale"), "11");
    Thread.sleep(1000);
    FileUtils.deleteDirectory(Q5);
    verifyAnalyzeOutput(String.format("ANALYZE TABLE dfs_test.`%s/%s` COMPUTE STATISTICS",
            tmpLocation, "parquetStale"), "11");
  }

  @Test
  public void testCapabilityVersion() throws Exception {
    test("ALTER SESSION SET `planner.slice_target` = 1");
    test("ALTER SESSION SET `store.format` = 'parquet'");
    test("ALTER SESSION SET `exec.statistics.capability_version` = 0");
    test("CREATE TABLE dfs_test.tmp.employeeCapVer AS SELECT * from cp.`employee.json`");
    verifyAnalyzeOutput("ANALYZE TABLE dfs_test.tmp.employeeCapVer COMPUTE STATISTICS",
            "16");
    testBuilder()
        .sqlQuery("SELECT tbl.`statistics_version`, tbl.`directories`[0].`computed` as computed"
            + " FROM dfs_test.tmp.`employeeCapVer/.stats.drill` tbl")
        .unOrdered()
        .baselineColumns("statistics_version", "computed")
        .baselineValues("v0", 3.141592653589793)
        .go();
    test("DROP TABLE dfs_test.tmp.`employeeCapVer/.stats.drill`");
    test("ALTER SESSION SET `exec.statistics.capability_version` = 1");
    verifyAnalyzeOutput("ANALYZE TABLE dfs_test.tmp.employeeCapVer COMPUTE STATISTICS",
            "16");
    testBuilder()
        .sqlQuery("SELECT statistics_version"
            + " FROM dfs_test.tmp.`employeeCapVer/.stats.drill` tbl")
        .unOrdered()
        .baselineColumns("statistics_version")
        .baselineValues("v1")
        .go();
    //Cannot exceed the current capability version - keep in sync with current capability_version
    errorMsgTestHelper("ALTER SESSION SET `exec.statistics.capability_version` = 2",
        "VALIDATION ERROR: Option exec.statistics.capability_version must be between 0 and 1");
  }

  @Test
  public void testUseStatistics() throws Exception {
    //Test ndv/rowcount for scan
    test("ALTER SESSION SET `planner.slice_target` = 1");
    test("ALTER SESSION SET `store.format` = 'parquet'");
    test("CREATE TABLE dfs_test.tmp.employeeUseStat AS SELECT * from cp.`employee.json`");
    test("CREATE TABLE dfs_test.tmp.departmentUseStat AS SELECT * from cp.`department.json`");
    test("ANALYZE TABLE dfs_test.tmp.employeeUseStat COMPUTE STATISTICS");
    test("ANALYZE TABLE dfs_test.tmp.departmentUseStat COMPUTE STATISTICS");

    String query = " select employee_id from dfs_test.tmp.employeeUseStat where department_id = 2";
    String[] expectedPlan1 = {"Filter\\(condition.*\\).*rowcount = 96.25,.*",
            "Scan.*columns=\\[`department_id`, `employee_id`\\].*rowcount = 1155.0.*"};
    PlanTestBase.testPlanWithAttributesMatchingPatterns(query, expectedPlan1, new String[]{});

    query = " select employee_id from dfs_test.tmp.employeeUseStat where department_id IN (2, 5)";
    String[] expectedPlan2 = {"Filter\\(condition.*\\).*rowcount = 192.5,.*",
            "Scan.*columns=\\[`department_id`, `employee_id`\\].*rowcount = 1155.0.*"};
    PlanTestBase.testPlanWithAttributesMatchingPatterns(query, expectedPlan2, new String[]{});

    query = "select employee_id from dfs_test.tmp.employeeUseStat where department_id IN (2, 5) and employee_id = 5";
    String[] expectedPlan3 = {"Filter\\(condition.*\\).*rowcount = 1.0,.*",
            "Scan.*columns=\\[`department_id`, `employee_id`\\].*rowcount = 1155.0.*"};
    PlanTestBase.testPlanWithAttributesMatchingPatterns(query, expectedPlan3, new String[]{});

    query = " select emp.employee_id from dfs_test.tmp.employeeUseStat emp join dfs_test.tmp.departmentUseStat dept"
        + " on emp.department_id = dept.department_id";
    String[] expectedPlan4 = {"HashJoin\\(condition.*\\).*rowcount = 1154.9999999999995,.*",
            "Scan.*columns=\\[`department_id`, `employee_id`\\].*rowcount = 1155.0.*",
            "Scan.*columns=\\[`department_id`\\].*rowcount = 12.0.*"};
    PlanTestBase.testPlanWithAttributesMatchingPatterns(query, expectedPlan4, new String[]{});

    query = " select emp.employee_id from dfs_test.tmp.employeeUseStat emp join dfs_test.tmp.departmentUseStat dept"
            + " on emp.department_id = dept.department_id where dept.department_id = 5";
    String[] expectedPlan5 = {"HashJoin\\(condition.*\\).*rowcount = 96.24999999999997,.*",
            "Scan.*columns=\\[`department_id`, `employee_id`\\].*rowcount = 1155.0.*",
            "Scan.*columns=\\[`department_id`\\].*rowcount = 12.0.*"};
    PlanTestBase.testPlanWithAttributesMatchingPatterns(query, expectedPlan5, new String[]{});

    query = " select emp.employee_id from dfs_test.tmp.employeeUseStat emp join dfs_test.tmp.departmentUseStat dept"
            + " on emp.department_id = dept.department_id"
            + " where dept.department_id = 5 and emp.employee_id = 10";
    String[] expectedPlan6 = {"MergeJoin\\(condition.*\\).*rowcount = 1.0096153846153846,.*",
            "Filter\\(condition=\\[=\\(\\$1, 10\\)\\]\\).*rowcount = 1.0096153846153846,.*",
            "Scan.*columns=\\[`department_id`, `employee_id`\\].*rowcount = 1155.0.*",
            "Filter\\(condition=\\[=\\(\\$0, 5\\)\\]\\).*rowcount = 1.0,.*",
            "Scan.*columns=\\[`department_id`\\].*rowcount = 12.0.*"};
    PlanTestBase.testPlanWithAttributesMatchingPatterns(query, expectedPlan6, new String[]{});

    query = " select emp.employee_id, count(*)"
            + " from dfs_test.tmp.employeeUseStat emp"
            + " group by emp.employee_id";
    String[] expectedPlan7 = {"HashAgg\\(group=\\[\\{0\\}\\], EXPR\\$1=\\[COUNT\\(\\)\\]\\).*rowcount = 1144.0,.*",
            "Scan.*columns=\\[`employee_id`\\].*rowcount = 1155.0.*"};
    PlanTestBase.testPlanWithAttributesMatchingPatterns(query, expectedPlan7, new String[]{});

    query = " select emp.employee_id from dfs_test.tmp.employeeUseStat emp join dfs_test.tmp.departmentUseStat dept"
            + " on emp.department_id = dept.department_id "
            + " group by emp.employee_id";
    String[] expectedPlan8 = {"HashAgg\\(group=\\[\\{0\\}\\]\\).*rowcount = 1107.7467109294196",
            "HashJoin\\(condition.*\\).*rowcount = 1154.9999999999995,.*",
            "Scan.*columns=\\[`employee_id`, `department_id`\\].*rowcount = 1155.0.*",
            "Scan.*columns=\\[`department_id`\\].*rowcount = 12.0.*"};
    PlanTestBase.testPlanWithAttributesMatchingPatterns(query, expectedPlan8, new String[]{});

    query = "select emp.employee_id, dept.department_description"
            + " from dfs_test.tmp.employeeUseStat emp join dfs_test.tmp.departmentUseStat dept"
            + " on emp.department_id = dept.department_id "
            + " group by emp.employee_id, emp.store_id, dept.department_description "
            + " having dept.department_description = 'FINANCE'";
    String[] expectedPlan9 = {"HashAgg\\(group=\\[\\{0, 1, 2\\}\\]\\).*rowcount = 92.31302263593369.*",
            "HashJoin\\(condition.*\\).*rowcount = 96.24999999999997,.*",
            "Scan.*columns=\\[`employee_id`, `store_id`, `department_id`\\].*rowcount = 1155.0.*",
            "Filter\\(condition=\\[=\\(\\$0, 'FINANCE'\\)\\]\\).*rowcount = 1.0,.*",
            "Scan.*columns=\\[`department_description`, `department_id`\\].*rowcount = 12.0.*"};
    PlanTestBase.testPlanWithAttributesMatchingPatterns(query, expectedPlan9, new String[]{});

    query = " select emp.employee_id from dfs_test.tmp.employeeUseStat emp join dfs_test.tmp.departmentUseStat dept\n"
            + " on emp.department_id = dept.department_id "
            + " group by emp.employee_id, emp.store_id "
            + " having emp.store_id = 7";
    String[] expectedPlan10 = {"HashAgg\\(group=\\[\\{0, 1\\}\\]\\).*rowcount = 11.744643162739475.*",
            "HashJoin\\(condition.*\\).*rowcount = 46.20000000000001,.*",
            "Filter\\(condition=\\[=\\(\\$1, 7\\)\\]\\).*rowcount = 46.2,.*",
            "Scan.*columns=\\[`employee_id`, `store_id`, `department_id`\\].*rowcount = 1155.0.*",
            "Scan.*columns=\\[`department_id`\\].*rowcount = 12.0.*"};
    PlanTestBase.testPlanWithAttributesMatchingPatterns(query, expectedPlan10, new String[]{});

    query = " select emp.employee_id from dfs_test.tmp.employeeUseStat emp join dfs_test.tmp.departmentUseStat dept\n"
            + " on emp.department_id = dept.department_id "
            + " group by emp.employee_id "
            + " having emp.employee_id = 7";
    String[] expectedPlan11 = {"StreamAgg\\(group=\\[\\{0\\}\\]\\).*rowcount = 7.585446705942692",
            "HashJoin\\(condition.*\\).*rowcount = 12.0,.*",
            "Filter\\(condition=\\[=\\(\\$0, 7\\)\\]\\).*rowcount = 1.0096153846153846,.*",
            "Scan.*columns=\\[`employee_id`, `department_id`\\].*rowcount = 1155.0.*",
            "Scan.*columns=\\[`department_id`\\].*rowcount = 12.0.*"};
    PlanTestBase.testPlanWithAttributesMatchingPatterns(query, expectedPlan11, new String[]{});
  }

  @Test
  public void testWithMetadataCaching() throws Exception {
    // copy the data into the temporary location
    test("ALTER SESSION SET `planner.slice_target` = 1");
    test("ALTER SESSION SET `store.format` = 'json'");
    String tmpLocation = getDfsTestTmpSchemaLocation();
    String query = String.format("select count(o_orderkey) from dfs_test.`%s/%s`", tmpLocation, "parquetStale");
    File dataDir1 = new File(tmpLocation + Path.SEPARATOR + "parquetStale");
    dataDir1.mkdir();
    FileUtils.copyDirectory(new File(String.format(String.format("%s/multilevel/parquet", TEST_RES_PATH))),
            dataDir1);
    FileUtils.deleteDirectory(new File(tmpLocation + Path.SEPARATOR + "parquetStale"
            + Path.SEPARATOR + ".stats.drill"));
    verifyAnalyzeOutput(String.format("ANALYZE TABLE dfs_test.`%s/%s` COMPUTE STATISTICS",
            getDfsTestTmpSchemaLocation(), "parquetStale"), "11");
    test(String.format("REFRESH TABLE METADATA dfs_test.`%s/%s`", tmpLocation, "parquetStale"));
    // Verify we recompute statistics once a new file/directory is added. Update the directory some
    // time after ANALYZE so that the timestamps are different.
    File Q4 = new File(tmpLocation + Path.SEPARATOR + "parquetStale" + Path.SEPARATOR
        + "1996" + Path.SEPARATOR + "Q4");
    File Q5 = new File(tmpLocation + Path.SEPARATOR + "parquetStale" + Path.SEPARATOR
        + "1996" + Path.SEPARATOR + "Q5");
    Thread.sleep(1000);
    FileUtils.copyDirectory(Q4, Q5);
    //test (String.format("REFRESH TABLE METADATA dfs_test.`%s/%s`", tmpLocation, "parquetStale"));
    // query should use STALE statistics
    String[] expectedStalePlan = {"Scan.*rowcount = 120.0.*"};
    PlanTestBase.testPlanWithAttributesMatchingPatterns(query, expectedStalePlan, new String[]{});
    // query should use Parquet Metadata, since statistics not available
    FileUtils.deleteDirectory(new File(tmpLocation + Path.SEPARATOR + "parquetStale"
        + Path.SEPARATOR + ".stats.drill"));
    // Issue with Metadata Cache?
    String[] expectedPlan1 = {"Scan.*rowcount = 20.0.*"};
    PlanTestBase.testPlanWithAttributesMatchingPatterns(query, expectedPlan1, new String[]{});
    // query should use the new statistics
    verifyAnalyzeOutput(String.format("ANALYZE TABLE dfs_test.`%s/%s` COMPUTE STATISTICS",
            getDfsTestTmpSchemaLocation(), "parquetStale"), "11");
    String[] expectedPlan2 = {"Scan.*rowcount = 130.0.*"};
    PlanTestBase.testPlanWithAttributesMatchingPatterns(query, expectedPlan2, new String[]{});
    FileUtils.deleteDirectory(Q5);
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
