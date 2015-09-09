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
package org.apache.drill.exec;

import org.apache.drill.BaseTestQuery;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.common.util.TestTools;
import org.apache.drill.exec.work.foreman.SqlUnsupportedException;
import org.apache.drill.exec.work.foreman.UnsupportedFunctionException;
import org.apache.drill.PlanTestBase;

import org.junit.Test;

public class TestWindowFunctions extends BaseTestQuery {
  static final String WORKING_PATH = TestTools.getWorkingPath();
  static final String TEST_RES_PATH = WORKING_PATH + "/src/test/resources";

  private static void throwAsUnsupportedException(UserException ex) throws Exception {
    SqlUnsupportedException.errorClassNameToException(ex.getOrCreatePBError(false).getException().getExceptionClass());
    throw ex;
  }

  @Test // DRILL-3196
  public void testSinglePartition() throws Exception {
    final String query = "select sum(n_nationKey) over(partition by n_nationKey) as col1, count(*) over(partition by n_nationKey) as col2 \n" +
        "from cp.`tpch/nation.parquet`";

    // Validate the plan
    final String[] expectedPlan = {"Window.*partition \\{0\\} order by \\[\\].*\\[SUM\\(\\$0\\), COUNT\\(\\)",
        "Scan.*columns=\\[`n_nationKey`\\].*"};
    final String[] excludedPatterns = {"Scan.*columns=\\[`\\*`\\].*"};
    PlanTestBase.testPlanMatchingPatterns(query, expectedPlan, excludedPatterns);

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("col1", "col2")
        .baselineValues(0l, 1l)
        .baselineValues(1l, 1l)
        .baselineValues(2l, 1l)
        .baselineValues(3l, 1l)
        .baselineValues(4l, 1l)
        .baselineValues(5l, 1l)
        .baselineValues(6l, 1l)
        .baselineValues(7l, 1l)
        .baselineValues(8l, 1l)
        .baselineValues(9l, 1l)
        .baselineValues(10l, 1l)
        .baselineValues(11l, 1l)
        .baselineValues(12l, 1l)
        .baselineValues(13l, 1l)
        .baselineValues(14l, 1l)
        .baselineValues(15l, 1l)
        .baselineValues(16l, 1l)
        .baselineValues(17l, 1l)
        .baselineValues(18l, 1l)
        .baselineValues(19l, 1l)
        .baselineValues(20l, 1l)
        .baselineValues(21l, 1l)
        .baselineValues(22l, 1l)
        .baselineValues(23l, 1l)
        .baselineValues(24l, 1l)
        .build()
        .run();
  }

  @Test // DRILL-3196
  public void testSinglePartitionDefinedInWindowList() throws Exception {
    final String query = "select sum(n_nationKey) over w as col \n" +
        "from cp.`tpch/nation.parquet` \n" +
        "window w as (partition by n_nationKey order by n_nationKey)";

    // Validate the plan
    final String[] expectedPlan = {"Window.*partition \\{0\\} order by \\[0\\].*SUM\\(\\$0\\)",
        "Scan.*columns=\\[`n_nationKey`\\].*"};
    final String[] excludedPatterns = {"Scan.*columns=\\[`\\*`\\].*"};
    PlanTestBase.testPlanMatchingPatterns(query, expectedPlan, excludedPatterns);

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("col")
        .baselineValues(0l)
        .baselineValues(1l)
        .baselineValues(2l)
        .baselineValues(3l)
        .baselineValues(4l)
        .baselineValues(5l)
        .baselineValues(6l)
        .baselineValues(7l)
        .baselineValues(8l)
        .baselineValues(9l)
        .baselineValues(10l)
        .baselineValues(11l)
        .baselineValues(12l)
        .baselineValues(13l)
        .baselineValues(14l)
        .baselineValues(15l)
        .baselineValues(16l)
        .baselineValues(17l)
        .baselineValues(18l)
        .baselineValues(19l)
        .baselineValues(20l)
        .baselineValues(21l)
        .baselineValues(22l)
        .baselineValues(23l)
        .baselineValues(24l)
        .build()
        .run();
  }

  @Test(expected = UnsupportedFunctionException.class) // DRILL-3182
  public void testWindowFunctionWithDistinct() throws Exception {
    try {
      final String query = "explain plan for select a2, count(distinct b2) over(partition by a2) \n" +
          "from cp.`tpch/nation.parquet`";

      test(query);
    } catch(UserException ex) {
      throwAsUnsupportedException(ex);
      throw ex;
    }
  }

  @Test(expected = UnsupportedFunctionException.class) // DRILL-3188
  public void testWindowFrame() throws Exception {
    try {
      final String query = "select a2, sum(a2) over(partition by a2 order by a2 rows between 1 preceding and 1 following ) \n" +
          "from cp.`tpch/nation.parquet` t \n" +
          "order by a2";

      test(query);
    } catch(UserException ex) {
        throwAsUnsupportedException(ex);
        throw ex;
    }
  }

  @Test(expected = UnsupportedFunctionException.class)  // DRILL-3188
  public void testRowsUnboundedPreceding() throws Exception {
    try {
      final String query = "explain plan for select sum(n_nationKey) over(partition by n_nationKey order by n_nationKey \n" +
      "rows UNBOUNDED PRECEDING)" +
      "from cp.`tpch/nation.parquet` t \n" +
      "order by n_nationKey";

      test(query);
    } catch(UserException ex) {
      throwAsUnsupportedException(ex);
      throw ex;
    }
  }

  @Test(expected = UnsupportedFunctionException.class) // DRILL-3359
  public void testFramesDefinedInWindowClause() throws Exception {
    try {
      final String query = "explain plan for select sum(n_nationKey) over w \n" +
          "from cp.`tpch/nation.parquet` \n" +
          "window w as (partition by n_nationKey order by n_nationKey rows UNBOUNDED PRECEDING)";

      test(query);
    } catch(UserException ex) {
      throwAsUnsupportedException(ex);
      throw ex;
    }
  }

  @Test(expected = UnsupportedFunctionException.class) // DRILL-3326
  public void testWindowWithAlias() throws Exception {
    try {
      String query = "explain plan for SELECT sum(n_nationkey) OVER (PARTITION BY n_name ORDER BY n_name ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) as col2 \n" +
          "from cp.`tpch/nation.parquet`";

      test(query);
    } catch(UserException ex) {
      throwAsUnsupportedException(ex);
      throw ex;
    }
  }

  @Test(expected = UnsupportedFunctionException.class) // DRILL-3189
  public void testWindowWithAllowDisallow() throws Exception {
    try {
      final String query = "select sum(n_nationKey) over(partition by n_nationKey \n" +
          "rows between unbounded preceding and unbounded following disallow partial) \n" +
          "from cp.`tpch/nation.parquet` \n" +
          "order by n_nationKey";

      test(query);
    } catch(UserException ex) {
      throwAsUnsupportedException(ex);
      throw ex;
    }
  }

  @Test // DRILL-3360
  public void testWindowInWindow() throws Exception {
    String query = "select rank() over(order by row_number() over(order by n_nationkey)) \n" +
        "from cp.`tpch/nation.parquet`";

    validationErrorHelper(query);
  }

  @Test // DRILL-3280, DRILL-3601, DRILL-3649
  public void testMissingOver() throws Exception {
    String query1 = "select lead(n_nationkey) from cp.`tpch/nation.parquet`";
    String query2 = "select rank(), cume_dist() over w \n" +
        "from cp.`tpch/nation.parquet` \n" +
        "window w as (partition by n_name order by n_nationkey)";
    String query3 = "select NTILE(1) from cp.`tpch/nation.parquet`";

    validationErrorHelper(query1);
    validationErrorHelper(query2);
    validationErrorHelper(query3);
  }

  @Test // DRILL-3344
  public void testWindowGroupBy() throws Exception {
    String query = "explain plan for SELECT max(n_nationkey) OVER (), n_name as col2 \n" +
        "from cp.`tpch/nation.parquet` \n" +
        "group by n_name";

    validationErrorHelper(query);
  }

  @Test // DRILL-3346
  public void testWindowGroupByOnView() throws Exception {
    try {
      String createView = "create view testWindowGroupByOnView(a, b) as \n" +
          "select n_nationkey, n_name from cp.`tpch/nation.parquet`";
      String query = "explain plan for SELECT max(a) OVER (), b as col2 \n" +
          "from testWindowGroupByOnView \n" +
          "group by b";

      test("use dfs_test.tmp");
      test(createView);
      validationErrorHelper(query);
    } finally {
      test("drop view testWindowGroupByOnView");
    }
  }

  @Test // DRILL-3188
  public void testWindowFrameEquivalentToDefault() throws Exception {
    final String query1 = "select sum(n_nationKey) over(partition by n_nationKey order by n_nationKey) as col\n" +
        "from cp.`tpch/nation.parquet` t \n" +
        "order by n_nationKey";

    final String query2 = "select sum(n_nationKey) over(partition by n_nationKey order by n_nationKey \n" +
        "range between unbounded preceding and current row) as col \n" +
        "from cp.`tpch/nation.parquet` t \n" +
        "order by n_nationKey";

    final String query3 = "select sum(n_nationKey) over(partition by n_nationKey \n" +
        "rows BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as col \n" +
        "from cp.`tpch/nation.parquet` t \n" +
        "order by n_nationKey";

    // Validate the plan
    final String[] expectedPlan1 = {"Window.*partition \\{0\\} order by \\[0\\].*SUM\\(\\$0\\)",
        "Scan.*columns=\\[`n_nationKey`\\].*"};
    final String[] excludedPatterns1 = {"Scan.*columns=\\[`\\*`\\].*"};
    PlanTestBase.testPlanMatchingPatterns(query1, expectedPlan1, excludedPatterns1);

    testBuilder()
        .sqlQuery(query1)
        .unOrdered()
        .baselineColumns("col")
        .baselineValues(0l)
        .baselineValues(1l)
        .baselineValues(2l)
        .baselineValues(3l)
        .baselineValues(4l)
        .baselineValues(5l)
        .baselineValues(6l)
        .baselineValues(7l)
        .baselineValues(8l)
        .baselineValues(9l)
        .baselineValues(10l)
        .baselineValues(11l)
        .baselineValues(12l)
        .baselineValues(13l)
        .baselineValues(14l)
        .baselineValues(15l)
        .baselineValues(16l)
        .baselineValues(17l)
        .baselineValues(18l)
        .baselineValues(19l)
        .baselineValues(20l)
        .baselineValues(21l)
        .baselineValues(22l)
        .baselineValues(23l)
        .baselineValues(24l)
        .build()
        .run();

    final String[] expectedPlan2 = {"Window.*partition \\{0\\} order by \\[0\\].*SUM\\(\\$0\\)",
        "Scan.*columns=\\[`n_nationKey`\\].*"};
    final String[] excludedPatterns2 = {"Scan.*columns=\\[`\\*`\\].*"};
    PlanTestBase.testPlanMatchingPatterns(query2, expectedPlan2, excludedPatterns2);

    testBuilder()
        .sqlQuery(query2)
        .unOrdered()
        .baselineColumns("col")
        .baselineValues(0l)
        .baselineValues(1l)
        .baselineValues(2l)
        .baselineValues(3l)
        .baselineValues(4l)
        .baselineValues(5l)
        .baselineValues(6l)
        .baselineValues(7l)
        .baselineValues(8l)
        .baselineValues(9l)
        .baselineValues(10l)
        .baselineValues(11l)
        .baselineValues(12l)
        .baselineValues(13l)
        .baselineValues(14l)
        .baselineValues(15l)
        .baselineValues(16l)
        .baselineValues(17l)
        .baselineValues(18l)
        .baselineValues(19l)
        .baselineValues(20l)
        .baselineValues(21l)
        .baselineValues(22l)
        .baselineValues(23l)
        .baselineValues(24l)
        .build()
        .run();

    final String[] expectedPlan3 = {"Window.*partition \\{0\\}.*SUM\\(\\$0\\)",
        "Scan.*columns=\\[`n_nationKey`\\].*"};
    final String[] excludedPatterns3 = {"Scan.*columns=\\[`\\*`\\].*"};
    PlanTestBase.testPlanMatchingPatterns(query3, expectedPlan3, excludedPatterns3);

    testBuilder()
        .sqlQuery(query3)
        .unOrdered()
        .baselineColumns("col")
        .baselineValues(0l)
        .baselineValues(1l)
        .baselineValues(2l)
        .baselineValues(3l)
        .baselineValues(4l)
        .baselineValues(5l)
        .baselineValues(6l)
        .baselineValues(7l)
        .baselineValues(8l)
        .baselineValues(9l)
        .baselineValues(10l)
        .baselineValues(11l)
        .baselineValues(12l)
        .baselineValues(13l)
        .baselineValues(14l)
        .baselineValues(15l)
        .baselineValues(16l)
        .baselineValues(17l)
        .baselineValues(18l)
        .baselineValues(19l)
        .baselineValues(20l)
        .baselineValues(21l)
        .baselineValues(22l)
        .baselineValues(23l)
        .baselineValues(24l)
        .build()
        .run();
  }

  @Test // DRILL-3204
  public void testWindowWithJoin() throws Exception {
    final String query = "select sum(t1.r_regionKey) over(partition by t1.r_regionKey) as col \n" +
        "from cp.`tpch/region.parquet` t1, cp.`tpch/nation.parquet` t2 \n" +
        "where t1.r_regionKey = t2.n_nationKey \n" +
        "group by t1.r_regionKey";

    // Validate the plan
    final String[] expectedPlan = {"Window.*partition \\{0\\}.*SUM\\(\\$0\\)",
        "Scan.*columns=\\[`n_nationKey`\\].*",
        "Scan.*columns=\\[`n_nationKey`\\].*"};
    final String[] excludedPatterns = {"Scan.*columns=\\[`\\*`\\].*"};
    PlanTestBase.testPlanMatchingPatterns(query, expectedPlan, excludedPatterns);

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("col")
        .baselineValues(0l)
        .baselineValues(1l)
        .baselineValues(2l)
        .baselineValues(3l)
        .baselineValues(4l)
        .build()
        .run();
  }

  @Test // DRILL-3298
  public void testCountEmptyPartitionByWithExchange() throws Exception {
    String query = String.format("select count(*) over (order by o_orderpriority) as cnt from dfs.`%s/multilevel/parquet` where o_custkey < 100", TEST_RES_PATH);
    try {
      // Validate the plan
      final String[] expectedPlan = {"Window.*partition \\{\\} order by \\[0\\].*COUNT\\(\\)",
          "Scan.*columns=\\[`o_custkey`, `o_orderpriority`\\]"};
      final String[] excludedPatterns = {"Scan.*columns=\\[`\\*`\\]"};
      test("alter session set `planner.slice_target` = 1");
      PlanTestBase.testPlanMatchingPatterns(query, expectedPlan, excludedPatterns);

      testBuilder()
          .sqlQuery(query)
          .ordered()
          .baselineColumns("cnt")
          .optionSettingQueriesForTestQuery("alter session set `planner.slice_target` = 1")
          .baselineValues(1l)
          .baselineValues(4l)
          .baselineValues(4l)
          .baselineValues(4l)
          .build()
          .run();
    } finally {
      test("alter session set `planner.slice_target` = " + ExecConstants.SLICE_TARGET_DEFAULT);
    }
  }

  /* Verify the output of aggregate functions (which are reduced
    * eg: avg(x) = sum(x)/count(x)) return results of the correct
    * data type (double)
    */
  @Test
  public void testAvgVarianceWindowFunctions() throws Exception {
    final String avgQuery = "select avg(n_nationkey) over (partition by n_nationkey) col1 " +
        "from cp.`tpch/nation.parquet` " +
        "where n_nationkey = 1";

    // Validate the plan
    final String[] expectedPlan1 = {"Window.*partition \\{0\\} order by \\[\\].*SUM\\(\\$0\\), COUNT\\(\\$0\\)",
        "Scan.*columns=\\[`n_nationkey`\\]"};
    final String[] excludedPatterns1 = {"Scan.*columns=\\[`\\*`\\]"};
    PlanTestBase.testPlanMatchingPatterns(avgQuery, expectedPlan1, excludedPatterns1);

    testBuilder()
        .sqlQuery(avgQuery)
        .unOrdered()
        .baselineColumns("col1")
        .baselineValues(1.0d)
        .go();

    final String varianceQuery = "select var_pop(n_nationkey) over (partition by n_nationkey) col1 " +
        "from cp.`tpch/nation.parquet` " +
        "where n_nationkey = 1";

    // Validate the plan
    final String[] expectedPlan2 = {"Window.*partition \\{0\\} order by \\[\\].*SUM\\(\\$1\\), SUM\\(\\$0\\), COUNT\\(\\$0\\)",
        "Scan.*columns=\\[`n_nationkey`\\]"};
    final String[] excludedPatterns2 = {"Scan.*columns=\\[`\\*`\\]"};
    PlanTestBase.testPlanMatchingPatterns(varianceQuery, expectedPlan2, excludedPatterns2);

    testBuilder()
        .sqlQuery(varianceQuery)
        .unOrdered()
        .baselineColumns("col1")
        .baselineValues(0.0d)
        .go();
  }

  @Test
  public void testWindowFunctionWithKnownType() throws Exception {
    final String query = "select sum(cast(col_int as int)) over (partition by col_varchar) as col1 " +
        "from cp.`jsoninput/large_int.json` limit 1";

    // Validate the plan
    final String[] expectedPlan1 = {"Window.*partition \\{0\\} order by \\[\\].*SUM\\(\\$1\\)",
        "Scan.*columns=\\[`col_varchar`, `col_int`\\]"};
    final String[] excludedPatterns1 = {"Scan.*columns=\\[`\\*`\\]"};
    PlanTestBase.testPlanMatchingPatterns(query, expectedPlan1, excludedPatterns1);

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("col1")
        .baselineValues(2147483649l)
        .go();

    final String avgQuery = "select avg(cast(col_int as int)) over (partition by col_varchar) as col1 " +
        "from cp.`jsoninput/large_int.json` limit 1";

    // Validate the plan
    final String[] expectedPlan2 = {"Window.*partition \\{0\\} order by \\[\\].*SUM\\(\\$1\\), COUNT\\(\\$1\\)",
        "Scan.*columns=\\[`col_varchar`, `col_int`\\]"};
    final String[] excludedPatterns2 = {"Scan.*columns=\\[`\\*`\\]"};
    PlanTestBase.testPlanMatchingPatterns(avgQuery, expectedPlan2, excludedPatterns2);

    testBuilder()
        .sqlQuery(avgQuery)
        .unOrdered()
        .baselineColumns("col1")
        .baselineValues(1.0737418245E9d)
        .go();
  }

  @Test
  public void testCompoundIdentifierInWindowDefinition() throws Exception {
    String root = FileUtils.getResourceAsFile("/multilevel/csv/1994/Q1/orders_94_q1.csv").toURI().toString();
    String query = String.format("SELECT count(*) OVER w as col1, count(*) OVER w as col2 \n" +
        "FROM dfs_test.`%s` \n" +
        "WINDOW w AS (PARTITION BY columns[1] ORDER BY columns[0] DESC)", root);

    // Validate the plan
    final String[] expectedPlan = {"Window.*partition \\{1\\} order by \\[0 DESC\\].*COUNT\\(\\)",
        "Scan.*columns=\\[`columns`\\[0\\], `columns`\\[1\\]\\]"};
    final String[] excludedPatterns = {"Scan.*columns=\\[`\\*`\\]"};
    PlanTestBase.testPlanMatchingPatterns(query, expectedPlan, excludedPatterns);

    // Validate the result
    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("col1", "col2")
        .baselineValues((long) 1, (long) 1)
        .baselineValues((long) 1, (long) 1)
        .baselineValues((long) 1, (long) 1)
        .baselineValues((long) 1, (long) 1)
        .baselineValues((long) 1, (long) 1)
        .baselineValues((long) 1, (long) 1)
        .baselineValues((long) 1, (long) 1)
        .baselineValues((long) 1, (long) 1)
        .baselineValues((long) 1, (long) 1)
        .baselineValues((long) 1, (long) 1)
        .build()
        .run();
  }

  @Test
  public void testRankWithGroupBy() throws Exception {
    final String query = "select dense_rank() over (order by l_suppkey) as rank1 " +
        " from cp.`tpch/lineitem.parquet` group by l_partkey, l_suppkey order by 1 desc limit 1";

    // Validate the plan
    final String[] expectedPlan = {"Window.*partition \\{\\} order by \\[1\\].*DENSE_RANK\\(\\)",
        "Scan.*columns=\\[`l_partkey`, `l_suppkey`\\]"};
    final String[] excludedPatterns = {"Scan.*columns=\\[`\\*`\\]"};
    PlanTestBase.testPlanMatchingPatterns(query, expectedPlan, excludedPatterns);

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("rank1")
        .baselineValues(100l)
        .go();
  }

  @Test // DRILL-3404
  public void testWindowSumAggIsNotNull() throws Exception {
    String query = String.format("select count(*) cnt from (select sum ( c1 ) over ( partition by c2 order by c1 asc nulls first ) w_sum from dfs.`%s/window/table_with_nulls.parquet` ) sub_query where w_sum is not null", TEST_RES_PATH);

    // Validate the plan
    final String[] expectedPlan = {"Window.*partition \\{1\\} order by \\[0 ASC-nulls-first\\].*SUM\\(\\$0\\)",
        "Scan.*columns=\\[`c1`, `c2`\\]"};
    final String[] excludedPatterns = {"Scan.*columns=\\[`\\*`\\]"};
    PlanTestBase.testPlanMatchingPatterns(query, expectedPlan, excludedPatterns);

    testBuilder()
      .sqlQuery(query)
      .ordered()
      .baselineColumns("cnt")
      .baselineValues(26l)
      .build().run();
  }

  @Test // DRILL-3292
  public void testWindowConstants() throws Exception {
    String query = "select rank() over w fn, sum(2) over w sumINTEGER, sum(employee_id) over w sumEmpId, sum(0.5) over w sumFLOAT \n" +
        "from cp.`employee.json` \n" +
        "where position_id = 2 \n" +
        "window w as(partition by position_id order by employee_id)";

    // Validate the plan
    final String[] expectedPlan = {"Window.*partition \\{0\\} order by \\[1\\].*RANK\\(\\), SUM\\(\\$2\\), SUM\\(\\$1\\), SUM\\(\\$3\\)",
        "Scan.*columns=\\[`position_id`, `employee_id`\\]"};
    final String[] excludedPatterns = {"Scan.*columns=\\[`\\*`\\]"};
    PlanTestBase.testPlanMatchingPatterns(query, expectedPlan, excludedPatterns);

    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("fn", "sumINTEGER", "sumEmpId", "sumFLOAT")
        .baselineValues(1l, 2l, 2l, 0.5)
        .baselineValues(2l, 4l, 6l, 1.0)
        .baselineValues(3l, 6l, 11l, 1.5)
        .baselineValues(4l, 8l, 31l, 2.0)
        .baselineValues(5l, 10l, 52l, 2.5)
        .baselineValues(6l, 12l, 74l, 3.0)
        .build()
        .run();
  }

  @Test // DRILL-3567
  public void testMultiplePartitions1() throws Exception {
    String root = FileUtils.getResourceAsFile("/store/text/data/t.json").toURI().toString();
    String query = String.format("select count(*) over(partition by b1 order by c1) as count1, \n" +
        "sum(a1)  over(partition by b1 order by c1) as sum1, \n" +
        "count(*) over(partition by a1 order by c1) as count2 \n" +
        "from dfs_test.`%s`", root);

    // Validate the plan
    final String[] expectedPlan = {"Window.*partition \\{2\\} order by \\[1\\].*COUNT\\(\\)",
        "Window.*partition \\{0\\} order by \\[1\\].*COUNT\\(\\), SUM\\(\\$2\\)",
        "Scan.*columns=\\[`b1`, `c1`, `a1`\\]"};
    final String[] excludedPatterns = {"Scan.*columns=\\[`\\*`\\]"};
    PlanTestBase.testPlanMatchingPatterns(query, expectedPlan, excludedPatterns);

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("count1", "sum1", "count2")
        .baselineValues(1l, 0l, 2l)
        .baselineValues(1l, 0l, 2l)
        .baselineValues(2l, 0l, 5l)
        .baselineValues(3l, 0l, 5l)
        .baselineValues(3l, 0l, 5l)
        .baselineValues(1l, 10l, 2l)
        .baselineValues(1l, 10l, 2l)
        .baselineValues(2l, 20l, 5l)
        .baselineValues(3l, 30l, 5l)
        .baselineValues(3l, 30l, 5l)
        .build()
        .run();
  }

  @Test // DRILL-3567
  public void testMultiplePartitions2() throws Exception {
    String root = FileUtils.getResourceAsFile("/store/text/data/t.json").toURI().toString();
    String query = String.format("select count(*) over(partition by b1 order by c1) as count1, \n" +
        "count(*) over(partition by a1 order by c1) as count2, \n" +
        "sum(a1)  over(partition by b1 order by c1) as sum1 \n" +
        "from dfs_test.`%s`", root);

    // Validate the plan
    final String[] expectedPlan = {"Window.*partition \\{2\\} order by \\[1\\].*COUNT\\(\\)",
        "Window.*partition \\{0\\} order by \\[1\\].*COUNT\\(\\), SUM\\(\\$2\\)",
        "Scan.*columns=\\[`b1`, `c1`, `a1`\\]"};
    final String[] excludedPatterns = {"Scan.*columns=\\[`\\*`\\]"};
    PlanTestBase.testPlanMatchingPatterns(query, expectedPlan, excludedPatterns);

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("count1", "count2", "sum1")
        .baselineValues(1l, 2l, 0l)
        .baselineValues(1l, 2l, 0l)
        .baselineValues(2l, 5l, 0l)
        .baselineValues(3l, 5l, 0l)
        .baselineValues(3l, 5l, 0l)
        .baselineValues(1l, 2l, 10l)
        .baselineValues(1l, 2l, 10l)
        .baselineValues(2l, 5l, 20l)
        .baselineValues(3l, 5l, 30l)
        .baselineValues(3l, 5l, 30l)
        .build()
        .run();
  }

  @Test // see DRILL-3574
  public void testWithAndWithoutPartitions() throws Exception {
    String root = FileUtils.getResourceAsFile("/store/text/data/t.json").toURI().toString();
    String query = String.format("select sum(a1) over(partition by b1, c1) as s1, sum(a1) over() as s2 \n" +
        "from dfs_test.`%s` \n" +
        "order by a1", root);
    test("alter session set `planner.slice_target` = 1");

    // Validate the plan
    final String[] expectedPlan = {"Window\\(window#0=\\[window\\(partition \\{\\}.*\n" +
        ".*UnionExchange"};
    PlanTestBase.testPlanMatchingPatterns(query, expectedPlan, new String[]{});

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("s1", "s2")
        .baselineValues(0l, 50l)
        .baselineValues(0l, 50l)
        .baselineValues(0l, 50l)
        .baselineValues(0l, 50l)
        .baselineValues(0l, 50l)
        .baselineValues(10l, 50l)
        .baselineValues(10l, 50l)
        .baselineValues(10l, 50l)
        .baselineValues(20l, 50l)
        .baselineValues(20l, 50l)
        .build()
        .run();
  }

  @Test // see DRILL-3657
  public void testConstantsInMultiplePartitions() throws Exception {
    String root = FileUtils.getResourceAsFile("/store/text/data/t.json").toURI().toString();
    String query = String.format(
        "select sum(1) over(partition by b1 order by a1) as sum1, sum(1) over(partition by a1) as sum2, rank() over(order by b1) as rank1, rank() over(order by 1) as rank2 \n" +
        "from dfs_test.`%s` \n" +
        "order by 1, 2, 3, 4", root);

    // Validate the plan
    final String[] expectedPlan = {"Window.*SUM\\(\\$3\\).*\n" +
        ".*SelectionVectorRemover.*\n" +
        ".*Sort.*\n" +
        ".*Window.*SUM\\(\\$2\\).*"
    };
    PlanTestBase.testPlanMatchingPatterns(query, expectedPlan, new String[]{});

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("sum1", "sum2", "rank1", "rank2")
        .baselineValues(2l, 5l, 1l, 1l)
        .baselineValues(2l, 5l, 1l, 1l)
        .baselineValues(2l, 5l, 6l, 1l)
        .baselineValues(2l, 5l, 6l, 1l)
        .baselineValues(3l, 5l, 3l, 1l)
        .baselineValues(3l, 5l, 3l, 1l)
        .baselineValues(3l, 5l, 3l, 1l)
        .baselineValues(3l, 5l, 8l, 1l)
        .baselineValues(3l, 5l, 8l, 1l)
        .baselineValues(3l, 5l, 8l, 1l)
        .build()
        .run();
  }

  @Test // DRILL-3580
  public void testExpressionInWindowFunction() throws Exception {
    String root = FileUtils.getResourceAsFile("/store/text/data/t.json").toURI().toString();
    String query = String.format("select a1, b1, sum(b1) over (partition by a1) as c1, sum(a1 + b1) over (partition by a1) as c2\n" +
        "from dfs_test.`%s`", root);

    // Validate the plan
    final String[] expectedPlan = {"Window\\(window#0=\\[window\\(partition \\{0\\} order by \\[\\].*\\[SUM\\(\\$1\\), SUM\\(\\$2\\)\\]"};
    PlanTestBase.testPlanMatchingPatterns(query, expectedPlan, new String[]{});

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("a1", "b1", "c1", "c2")
        .baselineValues(0l, 1l, 8l, 8l)
        .baselineValues(0l, 1l, 8l, 8l)
        .baselineValues(0l, 2l, 8l, 8l)
        .baselineValues(0l, 2l, 8l, 8l)
        .baselineValues(0l, 2l, 8l, 8l)
        .baselineValues(10l, 3l, 21l, 71l)
        .baselineValues(10l, 3l, 21l, 71l)
        .baselineValues(10l, 5l, 21l, 71l)
        .baselineValues(10l, 5l, 21l, 71l)
        .baselineValues(10l, 5l, 21l, 71l)
        .build()
        .run();
  }

  @Test // see DRILL-3657
  public void testProjectPushPastWindow() throws Exception {
    String query = "select sum(n_nationkey) over(partition by 1 order by 1) as col1, \n" +
            "count(n_nationkey) over(partition by 1 order by 1) as col2 \n" +
            "from cp.`tpch/nation.parquet` \n" +
            "limit 5";

    // Validate the plan
    final String[] expectedPlan = {"Scan.*columns=\\[`n_nationkey`\\].*"};
    PlanTestBase.testPlanMatchingPatterns(query, expectedPlan, new String[]{});

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("col1", "col2")
        .baselineValues(300l, 25l)
        .baselineValues(300l, 25l)
        .baselineValues(300l, 25l)
        .baselineValues(300l, 25l)
        .baselineValues(300l, 25l)
        .build()
        .run();
  }
}