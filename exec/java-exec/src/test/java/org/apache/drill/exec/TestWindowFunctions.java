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
    final String query = "explain plan for select sum(a2) over(partition by a2), count(*) over(partition by a2) \n" +
        "from cp.`tpch/nation.parquet`";

    test(query);
  }

  @Test // DRILL-3196
  public void testSinglePartitionDefinedInWindowList() throws Exception {
    final String query = "explain plan for select sum(a2) over w \n" +
        "from cp.`tpch/nation.parquet` \n" +
        "window w as (partition by a2 order by a2)";

    test(query);
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

  @Test(expected = UnsupportedFunctionException.class) // DRILL-3195
  public void testWindowFunctionNTILE() throws Exception {
    try {
      final String query = "explain plan for select NTILE(1) over(partition by n_name order by n_name) \n" +
          "from cp.`tpch/nation.parquet`";

      test(query);
    } catch(UserException ex) {
      throwAsUnsupportedException(ex);
      throw ex;
    }
  }

  @Test(expected = UnsupportedFunctionException.class) // DRILL-3195
  public void testWindowFunctionLAG() throws Exception {
    try {
      final String query = "explain plan for select LAG(n_nationKey, 1) over(partition by n_name order by n_name) \n" +
          "from cp.`tpch/nation.parquet`";

      test(query);
    } catch(UserException ex) {
      throwAsUnsupportedException(ex);
      throw ex;
    }
  }

  @Test(expected = UnsupportedFunctionException.class) // DRILL-3195
  public void testWindowFunctionLEAD() throws Exception {
    try {
      final String query = "explain plan for select LEAD(n_nationKey, 1) over(partition by n_name order by n_name) \n" +
          "from cp.`tpch/nation.parquet`";

      test(query);
    } catch(UserException ex) {
      throwAsUnsupportedException(ex);
      throw ex;
    }
  }

  @Test(expected = UnsupportedFunctionException.class) // DRILL-3195
  public void testWindowFunctionFIRST_VALUE() throws Exception {
    try {
      final String query = "explain plan for select FIRST_VALUE(n_nationKey) over(partition by n_name order by n_name) \n" +
          "from cp.`tpch/nation.parquet`";

      test(query);
    } catch(UserException ex) {
      throwAsUnsupportedException(ex);
      throw ex;
    }
  }

  @Test(expected = UnsupportedFunctionException.class) // DRILL-3195
  public void testWindowFunctionLAST_VALUE() throws Exception {
    try {
      final String query = "explain plan for select LAST_VALUE(n_nationKey) over(partition by n_name order by n_name) \n" +
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

  @Test // DRILL-3344
  public void testWindowGroupBy() throws Exception {
    String query = "explain plan for SELECT max(n_nationkey) OVER (), n_name as col2 \n" +
        "from cp.`tpch/nation.parquet` \n" +
        "group by n_name";

    parseErrorHelper(query);
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
      parseErrorHelper(query);
    } finally {
      test("drop view testWindowGroupByOnView");
    }
  }

  @Test // DRILL-3188
  public void testWindowFrameEquivalentToDefault() throws Exception {
    final String query1 = "explain plan for select sum(n_nationKey) over(partition by n_nationKey order by n_nationKey) \n" +
        "from cp.`tpch/nation.parquet` t \n" +
        "order by n_nationKey";

    final String query2 = "explain plan for select sum(n_nationKey) over(partition by n_nationKey order by n_nationKey \n" +
        "range between unbounded preceding and current row) \n" +
        "from cp.`tpch/nation.parquet` t \n" +
        "order by n_nationKey";

    final String query3 = "explain plan for select sum(n_nationKey) over(partition by n_nationKey \n" +
        "rows BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)" +
        "from cp.`tpch/nation.parquet` t \n" +
        "order by n_nationKey";

    test(query1);
    test(query2);
    test(query3);
  }

  @Test // DRILL-3204
  public void testWindowWithJoin() throws Exception {
    final String query = "select sum(t1.r_regionKey) over(partition by t1.r_regionKey)  \n" +
        "from cp.`tpch/region.parquet` t1, cp.`tpch/nation.parquet` t2 \n" +
        "where t1.r_regionKey = t2.n_nationKey \n" +
        "group by t1.r_regionKey";

    test(query);
  }

  @Test // DRILL-3298
  public void testCountEmptyPartitionByWithExchange() throws Exception {
    String query = String.format("select count(*) over (order by o_orderpriority) as cnt from dfs.`%s/multilevel/parquet` where o_custkey < 100", TEST_RES_PATH);
    try {
      testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("cnt")
        .optionSettingQueriesForTestQuery("alter session set `planner.slice_target` = 1")
        .baselineValues(1l)
        .baselineValues(4l)
        .baselineValues(4l)
        .baselineValues(4l)
        .build().run();
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

    testBuilder()
        .sqlQuery(avgQuery)
        .unOrdered()
        .baselineColumns("col1")
        .baselineValues(1.0d)
        .go();

    final String varianceQuery = "select var_pop(n_nationkey) over (partition by n_nationkey) col1 " +
        "from cp.`tpch/nation.parquet` " +
        "where n_nationkey = 1";

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

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("col1")
        .baselineValues(2147483649l)
        .go();

    final String avgQuery = "select avg(cast(col_int as int)) over (partition by col_varchar) as col1 " +
        "from cp.`jsoninput/large_int.json` limit 1";

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
}