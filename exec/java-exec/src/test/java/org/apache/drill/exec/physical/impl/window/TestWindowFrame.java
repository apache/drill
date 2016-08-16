/*******************************************************************************
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
 ******************************************************************************/
package org.apache.drill.exec.physical.impl.window;

import java.util.Properties;

import org.apache.drill.BaseTestQuery;
import org.apache.drill.DrillTestWrapper;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.UserRemoteException;
import org.apache.drill.common.util.TestTools;
import org.apache.drill.exec.ExecConstants;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.apache.drill.exec.proto.UserBitShared.DrillPBError.ErrorType;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestWindowFrame extends BaseTestQuery {

  private static final String TEST_RES_PATH = TestTools.getWorkingPath() + "/src/test/resources";

  @BeforeClass
  public static void setupMSortBatchSize() {
    // make sure memory sorter outputs 20 rows per batch
    final Properties props = cloneDefaultTestConfigProperties();
    props.put(ExecConstants.EXTERNAL_SORT_MSORT_MAX_BATCHSIZE, Integer.toString(20));

    updateTestCluster(1, DrillConfig.create(props));
  }

  private DrillTestWrapper buildWindowQuery(final String tableName, final boolean withPartitionBy, final int numBatches)
      throws Exception {
    return testBuilder()
      .sqlQuery(getFile("window/q1.sql"), TEST_RES_PATH, tableName, withPartitionBy ? "(partition by position_id)":"()")
      .ordered()
      .csvBaselineFile("window/" + tableName + (withPartitionBy ? ".pby" : "") + ".tsv")
      .baselineColumns("count", "sum")
      .expectsNumBatches(numBatches)
      .build();
  }

  private DrillTestWrapper buildWindowWithOrderByQuery(final String tableName, final boolean withPartitionBy,
                                                       final int numBatches) throws Exception {
    return testBuilder()
      .sqlQuery(getFile("window/q2.sql"), TEST_RES_PATH, tableName, withPartitionBy ? "(partition by position_id order by sub)" : "(order by sub)")
      .ordered()
      .csvBaselineFile("window/" + tableName + (withPartitionBy ? ".pby" : "") + ".oby.tsv")
      .baselineColumns("count", "sum", "row_number", "rank", "dense_rank", "cume_dist", "percent_rank")
      .expectsNumBatches(numBatches)
      .build();
  }

  private void runTest(final String tableName, final boolean withPartitionBy, final boolean withOrderBy, final int numBatches) throws Exception {

    DrillTestWrapper testWrapper = withOrderBy ?
      buildWindowWithOrderByQuery(tableName, withPartitionBy, numBatches) : buildWindowQuery(tableName, withPartitionBy, numBatches);
    testWrapper.run();
  }

  private void runTest(final String tableName, final int numBatches) throws Exception {
    // we do expect an "extra" empty batch
    runTest(tableName, true, true, numBatches + 1);
    runTest(tableName, true, false, numBatches + 1);
    runTest(tableName, false, true, numBatches + 1);
    runTest(tableName, false, false, numBatches + 1);
  }

  /**
   * Single batch with a single partition (position_id column)
   */
  @Test
  public void testB1P1() throws Exception {
    runTest("b1.p1", 1);
  }

  /**
   * Single batch with 2 partitions (position_id column)
   */
  @Test
  public void testB1P2() throws Exception {
    runTest("b1.p2", 1);
  }

  @Test
  public void testMultipleFramers() throws Exception {
    final String window = " OVER(PARTITION BY position_id ORDER by sub)";
    test("SELECT COUNT(*)"+window+", SUM(salary)"+window+", ROW_NUMBER()"+window+", RANK()"+window+" " +
      "FROM dfs_test.`"+TEST_RES_PATH+"/window/b1.p1`"
    );
  }

  @Test
  public void testUnboundedFollowing() throws Exception {
    testBuilder()
      .sqlQuery(getFile("window/q3.sql"), TEST_RES_PATH)
      .ordered()
      .sqlBaselineQuery(getFile("window/q4.sql"), TEST_RES_PATH)
      .build()
      .run();
  }

  @Test
  public void testAggregateRowsUnboundedAndCurrentRow() throws Exception {
    final String table = "dfs_test.`"+TEST_RES_PATH+"/window/b4.p4`";
    testBuilder()
      .sqlQuery(getFile("window/aggregate_rows_unbounded_current.sql"), table)
      .ordered()
      .sqlBaselineQuery(getFile("window/aggregate_rows_unbounded_current_baseline.sql"), table)
      .build()
      .run();
  }

  @Test
  public void testLastValueRowsUnboundedAndCurrentRow() throws Exception {
    final String table = "dfs_test.`"+TEST_RES_PATH+"/window/b4.p4`";
    testBuilder()
      .sqlQuery(getFile("window/last_value_rows_unbounded_current.sql"), table)
      .unOrdered()
      .sqlBaselineQuery(getFile("window/last_value_rows_unbounded_current_baseline.sql"), table)
      .build()
      .run();
  }

  @Test
  public void testAggregateRangeCurrentAndCurrent() throws Exception {
    final String table = "dfs_test.`"+TEST_RES_PATH+"/window/b4.p4`";
    testBuilder()
      .sqlQuery(getFile("window/aggregate_range_current_current.sql"), table)
      .unOrdered()
      .sqlBaselineQuery(getFile("window/aggregate_range_current_current_baseline.sql"), table)
      .build()
      .run();
  }

  @Test
  public void testFirstValueRangeCurrentAndCurrent() throws Exception {
    final String table = "dfs_test.`"+TEST_RES_PATH+"/window/b4.p4`";
    testBuilder()
      .sqlQuery(getFile("window/first_value_range_current_current.sql"), table)
      .unOrdered()
      .sqlBaselineQuery(getFile("window/first_value_range_current_current_baseline.sql"), table)
      .build()
      .run();
  }

  /**
   * 2 batches with 2 partitions (position_id column), each batch contains a different partition
   */
  @Test
  public void testB2P2() throws Exception {
    runTest("b2.p2", 2);
  }

  /**
   * 2 batches with 4 partitions, one partition has rows in both batches
   */
  @Test
  public void testB2P4() throws Exception {
    runTest("b2.p4", 2);
  }

  /**
   * 3 batches with 2 partitions, one partition has rows in all 3 batches
   */
  @Test
  public void testB3P2() throws Exception {
    runTest("b3.p2", 3);
  }

  /**
   * 4 batches with 4 partitions. After processing 1st batch, when innerNext() is called again, framer can process
   * current batch without the need to call next(incoming).
   */
  @Test
  public void testB4P4() throws Exception {
    runTest("b4.p4", 4);
  }

  @Test // DRILL-1862
  public void testEmptyPartitionBy() throws Exception {
    test("SELECT employee_id, position_id, salary, SUM(salary) OVER(ORDER BY position_id) FROM cp.`employee.json` LIMIT 10");
  }

  @Test // DRILL-3172
  public void testEmptyOverClause() throws Exception {
    test("SELECT employee_id, position_id, salary, SUM(salary) OVER() FROM cp.`employee.json` LIMIT 10");
  }

  @Test // DRILL-3218
  public void testMaxVarChar() throws Exception {
    test(getFile("window/q3218.sql"), TEST_RES_PATH);
  }

  @Test // DRILL-3220
  public void testCountConst() throws Exception {
    test(getFile("window/q3220.sql"), TEST_RES_PATH);
  }

  @Test // DRILL-3604
  public void testFix3604() throws Exception {
    // make sure the query doesn't fail
    test(getFile("window/3604.sql"), TEST_RES_PATH);
  }

  @Test // DRILL-3605
  public void testFix3605() throws Exception {
    testBuilder()
      .sqlQuery(getFile("window/3605.sql"), TEST_RES_PATH)
      .ordered()
      .csvBaselineFile("window/3605.tsv")
      .baselineColumns("col2", "lead_col2")
      .build()
      .run();
  }

  @Test // DRILL-3606
  public void testFix3606() throws Exception {
    testBuilder()
      .sqlQuery(getFile("window/3606.sql"), TEST_RES_PATH)
      .ordered()
      .csvBaselineFile("window/3606.tsv")
      .baselineColumns("col2", "lead_col2")
      .build()
      .run();
  }

  @Test
  public void testLead() throws Exception {
    testBuilder()
      .sqlQuery(getFile("window/lead.oby.sql"), TEST_RES_PATH)
      .ordered()
      .csvBaselineFile("window/b4.p4.lead.oby.tsv")
      .baselineColumns("lead")
      .build()
      .run();
  }

  @Test
  public void testLagWithPby() throws Exception {
    testBuilder()
      .sqlQuery(getFile("window/lag.pby.oby.sql"), TEST_RES_PATH)
      .ordered()
      .csvBaselineFile("window/b4.p4.lag.pby.oby.tsv")
      .baselineColumns("lag")
      .build()
      .run();
  }


  @Test
  public void testLag() throws Exception {
    testBuilder()
      .sqlQuery(getFile("window/lag.oby.sql"), TEST_RES_PATH)
      .ordered()
      .csvBaselineFile("window/b4.p4.lag.oby.tsv")
      .baselineColumns("lag")
      .build()
      .run();
  }

  @Test
  public void testLeadWithPby() throws Exception {
    testBuilder()
      .sqlQuery(getFile("window/lead.pby.oby.sql"), TEST_RES_PATH)
      .ordered()
      .csvBaselineFile("window/b4.p4.lead.pby.oby.tsv")
      .baselineColumns("lead")
      .build()
      .run();
  }

  @Test
  public void testFirstValue() throws Exception {
    testBuilder()
      .sqlQuery(getFile("window/fval.pby.sql"), TEST_RES_PATH)
      .ordered()
      .csvBaselineFile("window/b4.p4.fval.pby.tsv")
      .baselineColumns("first_value")
      .build()
      .run();
  }

  @Test
  public void testLastValue() throws Exception {
    testBuilder()
      .sqlQuery(getFile("window/lval.pby.oby.sql"), TEST_RES_PATH)
      .ordered()
      .csvBaselineFile("window/b4.p4.lval.pby.oby.tsv")
      .baselineColumns("last_value")
      .build()
      .run();
  }

  @Test
  public void testFirstValueAllTypes() throws Exception {
    // make sure all types are handled properly
    test(getFile("window/fval.alltypes.sql"), TEST_RES_PATH);
  }

  @Test
  public void testLastValueAllTypes() throws Exception {
    // make sure all types are handled properly
    test(getFile("window/fval.alltypes.sql"), TEST_RES_PATH);
  }

  @Test
  public void testNtile() throws Exception {
    testBuilder()
      .sqlQuery(getFile("window/ntile.sql"), TEST_RES_PATH)
      .ordered()
      .csvBaselineFile("window/b2.p4.ntile.tsv")
      .baselineColumns("ntile")
      .build()
      .run();
  }

  @Test
  public void test3648Fix() throws Exception {
    testBuilder()
      .sqlQuery(getFile("window/3648.sql"), TEST_RES_PATH)
      .ordered()
      .csvBaselineFile("window/3648.tsv")
      .baselineColumns("ntile")
      .build()
      .run();
  }

  @Test
  public void test3654Fix() throws Exception {
    test("SELECT FIRST_VALUE(col8) OVER(PARTITION BY col7 ORDER BY col8) FROM dfs_test.`%s/window/3648.parquet`", TEST_RES_PATH);
  }

  @Test
  public void test3643Fix() throws Exception {
    try {
      test("SELECT NTILE(0) OVER(PARTITION BY col7 ORDER BY col8) FROM dfs_test.`%s/window/3648.parquet`", TEST_RES_PATH);
      fail("Query should have failed");
    } catch (UserRemoteException e) {
      assertEquals(ErrorType.FUNCTION, e.getErrorType());
    }
  }

  @Test
  public void test3668Fix() throws Exception {
    testBuilder()
      .sqlQuery(getFile("window/3668.sql"), TEST_RES_PATH)
      .ordered()
      .baselineColumns("cnt").baselineValues(2L)
      .build()
      .run();
  }

  @Test
  public void testLeadParams() throws Exception {
    // make sure we only support default arguments for LEAD/LAG functions
    final String query = "SELECT %s OVER(PARTITION BY col7 ORDER BY col8) FROM dfs_test.`%s/window/3648.parquet`";

    test(query, "LEAD(col8, 1)", TEST_RES_PATH);
    test(query, "LAG(col8, 1)", TEST_RES_PATH);

    try {
      test(query, "LEAD(col8, 2)", TEST_RES_PATH);
      fail("query should fail");
    } catch (UserRemoteException e) {
      assertEquals(ErrorType.UNSUPPORTED_OPERATION, e.getErrorType());
    }

    try {
      test(query, "LAG(col8, 2)", TEST_RES_PATH);
      fail("query should fail");
    } catch (UserRemoteException e) {
      assertEquals(ErrorType.UNSUPPORTED_OPERATION, e.getErrorType());
    }
  }

  @Test
  public void testPartitionNtile() {
    Partition partition = new Partition();
    partition.updateLength(12, false);

    assertEquals(1, partition.ntile(5));
    partition.rowAggregated();
    assertEquals(1, partition.ntile(5));
    partition.rowAggregated();
    assertEquals(1, partition.ntile(5));

    partition.rowAggregated();
    assertEquals(2, partition.ntile(5));
    partition.rowAggregated();
    assertEquals(2, partition.ntile(5));
    partition.rowAggregated();
    assertEquals(2, partition.ntile(5));

    partition.rowAggregated();
    assertEquals(3, partition.ntile(5));
    partition.rowAggregated();
    assertEquals(3, partition.ntile(5));

    partition.rowAggregated();
    assertEquals(4, partition.ntile(5));
    partition.rowAggregated();
    assertEquals(4, partition.ntile(5));

    partition.rowAggregated();
    assertEquals(5, partition.ntile(5));
    partition.rowAggregated();
    assertEquals(5, partition.ntile(5));
  }

  @Test
  public void test4457() throws Exception {
    runSQL(String.format("CREATE TABLE dfs_test.tmp.`4457` AS " +
      "SELECT columns[0] AS c0, NULLIF(columns[1], 'null') AS c1 " +
      "FROM dfs_test.`%s/window/4457.csv`", TEST_RES_PATH));

    testBuilder()
      .sqlQuery("SELECT COALESCE(FIRST_VALUE(c1) OVER(ORDER BY c0 RANGE BETWEEN CURRENT ROW AND CURRENT ROW), 'EMPTY') AS fv FROM dfs_test.tmp.`4457`")
      .ordered()
      .baselineColumns("fv")
      .baselineValues("a")
      .baselineValues("b")
      .baselineValues("EMPTY")
      .go();
  }

  @Test
  public void test4657() throws Exception {
    testBuilder()
      .sqlQuery("select row_number() over(order by position_id) rn, rank() over(order by position_id) rnk from dfs_test.`%s/window/b3.p2`", TEST_RES_PATH)
      .ordered()
      .csvBaselineFile("window/4657.tsv")
      .baselineColumns("rn", "rnk")
      .expectsNumBatches(4) // we expect 3 data batches and the fast schema
      .go();
  }

}
