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
import org.apache.drill.common.util.TestTools;
import org.apache.drill.exec.ExecConstants;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestWindowFrame extends BaseTestQuery {

  private static final String TEST_RES_PATH = TestTools.getWorkingPath() + "/src/test/resources";
  private static final String QUERY_NO_ORDERBY =
    "select count(*) over pos_win `count`, sum(salary) over pos_win `sum` from dfs_test.`%s/window/%s` window pos_win as (partition by position_id)";
  private static final String QUERY_ORDERBY =
    "select count(*) over pos_win `count`, sum(salary) over pos_win `sum`, row_number() over pos_win `row_number`, rank() over pos_win `rank`, dense_rank() over pos_win `dense_rank`, cume_dist() over pos_win `cume_dist`, percent_rank() over pos_win `percent_rank`  from dfs_test.`%s/window/%s` window pos_win as (partition by position_id order by sub)";

  @BeforeClass
  public static void setupMSortBatchSize() {
    // make sure memory sorter outputs 20 rows per batch
    final Properties props = cloneDefaultTestConfigProperties();
    props.put(ExecConstants.EXTERNAL_SORT_MSORT_MAX_BATCHSIZE, Integer.toString(20));

    updateTestCluster(1, DrillConfig.create(props));
  }

  private DrillTestWrapper buildWindowQuery(final String tableName) throws Exception {
    return testBuilder()
      .sqlQuery(String.format(QUERY_NO_ORDERBY, TEST_RES_PATH, tableName))
      .ordered()
      .csvBaselineFile("window/" + tableName + ".tsv")
      .baselineColumns("count", "sum")
      .build();
  }

  private DrillTestWrapper buildWindowWithOrderByQuery(final String tableName) throws Exception {
    return testBuilder()
      .sqlQuery(String.format(QUERY_ORDERBY, TEST_RES_PATH, tableName))
      .ordered()
      .csvBaselineFile("window/" + tableName + ".subs.tsv")
      .baselineColumns("count", "sum", "row_number", "rank", "dense_rank", "cume_dist", "percent_rank")
      .build();
  }

  private void runTest(final String tableName, final boolean withOrderBy) throws Exception {

    DrillTestWrapper testWrapper = withOrderBy ?
      buildWindowWithOrderByQuery(tableName) : buildWindowQuery(tableName);
    testWrapper.run();
  }

  /**
   * Single batch with a single partition (position_id column)
   */
  @Test
  public void testB1P1() throws Exception {
    runTest("b1.p1", false);
  }

  /**
   * Single batch with a single partition (position_id column) and multiple sub-partitions (sub column)
   */
  @Test
  public void testB1P1OrderBy() throws Exception {
    runTest("b1.p1", true);
  }

  /**
   * Single batch with 2 partitions (position_id column)
   */
  @Test
  public void testB1P2() throws Exception {
    runTest("b1.p2", false);
  }

  /**
   * Single batch with 2 partitions (position_id column)
   * with order by clause
   */
  @Test
  public void testB1P2OrderBy() throws Exception {
    runTest("b1.p2", true);
  }

  /**
   * 2 batches with 2 partitions (position_id column), each batch contains a different partition
   */
  @Test
  public void testB2P2() throws Exception {
    runTest("b2.p2", false);
  }

  @Test
  public void testB2P2OrderBy() throws Exception {
    runTest("b2.p2", true);
  }

  /**
   * 2 batches with 4 partitions, one partition has rows in both batches
   */
  @Test
  public void testB2P4() throws Exception {
    runTest("b2.p4", false);
  }

  /**
   * 2 batches with 4 partitions, one partition has rows in both batches
   * no sub partition has rows in both batches
   */
  @Test
  public void testB2P4OrderBy() throws Exception {
    runTest("b2.p4", true);
  }

  /**
   * 3 batches with 2 partitions, one partition has rows in all 3 batches
   */
  @Test
  public void testB3P2() throws Exception {
    runTest("b3.p2", false);
  }

  /**
   * 3 batches with 2 partitions, one partition has rows in all 3 batches
   * 2 subs have rows in 2 batches
   */
  @Test
  public void testB3P2OrderBy() throws Exception {
    runTest("b3.p2", true);
  }

  /**
   * 4 batches with 4 partitions. After processing 1st batch, when innerNext() is called again, framer can process
   * current batch without the need to call next(incoming).
   */
  @Test
  public void testb4P4() throws Exception {
    runTest("b4.p4", false);
  }

  @Test
  public void testb4P4OrderBy() throws Exception {
    runTest("b4.p4", true);
  }

  @Test // DRILL-3218
  public void testMaxVarChar() throws Exception {
    test("select max(cast(columns[2] as char(2))) over(partition by cast(columns[2] as char(2)) order by cast(columns[0] as int)) from dfs_test.`%s/window/allData.csv`", TEST_RES_PATH);
  }

  @Test // DRILL-1862
  public void testEmptyPartitionBy() throws Exception {
    test("SELECT employee_id, position_id, salary, SUM(salary) OVER(ORDER BY position_id) FROM cp.`employee.json` LIMIT 10");
  }

  @Test // DRILL-3172
  public void testEmptyOverClause() throws Exception {
    test("SELECT employee_id, position_id, salary, SUM(salary) OVER() FROM cp.`employee.json` LIMIT 10");
  }

}
