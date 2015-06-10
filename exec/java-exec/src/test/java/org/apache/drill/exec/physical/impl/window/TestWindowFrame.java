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

  @BeforeClass
  public static void setupMSortBatchSize() {
    // make sure memory sorter outputs 20 rows per batch
    final Properties props = cloneDefaultTestConfigProperties();
    props.put(ExecConstants.EXTERNAL_SORT_MSORT_MAX_BATCHSIZE, Integer.toString(20));

    updateTestCluster(1, DrillConfig.create(props));
  }

  private DrillTestWrapper buildWindowQuery(final String tableName, final boolean withPartitionBy) throws Exception {
    return testBuilder()
      .sqlQuery(String.format(getFile("window/q1.sql"), TEST_RES_PATH, tableName, withPartitionBy ? "(partition by position_id)":"()"))
      .ordered()
      .csvBaselineFile("window/" + tableName + (withPartitionBy ? ".pby" : "") + ".tsv")
      .baselineColumns("count", "sum")
      .build();
  }

  private DrillTestWrapper buildWindowWithOrderByQuery(final String tableName, final boolean withPartitionBy) throws Exception {
    return testBuilder()
      .sqlQuery(String.format(getFile("window/q2.sql"), TEST_RES_PATH, tableName, withPartitionBy ? "(partition by position_id order by sub)":"(order by sub)"))
      .ordered()
      .csvBaselineFile("window/" + tableName + (withPartitionBy ? ".pby" : "") + ".oby.tsv")
      .baselineColumns("count", "sum", "row_number", "rank", "dense_rank", "cume_dist", "percent_rank")
      .build();
  }

  private void runTest(final String tableName, final boolean withPartitionBy, final boolean withOrderBy) throws Exception {

    DrillTestWrapper testWrapper = withOrderBy ?
      buildWindowWithOrderByQuery(tableName, withPartitionBy) : buildWindowQuery(tableName, withPartitionBy);
    testWrapper.run();
  }

  private void runTest(final String tableName) throws Exception {
    runTest(tableName, true, true);
    runTest(tableName, true, false);
    runTest(tableName, false, true);
    runTest(tableName, false, false);
  }

  /**
   * Single batch with a single partition (position_id column)
   */
  @Test
  public void testB1P1() throws Exception {
    runTest("b1.p1");
  }

  /**
   * Single batch with 2 partitions (position_id column)
   */
  @Test
  public void testB1P2() throws Exception {
    runTest("b1.p2");
  }

  /**
   * 2 batches with 2 partitions (position_id column), each batch contains a different partition
   */
  @Test
  public void testB2P2() throws Exception {
    runTest("b2.p2");
  }

  /**
   * 2 batches with 4 partitions, one partition has rows in both batches
   */
  @Test
  public void testB2P4() throws Exception {
    runTest("b2.p4");
  }

  /**
   * 3 batches with 2 partitions, one partition has rows in all 3 batches
   */
  @Test
  public void testB3P2() throws Exception {
    runTest("b3.p2");
  }

  /**
   * 4 batches with 4 partitions. After processing 1st batch, when innerNext() is called again, framer can process
   * current batch without the need to call next(incoming).
   */
  @Test
  public void testB4P4() throws Exception {
    runTest("b4.p4");
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

}
