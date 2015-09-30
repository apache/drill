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
package org.apache.drill;


import org.apache.drill.common.util.FileUtils;
import org.apache.drill.common.util.TestTools;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestCTASPartitionFilter extends PlanTestBase {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestCTASPartitionFilter.class);

  static final String WORKING_PATH = TestTools.getWorkingPath();
  static final String TEST_RES_PATH = WORKING_PATH + "/src/test/resources";

  private static void testExcludeFilter(String query, int expectedNumFiles,
      String excludedFilterPattern, int expectedRowCount) throws Exception {
    int actualRowCount = testSql(query);
    assertEquals(expectedRowCount, actualRowCount);
    String numFilesPattern = "numFiles=" + expectedNumFiles;
    testPlanMatchingPatterns(query, new String[]{numFilesPattern}, new String[]{excludedFilterPattern});
  }

  private static void testIncludeFilter(String query, int expectedNumFiles,
                                        String includedFilterPattern, int expectedRowCount) throws Exception {
    int actualRowCount = testSql(query);
    assertEquals(expectedRowCount, actualRowCount);
    String numFilesPattern = "numFiles=" + expectedNumFiles;
    testPlanMatchingPatterns(query, new String[]{numFilesPattern, includedFilterPattern}, new String[]{});
  }

  @Test
  public void withDistribution() throws Exception {
    test("alter session set `planner.slice_target` = 1");
    test("alter session set `store.partition.hash_distribute` = true");
    test("use dfs_test.tmp");
    test(String.format("create table orders_distribution partition by (o_orderpriority) as select * from dfs_test.`%s/multilevel/parquet`", TEST_RES_PATH));
    String query = "select * from orders_distribution where o_orderpriority = '1-URGENT'";
    testExcludeFilter(query, 1, "Filter", 24);
  }

  @Test
  public void withoutDistribution() throws Exception {
    test("alter session set `planner.slice_target` = 1");
    test("alter session set `store.partition.hash_distribute` = false");
    test("use dfs_test.tmp");
    test(String.format("create table orders_no_distribution partition by (o_orderpriority) as select * from dfs_test.`%s/multilevel/parquet`", TEST_RES_PATH));
    String query = "select * from orders_no_distribution where o_orderpriority = '1-URGENT'";
    testExcludeFilter(query, 2, "Filter", 24);
  }

  @Test
  public void testDRILL3410() throws Exception {
    test("alter session set `planner.slice_target` = 1");
    test("alter session set `store.partition.hash_distribute` = true");
    test("use dfs_test.tmp");
    test(String.format("create table drill_3410 partition by (o_orderpriority) as select * from dfs_test.`%s/multilevel/parquet`", TEST_RES_PATH));
    String query = "select * from drill_3410 where (o_orderpriority = '1-URGENT' and o_orderkey = 10) or (o_orderpriority = '2-HIGH' or o_orderkey = 11)";
    testIncludeFilter(query, 5, "Filter", 34);
  }

  @Test
  public void testDRILL3414() throws Exception {
    test("alter session set `planner.slice_target` = 1");
    test("alter session set `store.partition.hash_distribute` = true");
    test("use dfs_test.tmp");
    test(String.format("create table drill_3414 partition by (dir0, dir1) as select * from dfs_test.`%s/multilevel/csv`", TEST_RES_PATH));
    String query = ("select * from drill_3414 where (dir0=1994 or dir1='Q1') and (dir0=1995 or dir1='Q2' or columns[0] > 5000)");
    testIncludeFilter(query, 6, "Filter", 20);
  }

  @Test
  public void testDRILL3414_2() throws Exception {
    test("alter session set `planner.slice_target` = 1");
    test("alter session set `store.partition.hash_distribute` = true");
    test("use dfs_test.tmp");
    test(String.format("create table drill_3414_2 partition by (dir0, dir1) as select * from dfs_test.`%s/multilevel/csv`", TEST_RES_PATH));
    String query = ("select * from drill_3414_2 where (dir0=1994 or dir1='Q1') and (dir0=1995 or dir1='Q2' or columns[0] > 5000) or columns[0] < 3000");
    testIncludeFilter(query, 1, "Filter", 120);
  }
}