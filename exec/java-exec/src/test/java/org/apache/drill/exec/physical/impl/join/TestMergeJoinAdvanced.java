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
package org.apache.drill.exec.physical.impl.join;

import org.apache.drill.BaseTestQuery;
import org.apache.drill.common.util.TestTools;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class TestMergeJoinAdvanced extends BaseTestQuery {
  @Rule
  public final TestRule TIMEOUT = TestTools.getTimeoutRule(120000); // Longer timeout than usual.

  // Have to disable hash join to test merge join in this class
  @BeforeClass
  public static void disableMergeJoin() throws Exception {
    test("alter session set `planner.enable_hashjoin` = false");
  }

  @AfterClass
  public static void enableMergeJoin() throws Exception {
    test("alter session set `planner.enable_hashjoin` = true");
  }

  @Test
  public void testJoinWithDifferentTypesInCondition() throws Exception {
    String query = "select t1.full_name from cp.`employee.json` t1, cp.`department.json` t2 " +
        "where cast(t1.department_id as double) = t2.department_id and t1.employee_id = 1";

    testBuilder()
        .sqlQuery(query)
        .optionSettingQueriesForTestQuery("alter session set `planner.enable_hashjoin` = true")
        .unOrdered()
        .baselineColumns("full_name")
        .baselineValues("Sheri Nowmer")
        .go();


    query = "select t1.bigint_col from cp.`jsoninput/implicit_cast_join_1.json` t1, cp.`jsoninput/implicit_cast_join_1.json` t2 " +
        " where t1.bigint_col = cast(t2.bigint_col as int) and" + // join condition with bigint and int
        " t1.double_col  = cast(t2.double_col as float) and" + // join condition with double and float
        " t1.bigint_col = cast(t2.bigint_col as double)"; // join condition with bigint and double

    testBuilder()
        .sqlQuery(query)
        .optionSettingQueriesForTestQuery("alter session set `planner.enable_hashjoin` = true")
        .unOrdered()
        .baselineColumns("bigint_col")
        .baselineValues(1l)
        .go();

    query = "select count(*) col1 from " +
        "(select t1.date_opt from cp.`parquet/date_dictionary.parquet` t1, cp.`parquet/timestamp_table.parquet` t2 " +
        "where t1.date_opt = t2.timestamp_col)"; // join condition contains date and timestamp

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("col1")
        .baselineValues(4l)
        .go();
  }

  @Test
  @Ignore // TODO file JIRA to fix this
  public void testFix2967() throws Exception {
    setSessionOption(PlannerSettings.BROADCAST.getOptionName(), "false");
    setSessionOption(PlannerSettings.HASHJOIN.getOptionName(), "false");
    setSessionOption(ExecConstants.SLICE_TARGET, "1");
    setSessionOption(ExecConstants.MAX_WIDTH_PER_NODE_KEY, "23");

    final String TEST_RES_PATH = TestTools.getWorkingPath() + "/src/test/resources";

    try {
      test("select * from dfs_test.`%s/join/j1` j1 left outer join dfs_test.`%s/join/j2` j2 on (j1.c_varchar = j2.c_varchar)",
        TEST_RES_PATH, TEST_RES_PATH);
    } finally {
      setSessionOption(PlannerSettings.BROADCAST.getOptionName(), String.valueOf(PlannerSettings.BROADCAST.getDefault().bool_val));
      setSessionOption(PlannerSettings.HASHJOIN.getOptionName(), String.valueOf(PlannerSettings.HASHJOIN.getDefault().bool_val));
      setSessionOption(ExecConstants.SLICE_TARGET, String.valueOf(ExecConstants.SLICE_TARGET_DEFAULT));
      setSessionOption(ExecConstants.MAX_WIDTH_PER_NODE_KEY, String.valueOf(ExecConstants.MAX_WIDTH_PER_NODE.getDefault().num_val));
    }
  }

  private static void generateData(final BufferedWriter leftWriter, final BufferedWriter rightWriter,
                             final long left, final long right) throws IOException {
    for (int i=0; i < left; ++i) {
      leftWriter.write(String.format("{ \"k\" : %d , \"v\": %d }", 10000, i));
    }
    leftWriter.write(String.format("{ \"k\" : %d , \"v\": %d }", 10001, 10001));
    leftWriter.write(String.format("{ \"k\" : %d , \"v\": %d }", 10002, 10002));

    for (int i=0; i < right; ++i) {
      rightWriter.write(String.format("{ \"k1\" : %d , \"v1\": %d }", 10000, i));
    }
    rightWriter.write(String.format("{ \"k1\" : %d , \"v1\": %d }", 10004, 10004));
    rightWriter.write(String.format("{ \"k1\" : %d , \"v1\": %d }", 10005, 10005));
    rightWriter.write(String.format("{ \"k1\" : %d , \"v1\": %d }", 10006, 10006));

    leftWriter.close();
    rightWriter.close();
  }

  private static void testMultipleBatchJoin(final long right, final long left,
                                            final String joinType, final long expected) throws Exception {
    final String leftSide = BaseTestQuery.getTempDir("merge-join-left.json");
    final String rightSide = BaseTestQuery.getTempDir("merge-join-right.json");
    final BufferedWriter leftWriter = new BufferedWriter(new FileWriter(new File(leftSide)));
    final BufferedWriter rightWriter = new BufferedWriter(new FileWriter(new File(rightSide)));
    generateData(leftWriter, rightWriter, left, right);
    final String query1 = String.format("select count(*) c1 from dfs_test.`%s` L %s join dfs_test.`%s` R on L.k=R.k1",
      leftSide, joinType, rightSide);
    testBuilder()
      .sqlQuery(query1)
      .optionSettingQueriesForTestQuery("alter session set `planner.enable_hashjoin` = false")
      .unOrdered()
      .baselineColumns("c1")
      .baselineValues(expected)
      .go();
  }

  @Test
  public void testMergeInnerJoinLargeRight() throws Exception {
    testMultipleBatchJoin(1000l, 5000l, "inner", 5000l * 1000l);
  }

  @Test
  public void testMergeLeftJoinLargeRight() throws Exception {
    testMultipleBatchJoin(1000l, 5000l, "left", 5000l * 1000l +2l);
  }

  @Test
  public void testMergeRightJoinLargeRight() throws Exception {
    testMultipleBatchJoin(1000l, 5000l, "right", 5000l*1000l +3l);
  }

  @Test
  public void testMergeInnerJoinLargeLeft() throws Exception {
    testMultipleBatchJoin(5000l, 1000l, "inner", 5000l*1000l);
  }

  @Test
  public void testMergeLeftJoinLargeLeft() throws Exception {
    testMultipleBatchJoin(5000l, 1000l, "left", 5000l*1000l + 2l);
  }

  @Test
  public void testMergeRightJoinLargeLeft() throws Exception {
    testMultipleBatchJoin(5000l, 1000l, "right", 5000l*1000l + 3l);
  }

  // Following tests can take some time.
  @Test
  @Ignore
  public void testMergeInnerJoinRandomized() throws Exception {
    final Random r = new Random();
    final long right = r.nextInt(10001) + 1l;
    final long left = r.nextInt(10001) + 1l;
    testMultipleBatchJoin(left, right, "inner", left*right);
  }

  @Test
  @Ignore
  public void testMergeLeftJoinRandomized() throws Exception {
    final Random r = new Random();
    final long right = r.nextInt(10001) + 1l;
    final long left = r.nextInt(10001) + 1l;
    testMultipleBatchJoin(left, right, "left", left*right + 2l);
  }

  @Test
  @Ignore
  public void testMergeRightJoinRandomized() throws Exception {
    final Random r = new Random();
    final long right = r.nextInt(10001) + 1l;
    final long left = r.nextInt(10001) + 1l;
    testMultipleBatchJoin(left, right, "right", left * right + 3l);
  }

  @Test
  public void testDrill4165() throws Exception {
    final String query1 = "select count(*) cnt from cp.`tpch/lineitem.parquet` l1, cp.`tpch/lineitem.parquet` l2 where l1.l_partkey = l2.l_partkey and l1.l_suppkey < 30 and l2.l_suppkey < 30";
    testBuilder()
      .sqlQuery(query1)
      .unOrdered()
      .baselineColumns("cnt")
      .baselineValues(202452l)
      .go();
  }

  @Test
  public void testDrill4196() throws Exception {
    final String leftSide = BaseTestQuery.getTempDir("merge-join-left.json");
    final String rightSide = BaseTestQuery.getTempDir("merge-join-right.json");
    final BufferedWriter leftWriter = new BufferedWriter(new FileWriter(new File(leftSide)));
    final BufferedWriter rightWriter = new BufferedWriter(new FileWriter(new File(rightSide)));

    // output batch is 32k, create 60k left batch
    leftWriter.write(String.format("{ \"k\" : %d , \"v\": %d }", 9999, 9999));
    for (int i=0; i < 6000; ++i) {
      leftWriter.write(String.format("{ \"k\" : %d , \"v\": %d }", 10000, 10000));
    }
    leftWriter.write(String.format("{ \"k\" : %d , \"v\": %d }", 10001, 10001));
    leftWriter.write(String.format("{ \"k\" : %d , \"v\": %d }", 10002, 10002));

    // Keep all values same. Jon will consume entire right side.
    for (int i=0; i < 800; ++i) {
      rightWriter.write(String.format("{ \"k1\" : %d , \"v1\": %d }", 10000, 10000));
    }

    leftWriter.close();
    rightWriter.close();

    final String query1 = String.format("select count(*) c1 from dfs_test.`%s` L %s join dfs_test.`%s` R on L.k=R.k1",
      leftSide, "inner", rightSide);
    testBuilder()
      .sqlQuery(query1)
      .optionSettingQueriesForTestQuery("alter session set `planner.enable_hashjoin` = false")
      .unOrdered()
      .baselineColumns("c1")
      .baselineValues(6000*800L)
      .go();
  }
}
