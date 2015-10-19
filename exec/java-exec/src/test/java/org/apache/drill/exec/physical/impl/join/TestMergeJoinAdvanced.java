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
import org.junit.Test;

public class TestMergeJoinAdvanced extends BaseTestQuery {

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
}
