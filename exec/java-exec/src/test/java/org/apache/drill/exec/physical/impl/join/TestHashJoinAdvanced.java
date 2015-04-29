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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class TestHashJoinAdvanced extends BaseTestQuery {

  // Have to disable merge join, if this testcase is to test "HASH-JOIN".
  @BeforeClass
  public static void disableMergeJoin() throws Exception {
    test("alter session set `planner.enable_mergejoin` = false");
  }

  @AfterClass
  public static void enableMergeJoin() throws Exception {
    test("alter session set `planner.enable_mergejoin` = true");
  }

  @Test //DRILL-2197 Left Self Join with complex type in projection
  public void testLeftSelfHashJoinWithMap() throws Exception {
    final String query = " select a.id, b.oooi.oa.oab.oabc oabc, b.ooof.oa.oab oab from cp.`join/complex_1.json` a left outer join cp.`join/complex_1.json` b on a.id=b.id order by a.id";

    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .jsonBaselineFile("join/DRILL-2197-result-1.json")
      .build()
      .run();
  }

  @Test //DRILL-2197 Left Join with complex type in projection
  public void testLeftHashJoinWithMap() throws Exception {
    final String query = " select a.id, b.oooi.oa.oab.oabc oabc, b.ooof.oa.oab oab from cp.`join/complex_1.json` a left outer join cp.`join/complex_2.json` b on a.id=b.id order by a.id";

    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .jsonBaselineFile("join/DRILL-2197-result-2.json")
      .build()
      .run();
  }

  @Test
  public void testFOJWithRequiredTypes() throws Exception {
    String query = "select t1.varchar_col from " +
        "cp.`parquet/drill-2707_required_types.parquet` t1 full outer join cp.`parquet/alltypes.json` t2 " +
        "on t1.int_col = t2.INT_col order by t1.varchar_col limit 1";

    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("varchar_col")
        .baselineValues("doob")
        .go();
  }

  @Test  // DRILL-2771, similar problem as DRILL-2197 except problem reproduces with right outer join instead of left
  public void testRightJoinWithMap() throws Exception {
    final String query = " select a.id, b.oooi.oa.oab.oabc oabc, b.ooof.oa.oab oab from " +
        "cp.`join/complex_1.json` b right outer join cp.`join/complex_1.json` a on a.id = b.id order by a.id";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .jsonBaselineFile("join/DRILL-2197-result-1.json")
        .build()
        .run();
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
}
