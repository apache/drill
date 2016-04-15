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

import static org.junit.Assert.assertEquals;

import org.apache.drill.common.util.TestTools;
import org.junit.Test;

public class TestJoinNullable extends PlanTestBase {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestJoinNullable.class);

  static final String WORKING_PATH = TestTools.getWorkingPath();
  static final String TEST_RES_PATH = WORKING_PATH + "/src/test/resources";

  private static void enableJoin(boolean hj, boolean mj) throws Exception {

    test(String.format("alter session set `planner.enable_hashjoin` = %s", hj ? "true":"false"));
    test(String.format("alter session set `planner.enable_mergejoin` = %s", mj ? "true":"false"));
    test("alter session set `planner.slice_target` = 1");
  }

  private static void resetJoinOptions() throws Exception {
    test("alter session set `planner.enable_hashjoin` = true");
    test("alter session set `planner.enable_mergejoin` = true");
  }

  private static void testHelper(String query, int expectedRecordCount, boolean enableHJ, boolean enableMJ) throws Exception {
    try {
      enableJoin(enableHJ, enableMJ);
      final int actualRecordCount = testSql(query);
      assertEquals("Number of output rows", expectedRecordCount, actualRecordCount);
    } finally {
      resetJoinOptions();
    }
  }

  /** InnerJoin on nullable cols, HashJoin */
  @Test
  public void testHashInnerJoinOnNullableColumns() throws Exception {
    String query = String.format("select t1.a1, t1.b1, t2.a2, t2.b2 from dfs_test.`%s/jsoninput/nullable1.json` t1, " +
                   " dfs_test.`%s/jsoninput/nullable2.json` t2 where t1.b1 = t2.b2", TEST_RES_PATH, TEST_RES_PATH);
    testHelper(query, 1, true, false);
  }

  /** LeftOuterJoin on nullable cols, HashJoin */
  @Test
  public void testHashLOJOnNullableColumns() throws Exception {
    String query = String.format("select t1.a1, t1.b1, t2.a2, t2.b2 from dfs_test.`%s/jsoninput/nullable1.json` t1 " +
                      " left outer join dfs_test.`%s/jsoninput/nullable2.json` t2 " +
                      " on t1.b1 = t2.b2", TEST_RES_PATH, TEST_RES_PATH);

    testHelper(query, 2, true, false);
  }

  /** RightOuterJoin on nullable cols, HashJoin */
  @Test
  public void testHashROJOnNullableColumns() throws Exception {
    String query = String.format("select t1.a1, t1.b1, t2.a2, t2.b2 from dfs_test.`%s/jsoninput/nullable1.json` t1 " +
                      " right outer join dfs_test.`%s/jsoninput/nullable2.json` t2 " +
                      " on t1.b1 = t2.b2", TEST_RES_PATH, TEST_RES_PATH);

    testHelper(query, 4, true, false);
  }

  /** FullOuterJoin on nullable cols, HashJoin */
  @Test
  public void testHashFOJOnNullableColumns() throws Exception {
    String query = String.format("select t1.a1, t1.b1, t2.a2, t2.b2 from dfs_test.`%s/jsoninput/nullable1.json` t1 " +
                      " full outer join dfs_test.`%s/jsoninput/nullable2.json` t2 " +
                      " on t1.b1 = t2.b2", TEST_RES_PATH, TEST_RES_PATH);

    testHelper(query, 5, true, false);
  }

  /** InnerJoin on nullable cols, MergeJoin */
  @Test
  public void testMergeInnerJoinOnNullableColumns() throws Exception {
    String query =
        String.format(
            "select t1.a1, t1.b1, t2.a2, t2.b2 "
            + "  from dfs_test.`%s/jsoninput/nullable1.json` t1, "
            + "       dfs_test.`%s/jsoninput/nullable2.json` t2 "
            + " where t1.b1 = t2.b2", TEST_RES_PATH, TEST_RES_PATH);

    testHelper(query, 1, false, true);
  }

  /** LeftOuterJoin on nullable cols, MergeJoin */
  @Test
  public void testMergeLOJNullable() throws Exception {
    String query = String.format("select t1.a1, t1.b1, t2.a2, t2.b2 from dfs_test.`%s/jsoninput/nullable1.json` t1 " +
                      " left outer join dfs_test.`%s/jsoninput/nullable2.json` t2 " +
                      " on t1.b1 = t2.b2", TEST_RES_PATH, TEST_RES_PATH);
    testHelper(query, 2, false, true);
  }

  /** RightOuterJoin on nullable cols, MergeJoin */
  @Test
  public void testMergeROJOnNullableColumns() throws Exception {
    String query = String.format("select t1.a1, t1.b1, t2.a2, t2.b2 from dfs_test.`%s/jsoninput/nullable1.json` t1 " +
                      " right outer join dfs_test.`%s/jsoninput/nullable2.json` t2 " +
                      " on t1.b1 = t2.b2", TEST_RES_PATH, TEST_RES_PATH);
    testHelper(query, 4, false, true);
  }


  /** Left outer join, merge, nullable col. - unordered inputs. */
  @Test
  public void testMergeLOJNullableNoOrderedInputs() throws Exception {
    String query =
        String.format(
            "SELECT * "
            + "FROM               dfs_test.`%s/jsoninput/nullableOrdered1.json` t1 "
            + "   left outer join dfs_test.`%s/jsoninput/nullableOrdered2.json` t2 "
            + "      using ( key )",
            TEST_RES_PATH, TEST_RES_PATH);
    testHelper(query, 6, false, true);
  }

  /** Left outer join, merge, nullable col. - ordered right, ASC NULLS FIRST (nulls low). */
  @Test
  public void testMergeLOJNullableOneOrderedInputAscNullsFirst() throws Exception {
    String query =
        String.format(
            "SELECT * "
            + "from         dfs_test.`%s/jsoninput/nullableOrdered1.json` t1 "
            + "  LEFT OUTER JOIN "
            + "    ( SELECT key, data "
            + "        FROM dfs_test.`%s/jsoninput/nullableOrdered2.json` t2 "
            + "        ORDER BY 1 ASC NULLS FIRST ) t2 "
            + "    USING ( key )",
            TEST_RES_PATH, TEST_RES_PATH);
    testHelper(query, 6, false, true);
  }

  /** Left outer join, merge, nullable col.  - ordered right, ASC NULLS LAST (nulls high). */
  @Test
  public void testMergeLOJNullableOneOrderedInputAscNullsLast() throws Exception {
    String query =
        String.format(
            "SELECT * "
            + "FROM         dfs_test.`%s/jsoninput/nullableOrdered1.json` t1 "
            + "  LEFT OUTER JOIN "
            + "    ( SELECT key, data "
            + "        FROM dfs_test.`%s/jsoninput/nullableOrdered2.json` t2 "
            + "        ORDER BY 1 ASC NULLS LAST ) t2 "
            + "    USING ( key )",
            TEST_RES_PATH, TEST_RES_PATH);
    testHelper(query, 6, false, true);
  }

  /** Left outer join, merge, nullable col. - ordered right, DESC NULLS FIRST (nulls high). */
  @Test
  public void testMergeLOJNullableOneOrderedInputDescNullsFirst() throws Exception {
    String query =
        String.format(
            "SELECT * "
            + "FROM         dfs_test.`%s/jsoninput/nullableOrdered1.json` t1 "
            + "  LEFT OUTER JOIN "
            + "    ( SELECT key, data "
            + "        FROM dfs_test.`%s/jsoninput/nullableOrdered2.json` t2 "
            + "        ORDER BY 1 DESC NULLS FIRST ) t2 "
            + "    USING ( key )",
            TEST_RES_PATH, TEST_RES_PATH);
    testHelper(query, 6, false, true);
  }

  /** Left outer join, merge, nullable col. - ordered right, DESC NULLS LAST (nulls low). */
  @Test
  public void testMergeLOJNullableOneOrderedInputDescNullsLast() throws Exception {
    String query =
        String.format(
            "SELECT * "
            + "FROM         dfs_test.`%s/jsoninput/nullableOrdered1.json` t1 "
            + "  LEFT OUTER JOIN "
            + "    ( SELECT key, data "
            + "        FROM dfs_test.`%s/jsoninput/nullableOrdered2.json` t2 "
            + "        ORDER BY 1 DESC NULLS LAST ) t2 "
            + "    USING ( key )",
            TEST_RES_PATH, TEST_RES_PATH);
    testHelper(query, 6, false, true);
  }

  /** Left outer join, merge, nullable col. - ordered inputs, both ASC NULLS FIRST (nulls low). */
  @Test
  public void testMergeLOJNullableBothInputsOrderedAscNullsFirstVsAscNullsFirst() throws Exception {
    String query =
        String.format(
            "SELECT * "
            + "from ( SELECT key, data "
            + "         FROM dfs_test.`%s/jsoninput/nullableOrdered1.json` "
            + "         ORDER BY 1 ASC NULLS FIRST ) t1 "
            + "  LEFT OUTER JOIN "
            + "     ( SELECT key, data "
            + "         FROM dfs_test.`%s/jsoninput/nullableOrdered2.json` "
            + "         ORDER BY 1 ASC NULLS FIRST ) t2 "
            + "    USING ( key )",
            TEST_RES_PATH, TEST_RES_PATH);
    testHelper(query, 6, false, true);
  }

  /** Left outer join, merge, nullable col. - ordered inputs, different null order. */
  @Test
  public void testMergeLOJNullableBothInputsOrderedAscNullsLastVsAscNullsFirst() throws Exception {
    String query =
        String.format(
            "SELECT * "
            + "from ( SELECT key, data "
            + "         FROM dfs_test.`%s/jsoninput/nullableOrdered1.json` "
            + "         ORDER BY 1 ASC NULLS LAST  ) t1 "
            + "  LEFT OUTER JOIN "
            + "     ( SELECT key, data "
            + "         FROM dfs_test.`%s/jsoninput/nullableOrdered2.json` "
            + "         ORDER BY 1 ASC NULLS FIRST ) t2 "
            + "    USING ( key )",
            TEST_RES_PATH, TEST_RES_PATH);
    testHelper(query, 6, false, true);
  }

  /** Left outer join, merge, nullable col. - ordered inputs, other different null order. */
  @Test
  public void testMergeLOJNullableBothInputsOrderedAscNullsFirstVsAscNullsLast() throws Exception {
    String query =
        String.format(
            "SELECT * "
            + "from ( SELECT key, data "
            + "         FROM dfs_test.`%s/jsoninput/nullableOrdered1.json` "
            + "         ORDER BY 1 ASC NULLS FIRST ) t1 "
            + "  LEFT OUTER JOIN "
            + "     ( SELECT key, data "
            + "         FROM dfs_test.`%s/jsoninput/nullableOrdered2.json` "
            + "         ORDER BY 1 ASC NULLS LAST  ) t2 "
            + "    USING ( key )",
            TEST_RES_PATH, TEST_RES_PATH);
    testHelper(query, 6, false, true);
  }

  /** Left outer join, merge, nullable col. - ordered inputs, both ASC NULLS LAST (nulls high) */
  @Test
  public void testMergeLOJNullableBothInputsOrderedAscNullsLastVsAscNullsLast() throws Exception {
    String query =
        String.format(
            "SELECT * "
            + "from ( SELECT key, data "
            + "         FROM dfs_test.`%s/jsoninput/nullableOrdered1.json` "
            + "         ORDER BY 1 ASC NULLS LAST  ) t1 "
            + "  LEFT OUTER JOIN "
            + "     ( SELECT key, data "
            + "         FROM dfs_test.`%s/jsoninput/nullableOrdered2.json` "
            + "         ORDER BY 1 ASC NULLS LAST  ) t2 "
            + "    USING ( key )",
            TEST_RES_PATH, TEST_RES_PATH);
    testHelper(query, 6, false, true);
  }

  /** Left outer join, merge, nullable col. - ordered inputs, DESC vs. ASC, NULLS
      FIRST (nulls high vs. nulls low). */
  @Test
  public void testMergeLOJNullableBothInputsOrderedDescNullsFirstVsAscNullsFirst() throws Exception {
    String query =
        String.format(
            "SELECT * "
            + "from ( SELECT key, data "
            + "         FROM dfs_test.`%s/jsoninput/nullableOrdered1.json` "
            + "         ORDER BY 1 DESC NULLS FIRST ) t1 "
            + "  LEFT OUTER JOIN "
            + "     ( SELECT key, data "
            + "         FROM dfs_test.`%s/jsoninput/nullableOrdered2.json` "
            + "         ORDER BY 1 ASC NULLS FIRST ) t2 "
            + "    USING ( key )",
            TEST_RES_PATH, TEST_RES_PATH);
    testHelper(query, 6, false, true);
  }

  /** Left outer join, merge, nullable col. - ordered inputs, DESC vs. ASC, NULLS
      LAST vs. FIRST (both nulls low). */
  @Test
  public void testMergeLOJNullableBothInputsOrderedDescNullsLastVsAscNullsFirst() throws Exception {
    String query =
        String.format(
            "SELECT * "
            + "from ( SELECT key, data "
            + "         FROM dfs_test.`%s/jsoninput/nullableOrdered1.json` "
            + "         ORDER BY 1 DESC NULLS LAST  ) t1 "
            + "  LEFT OUTER JOIN "
            + "     ( SELECT key, data "
            + "         FROM dfs_test.`%s/jsoninput/nullableOrdered2.json` "
            + "         ORDER BY 1 ASC NULLS FIRST ) t2 "
            + "    USING ( key )",
            TEST_RES_PATH, TEST_RES_PATH);
    testHelper(query, 6, false, true);
  }

  /** Left outer join, merge, nullable col. - ordered inputs, DESC vs. ASC, NULLS
      FIRST vs. LAST (both nulls high). */
  @Test
  public void testMergeLOJNullableBothInputsOrderedDescNullsFirstVsAscNullsLast() throws Exception {
    String query =
        String.format(
            "SELECT * "
            + "from ( SELECT key, data "
            + "         FROM dfs_test.`%s/jsoninput/nullableOrdered1.json` "
            + "         ORDER BY 1 DESC NULLS FIRST ) t1 "
            + "  LEFT OUTER JOIN "
            + "     ( SELECT key, data "
            + "         FROM dfs_test.`%s/jsoninput/nullableOrdered2.json` "
            + "         ORDER BY 1 ASC NULLS LAST  ) t2 "
            + "    USING ( key )",
            TEST_RES_PATH, TEST_RES_PATH);
    testHelper(query, 6, false, true);
  }

  /** Left outer join, merge, nullable col. - ordered inputs, DESC vs. ASC, NULLS
      LAST (nulls low vs. nulls high). */
  @Test
  public void testMergeLOJNullableBothInputsOrderedDescNullsLastVsAscNullsLast() throws Exception {
    String query =
        String.format(
            "SELECT * "
            + "from ( SELECT key, data "
            + "         FROM dfs_test.`%s/jsoninput/nullableOrdered1.json` "
            + "         ORDER BY 1 DESC NULLS LAST  ) t1 "
            + "  LEFT OUTER JOIN "
            + "     ( SELECT key, data "
            + "         FROM dfs_test.`%s/jsoninput/nullableOrdered2.json` "
            + "         ORDER BY 1 ASC NULLS LAST  ) t2 "
            + "    USING ( key )",
            TEST_RES_PATH, TEST_RES_PATH);
    testHelper(query, 6, false, true);
  }

  @Test
  public void withDistinctFromJoinConditionHashJoin() throws Exception {
    final String query = "SELECT * FROM " +
            "cp.`jsonInput/nullableOrdered1.json` t1 JOIN " +
            "cp.`jsonInput/nullableOrdered2.json` t2 " +
            "ON t1.key IS NOT DISTINCT FROM t2.key AND t1.data is NOT null";
    testPlanSubstrPatterns(query, new String[] { "HashJoin", "IS NOT DISTINCT FROM" }, null);
    nullEqualJoinHelper(query);
  }

  @Test
  public void withDistinctFromJoinConditionMergeJoin() throws Exception {
    try {
      test("alter session set `planner.enable_hashjoin` = false");
      final String query = "SELECT * FROM " +
              "cp.`jsonInput/nullableOrdered1.json` t1 JOIN " +
              "cp.`jsonInput/nullableOrdered2.json` t2 " +
              "ON t1.key IS NOT DISTINCT FROM t2.key";
      testPlanSubstrPatterns(query, new String[] { "MergeJoin", "IS NOT DISTINCT FROM" }, null);
      nullEqualJoinHelper(query);
    } finally {
      test("alter session set `planner.enable_hashjoin` = true");
    }
  }

  @Test
  public void withNullEqualHashJoin() throws Exception {
    final String query = "SELECT * FROM " +
            "cp.`jsonInput/nullableOrdered1.json` t1 JOIN " +
            "cp.`jsonInput/nullableOrdered2.json` t2 " +
            "ON t1.key = t2.key OR (t1.key IS NULL AND t2.key IS NULL)";
    testPlanSubstrPatterns(query, new String[] { "HashJoin", "IS NOT DISTINCT FROM" }, null);
    nullEqualJoinHelper(query);
  }

  @Test
  public void withNullEqualMergeJoin() throws Exception {
    try {
      test("alter session set `planner.enable_hashjoin` = false");
      final String query = "SELECT * FROM " +
          "cp.`jsonInput/nullableOrdered1.json` t1 JOIN " +
          "cp.`jsonInput/nullableOrdered2.json` t2 " +
          "ON t1.key = t2.key OR (t1.key IS NULL AND t2.key IS NULL)";
      testPlanSubstrPatterns(query, new String[] { "MergeJoin", "IS NOT DISTINCT FROM" }, null);
      nullEqualJoinHelper(query);
    } finally {
      test("alter session set `planner.enable_hashjoin` = true");
    }
  }

  @Test
  public void withNullEqualInWhereConditionHashJoin() throws Exception {
    final String query = "SELECT * FROM " +
        "cp.`jsonInput/nullableOrdered1.json` t1, " +
        "cp.`jsonInput/nullableOrdered2.json` t2 " +
        "WHERE t1.key = t2.key OR (t1.key IS NULL AND t2.key IS NULL)";
    testPlanSubstrPatterns(query, new String[] { "HashJoin", "IS NOT DISTINCT FROM" }, null);
    nullEqualJoinHelper(query);
  }

  @Test
  public void withNullEqualInWhereConditionMergeJoin() throws Exception {
    try {
      test("alter session set `planner.enable_hashjoin` = false");
      final String query = "SELECT * FROM " +
          "cp.`jsonInput/nullableOrdered1.json` t1, " +
          "cp.`jsonInput/nullableOrdered2.json` t2 " +
          "WHERE t1.key = t2.key OR (t1.key IS NULL AND t2.key IS NULL)";
      testPlanSubstrPatterns(query, new String[] { "MergeJoin", "IS NOT DISTINCT FROM" }, null);
      nullEqualJoinHelper(query);
    } finally {
      test("alter session set `planner.enable_hashjoin` = true");
    }
  }

  @Test
  public void withNullEqualInWhereConditionNegative() throws Exception {
    final String query = "SELECT * FROM " +
        "cp.`jsonInput/nullableOrdered1.json` t1, " +
        "cp.`jsonInput/nullableOrdered2.json` t2, " +
        "cp.`jsonInput/nullableOrdered3.json` t3 " +
        "WHERE t1.key = t2.key OR (t1.key IS NULL AND t2.key IS NULL)";
    errorMsgTestHelper(query,
        "This query cannot be planned possibly due to either a cartesian join or an inequality join");
  }

  @Test
  public void withNullEqualInWhereConditionThreeTableHashJoin() throws Exception {
    final String query = "SELECT * FROM " +
        "cp.`jsonInput/nullableOrdered1.json` t1, " +
        "cp.`jsonInput/nullableOrdered2.json` t2, " +
        "cp.`jsonInput/nullableOrdered3.json` t3 " +
        "WHERE (t1.key = t2.key OR (t1.key IS NULL AND t2.key IS NULL)) AND" +
        "(t2.key = t3.key OR (t2.key IS NULL AND t3.key IS NULL))";
    testPlanSubstrPatterns(query, new String[] { "HashJoin", "IS NOT DISTINCT FROM" }, null);
  }

  @Test
  public void withNullEqualInWhereConditionThreeTableMergeJoin() throws Exception {
    try {
      test("alter session set `planner.enable_hashjoin` = false");
      final String query = "SELECT * FROM " +
          "cp.`jsonInput/nullableOrdered1.json` t1, " +
          "cp.`jsonInput/nullableOrdered2.json` t2, " +
          "cp.`jsonInput/nullableOrdered3.json` t3 " +
          "WHERE (t1.key = t2.key OR (t1.key IS NULL AND t2.key IS NULL)) AND" +
          "(t2.key = t3.key OR (t2.key IS NULL AND t3.key IS NULL))";
      testPlanSubstrPatterns(query, new String[]{"MergeJoin", "IS NOT DISTINCT FROM"}, null);
      nullEqual3WayJoinHelper(query);
    } finally {
      test("alter session set `planner.enable_hashjoin` = true");
    }
  }

  public void nullEqualJoinHelper(final String query) throws Exception {
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("key", "data", "data0", "key0")
        .baselineValues(null, "L_null_1", "R_null_1", null)
        .baselineValues(null, "L_null_2", "R_null_1", null)
        .baselineValues("A", "L_A_1", "R_A_1", "A")
        .baselineValues("A", "L_A_2", "R_A_1", "A")
        .baselineValues(null, "L_null_1", "R_null_2", null)
        .baselineValues(null, "L_null_2", "R_null_2", null)
        .baselineValues(null, "L_null_1", "R_null_3", null)
        .baselineValues(null, "L_null_2", "R_null_3", null)
        .go();
  }

  public void nullEqual3WayJoinHelper(final String query) throws Exception {
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("key", "data", "data0", "key0", "data1", "key1")
        .baselineValues(null, "L_null_1", "R_null_1", null, "RR_null_1", null)
        .baselineValues(null, "L_null_2", "R_null_1", null, "RR_null_1", null)
        .baselineValues("A", "L_A_1", "R_A_1", "A", "RR_A_1", "A")
        .baselineValues("A", "L_A_2", "R_A_1", "A", "RR_A_1", "A")
        .baselineValues("A", "L_A_1", "R_A_1", "A", "RR_A_2", "A")
        .baselineValues("A", "L_A_2", "R_A_1", "A", "RR_A_2", "A")
        .baselineValues(null, "L_null_1", "R_null_2", null, "RR_null_1", null)
        .baselineValues(null, "L_null_2", "R_null_2", null, "RR_null_1", null)
        .baselineValues(null, "L_null_1", "R_null_3", null, "RR_null_1", null)
        .baselineValues(null, "L_null_2", "R_null_3", null, "RR_null_1", null)
        .go();
  }

  @Test
  public void withNullEqualAdditionFilter() throws Exception {
    final String query = "SELECT * FROM " +
        "cp.`jsonInput/nullableOrdered1.json` t1 JOIN " +
        "cp.`jsonInput/nullableOrdered2.json` t2 " +
        "ON (t1.key = t2.key OR (t1.key IS NULL AND t2.key IS NULL)) AND t1.data LIKE '%1%'";

    testPlanSubstrPatterns(query,
        new String[] {
            "HashJoin(condition=[IS NOT DISTINCT FROM($1, $5)], joinType=[inner])",
            "Filter(condition=[$3])", // 'like' is pushed into project
            "[LIKE($2, '%1%')]"
        },
        null);

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("key", "data", "data0", "key0")
        .baselineValues(null, "L_null_1", "R_null_1", null)
        .baselineValues("A", "L_A_1", "R_A_1", "A")
        .baselineValues(null, "L_null_1", "R_null_2", null)
        .baselineValues(null, "L_null_1", "R_null_3", null)
        .go();
  }

  @Test
  public void withMixedEqualAndIsNotDistinctHashJoin() throws Exception {
    enableJoin(true, false);
    try {
      final String query = "SELECT * FROM " +
          "cp.`jsonInput/nullEqualJoin1.json` t1 JOIN " +
          "cp.`jsonInput/nullEqualJoin2.json` t2 " +
          "ON t1.key = t2.key AND t1.data is not distinct from t2.data";
      testPlanOneExpectedPattern(query, "HashJoin.*condition.*AND\\(=\\(.*IS NOT DISTINCT FROM*");
      nullMixedComparatorEqualJoinHelper(query);
    } finally {
      resetJoinOptions();
    }
  }

  @Test
  public void withMixedEqualAndIsNotDistinctMergeJoin() throws Exception {
    enableJoin(false, true);
    try {
      final String query = "SELECT * FROM " +
          "cp.`jsonInput/nullEqualJoin1.json` t1 JOIN " +
          "cp.`jsonInput/nullEqualJoin2.json` t2 " +
          "ON t1.key = t2.key AND t1.data is not distinct from t2.data";
      testPlanOneExpectedPattern(query, "MergeJoin.*condition.*AND\\(=\\(.*IS NOT DISTINCT FROM*");
      nullMixedComparatorEqualJoinHelper(query);
    } finally {
      resetJoinOptions();
    }
  }

  @Test
  public void withMixedEqualAndIsNotDistinctFilterHashJoin() throws Exception {
    enableJoin(true, false);
    try {
      final String query = "SELECT * FROM " +
          "cp.`jsonInput/nullEqualJoin1.json` t1 JOIN " +
          "cp.`jsonInput/nullEqualJoin2.json` t2 " +
          "ON t1.key = t2.key " +
          "WHERE t1.data is not distinct from t2.data";
      // Expected the filter to be pushed into the join
      testPlanOneExpectedPattern(query, "HashJoin.*condition.*AND\\(=\\(.*IS NOT DISTINCT FROM*");
      nullMixedComparatorEqualJoinHelper(query);
    } finally {
      resetJoinOptions();
    }
  }

  @Test
  public void withMixedEqualAndIsNotDistinctFilterMergeJoin() throws Exception {
    enableJoin(false, true);
    try {
      final String query = "SELECT * FROM " +
          "cp.`jsonInput/nullEqualJoin1.json` t1 JOIN " +
          "cp.`jsonInput/nullEqualJoin2.json` t2 " +
          "ON t1.key = t2.key " +
          "WHERE t1.data is not distinct from t2.data";
      // Expected the filter to be pushed into the join
      testPlanOneExpectedPattern(query, "MergeJoin.*condition.*AND\\(=\\(.*IS NOT DISTINCT FROM*");
      nullMixedComparatorEqualJoinHelper(query);
    } finally {
      resetJoinOptions();
    }
  }

  public void nullMixedComparatorEqualJoinHelper(final String query) throws Exception {
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("key", "data", "data0", "key0")
        .baselineValues("A", "L_A_1", "L_A_1", "A")
        .baselineValues("A", null, null, "A")
        .baselineValues("B", null, null, "B")
        .baselineValues("B", "L_B_1", "L_B_1", "B")
        .go();
  }
}
