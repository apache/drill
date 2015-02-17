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
import org.junit.Ignore;
import org.junit.Test;

public class TestJoinNullable extends BaseTestQuery{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestJoinNullable.class);

  static final String WORKING_PATH = TestTools.getWorkingPath();
  static final String TEST_RES_PATH = WORKING_PATH + "/src/test/resources";

  private static void enableJoin(boolean hj, boolean mj) throws Exception {

    test(String.format("alter session set `planner.enable_hashjoin` = %s", hj ? "true":"false"));
    test(String.format("alter session set `planner.enable_mergejoin` = %s", mj ? "true":"false"));
    test("alter session set `planner.slice_target` = 1");
  }

  /** InnerJoin on nullable cols, HashJoin */
  @Test
  public void testHashInnerJoinOnNullableColumns() throws Exception {
    String query = String.format("select t1.a1, t1.b1, t2.a2, t2.b2 from dfs_test.`%s/jsoninput/nullable1.json` t1, " +
                   " dfs_test.`%s/jsoninput/nullable2.json` t2 where t1.b1 = t2.b2", TEST_RES_PATH, TEST_RES_PATH);
    final int expectedRecordCount = 1;

    enableJoin(true, false);
    final int actualRecordCount = testSql(query);
    assertEquals("Number of output rows", expectedRecordCount, actualRecordCount);
  }

  /** LeftOuterJoin on nullable cols, HashJoin */
  @Test
  public void testHashLOJOnNullableColumns() throws Exception {
    String query = String.format("select t1.a1, t1.b1, t2.a2, t2.b2 from dfs_test.`%s/jsoninput/nullable1.json` t1 " +
                      " left outer join dfs_test.`%s/jsoninput/nullable2.json` t2 " +
                      " on t1.b1 = t2.b2", TEST_RES_PATH, TEST_RES_PATH);

    final int expectedRecordCount = 2;

    enableJoin(true, false);
    final int actualRecordCount = testSql(query);
    assertEquals("Number of output rows", expectedRecordCount, actualRecordCount);
  }

  /** RightOuterJoin on nullable cols, HashJoin */
  @Test
  public void testHashROJOnNullableColumns() throws Exception {
    String query = String.format("select t1.a1, t1.b1, t2.a2, t2.b2 from dfs_test.`%s/jsoninput/nullable1.json` t1 " +
                      " right outer join dfs_test.`%s/jsoninput/nullable2.json` t2 " +
                      " on t1.b1 = t2.b2", TEST_RES_PATH, TEST_RES_PATH);

    final int expectedRecordCount = 4;

    enableJoin(true, false);
    final int actualRecordCount = testSql(query);
    assertEquals("Number of output rows", expectedRecordCount, actualRecordCount);
  }

  /** FullOuterJoin on nullable cols, HashJoin */
  @Test
  public void testHashFOJOnNullableColumns() throws Exception {
    String query = String.format("select t1.a1, t1.b1, t2.a2, t2.b2 from dfs_test.`%s/jsoninput/nullable1.json` t1 " +
                      " full outer join dfs_test.`%s/jsoninput/nullable2.json` t2 " +
                      " on t1.b1 = t2.b2", TEST_RES_PATH, TEST_RES_PATH);

    final int expectedRecordCount = +5;

    enableJoin(true, false);
    final int actualRecordCount = testSql(query);
    assertEquals("Number of output rows", expectedRecordCount, actualRecordCount);
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
    final int expectedRecordCount = 1;

    enableJoin(false, true);
    final int actualRecordCount = testSql(query);
    assertEquals("Number of output rows", expectedRecordCount, actualRecordCount);
  }

  /** LeftOuterJoin on nullable cols, MergeJoin */
  @Test
  public void testMergeLOJNullable() throws Exception {
    String query = String.format("select t1.a1, t1.b1, t2.a2, t2.b2 from dfs_test.`%s/jsoninput/nullable1.json` t1 " +
                      " left outer join dfs_test.`%s/jsoninput/nullable2.json` t2 " +
                      " on t1.b1 = t2.b2", TEST_RES_PATH, TEST_RES_PATH);

    final int expectedRecordCount = 2;

    enableJoin(false, true);
    final int actualRecordCount = testSql(query);
    assertEquals("Number of output rows", expectedRecordCount, actualRecordCount);
  }

  /** RightOuterJoin on nullable cols, MergeJoin */
  @Test
  public void testMergeROJOnNullableColumns() throws Exception {
    String query = String.format("select t1.a1, t1.b1, t2.a2, t2.b2 from dfs_test.`%s/jsoninput/nullable1.json` t1 " +
                      " right outer join dfs_test.`%s/jsoninput/nullable2.json` t2 " +
                      " on t1.b1 = t2.b2", TEST_RES_PATH, TEST_RES_PATH);

    final int expectedRecordCount = 4;

    enableJoin(false, true);
    final int actualRecordCount = testSql(query);
    assertEquals("Number of output rows", expectedRecordCount, actualRecordCount);
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
    final int expectedRecordCount = 6;

    enableJoin(false, true);
    final int actualRecordCount = testSql(query);
    assertEquals("Number of output rows", expectedRecordCount, actualRecordCount);
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
    final int expectedRecordCount = 6;

    enableJoin(false, true);
    final int actualRecordCount = testSql(query);
    assertEquals("Number of output rows", expectedRecordCount, actualRecordCount);
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
    final int expectedRecordCount = 6;

    enableJoin(false, true);
    final int actualRecordCount = testSql(query);
    assertEquals("Number of output rows", expectedRecordCount, actualRecordCount);
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
    final int expectedRecordCount = 6;

    enableJoin(false, true);
    final int actualRecordCount = testSql(query);
    assertEquals("Number of output rows", expectedRecordCount, actualRecordCount);
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
    final int expectedRecordCount = 6;

    enableJoin(false, true);
    final int actualRecordCount = testSql(query);
    assertEquals("Number of output rows", expectedRecordCount, actualRecordCount);
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
    final int expectedRecordCount = 6;

    enableJoin(false, true);
    final int actualRecordCount = testSql(query);
    assertEquals("Number of output rows", expectedRecordCount, actualRecordCount);
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
    final int expectedRecordCount = 6;

    enableJoin(false, true);
    final int actualRecordCount = testSql(query);
    assertEquals("Number of output rows", expectedRecordCount, actualRecordCount);
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
    final int expectedRecordCount = 6;

    enableJoin(false, true);
    final int actualRecordCount = testSql(query);
    assertEquals("Number of output rows", expectedRecordCount, actualRecordCount);
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
    final int expectedRecordCount = 6;

    enableJoin(false, true);
    final int actualRecordCount = testSql(query);
    assertEquals("Number of output rows", expectedRecordCount, actualRecordCount);
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
    final int expectedRecordCount = 6;

    enableJoin(false, true);
    final int actualRecordCount = testSql(query);
    assertEquals("Number of output rows", expectedRecordCount, actualRecordCount);
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
    final int expectedRecordCount = 6;

    enableJoin(false, true);
    final int actualRecordCount = testSql(query);
    assertEquals("Number of output rows", expectedRecordCount, actualRecordCount);
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
    final int expectedRecordCount = 6;

    enableJoin(false, true);
    final int actualRecordCount = testSql(query);
    assertEquals("Number of output rows", expectedRecordCount, actualRecordCount);
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
    final int expectedRecordCount = 6;

    enableJoin(false, true);
    final int actualRecordCount = testSql(query);
    assertEquals("Number of output rows", expectedRecordCount, actualRecordCount);
  }

}
