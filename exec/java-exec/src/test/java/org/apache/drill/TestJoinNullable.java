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

public class TestJoinNullable extends BaseTestQuery{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestJoinNullable.class);

  static final String WORKING_PATH = TestTools.getWorkingPath();
  static final String TEST_RES_PATH = WORKING_PATH + "/src/test/resources";

  private static void enableJoin(boolean hj, boolean mj) throws Exception {

    test(String.format("alter session set `planner.enable_hashjoin` = %s", hj ? "true":"false"));
    test(String.format("alter session set `planner.enable_mergejoin` = %s", mj ? "true":"false"));
    test("alter session set `planner.slice_target` = 1");
  }

  @Test  // InnerJoin on nullable cols, HashJoin
  public void testHashInnerJoinOnNullableColumns() throws Exception {
    String query = String.format("select t1.a1, t1.b1, t2.a2, t2.b2 from dfs_test.`%s/jsoninput/nullable1.json` t1, " +
                   " dfs_test.`%s/jsoninput/nullable2.json` t2 where t1.b1 = t2.b2", TEST_RES_PATH ,TEST_RES_PATH);
    int actualRecordCount;
    int expectedRecordCount = 1;

    enableJoin(true, false);
    actualRecordCount = testSql(query);
    assertEquals(String.format("Received unexepcted number of rows in output: expected=%d, received=%s",
        expectedRecordCount, actualRecordCount), expectedRecordCount, actualRecordCount);
  }

  @Test // InnerJoin on nullable cols, MergeJoin
  public void testMergeInnerJoinOnNullableColumns() throws Exception {
    String query = String.format("select t1.a1, t1.b1, t2.a2, t2.b2 from dfs_test.`%s/jsoninput/nullable1.json` t1, " +
                   " dfs_test.`%s/jsoninput/nullable2.json` t2 where t1.b1 = t2.b2", TEST_RES_PATH ,TEST_RES_PATH);
    int actualRecordCount;
    int expectedRecordCount = 1;

    enableJoin(false, true);
    actualRecordCount = testSql(query);
    assertEquals(String.format("Received unexepcted number of rows in output: expected=%d, received=%s",
        expectedRecordCount, actualRecordCount), expectedRecordCount, actualRecordCount);
  }

  @Test // LeftOuterJoin on nullable cols, HashJoin
  public void testHashLOJOnNullableColumns() throws Exception {
    String query = String.format("select t1.a1, t1.b1, t2.a2, t2.b2 from dfs_test.`%s/jsoninput/nullable1.json` t1 " +
                      " left outer join dfs_test.`%s/jsoninput/nullable2.json` t2 " +
                      " on t1.b1 = t2.b2", TEST_RES_PATH ,TEST_RES_PATH);

    int actualRecordCount;
    int expectedRecordCount = 2;

    enableJoin(true, false);
    actualRecordCount = testSql(query);
    assertEquals(String.format("Received unexepcted number of rows in output: expected=%d, received=%s",
        expectedRecordCount, actualRecordCount), expectedRecordCount, actualRecordCount);
  }

  @Test // RightOuterJoin on nullable cols, HashJoin
  public void testHashROJOnNullableColumns() throws Exception {
    String query = String.format("select t1.a1, t1.b1, t2.a2, t2.b2 from dfs_test.`%s/jsoninput/nullable1.json` t1 " +
                      " right outer join dfs_test.`%s/jsoninput/nullable2.json` t2 " +
                      " on t1.b1 = t2.b2", TEST_RES_PATH ,TEST_RES_PATH);

    int actualRecordCount;
    int expectedRecordCount = 4;

    enableJoin(true, false);
    actualRecordCount = testSql(query);
    assertEquals(String.format("Received unexepcted number of rows in output: expected=%d, received=%s",
        expectedRecordCount, actualRecordCount), expectedRecordCount, actualRecordCount);
  }

  @Test // FullOuterJoin on nullable cols, HashJoin
  public void testHashFOJOnNullableColumns() throws Exception {
    String query = String.format("select t1.a1, t1.b1, t2.a2, t2.b2 from dfs_test.`%s/jsoninput/nullable1.json` t1 " +
                      " full outer join dfs_test.`%s/jsoninput/nullable2.json` t2 " +
                      " on t1.b1 = t2.b2", TEST_RES_PATH ,TEST_RES_PATH);

    int actualRecordCount;
    int expectedRecordCount = 5;

    enableJoin(true, false);
    actualRecordCount = testSql(query);
    assertEquals(String.format("Received unexepcted number of rows in output: expected=%d, received=%s",
        expectedRecordCount, actualRecordCount), expectedRecordCount, actualRecordCount);
  }

  @Test // LeftOuterJoin on nullable cols, MergeJoin
  public void testMergeLOJOnNullableColumns() throws Exception {
    String query = String.format("select t1.a1, t1.b1, t2.a2, t2.b2 from dfs_test.`%s/jsoninput/nullable1.json` t1 " +
                      " left outer join dfs_test.`%s/jsoninput/nullable2.json` t2 " +
                      " on t1.b1 = t2.b2", TEST_RES_PATH ,TEST_RES_PATH);

    int actualRecordCount;
    int expectedRecordCount = 2;

    enableJoin(false, true);
    actualRecordCount = testSql(query);
    assertEquals(String.format("Received unexepcted number of rows in output: expected=%d, received=%s",
        expectedRecordCount, actualRecordCount), expectedRecordCount, actualRecordCount);
  }

  @Test // RightOuterJoin on nullable cols, MergeJoin
  public void testMergeROJOnNullableColumns() throws Exception {
    String query = String.format("select t1.a1, t1.b1, t2.a2, t2.b2 from dfs_test.`%s/jsoninput/nullable1.json` t1 " +
                      " right outer join dfs_test.`%s/jsoninput/nullable2.json` t2 " +
                      " on t1.b1 = t2.b2", TEST_RES_PATH ,TEST_RES_PATH);

    int actualRecordCount;
    int expectedRecordCount = 4;

    enableJoin(false, true);
    actualRecordCount = testSql(query);
    assertEquals(String.format("Received unexepcted number of rows in output: expected=%d, received=%s",
        expectedRecordCount, actualRecordCount), expectedRecordCount, actualRecordCount);
  }

}
