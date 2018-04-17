/*
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
package org.apache.drill.exec.store.parquet;

import org.apache.drill.PlanTestBase;
import org.apache.drill.exec.util.StoragePluginTestUtils;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;

public class TestParquetFilterPushdownWithTransitivePredicates extends PlanTestBase {

  private static final String TABLE_PATH = "parquetFilterPush/transitiveClosure";
  private static final String FIRST_TABLE_NAME = String.format("%s.`%s/%s`",
      StoragePluginTestUtils.DFS_PLUGIN_NAME, TABLE_PATH, "first");
  private static final String SECOND_TABLE_NAME = String.format("%s.`%s/%s`",
      StoragePluginTestUtils.DFS_PLUGIN_NAME, TABLE_PATH, "second");
  private static final String THIRD_TABLE_NAME = String.format("%s.`%s/%s`",
      StoragePluginTestUtils.DFS_PLUGIN_NAME, TABLE_PATH, "third");

  @BeforeClass
  public static void copyData() {
    dirTestWatcher.copyResourceToRoot(Paths.get(TABLE_PATH));
  }

  @Test
  public void testForSeveralInnerJoins() throws Exception {
    String query = String.format("SELECT * FROM %s t1 JOIN %s t2 ON t1.`month` = t2.`month` " +
            "JOIN %s t3 ON t1.`period` = t3.`period` WHERE t2.`month` = 7 AND t1.`period` = 2",
        FIRST_TABLE_NAME, SECOND_TABLE_NAME, THIRD_TABLE_NAME);

    int actualRowCount = testSql(query);
    int expectedRowCount = 24;
    assertEquals("Expected and actual row count should match", expectedRowCount, actualRowCount);

    final String[] expectedPlan = {"first.*numRowGroups=1", "second.*numRowGroups=1", "third.*numRowGroups=3"};
    testPlanMatchingPatterns(query, expectedPlan);
  }

  @Test
  public void testForFilterInJoinOperator() throws Exception {
    String query = String.format("SELECT * FROM %s t1 JOIN %s t2 ON t1.`month` = t2.`month` AND t2.`month` = 7 " +
            "JOIN %s t3 ON t1.`period` = t3.`period` AND t1.`period` = 2",
        FIRST_TABLE_NAME, SECOND_TABLE_NAME, THIRD_TABLE_NAME);

    int actualRowCount = testSql(query);
    int expectedRowCount = 24;
    assertEquals("Expected and actual row count should match", expectedRowCount, actualRowCount);

    final String[] expectedPlan = {"first.*numRowGroups=1", "second.*numRowGroups=1", "third.*numRowGroups=3"};
    testPlanMatchingPatterns(query, expectedPlan);
  }

  @Test
  public void testForLeftAndRightJoins() throws Exception {
    String query = String.format("SELECT * FROM %s t1 RIGHT JOIN %s t2 ON t1.`year` = t2.`year` " +
            "LEFT JOIN %s t3 ON t1.`period` = t3.`period` WHERE t2.`year` = 1987 AND t1.`period` = 1",
        FIRST_TABLE_NAME, SECOND_TABLE_NAME, THIRD_TABLE_NAME);

    int actualRowCount = testSql(query);
    int expectedRowCount = 54;
    assertEquals("Expected and actual row count should match", expectedRowCount, actualRowCount);

    final String[] expectedPlan = {"first.*numRowGroups=2", "second.*numRowGroups=2", "third.*numRowGroups=3"};
    testPlanMatchingPatterns(query, expectedPlan);
  }

  @Test
  public void testForCommaSeparatedJoins() throws Exception {
    String query = String.format("SELECT * FROM %s t1, %s t2, %s t3 WHERE t1.`year` = t2.`year` " +
            "AND t1.`period` = t3.`period` AND t2.`year` = 1990 AND t3.`period` = 1",
        FIRST_TABLE_NAME, SECOND_TABLE_NAME, THIRD_TABLE_NAME);

    int actualRowCount = testSql(query);
    int expectedRowCount = 24;
    assertEquals("Expected and actual row count should match", expectedRowCount, actualRowCount);

    final String[] expectedPlan = {"first.*numRowGroups=2", "second.*numRowGroups=2", "third.*numRowGroups=3"};
    testPlanMatchingPatterns(query, expectedPlan);
  }

  @Test
  public void testForInAndNotOperators() throws Exception {
    String query = String.format("SELECT * FROM %s t1 JOIN %s t2 " +
            "ON t1.`year` = t2.`year` JOIN %s t3 ON t1.`period` = t3.`period` " +
            "WHERE t2.`year` NOT IN (1987, 1988) AND t3.`period` IN (1)",
        FIRST_TABLE_NAME, SECOND_TABLE_NAME, THIRD_TABLE_NAME);


    int actualRowCount = testSql(query);
    int expectedRowCount = 24;
    assertEquals("Expected and actual row count should match", expectedRowCount, actualRowCount);

    final String[] expectedPlan = {"first.*numRowGroups=3", "second.*numRowGroups=2", "third.*numRowGroups=3"};
    testPlanMatchingPatterns(query, expectedPlan);
  }

  @Test
  public void testForBetweenOperator() throws Exception {
    String query = String.format("SELECT * FROM %s t1 JOIN %s t2 " +
            "ON t1.`year` = t2.`year` JOIN %s t3 ON t1.`period` = t3.`period` " +
            "WHERE t2.`year` BETWEEN 1988 AND 1991 AND t3.`period` BETWEEN 2 AND 4 ",
        FIRST_TABLE_NAME, SECOND_TABLE_NAME, THIRD_TABLE_NAME);

    int actualRowCount = testSql(query);
    int expectedRowCount = 60;
    assertEquals("Expected and actual row count should match", expectedRowCount, actualRowCount);

    final String[] expectedPlan = {"first.*numRowGroups=7", "second.*numRowGroups=4", "third.*numRowGroups=6"};
    testPlanMatchingPatterns(query, expectedPlan);
  }

  @Test
  public void testForGreaterThanAndLessThanOperators() throws Exception {
    String query = String.format("SELECT * FROM %s t1 JOIN %s t2 " +
            "ON t1.`year` = t2.`year` JOIN %s t3 ON t1.`period` = t3.`period` " +
            "WHERE t2.`year` >= 1990 AND t3.`period` < 2",
        FIRST_TABLE_NAME, SECOND_TABLE_NAME, THIRD_TABLE_NAME);

    int actualRowCount = testSql(query);
    int expectedRowCount = 24;
    assertEquals("Expected and actual row count should match", expectedRowCount, actualRowCount);

    final String[] expectedPlan = {"first.*numRowGroups=3", "second.*numRowGroups=2", "third.*numRowGroups=3"};
    testPlanMatchingPatterns(query, expectedPlan);
  }

  @Test
  @Ignore // For now plan has "first.*numRowGroups=7". Replacing left join to inner should be made earlier.
  public void testForTwoExists() throws Exception {
    String query = String.format("SELECT * from %s t1 " +
        " WHERE EXISTS (SELECT * FROM %s t2 WHERE t1.`year` = t2.`year` AND t2.`year` = 1988) " +
        " AND EXISTS (SELECT * FROM %s t3 WHERE t1.`period` = t3.`period` AND t3.`period` = 2)",
        FIRST_TABLE_NAME, SECOND_TABLE_NAME, THIRD_TABLE_NAME);

    int actualRowCount = testSql(query);
    int expectedRowCount = 2;
    assertEquals("Expected and actual row count should match", expectedRowCount, actualRowCount);

    final String[] expectedPlan = {"first.*numRowGroups=2", "second.*numRowGroups=2", "third.*numRowGroups=3"};
    testPlanMatchingPatterns(query, expectedPlan);
  }

  @Test
  @Ignore // For now plan has "first.*numRowGroups=16"
  public void testForFilterInHaving() throws Exception {
    String query = String.format("SELECT t1.`year`, t2.`year`, t1.`period`, t3.`period` FROM %s t1 " +
        "JOIN %s t2 ON t1.`year` = t2.`year` " +
        "JOIN %s t3 ON t1.`period` = t3.`period` " +
        "GROUP BY t1.`year`, t2.`year`, t1.`period`, t3.`period` " +
        "HAVING t2.`year` = 1987 AND t3.`period` = 1",
        FIRST_TABLE_NAME, SECOND_TABLE_NAME, THIRD_TABLE_NAME);

    int actualRowCount = testSql(query);
    int expectedRowCount = 1;
    assertEquals("Expected and actual row count should match", expectedRowCount, actualRowCount);

    final String[] expectedPlan = {"first.*numRowGroups=2", "second.*numRowGroups=2", "third.*numRowGroups=3"};
    testPlanMatchingPatterns(query, expectedPlan);
  }

  @Test
  @Ignore // For now plan has "first.*numRowGroups=16", "second.*numRowGroups=6"
  public void testForOrOperator() throws Exception {
    String query = String.format("SELECT * FROM %s t1 " +
            "JOIN %s t2 ON t1.`month` = t2.`month` " +
            "WHERE t2.`month` = 4 OR t1.`month` = 11",
        FIRST_TABLE_NAME, SECOND_TABLE_NAME);

    int actualRowCount = testSql(query);
    int expectedRowCount = 13;
    assertEquals("Expected and actual row count should match", expectedRowCount, actualRowCount);

    final String[] expectedPlan = {"first.*numRowGroups=4", "second.*numRowGroups=2"};
    testPlanMatchingPatterns(query, expectedPlan);
  }
}

