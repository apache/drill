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
package org.apache.drill.exec.planner.logical;

import org.apache.drill.PlanTestBase;
import org.apache.drill.categories.PlannerTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;

@Category(PlannerTest.class)
public class TestTransitiveClosure extends PlanTestBase {

  @BeforeClass
  public static void setupTestFiles() {
    dirTestWatcher.copyResourceToRoot(Paths.get("join"));
  }


  @Test // CALCITE-2200: (query with infinite loop)
  public void simpleInfiniteLoop() throws Exception {
    String query = "SELECT t1.department_id FROM cp.`employee.json` t1 " +
        " WHERE t1.department_id IN (SELECT department_id FROM cp.`department.json` t2 " +
        "                            WHERE t1.department_id = t2.department_id " +
        "                            OR (t1.department_id IS NULL and t2.department_id IS NULL))";
    int actualRowCount = testSql(query);
    int expectedRowCount = 1155;
    assertEquals("Expected and actual row count should match", expectedRowCount, actualRowCount);

    // TODO: After resolving CALCITE-2257 there will not be Filter with OR(IS NOT NULL($0), IS NULL($0)) condition
    // Then remove testPlanMatchingPatterns() from this test
    final String[] expectedPlan =
        new String[] {"Filter\\(condition=\\[OR\\(IS NOT NULL\\(\\$0\\), IS NULL\\(\\$0\\)\\)\\]\\)"};
    final String[] excludedPlan ={};
    testPlanMatchingPatterns(query, expectedPlan, excludedPlan);
  }

  @Test // CALCITE-2205 (query with infinite loop)
  public void infiniteLoopWhilePlaningComplexQuery() throws Exception {
    String sql = "SELECT * FROM ( " +
        "                  SELECT c_timestamp, " +
        "                         c_varchar, " +
        "                         c_date, " +
        "                         c_integer " +
        "                  FROM   dfs.`join/j1` ) AS sq3(a,b,c,d) " +
        "INNER JOIN " +
        "           ( " +
        "                            SELECT           sq2.x, " +
        "                                             sq2.y " +
        "                            FROM             ( " +
        "                                                    SELECT c_date, " +
        "                                                           c_varchar " +
        "                                                    FROM   dfs.`join/j1` " +
        "                                                    WHERE  c_bigint IS NOT NULL " +
        "                                                    AND    c_integer IN ( -499871720, " +
        "                                                                         -499763274, " +
        "                                                                         -499564607 , " +
        "                                                                         -499395929, " +
        "                                                                         -499233550, " +
        "                                                                         -499154096, " +
        "                                                                         -498966611, " +
        "                                                                         -498828740, " +
        "                                                                         -498749284 ) " +
        "                                                    AND    c_boolean IS true ) AS sq1(x, y) " +
        "                            RIGHT OUTER JOIN " +
        "                                             ( " +
        "                                                    SELECT c_timestamp, " +
        "                                                           c_varchar " +
        "                                                    FROM   dfs.`join/j2` ) AS sq2(x, y) " +
        "                            ON               ( " +
        "                                                              sq1.x = cast(sq2.x AS date)) " +
        "                            WHERE            sq2.x NOT IN ( '2015-03-01 01:49:46', " +
        "                                                           '2015-03-01 01:42:47', " +
        "                                                           '2015-03-01 01:30:41' ) " +
        "                            ORDER BY         sq2.x nulls first limit 5 offset 5 ) AS sq4(a,b) " +
        "ON (sq3.a = sq4.a)";

    int actualRecordCount = testSql(sql);
    int expectedRecordCount = 1;
    assertEquals(String.format("Received unexpected number of rows in output: expected = %d, received = %s",
        expectedRecordCount, actualRecordCount), expectedRecordCount, actualRecordCount);
  }
}
