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
package org.apache.drill.exec.planner.sql;

import org.apache.drill.categories.SqlTest;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;

@Category(SqlTest.class)
public class TestPlanCachePlanning extends ClusterTest {
  @BeforeClass
  public static void setupCluster() throws Exception {
    startCluster(ClusterFixture.builder(dirTestWatcher));
  }

  @Before
  public void disableCacheForBaseline() {
    client.alterSession(PlannerSettings.ENABLE_PLAN_CACHE_OPTION, false);
  }

  @Test
  public void testHavingMatchesGroupedConcatenationOnMissAndHit() throws Exception {
    String sql = "SELECT x || 'a' AS k, COUNT(*) AS n "
        + "FROM (VALUES ('b', 1), ('b', 2), ('c', 3)) AS t(x, v) "
        + "WHERE v > %d GROUP BY x || 'a' HAVING x || 'a' = 'ba'";
    client.testBuilder().sqlQuery(sql, 0).unOrdered()
        .baselineColumns("k", "n").baselineValues("ba", 2L).go();

    client.alterSession(PlannerSettings.ENABLE_PLAN_CACHE_OPTION, true);
    long hits = cache().getHitCount();
    client.testBuilder().sqlQuery(sql, 0).unOrdered()
        .baselineColumns("k", "n").baselineValues("ba", 2L).go();
    cache().awaitWrites();
    assertEquals(hits, cache().getHitCount());

    // WHERE values remain bindable even though the matching HAVING literals are preserved.
    client.testBuilder().sqlQuery(sql, 1).unOrdered()
        .baselineColumns("k", "n").baselineValues("ba", 1L).go();
    assertEquals(hits + 1, cache().getHitCount());
  }

  @Test
  public void testHavingMatchesGroupedArithmeticAndKeepsLiteralsInKey() throws Exception {
    String sql = "SELECT x + 1 AS k, COUNT(*) AS n "
        + "FROM (VALUES (1), (1), (2)) AS t(x) GROUP BY x + 1 HAVING x + 1 = %d";
    client.testBuilder().sqlQuery(sql, 2).unOrdered()
        .baselineColumns("k", "n").baselineValues(2, 2L).go();

    client.alterSession(PlannerSettings.ENABLE_PLAN_CACHE_OPTION, true);
    long hits = cache().getHitCount();
    client.testBuilder().sqlQuery(sql, 2).unOrdered()
        .baselineColumns("k", "n").baselineValues(2, 2L).go();
    cache().awaitWrites();
    assertEquals(hits, cache().getHitCount());
    client.testBuilder().sqlQuery(sql, 2).unOrdered()
        .baselineColumns("k", "n").baselineValues(2, 2L).go();
    assertEquals(hits + 1, cache().getHitCount());

    // A different preserved HAVING literal must not reuse the first template.
    client.testBuilder().sqlQuery(sql, 3).unOrdered()
        .baselineColumns("k", "n").baselineValues(3, 1L).go();
    cache().awaitWrites();
    assertEquals(hits + 1, cache().getHitCount());
  }

  @Test
  public void testQualifyMatchesOrderedProjectionOnMissAndHit() throws Exception {
    String sql = "SELECT x + 1 AS k, ROW_NUMBER() OVER (ORDER BY x + 1) AS rn "
        + "FROM (VALUES (1, 1), (1, 2), (2, 3)) AS t(x, v) "
        + "WHERE v > %d QUALIFY x + 1 = 2 "
        + "AND ROW_NUMBER() OVER (ORDER BY x + 1) = 1 ORDER BY x + 1";
    client.testBuilder().sqlQuery(sql, 0).unOrdered()
        .baselineColumns("k", "rn").baselineValues(2, 1L).go();

    client.alterSession(PlannerSettings.ENABLE_PLAN_CACHE_OPTION, true);
    long hits = cache().getHitCount();
    client.testBuilder().sqlQuery(sql, 0).unOrdered()
        .baselineColumns("k", "rn").baselineValues(2, 1L).go();
    cache().awaitWrites();
    assertEquals(hits, cache().getHitCount());
    client.testBuilder().sqlQuery(sql, 1).unOrdered()
        .baselineColumns("k", "rn").baselineValues(2, 1L).go();
    assertEquals(hits + 1, cache().getHitCount());
  }

  private PlanCache cache() {
    return cluster.drillbit().getContext().getPlanCache();
  }
}
