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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.sql.SqlNode;
import org.apache.drill.categories.SqlTest;
import org.apache.drill.common.config.DrillProperties;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.metrics.DrillMetrics;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.rowSet.DirectRowSet;
import org.apache.drill.exec.physical.rowSet.RowSetReader;
import org.apache.drill.exec.planner.PhysicalPlanReader;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.planner.sql.conversion.SqlConverter;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.UserBitShared.UserCredentials;
import org.apache.drill.exec.rpc.user.UserSession;
import org.apache.drill.exec.util.Pointer;
import org.apache.drill.test.ClientFixture;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

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
    long misses = metric("misses");
    client.testBuilder().sqlQuery(sql, 0).unOrdered()
        .baselineColumns("k", "n").baselineValues("ba", 2L).go();
    cache().awaitWrites();
    assertEquals(hits, cache().getHitCount());
    assertEquals(misses + 1, metric("misses"));

    // WHERE values remain bindable even though the matching HAVING literals are preserved.
    client.testBuilder().sqlQuery(sql, 1).unOrdered()
        .baselineColumns("k", "n").baselineValues("ba", 1L).go();
    assertEquals(hits + 1, cache().getHitCount());
    assertEquals(hits + 1, metric("hits"));
    assertEquals(misses + 1, metric("misses"));
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

  @Test
  public void testClearPlanCacheForcesMissThenAllowsNewHits() throws Exception {
    String sql = "SELECT x AS clear_value FROM (VALUES (1), (2)) AS t(x) WHERE x > %d";
    client.alterSession(PlannerSettings.ENABLE_PLAN_CACHE_OPTION, true);
    client.testBuilder().sqlQuery(sql, 0).unOrdered()
        .baselineColumns("clear_value").baselineValues(1).baselineValues(2).go();
    cache().awaitWrites();
    long hits = cache().getHitCount();
    client.testBuilder().sqlQuery(sql, 1).unOrdered()
        .baselineColumns("clear_value").baselineValues(2).go();
    assertEquals(hits + 1, cache().getHitCount());

    long invalidations = metric("invalidations");
    clearCacheWithSql();
    assertEquals(0L, metric("entries"));
    assertEquals(invalidations + 1, metric("invalidations"));
    assertEquals(hits + 1, cache().getHitCount());
    client.testBuilder().sqlQuery(sql, 0).unOrdered()
        .baselineColumns("clear_value").baselineValues(1).baselineValues(2).go();
    cache().awaitWrites();
    assertEquals(hits + 1, cache().getHitCount());
    client.testBuilder().sqlQuery(sql, 1).unOrdered()
        .baselineColumns("clear_value").baselineValues(2).go();
    assertEquals(hits + 2, cache().getHitCount());

    client.alterSession(PlannerSettings.ENABLE_PLAN_CACHE_OPTION, false);
    clearCacheWithSql();
    clearCacheWithSql();
    assertEquals(0L, metric("entries"));
  }

  private void clearCacheWithSql() throws Exception {
    client.testBuilder().sqlQuery("ALTER SYSTEM CLEAR PLAN CACHE").unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(true, String.format("Plan cache cleared on Drillbit %s:%d.",
            cluster.drillbit().getContext().getEndpoint().getAddress(),
            cluster.drillbit().getContext().getEndpoint().getUserPort())).go();
  }

  private long metric(String name) {
    return (Long) DrillMetrics.getRegistry().getGauges().get("drill.plan_cache." + name).getValue();
  }

  @Test
  public void testDifferentSessionOptionsKeepSeparateCachedPlans() throws Exception {
    String sql = "SELECT x AS option_value FROM (VALUES (1), (2)) AS t(x) WHERE x > %d";
    try (ClientFixture first = cluster.clientBuilder().property(DrillProperties.USER, "options-test").build();
         ClientFixture second = cluster.clientBuilder().property(DrillProperties.USER, "options-test").build()) {
      first.alterSession(PlannerSettings.ENABLE_PLAN_CACHE_OPTION, true);
      second.alterSession(PlannerSettings.ENABLE_PLAN_CACHE_OPTION, true);
      first.alterSession(ExecConstants.SLICE_TARGET, 10000);
      second.alterSession(ExecConstants.SLICE_TARGET, 20000);
      long hits = cache().getHitCount();
      first.testBuilder().sqlQuery(sql, 0).unOrdered()
          .baselineColumns("option_value").baselineValues(1).baselineValues(2).go();
      cache().awaitWrites();
      second.testBuilder().sqlQuery(sql, 0).unOrdered()
          .baselineColumns("option_value").baselineValues(1).baselineValues(2).go();
      cache().awaitWrites();
      assertEquals(hits, cache().getHitCount());

      // Alternating clients must keep hitting their own entry rather than evicting each other.
      first.testBuilder().sqlQuery(sql, 1).unOrdered()
          .baselineColumns("option_value").baselineValues(2).go();
      second.testBuilder().sqlQuery(sql, 1).unOrdered()
          .baselineColumns("option_value").baselineValues(2).go();
      assertEquals(hits + 2, cache().getHitCount());
    }
  }

  @Test
  public void testPublicationUsesSnapshotBeforeExecutionMutatesOperators() throws Exception {
    UserSession session = UserSession.Builder.newBuilder()
        .withCredentials(UserCredentials.newBuilder().setUserName("snapshot-test").build())
        .withOptionManager(cluster.drillbit().getContext().getOptionManager())
        .setSupportComplexTypes(true).build();
    session.getOptions().setLocalOption(PlannerSettings.ENABLE_PLAN_CACHE_OPTION, true);
    String sql = "SELECT x AS snapshot_value FROM (VALUES (1), (2)) AS t(x) WHERE x > 0";
    try (QueryContext context = new QueryContext(session, cluster.drillbit().getContext(), QueryId.getDefaultInstance())) {
      PhysicalPlan planned = DrillSqlWorker.getPlan(context, sql, new Pointer<>());
      Runnable publish = context.takePendingPlanCacheInsert();
      assertNotNull(publish);
      List<Integer> originalIds = planned.getSortedOperators().stream()
          .map(PhysicalOperator::getOperatorId).collect(Collectors.toList());
      planned.getSortedOperators().forEach(operator -> operator.setOperatorId(operator.getOperatorId() + 1000));

      publish.run();
      cache().awaitWrites();
      SqlConverter converter = new SqlConverter(context);
      SqlNode parsed = converter.parse(sql);
      PlanCache.ContextSnapshot snapshot = PlanCache.ContextSnapshot.resolve(converter.getDefaultSchema(), parsed, context);
      assertNotNull(snapshot);
      PlanCacheParameterizer.Candidate candidate = PlanCacheParameterizer.parameterize(parsed, converter.getTypeFactory());
      String key = context.getQueryUserName() + '\n' + session.getDefaultSchemaPath() + '\n'
          + snapshot.keyFingerprint() + '\n' + candidate.template;
      PlanCache.Entry entry = cache().get(key);
      assertNotNull(entry);
      PhysicalPlan cached = entry.bind(candidate.literals, cluster.drillbit().getContext().getPlanReader());
      List<Integer> cachedIds = cached.getSortedOperators().stream()
          .map(PhysicalOperator::getOperatorId).collect(Collectors.toList());
      assertEquals(originalIds, cachedIds);
      assertNotEquals(planned.getSortedOperators().stream().map(PhysicalOperator::getOperatorId)
          .collect(Collectors.toList()), cachedIds);
    }
  }

  @Test
  public void testClearPreventsPublicationPendingQueryCompletion() throws Exception {
    UserSession session = UserSession.Builder.newBuilder()
        .withCredentials(UserCredentials.newBuilder().setUserName("clear-pending-test").build())
        .withOptionManager(cluster.drillbit().getContext().getOptionManager())
        .setSupportComplexTypes(true).build();
    session.getOptions().setLocalOption(PlannerSettings.ENABLE_PLAN_CACHE_OPTION, true);
    try (QueryContext context = new QueryContext(session, cluster.drillbit().getContext(), QueryId.getDefaultInstance())) {
      DrillSqlWorker.getPlan(context,
          "SELECT x AS pending_value FROM (VALUES (1), (2)) AS t(x) WHERE x > 0", new Pointer<>());
      Runnable publish = context.takePendingPlanCacheInsert();
      assertNotNull(publish);
      clearCacheWithSql();
      publish.run();
      cache().awaitWrites();
      assertEquals(0L, metric("entries"));
    }
  }

  @Test
  public void testBindingFailureInvalidatesAndReplansBeforeExecution() throws Exception {
    UserSession session = UserSession.Builder.newBuilder()
        .withCredentials(UserCredentials.newBuilder().setUserName("binding-failure-test").build())
        .withOptionManager(cluster.drillbit().getContext().getOptionManager())
        .setSupportComplexTypes(true).build();
    session.getOptions().setLocalOption(PlannerSettings.ENABLE_PLAN_CACHE_OPTION, true);
    String sql = "SELECT x AS fallback_value FROM (VALUES (1), (2)) AS t(x) WHERE x > %d";
    PhysicalPlanReader reader = cluster.drillbit().getContext().getPlanReader();
    try (QueryContext context = new QueryContext(session, cluster.drillbit().getContext(), QueryId.getDefaultInstance())) {
      PhysicalPlan planned = DrillSqlWorker.getPlan(context, String.format(sql, 0), new Pointer<>());
      context.takePendingPlanCacheInsert();
      SqlConverter converter = new SqlConverter(context);
      SqlNode parsed = converter.parse(String.format(sql, 0));
      PlanCache.ContextSnapshot snapshot = PlanCache.ContextSnapshot.resolve(converter.getDefaultSchema(), parsed, context);
      PlanCacheParameterizer.Candidate candidate = PlanCacheParameterizer.parameterize(parsed, converter.getTypeFactory());
      String key = context.getQueryUserName() + '\n' + session.getDefaultSchemaPath() + '\n'
          + snapshot.keyFingerprint() + '\n' + candidate.template;
      String json = reader.writeJson(planned);
      assertTrue(json.contains("bound_dynamic_param(0,"));
      // Readable plan JSON with a nonexistent slot must fail binding before execution.
      assertTrue(cache().put(key, json.replace("bound_dynamic_param(0,", "bound_dynamic_param(999,"),
          null, reader, snapshot));
    }

    long hits = cache().getHitCount();
    long invalidations = metric("invalidations");
    try (QueryContext context = new QueryContext(session, cluster.drillbit().getContext(), QueryId.getDefaultInstance())) {
      PhysicalPlan replanned = DrillSqlWorker.getPlan(context, String.format(sql, 0), new Pointer<>());
      assertFalse(context.isPlanCacheHit());
      assertEquals(invalidations + 1, metric("invalidations"));
      assertEquals(Arrays.asList(1, 2), executeValues(replanned));
      Runnable publish = context.takePendingPlanCacheInsert();
      assertNotNull(publish);
      publish.run();
      cache().awaitWrites();
    }
    try (QueryContext context = new QueryContext(session, cluster.drillbit().getContext(), QueryId.getDefaultInstance())) {
      PhysicalPlan rebound = DrillSqlWorker.getPlan(context, String.format(sql, 1), new Pointer<>());
      assertTrue(context.isPlanCacheHit());
      assertEquals(Arrays.asList(2), executeValues(rebound));
      assertEquals(hits + 1, cache().getHitCount());
    }
  }

  private List<Integer> executeValues(PhysicalPlan plan) throws Exception {
    DirectRowSet rows = client.queryBuilder().physical(cluster.drillbit().getContext().getPlanReader().writeJson(plan)).rowSet();
    try {
      List<Integer> values = new ArrayList<>();
      RowSetReader reader = rows.reader();
      while (reader.next()) {
        values.add(reader.scalar(0).getInt());
      }
      return values;
    } finally {
      rows.clear();
    }
  }
}
