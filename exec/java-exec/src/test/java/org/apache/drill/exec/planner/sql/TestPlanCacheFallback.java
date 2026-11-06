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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.apache.drill.categories.SqlTest;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.exceptions.UserRemoteException;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.metrics.DrillMetrics;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.UserBitShared.UserCredentials;
import org.apache.drill.exec.rpc.user.UserSession;
import org.apache.drill.exec.testing.Controls;
import org.apache.drill.exec.testing.ControlsInjectionUtil;
import org.apache.drill.exec.util.Pointer;
import org.apache.drill.exec.work.foreman.ForemanSetupException;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@Category(SqlTest.class)
public class TestPlanCacheFallback extends ClusterTest {
  private static final String FAILURE_SITE = "plan-cache-parameterized-planning";
  private static final String SQL = "SELECT x FROM (SELECT x FROM (VALUES (1), (2), (3)) AS t(x)) AS s WHERE x > 1";
  private static final String CSV_TABLE = "dfs.tmp.`plan_cache_fallback.csvh`";
  private final Logger logger = (Logger) LoggerFactory.getLogger(DrillSqlWorker.class);
  private final ListAppender<ILoggingEvent> logs = new ListAppender<>();
  private Level previousLevel;

  @BeforeClass
  public static void setupCluster() throws Exception {
    startCluster(ClusterFixture.builder(dirTestWatcher));
    // Initialize the function registry before fault injection so its separate first-error
    // synchronization retry does not make these tests depend on execution order.
    cluster.drillbit().getContext().getFunctionImplementationRegistry().syncWithRemoteRegistry(-1);
    Path file = dirTestWatcher.getDfsTestTmpDir().toPath().resolve("plan_cache_fallback.csvh");
    Files.write(file, Arrays.asList("id,label", "1,one", "2,two", "3,three"), StandardCharsets.UTF_8);
  }

  @Before
  public void setup() throws Exception {
    client.alterSession(PlannerSettings.ENABLE_PLAN_CACHE_OPTION, false);
    client.resetSession(PlannerSettings.ENABLE_PLAN_CACHE_FALLBACK_OPTION);
    cache().awaitWrites();
    cache().clear();
    previousLevel = logger.getLevel();
    logger.setLevel(Level.WARN);
    logs.start();
    logger.addAppender(logs);
  }

  @After
  public void restoreLogging() {
    logger.detachAppender(logs);
    logs.stop();
    logger.setLevel(previousLevel);
  }

  @Test
  public void testDefaultFallbackReparsesOriginalSqlAndDoesNotPublish() throws Exception {
    try (UserSession session = session()) {
      ControlsInjectionUtil.setControls(session, failureControls());
      long failures = metric("planning_failures");
      long misses = metric("misses");
      try (QueryContext context = context(session)) {
        assertTrue(context.getOptions().getOption(PlannerSettings.PLAN_CACHE_FALLBACK));
        context.getOptions().setLocalOption(ExecConstants.SLICE_TARGET, 17L);
        context.getOptions().setLocalOption(ExecConstants.QUERY_MAX_ROWS, 1L);
        Pointer<String> textPlan = new Pointer<>();
        PhysicalPlan plan = DrillSqlWorker.getPlan(context, SQL, textPlan);
        assertFalse(context.isPlanCacheHit());
        assertNull(context.takePendingPlanCacheInsert());
        assertEquals(17L, context.getOptions().getOption(ExecConstants.SLICE_TARGET).num_val.longValue());
        assertEquals(1L, context.getOptions().getOption(ExecConstants.QUERY_MAX_ROWS).num_val.longValue());
        assertFalse(cluster.drillbit().getContext().getPlanReader().writeJson(plan).contains("bound_dynamic_param("));
        assertNotNull(textPlan.value);
        assertFalse(textPlan.value.contains("?0"));
        // The original literals and the client row limit must both survive retry.
        assertEquals(1, client.queryBuilder()
            .physical(cluster.drillbit().getContext().getPlanReader().writeJson(plan)).run().recordCount());
      }
      assertEquals(failures + 1, metric("planning_failures"));
      assertEquals(misses + 1, metric("misses"));
      assertEquals(0L, metric("entries"));
      ILoggingEvent warning = onlyWarning();
      assertTrue(warning.getFormattedMessage().contains("retrying the original SQL"));
      assertTrue(warning.getFormattedMessage().contains("SQL template:"));
      assertEquals(IOException.class.getName(), warning.getThrowableProxy().getClassName());
      cache().clear();
      assertEquals(failures + 1, metric("planning_failures"));
    }
  }

  @Test
  public void testDisabledFallbackWarnsAndPropagatesParameterizedFailure() throws Exception {
    try (UserSession session = session()) {
      session.getOptions().setLocalOption(PlannerSettings.ENABLE_PLAN_CACHE_FALLBACK_OPTION, false);
      ControlsInjectionUtil.setControls(session, failureControls());
      long failures = metric("planning_failures");
      try (QueryContext context = context(session)) {
        QueryInputException error = assertThrows(QueryInputException.class,
            () -> DrillSqlWorker.getPlan(context, SQL));
        assertEquals(IOException.class, error.getCause().getClass());
        assertEquals(FAILURE_SITE, error.getCause().getMessage());
        assertNull(context.takePendingPlanCacheInsert());
      }
      assertEquals(failures, metric("planning_failures"));
      assertTrue(onlyWarning().getFormattedMessage().contains(PlannerSettings.ENABLE_PLAN_CACHE_FALLBACK_OPTION));
      assertEquals(0L, metric("entries"));
    }
  }

  @Test
  public void testBothPlanningAttemptsFailWithoutCounting() throws Exception {
    String invalidSql = "SELECT missing_column FROM (VALUES (1), (2)) AS t(x) WHERE x > 0";
    try (UserSession session = session()) {
      session.getOptions().setLocalOption(PlannerSettings.ENABLE_PLAN_CACHE_OPTION, false);
      UserException ordinary;
      try (QueryContext context = context(session)) {
        ordinary = assertThrows(UserException.class, () -> DrillSqlWorker.getPlan(context, invalidSql));
      }
      session.getOptions().setLocalOption(PlannerSettings.ENABLE_PLAN_CACHE_OPTION, true);
      long failures = metric("planning_failures");
      long misses = metric("misses");
      try (QueryContext context = context(session)) {
        UserException retried = assertThrows(UserException.class, () -> DrillSqlWorker.getPlan(context, invalidSql));
        assertEquals(ordinary.getErrorType(), retried.getErrorType());
        assertEquals(ordinary.getOriginalMessage(), retried.getOriginalMessage());
        assertNull(context.takePendingPlanCacheInsert());
      }
      assertEquals(failures, metric("planning_failures"));
      assertEquals(misses + 1, metric("misses"));
      onlyWarning();
    }
  }

  @Test
  public void testFallbackFailurePropagatesOrdinaryException() throws Exception {
    try (UserSession session = session()) {
      String controls = Controls.newBuilder()
          .addException(DrillSqlWorker.class, FAILURE_SITE, IOException.class)
          .addException(DrillSqlWorker.class, "sql-parsing", ForemanSetupException.class, 1, 1).build();
      ControlsInjectionUtil.setControls(session, controls);
      long failures = metric("planning_failures");
      try (QueryContext context = context(session)) {
        ForemanSetupException error = assertThrows(ForemanSetupException.class,
            () -> DrillSqlWorker.getPlan(context, SQL));
        assertEquals("sql-parsing", error.getMessage());
        assertNull(context.takePendingPlanCacheInsert());
      }
      assertEquals(failures, metric("planning_failures"));
      onlyWarning();
    }
  }

  @Test
  public void testQueryWithoutParametersDoesNotRetry() throws Exception {
    try (UserSession session = session(); QueryContext context = context(session)) {
      long failures = metric("planning_failures");
      assertThrows(UserException.class,
          () -> DrillSqlWorker.getPlan(context, "SELECT missing_column FROM (VALUES (1)) AS t(x)"));
      assertEquals(failures, metric("planning_failures"));
      assertTrue(warnings().isEmpty());
    }
  }

  @Test
  public void testFallbackExecutionThenOrdinaryMissAndHit() throws Exception {
    checkCsvRows(csvSql(1), "2", "two", "3", "three");
    client.alterSession(PlannerSettings.ENABLE_PLAN_CACHE_OPTION, true);
    // Leave fallback at its default, set through the normal session option path.
    long failures = metric("planning_failures");
    long hits = metric("hits");
    long misses = metric("misses");
    client.setControls(failureControls());
    checkCsvRows(csvSql(1), "2", "two", "3", "three");
    cache().awaitWrites();
    assertEquals(failures + 1, metric("planning_failures"));
    assertEquals(misses + 1, metric("misses"));
    assertEquals(hits, metric("hits"));
    assertEquals(0L, metric("entries"));
    checkCsvRows(csvSql(1), "2", "two", "3", "three");
    cache().awaitWrites();
    assertEquals(1L, metric("entries"));
    assertEquals(misses + 2, metric("misses"));
    checkCsvRows(csvSql(2), "3", "three");
    assertEquals(hits + 1, metric("hits"));
    checkCsvRows(csvSql(9));
    assertEquals(hits + 2, metric("hits"));
    assertEquals(misses + 2, metric("misses"));
    assertEquals(failures + 1, metric("planning_failures"));
    assertTrue(onlyWarning().getFormattedMessage().contains("retrying the original SQL"));
  }

  @Test
  public void testDisabledFallbackThroughClient() throws Exception {
    client.alterSession(PlannerSettings.ENABLE_PLAN_CACHE_OPTION, true);
    client.alterSession(PlannerSettings.ENABLE_PLAN_CACHE_FALLBACK_OPTION, false);
    long failures = metric("planning_failures");
    long misses = metric("misses");
    client.setControls(failureControls());
    UserRemoteException error = assertThrows(UserRemoteException.class,
        () -> client.queryBuilder().sql(csvSql(1)).run());
    assertTrue(error.getMessage(), error.getMessage().contains(FAILURE_SITE));
    cache().awaitWrites();
    assertEquals(failures, metric("planning_failures"));
    assertEquals(misses + 1, metric("misses"));
    assertEquals(0L, metric("entries"));
    assertTrue(onlyWarning().getFormattedMessage().contains("fallback is disabled"));

    // The same original SQL is valid; after resetting the option, ordinary planning works.
    client.resetSession(PlannerSettings.ENABLE_PLAN_CACHE_FALLBACK_OPTION);
    checkCsvRows(csvSql(1), "2", "two", "3", "three");
    cache().awaitWrites();
    assertEquals(1L, metric("entries"));
    assertEquals(failures, metric("planning_failures"));
  }

  @Test
  public void testBothPlanningAttemptsFailThroughClient() throws Exception {
    String invalidSql = "SELECT missing_column FROM (VALUES (1), (2)) AS t(x) WHERE x > 0";
    UserRemoteException baseline = assertThrows(UserRemoteException.class,
        () -> client.queryBuilder().sql(invalidSql).run());
    client.alterSession(PlannerSettings.ENABLE_PLAN_CACHE_OPTION, true);
    long failures = metric("planning_failures");
    long misses = metric("misses");
    UserRemoteException error = assertThrows(UserRemoteException.class,
        () -> client.queryBuilder().sql(invalidSql).run());
    assertEquals(baseline.getErrorType(), error.getErrorType());
    assertEquals(baseline.getOriginalMessage(), error.getOriginalMessage());
    cache().awaitWrites();
    assertEquals(failures, metric("planning_failures"));
    assertEquals(misses + 1, metric("misses"));
    assertEquals(0L, metric("entries"));
    assertTrue(onlyWarning().getFormattedMessage().contains("retrying the original SQL"));
  }

  @Test
  public void testFallbackPlanningCountsEvenWhenExecutionFails() throws Exception {
    String sql = "SELECT CAST(label AS INTEGER) AS n FROM " + CSV_TABLE + " WHERE id = '1'";
    UserRemoteException baseline = assertThrows(UserRemoteException.class,
        () -> client.queryBuilder().sql(sql).run());
    client.alterSession(PlannerSettings.ENABLE_PLAN_CACHE_OPTION, true);
    long failures = metric("planning_failures");
    client.setControls(failureControls());
    UserRemoteException error = assertThrows(UserRemoteException.class,
        () -> client.queryBuilder().sql(sql).run());
    assertEquals(baseline.getErrorType(), error.getErrorType());
    assertTrue(error.getMessage(), error.getMessage().contains("one"));
    cache().awaitWrites();
    assertEquals(failures + 1, metric("planning_failures"));
    assertEquals(0L, metric("entries"));
    assertTrue(onlyWarning().getFormattedMessage().contains("retrying the original SQL"));
  }

  private String csvSql(int threshold) {
    return "SELECT id, label FROM " + CSV_TABLE
        + " WHERE CAST(id AS INTEGER) > CAST('" + threshold + "' AS INTEGER)";
  }

  private void checkCsvRows(String sql, String... values) throws Exception {
    org.apache.drill.test.TestBuilder builder = client.testBuilder().sqlQuery(sql)
        .unOrdered().baselineColumns("id", "label");
    for (int i = 0; i < values.length; i += 2) {
      builder.baselineValues(values[i], values[i + 1]);
    }
    if (values.length == 0) {
      builder.expectsEmptyResultSet();
    }
    builder.go();
  }

  private UserSession session() {
    UserSession session = UserSession.Builder.newBuilder()
        .withCredentials(UserCredentials.newBuilder().setUserName("fallback-test").build())
        .withOptionManager(cluster.drillbit().getContext().getOptionManager())
        .setSupportComplexTypes(true).build();
    session.getOptions().setLocalOption(PlannerSettings.ENABLE_PLAN_CACHE_OPTION, true);
    return session;
  }

  private QueryContext context(UserSession session) {
    QueryContext context = new QueryContext(session, cluster.drillbit().getContext(), QueryId.getDefaultInstance());
    // Keep the separate dynamic-function retry from consuming one-shot fault injections.
    context.getFunctionRegistry().syncWithRemoteRegistry(context.getDrillOperatorTable().getFunctionRegistryVersion());
    context.reloadDrillOperatorTable();
    return context;
  }

  private String failureControls() {
    // Fire twice to verify that retry bypasses the parameterized path entirely.
    return Controls.newBuilder().addException(DrillSqlWorker.class, FAILURE_SITE, IOException.class, 0, 2).build();
  }

  private PlanCache cache() {
    return cluster.drillbit().getContext().getPlanCache();
  }

  private long metric(String suffix) {
    return (Long) DrillMetrics.getRegistry().getGauges().get("drill.plan_cache." + suffix).getValue();
  }

  private List<ILoggingEvent> warnings() {
    return logs.list.stream().filter(event -> event.getLevel() == Level.WARN).collect(Collectors.toList());
  }

  private ILoggingEvent onlyWarning() {
    List<ILoggingEvent> warnings = warnings();
    assertEquals(1, warnings.size());
    return warnings.get(0);
  }
}
