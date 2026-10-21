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
package org.apache.drill.exec.server.rest.workflow;

import org.apache.drill.exec.server.rest.SavedQueryResources;
import org.apache.drill.exec.work.WorkManager;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/** Runner paths that need a live Drillbit: statement execution and dry runs. */
public class TestWorkflowRunner extends ClusterTest {

  private static WorkflowRunner runner;

  @BeforeClass
  public static void setup() throws Exception {
    startCluster(ClusterFixture.builder(dirTestWatcher));
    runner = WorkflowRunner.getOrCreate(cluster.drillbit().getManager(),
        cluster.drillbit().getContext().getStoreProvider());
  }

  @AfterClass
  public static void tearDown() {
    runner.close();
  }

  private static final String NLJ_OFF = "ALTER SESSION SET `planner.enable_nljoin_for_scalar_only` = false";
  private static final String SMALL_CROSS_JOIN = "SELECT COUNT(*) FROM cp.`employee.json` a, cp.`employee.json` b"
      + " WHERE a.employee_id < 3 AND b.employee_id < 3";

  @Test
  public void alterSessionCarriesOverToLaterStatementsInTheSameCall() throws Exception {
    // A non-scalar cross join only plans once the ALTER SESSION has applied to the shared session.
    assertThrows(Exception.class, () -> runner.executeStatements(List.of(SMALL_CROSS_JOIN), null, 60));
    runner.executeStatements(List.of(NLJ_OFF, SMALL_CROSS_JOIN), null, 60);
  }

  @Test(timeout = 120_000)
  public void timeoutCancelsTheQueryAndSkipsLaterStatements() throws Exception {
    String slow = "SELECT COUNT(*) FROM cp.`employee.json` a, cp.`employee.json` b, cp.`employee.json` c,"
        + " cp.`employee.json` d";
    String marker = "CREATE TABLE dfs.tmp.wf_timeout_marker AS SELECT 1 AS x FROM (VALUES(1))";
    long start = System.currentTimeMillis();
    assertThrows(TimeoutException.class, () -> runner.executeStatements(List.of(NLJ_OFF, slow, marker), null, 1));
    assertTrue("cancel should land well within the grace period",
        System.currentTimeMillis() - start < WorkflowRunner.CANCEL_GRACE_SECONDS * 1000L);

    WorkManager wm = cluster.drillbit().getManager();
    long deadline = System.currentTimeMillis() + 10_000;
    while (wm.getRemainingQueries().get("queriesCount") > 0 && System.currentTimeMillis() < deadline) {
      Thread.sleep(100);
    }
    assertEquals(Integer.valueOf(0), wm.getRemainingQueries().get("queriesCount"));
    assertFalse("statement after the timeout must not run",
        new File(dirTestWatcher.getDfsTestTmpDir(), "wf_timeout_marker").exists());
  }

  @Test
  public void executeStatementsReportsAFailureAfterTheFirstStatement() {
    assertThrows(Exception.class,
        () -> runner.executeStatements(List.of("SELECT 1", "SELECT * FROM nonexistent_tbl"), null, 60));
  }

  @Test
  public void dryRunOfUnsavedWorkflowDoesNotNeedAnId() {
    WorkflowRun existing = new WorkflowRun();
    existing.id = UUID.randomUUID().toString();
    existing.scheduleId = "some-other-workflow";
    existing.executedAt = "2026-10-20T00:00:00Z";
    existing.status = "success";
    existing.rowCount = 3L;
    runner.stores().runs().put(existing.id, existing);

    Workflow wf = new Workflow();
    wf.savedQueryId = "missing-query";
    WorkflowRunner.DryRunResult r = runner.dryRun(wf);
    assertNotNull(r.context);
    assertEquals("error", r.context.status);
  }

  private static String saveQuery(String sql) {
    return saveQuery(sql, "anonymous");
  }

  private static String saveQuery(String sql, String owner) {
    String id = UUID.randomUUID().toString();
    runner.stores().savedQueries().put(id,
        new SavedQueryResources.SavedQuery(id, "q", null, sql, null, owner, 0, 0, null, false, 0));
    return id;
  }

  /** An enabled, orphan (so no membership check) daily workflow stored under a fresh id. */
  private static Workflow scheduled(Instant nextRunAt) {
    Workflow wf = new Workflow();
    wf.id = UUID.randomUUID().toString();
    wf.owner = "anonymous";
    wf.savedQueryId = saveQuery("SELECT 1 AS x");
    wf.schedule.enabled = true;
    wf.schedule.timeoutSeconds = 60;
    wf.nextRunAt = nextRunAt.toString();
    runner.stores().workflows().put(wf.id, wf);
    return wf;
  }

  private static WorkflowMigration.LegacySchedule legacy(String id) {
    WorkflowMigration.LegacySchedule o = new WorkflowMigration.LegacySchedule();
    o.id = id;
    o.savedQueryId = "none";
    o.owner = "anonymous";
    o.frequency = "daily";
    return o;
  }

  @Test
  public void migrationSkipsACorruptEntryAndKeepsGoing() {
    String first = UUID.randomUUID().toString();
    String last = UUID.randomUUID().toString();
    List<Object> entries = List.of(Map.entry("a", legacy(first)), "corrupt", Map.entry("c", legacy(last)));
    Iterator<Object> backing = entries.iterator();
    // Like Iterators.transform over a store: the backing iterator advances, then deserialization throws.
    Iterator<Map.Entry<String, WorkflowMigration.LegacySchedule>> it = new Iterator<>() {
      @Override public boolean hasNext() { return backing.hasNext(); }
      @SuppressWarnings("unchecked")
      @Override public Map.Entry<String, WorkflowMigration.LegacySchedule> next() {
        Object o = backing.next();
        if (o instanceof String) {
          throw new IllegalStateException("cannot deserialize " + o);
        }
        return (Map.Entry<String, WorkflowMigration.LegacySchedule>) o;
      }
    };

    assertEquals(1, runner.migrateEntries(it));
    assertNotNull(runner.stores().workflows().get(first));
    assertNotNull(runner.stores().workflows().get(last));
    assertEquals(0, runner.migrateEntries(List.of(Map.entry("a", legacy(first))).iterator()));
  }

  @Test
  public void isStaleOnlyAfterTheLongestHealthyRun() {
    WorkflowConfig cfg = new WorkflowConfig();
    Workflow wf = new Workflow();
    wf.schedule.timeoutSeconds = 60;
    wf.actions.add(new Workflow.ActionSpec());
    long bound = WorkflowRunner.maxRunSeconds(wf, cfg);
    assertEquals(2 * (60 + 30) + Math.max(cfg.actionTimeoutSeconds, 60) + 30 + WorkflowRunner.STALE_GRACE_SECONDS, bound);
    Instant now = Instant.now();
    assertFalse("not running is never stale", WorkflowRunner.isStale(wf, now, cfg));
    wf.isRunning = true;
    assertTrue("a flag with no start time predates runStartedAt", WorkflowRunner.isStale(wf, now, cfg));
    wf.runStartedAt = now.minusSeconds(bound - 60).toString();
    assertFalse(WorkflowRunner.isStale(wf, now, cfg));
    wf.runStartedAt = now.minusSeconds(bound + 60).toString();
    assertTrue(WorkflowRunner.isStale(wf, now, cfg));
  }

  @Test
  public void processDueClearsAStaleRunningFlag() {
    Instant now = Instant.now();
    Workflow wf = scheduled(now.plusSeconds(3600));
    wf.isRunning = true;
    wf.runStartedAt = now.minusSeconds(86_400).toString();
    runner.stores().workflows().put(wf.id, wf);

    runner.processDue(wf, now, new WorkflowConfig());
    Workflow stored = runner.stores().workflows().get(wf.id);
    assertFalse(stored.isRunning);
    assertEquals(null, stored.runStartedAt);
  }

  @Test
  public void processDueAdvancesASlotAbandonedByADeadDrillbit() {
    Instant now = Instant.now();
    WorkflowConfig cfg = new WorkflowConfig();
    Workflow wf = scheduled(now.minusSeconds(86_400));
    assertTrue(new RunClaims(runner.stores().claims())
        .claim(wf.id + "_" + Instant.parse(wf.nextRunAt).toEpochMilli(), "dead-drillbit"));

    runner.processDue(wf, now, cfg);
    Workflow stored = runner.stores().workflows().get(wf.id);
    assertTrue(Instant.parse(stored.nextRunAt).isAfter(now));
    assertFalse(stored.isRunning);
    assertEquals(null, stored.lastRunAt);
  }

  @Test
  public void processDueLeavesARecentlyClaimedSlotAlone() {
    Instant now = Instant.now();
    Workflow wf = scheduled(now.minusSeconds(5));
    String slot = wf.nextRunAt;
    assertTrue(new RunClaims(runner.stores().claims())
        .claim(wf.id + "_" + Instant.parse(slot).toEpochMilli(), "busy-drillbit"));

    runner.processDue(wf, now, new WorkflowConfig());
    assertEquals(slot, runner.stores().workflows().get(wf.id).nextRunAt);
  }

  @Test
  public void runFailsWhenTheSavedQueryBelongsToSomeoneElse() {
    Workflow wf = new Workflow();
    wf.id = UUID.randomUUID().toString();
    wf.owner = "anonymous";
    wf.savedQueryId = saveQuery("SELECT 1 AS x", "olivia");
    runner.stores().workflows().put(wf.id, wf);

    WorkflowRun run = runner.runNow(wf);
    assertEquals("error", run.status);
    assertTrue(run.errorMessage, run.errorMessage.contains("Saved query is owned by olivia, workflow runs as anonymous"));
    assertFalse(runner.stores().workflows().get(wf.id).isRunning);
  }

  @Test
  public void dryRunCountsBeyondThePreviewWhenSqlEndsInSemicolon() {
    Workflow wf = new Workflow();
    wf.savedQueryId = saveQuery("SELECT * FROM cp.`employee.json`;");
    WorkflowRunner.DryRunResult r = runner.dryRun(wf);
    assertEquals(r.context.error, "success", r.context.status);
    assertEquals(WorkflowRunner.PREVIEW_ROWS, r.context.rows.size());
    assertEquals(1155L, r.context.rowCount);
  }

  @Test
  public void runNowRecordsTheRunAndClearsIsRunning() {
    Workflow wf = new Workflow();
    wf.id = UUID.randomUUID().toString();
    wf.savedQueryId = saveQuery("SELECT 1 AS x");
    runner.stores().workflows().put(wf.id, wf);

    WorkflowRun run = runner.runNow(wf);
    assertEquals("success", run.status);
    assertEquals("manual", run.trigger);
    assertEquals(Long.valueOf(1), run.rowCount);
    assertNotNull(runner.stores().runs().get(run.id));
    Workflow stored = runner.stores().workflows().get(wf.id);
    assertFalse(stored.isRunning);
    assertEquals(null, stored.runStartedAt);
    assertEquals("success", stored.lastStatus);
  }
}
