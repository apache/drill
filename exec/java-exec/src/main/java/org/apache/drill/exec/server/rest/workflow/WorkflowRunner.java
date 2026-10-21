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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.GlobalEventExecutor;
import io.netty.util.concurrent.Promise;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.rpc.user.UserSession;
import org.apache.drill.exec.server.rest.ProjectResources;
import org.apache.drill.exec.server.rest.QueryWrapper;
import org.apache.drill.exec.server.rest.RestQueryRunner;
import org.apache.drill.exec.server.rest.SavedQueryResources;
import org.apache.drill.exec.server.rest.WebSessionResources;
import org.apache.drill.exec.server.rest.WebUserConnection;
import org.apache.drill.exec.server.rest.auth.DrillUserPrincipal;
import org.apache.drill.exec.store.sys.PersistentStoreProvider;
import org.apache.drill.exec.work.WorkManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Background service that runs due workflows: claim the slot, run the saved query,
 * run the action pipeline, record the run.
 */
public class WorkflowRunner implements AutoCloseable, WorkflowServices {
  private static final Logger logger = LoggerFactory.getLogger(WorkflowRunner.class);
  static final int PREVIEW_ROWS = 100;
  /** ponytail: snapshots live in ZooKeeper (1 MB node cap), so only 20 preview rows are stored. */
  static final int STORED_PREVIEW_ROWS = 20;
  /** How long a timed-out call waits for Drill to finish cancelling its query. */
  static final int CANCEL_GRACE_SECONDS = 30;
  /** Slack added to {@link #maxRunSeconds} before a running flag is treated as left behind by a crash. */
  static final long STALE_GRACE_SECONDS = 600;
  private static volatile WorkflowRunner instance;

  public static class DryRunResult {
    public RunContext context;
    public List<ActionOutcome> actions;
  }

  private final WorkManager workManager;
  private final WorkflowStores stores;
  private final ActionTypeRegistry registry = ActionTypeRegistry.defaults();
  private final RunClaims claims;
  private final ScheduledExecutorService ticker = Executors.newSingleThreadScheduledExecutor(daemon("drill-workflow-runner"));
  private final ExecutorService workers = Executors.newCachedThreadPool(daemon("drill-workflow-worker"));
  private final ObjectMapper mapper = new ObjectMapper();
  private final AtomicBoolean started = new AtomicBoolean();

  private WorkflowRunner(WorkManager workManager, PersistentStoreProvider provider) {
    this.workManager = workManager;
    this.stores = new WorkflowStores(provider, workManager);
    this.claims = new RunClaims(stores.claims());
  }

  public static WorkflowRunner getOrCreate(WorkManager wm, PersistentStoreProvider sp) {
    if (instance == null) {
      synchronized (WorkflowRunner.class) {
        if (instance == null) {
          instance = new WorkflowRunner(wm, sp);
        }
      }
    }
    return instance;
  }

  public static WorkflowRunner getInstance() {
    return instance;
  }

  public WorkflowStores stores() {
    return stores;
  }

  public ActionTypeRegistry registry() {
    return registry;
  }

  /** Idempotent: a second call (e.g. from another Drillbit sharing this JVM) does nothing. */
  public void start() {
    if (!started.compareAndSet(false, true)) {
      return;
    }
    try {
      migrateLegacy();
    } catch (Exception e) {
      logger.error("Legacy schedule migration failed; workflows still run", e);
    }
    try {
      fillMissingOwners();
    } catch (Exception e) {
      logger.error("Could not fill in missing workflow owners", e);
    }
    ticker.scheduleAtFixedRate(this::tick, 30, 60, TimeUnit.SECONDS);
  }

  // ---------------------------------------------------------------- scheduling

  /** Never throws: an exception escaping a scheduleAtFixedRate task cancels every later tick. */
  void tick() {
    try {
      Instant now = Instant.now();
      WorkflowConfig cfg = stores.loadConfig();
      try {
        claims.prune(now.minus(Duration.ofDays(7)));
      } catch (Exception e) {
        logger.debug("Claim pruning failed", e);
      }
      Iterator<Map.Entry<String, Workflow>> it = stores.workflows().getAll();
      while (it.hasNext()) {
        Workflow wf = it.next().getValue();
        if (wf == null) {
          continue;
        }
        try {
          processDue(wf, now, cfg);
        } catch (Exception e) {
          logger.warn("Workflow {} failed to process", wf.id, e);
        }
      }
    } catch (Exception e) {
      logger.error("Workflow tick failed", e);
    }
  }

  /** Upper bound on a healthy run: the preview and COUNT queries, every action, their cancel graces, and slack. */
  static long maxRunSeconds(Workflow wf, WorkflowConfig cfg) {
    long q = wf.schedule.timeoutSeconds + CANCEL_GRACE_SECONDS;
    long perAction = Math.max(cfg.actionTimeoutSeconds, wf.schedule.timeoutSeconds) + CANCEL_GRACE_SECONDS;
    return 2 * q + wf.actions.size() * perAction + STALE_GRACE_SECONDS;
  }

  /** True when isRunning was left behind by a Drillbit that died mid-run. */
  static boolean isStale(Workflow wf, Instant now, WorkflowConfig cfg) {
    return wf.isRunning && (wf.runStartedAt == null
        || Instant.parse(wf.runStartedAt).plusSeconds(maxRunSeconds(wf, cfg)).isBefore(now));
  }

  void processDue(Workflow wf, Instant now, WorkflowConfig cfg) {
    if (!wf.schedule.enabled) {
      return;
    }
    if (isStale(wf, now, cfg)) {
      logger.warn("Workflow {} was marked running since {}; clearing the stale flag", wf.id, wf.runStartedAt);
      wf.isRunning = false;
      wf.runStartedAt = null;
      stores.workflows().put(wf.id, wf);
    }
    if (WorkflowSchedule.isExpired(wf, now, cfg)) {
      disable(wf, "expired");
      return;
    }
    if (ownerLostAccess(wf)) {
      disable(wf, "owner_lost_access");
      return;
    }
    if (wf.nextRunAt == null || Instant.parse(wf.nextRunAt).isAfter(now)) {
      return;
    }
    Instant slot = Instant.parse(wf.nextRunAt);
    if (!claims.claim(wf.id + "_" + slot.toEpochMilli(), drillbitId())) {
      // Another Drillbit owns this slot. If it is far older than any healthy run, that Drillbit died
      // before advancing nextRunAt; move on to the next slot (idempotent across Drillbits).
      if (slot.plusSeconds(maxRunSeconds(wf, cfg)).isBefore(now)) {
        logger.warn("Workflow {} slot {} was claimed but never finished; advancing", wf.id, wf.nextRunAt);
        wf.nextRunAt = WorkflowSchedule.nextRun(wf.schedule, now, ZoneId.systemDefault()).toString();
        stores.workflows().put(wf.id, wf);
      }
      return;
    }
    if (wf.isRunning) {
      WorkflowRun skipped = newRun(wf, "schedule", now);
      skipped.status = "skipped";
      skipped.errorMessage = "Skipped: previous run still in progress";
      stores.runs().put(skipped.id, skipped);
      wf.nextRunAt = WorkflowSchedule.nextRun(wf.schedule, now, ZoneId.systemDefault()).toString();
      stores.workflows().put(wf.id, wf);
      return;
    }
    execute(wf, "schedule");
  }

  private void disable(Workflow wf, String status) {
    wf.schedule.enabled = false;
    wf.status = status;
    stores.workflows().put(wf.id, wf);
    logger.info("Workflow {} disabled: {}", wf.id, status);
  }

  private boolean ownerLostAccess(Workflow wf) {
    if (wf.projectId == null) {
      return false;
    }
    ProjectResources.Project p = stores.project(wf.projectId);
    return p == null || !WorkflowPermissions.isMember(p, wf.owner);
  }

  // ---------------------------------------------------------------- execution

  public WorkflowRun runNow(Workflow wf) {
    return execute(wf, "manual");
  }

  public DryRunResult dryRun(Workflow wf) {
    SavedQueryResources.SavedQuery q = stores.savedQuery(wf.savedQueryId);
    DryRunResult r = new DryRunResult();
    r.context = runQuery(wf, q);
    r.actions = pipeline().run(wf, q == null ? null : q.getSql(), r.context, this, true);
    return r;
  }

  private WorkflowRun execute(Workflow wf, String trigger) {
    Instant now = Instant.now();
    // Mark a freshly read copy so an edit saved since the caller loaded wf is not overwritten.
    Workflow marked = stores.workflows().get(wf.id);
    if (marked != null) {
      marked.isRunning = true;
      marked.runStartedAt = now.toString();
      stores.workflows().put(marked.id, marked);
    }
    WorkflowRun run = newRun(wf, trigger, now);
    try {
      SavedQueryResources.SavedQuery q = stores.savedQuery(wf.savedQueryId);
      RunContext ctx = runQuery(wf, q);
      run.actions = pipeline().run(wf, q == null ? null : q.getSql(), ctx, this, false);
      fillRun(run, ctx);
    } catch (Exception e) {
      run.status = "error";
      run.errorMessage = e.getMessage();
      logger.error("Workflow {} run failed", wf.id, e);
    } finally {
      Workflow fresh = stores.workflows().get(wf.id);
      Workflow target = fresh != null ? fresh : wf;
      target.isRunning = false;
      target.runStartedAt = null;
      target.lastRunAt = now.toString();
      target.lastStatus = run.status;
      target.consecutiveFailures = "error".equals(run.status) ? target.consecutiveFailures + 1 : 0;
      if ("schedule".equals(trigger) || target.nextRunAt == null) {
        target.nextRunAt = WorkflowSchedule.nextRun(target.schedule, now, ZoneId.systemDefault()).toString();
      }
      if (fresh != null) {
        stores.workflows().put(target.id, target);
      }
    }
    stores.runs().put(run.id, run);
    WorkflowRunLog.log(wf, run, drillbitId());
    enforceRetention(wf.id, wf.retentionCount);
    return run;
  }

  private ActionPipeline pipeline() {
    return new ActionPipeline(registry, workers, stores.loadConfig().actionTimeoutSeconds * 1000L);
  }

  private RunContext runQuery(Workflow wf, SavedQueryResources.SavedQuery q) {
    RunContext ctx = new RunContext();
    ctx.intervalMs = WorkflowSchedule.intervalMillis(wf.schedule);
    long start = System.currentTimeMillis();
    int timeout = wf.schedule.timeoutSeconds > 0 ? wf.schedule.timeoutSeconds : 300;
    String runAs = runAs(wf);
    if (q == null) {
      ctx.status = "error";
      ctx.error = "Saved query not found: " + wf.savedQueryId;
    } else if (!runAs.equals(q.getOwner())) {
      // The query's author could otherwise edit SQL that then runs with the workflow owner's access.
      ctx.status = "error";
      ctx.error = "Saved query is owned by " + q.getOwner() + ", workflow runs as " + runAs
          + "; point the workflow at a saved query its owner owns";
    } else {
      try {
        String sql = SaveToTableAction.stripSemicolon(q.getSql());
        RestQueryRunner.QueryResult r = query(List.of(sql), PREVIEW_ROWS, timeout, wf.owner);
        ctx.rows = r.rows != null ? r.rows : List.of();
        ctx.columns = r.columns != null ? new ArrayList<>(r.columns) : List.of();
        ctx.rowCount = ctx.rows.size() < PREVIEW_ROWS ? ctx.rows.size() : countRows(sql, timeout, wf.owner);
        ctx.status = "success";
      } catch (TimeoutException e) {
        ctx.status = "error";
        ctx.error = "Query timed out after " + timeout + " seconds";
      } catch (Exception e) {
        ctx.status = "error";
        ctx.error = e.getMessage();
      }
    }
    ctx.durationMs = System.currentTimeMillis() - start;
    // An unsaved workflow (dry run from the editor) has no id and no history.
    WorkflowRun prev = wf.id == null ? null : previousRun(wf.id);
    if ("success".equals(ctx.status) && prev != null && prev.rowCount != null) {
      ctx.rowCountDelta = ctx.rowCount - prev.rowCount;
      ctx.deltaPercent = prev.rowCount == 0 ? null : 100.0 * ctx.rowCountDelta / prev.rowCount;
    }
    return ctx;
  }

  /** The user a workflow's queries run as; a workflow without an owner runs anonymously. */
  static String runAs(Workflow wf) {
    return wf.owner == null || wf.owner.isEmpty() ? DrillUserPrincipal.ANONYMOUS_USER : wf.owner;
  }

  /** Only runs when the preview hit its cap, so small results cost one query. */
  private long countRows(String sql, int timeout, String owner) throws Exception {
    RestQueryRunner.QueryResult r = query(List.of("SELECT COUNT(*) AS cnt FROM (" + sql + "\n)"), 1, timeout, owner);
    return Long.parseLong(r.rows.get(0).get("cnt"));
  }

  private void fillRun(WorkflowRun run, RunContext ctx) {
    run.status = ctx.status;
    run.errorMessage = ctx.error;
    run.rowCount = ctx.rowCount;
    run.duration = ctx.durationMs;
    run.rowCountDelta = ctx.rowCountDelta;
    run.previousRowCount = ctx.rowCountDelta == null ? null : ctx.rowCount - ctx.rowCountDelta;
    try {
      run.previewRows = mapper.writeValueAsString(ctx.rows.subList(0, Math.min(STORED_PREVIEW_ROWS, ctx.rows.size())));
      run.previewColumns = mapper.writeValueAsString(ctx.columns);
    } catch (Exception e) {
      logger.debug("Failed to serialize preview", e);
    }
  }

  private WorkflowRun newRun(Workflow wf, String trigger, Instant now) {
    WorkflowRun run = new WorkflowRun();
    run.id = UUID.randomUUID().toString();
    run.scheduleId = wf.id;
    run.savedQueryId = wf.savedQueryId;
    run.executedAt = now.toString();
    run.trigger = trigger;
    return run;
  }

  /** ponytail: full scan of runs per execution; index by workflow if run counts grow large. */
  private WorkflowRun previousRun(String workflowId) {
    WorkflowRun best = null;
    Iterator<Map.Entry<String, WorkflowRun>> it = stores.runs().getAll();
    while (it.hasNext()) {
      WorkflowRun r = it.next().getValue();
      if (r != null && workflowId.equals(r.scheduleId) && !"skipped".equals(r.status)
          && (best == null || r.executedAt.compareTo(best.executedAt) > 0)) {
        best = r;
      }
    }
    return best;
  }

  private void enforceRetention(String workflowId, int retentionCount) {
    int limit = retentionCount > 0 ? retentionCount : 30;
    List<WorkflowRun> runs = new ArrayList<>();
    stores.runs().getAll().forEachRemaining(e -> {
      if (e.getValue() != null && workflowId.equals(e.getValue().scheduleId)) {
        runs.add(e.getValue());
      }
    });
    runs.sort((a, b) -> b.executedAt.compareTo(a.executedAt));
    for (int i = limit; i < runs.size(); i++) {
      stores.runs().delete(runs.get(i).id);
    }
  }

  // ---------------------------------------------------------------- services

  @Override
  public void executeStatements(List<String> sql, String userName, int timeoutSeconds) throws Exception {
    query(sql, 1, timeoutSeconds, userName);
  }

  @Override
  public void sendMail(List<String> to, String subject, String body) throws Exception {
    SmtpMailer.send(stores.smtp().get(WorkflowStores.DEFAULT_KEY), to, subject, body);
  }

  /**
   * Runs statements in order on one session (so ALTER SESSION carries over) and returns the
   * last statement's result. On timeout the running Drill query is cancelled, later statements
   * never start, and this waits (up to {@link #CANCEL_GRACE_SECONDS}) for the cancel to land so
   * the caller does not mark the workflow idle while its query is still running.
   */
  private RestQueryRunner.QueryResult query(List<String> statements, int rowLimit, int timeoutSeconds, String userName)
      throws Exception {
    StatementBatch batch = new StatementBatch(statements, rowLimit, userName);
    Future<RestQueryRunner.QueryResult> f = workers.submit(batch);
    try {
      return f.get(timeoutSeconds, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      batch.cancel();
      awaitCancelled(f);
      throw e;
    } catch (InterruptedException e) {
      // The caller gave up (e.g. an action timeout); do not leave statements running behind it.
      batch.cancel();
      Thread.currentThread().interrupt();
      throw e;
    } catch (ExecutionException e) {
      throw e.getCause() instanceof Exception ? (Exception) e.getCause() : e;
    }
  }

  private static void awaitCancelled(Future<?> f) {
    try {
      f.get(CANCEL_GRACE_SECONDS, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      logger.warn("Workflow query still running {} seconds after it was cancelled", CANCEL_GRACE_SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (Exception e) {
      // Expected: the cancelled statement or batch reports its own failure.
      logger.debug("Cancelled workflow query ended", e);
    }
  }

  /**
   * One call's statements. A WebUserConnection is single-use (its completion latch counts
   * down once), so each statement gets a fresh connection on the shared session.
   */
  private final class StatementBatch implements Callable<RestQueryRunner.QueryResult> {
    private final List<String> statements;
    private final int rowLimit;
    private final String userName;
    private boolean cancelled;
    private RestQueryRunner current;

    StatementBatch(List<String> statements, int rowLimit, String userName) {
      this.statements = statements;
      this.rowLimit = rowLimit;
      this.userName = userName;
    }

    @Override
    public RestQueryRunner.QueryResult call() throws Exception {
      WebSessionResources session = session(userName);
      try {
        RestQueryRunner.QueryResult last = null;
        for (String sql : statements) {
          RestQueryRunner runner;
          synchronized (this) {
            if (cancelled) {
              throw new CancellationException("Workflow statements cancelled before: " + sql);
            }
            QueryWrapper w = new QueryWrapper.RestQueryBuilder().query(sql).queryType("SQL").rowLimit(rowLimit).build();
            runner = new RestQueryRunner(w, workManager, new WebUserConnection(session));
            current = runner;
          }
          last = runner.run();
        }
        return last;
      } finally {
        session.close();
      }
    }

    void cancel() {
      RestQueryRunner running;
      synchronized (this) {
        cancelled = true;
        running = current;
      }
      if (running != null) {
        running.cancel();
      }
    }
  }

  /** Opens a session for the workflow owner; the caller closes it. */
  private WebSessionResources session(String userName) {
    DrillConfig config = workManager.getContext().getConfig();
    BufferAllocator allocator = workManager.getContext().getAllocator().newChildAllocator("WorkflowRunner:session",
        config.getLong(ExecConstants.HTTP_SESSION_MEMORY_RESERVATION),
        config.getLong(ExecConstants.HTTP_SESSION_MEMORY_MAXIMUM));
    String user = userName == null || userName.isEmpty() ? DrillUserPrincipal.ANONYMOUS_USER : userName;
    UserSession session = UserSession.Builder.newBuilder()
        .withCredentials(UserBitShared.UserCredentials.newBuilder().setUserName(user).build())
        .withOptionManager(workManager.getContext().getOptionManager())
        .setSupportComplexTypes(config.getBoolean(ExecConstants.CLIENT_SUPPORT_COMPLEX_TYPES))
        .build();
    Promise<Void> closeFuture = new DefaultPromise<>(GlobalEventExecutor.INSTANCE);
    return new WebSessionResources(allocator, null, session, closeFuture);
  }

  public String drillbitId() {
    DrillbitEndpoint ep = workManager.getContext().getEndpoint();
    return ep.getAddress() + ":" + ep.getUserPort();
  }

  // ---------------------------------------------------------------- migration & lifecycle

  /**
   * Workflows migrated before ownerless legacy schedules were mapped to anonymous have a null owner.
   * They run as anonymous but fail every membership check, so record anonymous explicitly.
   */
  void fillMissingOwners() {
    Iterator<Map.Entry<String, Workflow>> it = stores.workflows().getAll();
    while (it.hasNext()) {
      Map.Entry<String, Workflow> e = it.next();
      Workflow wf = e.getValue();
      if (wf != null && (wf.owner == null || wf.owner.isEmpty())) {
        wf.owner = DrillUserPrincipal.ANONYMOUS_USER;
        stores.workflows().put(e.getKey(), wf);
        logger.info("Workflow {} had no owner; set to {}", wf.id, wf.owner);
      }
    }
  }

  private void migrateLegacy() {
    WorkflowConfig cfg = stores.loadConfig();
    if (cfg.legacyMigrated) {
      return;
    }
    if (migrateEntries(stores.legacySchedules().getAll()) == 0) {
      cfg.legacyMigrated = true;
      stores.saveConfig(cfg);
    }
  }

  /**
   * Migrates each entry independently and returns how many failed. The store iterators advance before
   * deserializing, so a corrupt entry is skipped and later ones still migrate. Re-running is idempotent
   * (existing workflow ids are skipped), so the caller marks migration done only when nothing failed.
   */
  int migrateEntries(Iterator<Map.Entry<String, WorkflowMigration.LegacySchedule>> it) {
    int count = 0;
    int failed = 0;
    while (it.hasNext()) {
      String key = null;
      try {
        Map.Entry<String, WorkflowMigration.LegacySchedule> e = it.next();
        key = e.getKey();
        WorkflowMigration.LegacySchedule old = e.getValue();
        if (old == null || stores.workflows().contains(old.id)) {
          continue;
        }
        SavedQueryResources.SavedQuery q = stores.savedQuery(old.savedQueryId);
        Workflow wf = WorkflowMigration.convert(old, stores.projectForQuery(old.savedQueryId),
            q == null ? null : q.getName());
        stores.workflows().put(wf.id, wf);
        count++;
      } catch (Exception ex) {
        failed++;
        logger.error("Failed to migrate legacy schedule {}", key == null ? "(unreadable entry)" : key, ex);
      }
    }
    if (failed > 0) {
      logger.warn("Migrated {} legacy schedules; {} failed and will be retried on the next start", count, failed);
    } else {
      logger.info("Migrated {} legacy schedules to workflows", count);
    }
    return failed;
  }

  @Override
  public void close() {
    ticker.shutdown();
    workers.shutdown();
    try {
      if (!ticker.awaitTermination(10, TimeUnit.SECONDS)) {
        ticker.shutdownNow();
      }
      if (!workers.awaitTermination(10, TimeUnit.SECONDS)) {
        workers.shutdownNow();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    instance = null;
  }

  private static ThreadFactory daemon(String name) {
    return r -> {
      Thread t = new Thread(r, name);
      t.setDaemon(true);
      return t;
    };
  }
}
