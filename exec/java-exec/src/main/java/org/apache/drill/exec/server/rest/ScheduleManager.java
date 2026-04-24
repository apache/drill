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
package org.apache.drill.exec.server.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.GlobalEventExecutor;
import io.netty.util.concurrent.Promise;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.rpc.user.UserSession;
import org.apache.drill.exec.server.rest.auth.DrillUserPrincipal;
import org.apache.drill.exec.server.rest.ScheduleResources.QueryScheduleModel;
import org.apache.drill.exec.server.rest.ScheduleResources.QuerySnapshotModel;
import org.apache.drill.exec.server.rest.SavedQueryResources.SavedQuery;
import org.apache.drill.exec.server.rest.WorkflowConfigResources.WorkflowConfig;
import org.apache.drill.exec.store.sys.PersistentStore;
import org.apache.drill.exec.store.sys.PersistentStoreConfig;
import org.apache.drill.exec.store.sys.PersistentStoreProvider;
import org.apache.drill.exec.work.WorkManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAdjusters;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Background scheduler service that periodically checks for due scheduled
 * queries and executes them. Supports result persistence (CTAS/INSERT),
 * overlap protection, timeouts, diff detection, AI summary stubs, and alerts.
 *
 * <p>Uses the same model classes as {@link ScheduleResources} to ensure
 * consistent serialization.</p>
 *
 * <p>Singleton with lazy initialization. Starts automatically with Drill
 * and shuts down when the Drillbit stops.</p>
 */
public class ScheduleManager implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(ScheduleManager.class);
  private static volatile ScheduleManager instance;

  private static final String SCHEDULE_STORE = "drill.sqllab.schedules";
  private static final String SNAPSHOT_STORE = "drill.sqllab.snapshots";
  private static final String SAVED_QUERY_STORE = "drill.sqllab.saved_queries";
  private static final String WORKFLOW_CONFIG_STORE = "drill.sqllab.workflow_config";
  private static final String WORKFLOW_CONFIG_KEY = "default";

  private static final int CHECK_INTERVAL_SECONDS = 60;
  private static final int INITIAL_DELAY_SECONDS = 30;
  private static final int DEFAULT_TIMEOUT_SECONDS = 300;
  private static final int PREVIEW_ROW_LIMIT = 20;

  private final WorkManager workManager;
  private final PersistentStoreProvider storeProvider;
  private final ScheduledExecutorService scheduler;
  private final ExecutorService queryExecutor;
  private final ObjectMapper objectMapper;
  private volatile boolean running;

  private volatile PersistentStore<QueryScheduleModel> scheduleStore;
  private volatile PersistentStore<QuerySnapshotModel> snapshotStore;
  private volatile PersistentStore<SavedQuery> savedQueryStore;
  private volatile PersistentStore<WorkflowConfig> workflowConfigStore;

  private ScheduleManager(WorkManager workManager, PersistentStoreProvider storeProvider) {
    this.workManager = workManager;
    this.storeProvider = storeProvider;
    this.objectMapper = new ObjectMapper();
    this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
      Thread t = new Thread(r, "drill-schedule-manager");
      t.setDaemon(true);
      return t;
    });
    this.queryExecutor = Executors.newCachedThreadPool(r -> {
      Thread t = new Thread(r, "drill-schedule-query-executor");
      t.setDaemon(true);
      return t;
    });
  }

  public static ScheduleManager getOrCreate(WorkManager wm, PersistentStoreProvider sp) {
    if (instance == null) {
      synchronized (ScheduleManager.class) {
        if (instance == null) {
          instance = new ScheduleManager(wm, sp);
        }
      }
    }
    return instance;
  }

  public static ScheduleManager getInstance() {
    return instance;
  }

  public void start() {
    if (running) {
      return;
    }
    running = true;
    scheduler.scheduleAtFixedRate(this::checkAndExecute,
        INITIAL_DELAY_SECONDS, CHECK_INTERVAL_SECONDS, TimeUnit.SECONDS);
    logger.info("ScheduleManager started - checking every {} seconds", CHECK_INTERVAL_SECONDS);
  }

  // ==================== Public API ====================

  /**
   * Executes a schedule immediately, bypassing the timer.
   * Called from the REST "Run Now" endpoint.
   *
   * @param scheduleId the schedule ID to execute
   * @return the resulting snapshot, or null if the schedule was not found
   */
  public QuerySnapshotModel executeNow(String scheduleId) {
    PersistentStore<QueryScheduleModel> store = getScheduleStore();
    if (store == null) {
      return null;
    }
    QueryScheduleModel schedule = store.get(scheduleId);
    if (schedule == null) {
      return null;
    }

    SavedQuery savedQuery = lookupSavedQuery(schedule.savedQueryId);
    if (savedQuery == null) {
      logger.warn("Saved query {} not found for schedule {}", schedule.savedQueryId, schedule.id);
      QuerySnapshotModel errorSnap = buildSnapshot(schedule, Instant.now(),
          false, 0, 0, "Saved query not found: " + schedule.savedQueryId,
          null, null, null, null);
      persistSnapshot(errorSnap);
      return errorSnap;
    }

    Instant now = Instant.now();
    logger.info("Executing schedule now (manual trigger): {} (schedule: {})",
        savedQuery.getName(), schedule.id);

    QuerySnapshotModel snapshot = executeScheduleInternal(schedule, savedQuery, now, store);
    enforceRetention(schedule.id, schedule.retentionCount);
    return snapshot;
  }

  // ==================== Core Loop ====================

  private void checkAndExecute() {
    try {
      PersistentStore<QueryScheduleModel> store = getScheduleStore();
      if (store == null) {
        return;
      }

      Instant now = Instant.now();
      WorkflowConfig wfConfig = loadWorkflowConfig();
      Iterator<Map.Entry<String, QueryScheduleModel>> iter = store.getAll();

      while (iter.hasNext()) {
        QueryScheduleModel schedule = iter.next().getValue();
        try {
          processSchedule(schedule, now, wfConfig, store);
        } catch (Exception e) {
          logger.error("Error processing schedule {}: {}", schedule.id, e.getMessage(), e);
        }
      }
    } catch (Exception e) {
      logger.error("Error in ScheduleManager check cycle", e);
    }
  }

  // ==================== Schedule Processing ====================

  private void processSchedule(QueryScheduleModel schedule, Instant now,
      WorkflowConfig wfConfig, PersistentStore<QueryScheduleModel> store) {

    // --- Compute status ---
    computeStatus(schedule, now, wfConfig);

    // --- Check paused: skip silently ---
    if (schedule.paused) {
      return;
    }

    // --- Check disabled/expired ---
    if (!schedule.enabled) {
      return;
    }

    // --- Check expiration ---
    if (isExpired(schedule, now, wfConfig)) {
      logger.info("Schedule {} has expired - auto-disabling", schedule.id);
      schedule.enabled = false;
      schedule.status = "expired";
      store.put(schedule.id, schedule);
      return;
    }

    // --- Check if due ---
    if (schedule.nextRunAt == null || schedule.nextRunAt.isEmpty()) {
      return;
    }
    try {
      Instant nextRun = Instant.parse(schedule.nextRunAt);
      if (nextRun.isAfter(now)) {
        return;
      }
    } catch (Exception e) {
      logger.debug("Could not parse nextRunAt '{}' for schedule {}", schedule.nextRunAt, schedule.id);
      return;
    }

    // --- Overlap protection: skip if already running ---
    if (schedule.isRunning) {
      logger.warn("Schedule {} is still running from previous execution - skipping", schedule.id);
      QuerySnapshotModel skippedSnap = buildSnapshot(schedule, now,
          false, 0, 0, "Skipped: previous execution still running",
          null, null, null, null);
      skippedSnap.status = "skipped";
      persistSnapshot(skippedSnap);
      // Advance next run so we don't keep trying every cycle
      schedule.lastRunAt = now.toString();
      schedule.nextRunAt = computeNextRunAt(schedule, now);
      store.put(schedule.id, schedule);
      return;
    }

    // --- Look up saved query ---
    SavedQuery savedQuery = lookupSavedQuery(schedule.savedQueryId);
    if (savedQuery == null) {
      logger.warn("Saved query {} not found for schedule {}", schedule.savedQueryId, schedule.id);
      QuerySnapshotModel errorSnap = buildSnapshot(schedule, now,
          false, 0, 0, "Saved query not found: " + schedule.savedQueryId,
          null, null, null, null);
      persistSnapshot(errorSnap);
      schedule.lastRunAt = now.toString();
      schedule.nextRunAt = computeNextRunAt(schedule, now);
      store.put(schedule.id, schedule);
      return;
    }

    logger.info("Executing scheduled query: {} (schedule: {})", savedQuery.getName(), schedule.id);

    // --- Execute with overlap protection ---
    executeScheduleInternal(schedule, savedQuery, now, store);

    enforceRetention(schedule.id, schedule.retentionCount);
  }

  /**
   * Core execution logic shared by scheduled runs and manual "Run Now" triggers.
   * Handles overlap protection, timeout, result persistence, preview, diff, AI summary, and alerts.
   */
  private QuerySnapshotModel executeScheduleInternal(QueryScheduleModel schedule,
      SavedQuery savedQuery, Instant now, PersistentStore<QueryScheduleModel> store) {

    // Mark as running (overlap protection)
    schedule.isRunning = true;
    store.put(schedule.id, schedule);

    long startTime = System.currentTimeMillis();
    boolean success = false;
    int rowCount = 0;
    String errorMessage = null;
    String resultPath = null;
    List<Map<String, String>> resultRows = null;
    Collection<String> resultColumns = null;

    try {
      String sql = savedQuery.getSql();
      int timeout = schedule.timeoutSeconds > 0 ? schedule.timeoutSeconds : DEFAULT_TIMEOUT_SECONDS;
      String userName = effectiveUserName(schedule);

      // --- Result persistence: build CTAS/INSERT SQL ---
      if (schedule.persistResults) {
        resultPath = executePersistentQuery(schedule, sql, timeout, userName);
        // After persistence, run original query with limited rows for preview
        RestQueryRunner.QueryResult previewResult = executeQueryWithTimeout(sql, PREVIEW_ROW_LIMIT, timeout, userName);
        if (previewResult != null && previewResult.rows != null) {
          resultRows = previewResult.rows;
          resultColumns = previewResult.columns;
          rowCount = previewResult.rows.size();
        }
        success = true;
      } else {
        // --- Standard execution with timeout ---
        RestQueryRunner.QueryResult result = executeQueryWithTimeout(sql, 100, timeout, userName);
        success = true;
        if (result != null) {
          if (result.rows != null) {
            rowCount = result.rows.size();
            resultRows = result.rows;
          }
          resultColumns = result.columns;
        }
      }

      logger.info("Schedule {} executed successfully - {} rows", schedule.id, rowCount);
    } catch (TimeoutException e) {
      int timeout = schedule.timeoutSeconds > 0 ? schedule.timeoutSeconds : DEFAULT_TIMEOUT_SECONDS;
      errorMessage = "Query timed out after " + timeout + " seconds";
      logger.error("Schedule {} timed out: {}", schedule.id, errorMessage);
    } catch (Exception e) {
      errorMessage = e.getMessage();
      logger.error("Schedule {} execution failed: {}", schedule.id, errorMessage);
    } finally {
      // Clear running flag (overlap protection)
      schedule.isRunning = false;
      schedule.lastRunAt = now.toString();
      schedule.nextRunAt = computeNextRunAt(schedule, now);
      store.put(schedule.id, schedule);
    }

    long duration = System.currentTimeMillis() - startTime;

    // --- Build preview data ---
    String previewRowsJson = null;
    String previewColumnsJson = null;
    if (resultRows != null && !resultRows.isEmpty()) {
      try {
        List<Map<String, String>> preview = resultRows.size() > PREVIEW_ROW_LIMIT
            ? resultRows.subList(0, PREVIEW_ROW_LIMIT) : resultRows;
        previewRowsJson = objectMapper.writeValueAsString(preview);
      } catch (Exception e) {
        logger.debug("Failed to serialize preview rows", e);
      }
    }
    if (resultColumns != null) {
      try {
        previewColumnsJson = objectMapper.writeValueAsString(new ArrayList<>(resultColumns));
      } catch (Exception e) {
        logger.debug("Failed to serialize preview columns", e);
      }
    }

    // --- Diff detection ---
    Integer previousRowCount = null;
    Integer rowCountDelta = null;
    QuerySnapshotModel previousSnap = findPreviousSnapshot(schedule.id);
    if (previousSnap != null && previousSnap.rowCount != null) {
      previousRowCount = previousSnap.rowCount;
      rowCountDelta = rowCount - previousRowCount;
    }

    // --- AI Summary (stub) ---
    String aiSummary = null;
    if (schedule.aiSummaryEnabled && success) {
      int maxRows = schedule.aiSummaryMaxRows > 0 ? schedule.aiSummaryMaxRows : 100;
      if (rowCount <= maxRows) {
        // TODO: Integrate with Prospector API (/api/v1/ai/chat) to generate
        // actual AI summaries. This requires an HTTP client call from a
        // background thread, which is complex and deferred to a future phase.
        logger.info("Schedule {} qualifies for AI summary ({} rows <= {} max) - pending Prospector integration",
            schedule.id, rowCount, maxRows);
        aiSummary = "AI summary pending - feature requires Prospector configuration";
      } else {
        logger.debug("Schedule {} has {} rows, exceeds aiSummaryMaxRows ({}), skipping AI summary",
            schedule.id, rowCount, maxRows);
      }
    }

    // --- Alert evaluation ---
    String triggeredAlerts = evaluateAlerts(schedule, success, rowCount, duration, errorMessage);

    // --- Record snapshot ---
    QuerySnapshotModel snapshot = buildSnapshot(schedule, now, success, rowCount, duration,
        errorMessage, resultPath, aiSummary, triggeredAlerts,
        previewRowsJson);
    snapshot.previewColumns = previewColumnsJson;
    snapshot.previousRowCount = previousRowCount;
    snapshot.rowCountDelta = rowCountDelta;

    persistSnapshot(snapshot);
    return snapshot;
  }

  // ==================== Query Execution ====================

  /**
   * Executes a query with a timeout using a separate thread.
   */
  private RestQueryRunner.QueryResult executeQueryWithTimeout(String sql, int rowLimit, int timeoutSeconds,
      String userName) throws Exception {
    Callable<RestQueryRunner.QueryResult> task = () -> {
      WebUserConnection conn = null;
      try {
        conn = createSchedulerConnection(userName);
        QueryWrapper wrapper = new QueryWrapper.RestQueryBuilder()
            .query(sql)
            .queryType("SQL")
            .rowLimit(rowLimit)
            .build();
        RestQueryRunner runner = new RestQueryRunner(wrapper, workManager, conn);
        return runner.run();
      } finally {
        if (conn != null) {
          try {
            conn.cleanupSession();
          } catch (Exception e) {
            logger.debug("Error cleaning up scheduler connection", e);
          }
        }
      }
    };

    Future<RestQueryRunner.QueryResult> future = queryExecutor.submit(task);
    try {
      return future.get(timeoutSeconds, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      future.cancel(true);
      throw e;
    }
  }

  // ==================== Result Persistence (CTAS/INSERT) ====================

  /**
   * Executes the query with result persistence (CTAS or INSERT INTO).
   * Returns the result path where data was stored.
   */
  private String executePersistentQuery(QueryScheduleModel schedule, String originalSql, int timeout,
      String userName) throws Exception {
    String mode = schedule.resultMode != null ? schedule.resultMode : "overwrite";
    String location = schedule.resultLocation != null ? schedule.resultLocation : "dfs.tmp";
    String format = schedule.resultFormat != null ? schedule.resultFormat : "parquet";

    String tablePath;
    String persistSql;

    switch (mode) {
      case "append":
        tablePath = buildTablePath(location, schedule.id, null);
        persistSql = "INSERT INTO " + tablePath + " SELECT * FROM (" + originalSql + ")";
        break;

      case "newTablePerRun":
        String timestamp = String.valueOf(System.currentTimeMillis());
        tablePath = buildTablePath(location, schedule.id, "run_" + timestamp);
        persistSql = "CREATE TABLE " + tablePath + " AS SELECT * FROM (" + originalSql + ")";
        break;

      case "overwrite":
      default:
        tablePath = buildTablePath(location, schedule.id, null);
        // Drop existing table first
        try {
          executeQueryWithTimeout("DROP TABLE IF EXISTS " + tablePath, 0, timeout, userName);
        } catch (Exception e) {
          logger.debug("Drop table before overwrite failed (may not exist): {}", e.getMessage());
        }
        persistSql = "CREATE TABLE " + tablePath + " AS SELECT * FROM (" + originalSql + ")";
        break;
    }

    // Set output format
    try {
      executeQueryWithTimeout("ALTER SESSION SET `store.format` = '" + format + "'", 0, timeout, userName);
    } catch (Exception e) {
      logger.warn("Failed to set store format to '{}': {}", format, e.getMessage());
    }

    // Execute the CTAS/INSERT
    executeQueryWithTimeout(persistSql, 0, timeout, userName);
    logger.info("Persisted results for schedule {} to {}", schedule.id, tablePath);
    return tablePath;
  }

  /**
   * Builds a fully qualified table path for result persistence.
   */
  private String buildTablePath(String location, String scheduleId, String suffix) {
    // Sanitize schedule ID for use in table path (replace hyphens)
    String safeId = scheduleId.replace("-", "_");
    StringBuilder sb = new StringBuilder();
    sb.append(location).append(".`schedules`.`").append(safeId).append('`');
    if (suffix != null && !suffix.isEmpty()) {
      sb.append(".`").append(suffix).append('`');
    }
    return sb.toString();
  }

  // ==================== Alert Evaluation ====================

  /**
   * Evaluates alert rules defined in the schedule and returns
   * a JSON string of triggered alerts, or null if none triggered.
   */
  private String evaluateAlerts(QueryScheduleModel schedule, boolean success,
      int rowCount, long duration, String errorMessage) {
    if (schedule.alertRules == null || schedule.alertRules.isEmpty()) {
      return null;
    }

    try {
      List<Map<String, Object>> rules = objectMapper.readValue(schedule.alertRules,
          objectMapper.getTypeFactory().constructCollectionType(List.class, LinkedHashMap.class));

      List<Map<String, Object>> triggered = new ArrayList<>();

      for (Map<String, Object> rule : rules) {
        Boolean enabled = (Boolean) rule.get("enabled");
        if (enabled == null || !enabled) {
          continue;
        }

        String type = (String) rule.get("type");
        if (type == null) {
          continue;
        }

        Number thresholdNum = (Number) rule.get("threshold");
        long threshold = thresholdNum != null ? thresholdNum.longValue() : 0;

        boolean fire = false;
        String message = null;

        switch (type) {
          case "rowCountAbove":
            if (rowCount > threshold) {
              fire = true;
              message = "Row count " + rowCount + " exceeds threshold " + threshold;
            }
            break;
          case "rowCountBelow":
            if (rowCount < threshold) {
              fire = true;
              message = "Row count " + rowCount + " is below threshold " + threshold;
            }
            break;
          case "queryFailed":
            if (!success) {
              fire = true;
              message = "Query failed: " + (errorMessage != null ? errorMessage : "unknown error");
            }
            break;
          case "durationAbove":
            if (duration > threshold * 1000) {
              fire = true;
              message = "Duration " + duration + "ms exceeds threshold " + (threshold * 1000) + "ms";
            }
            break;
          case "durationExceedsInterval":
            long intervalMillis = estimateIntervalMillis(schedule);
            if (intervalMillis > 0 && duration > intervalMillis / 2) {
              fire = true;
              message = "Duration " + duration + "ms exceeds 50% of interval " + intervalMillis + "ms";
            }
            break;
          default:
            logger.debug("Unknown alert rule type: {}", type);
            break;
        }

        if (fire) {
          Map<String, Object> alert = new LinkedHashMap<>();
          alert.put("type", type);
          alert.put("message", message);
          alert.put("firedAt", Instant.now().toString());
          triggered.add(alert);
        }
      }

      if (triggered.isEmpty()) {
        return null;
      }
      return objectMapper.writeValueAsString(triggered);
    } catch (Exception e) {
      logger.debug("Failed to evaluate alert rules for schedule {}: {}", schedule.id, e.getMessage());
      return null;
    }
  }

  /**
   * Estimates the schedule interval in milliseconds for alert comparison.
   */
  private long estimateIntervalMillis(QueryScheduleModel schedule) {
    String freq = schedule.frequency != null ? schedule.frequency : "daily";
    switch (freq) {
      case "hourly":
        return 3_600_000L;
      case "daily":
        return 86_400_000L;
      case "weekly":
        return 604_800_000L;
      case "monthly":
        return 2_592_000_000L;
      default:
        return 86_400_000L;
    }
  }

  // ==================== Status Computation ====================

  /**
   * Computes the effective status of a schedule based on its flags and expiration.
   */
  private void computeStatus(QueryScheduleModel schedule, Instant now, WorkflowConfig wfConfig) {
    if (!schedule.enabled) {
      if (isExpired(schedule, now, wfConfig)) {
        schedule.status = "expired";
      } else {
        schedule.status = "disabled";
      }
    } else if (schedule.paused) {
      schedule.status = "paused";
    } else {
      schedule.status = "active";
    }
  }

  // ==================== Diff Detection ====================

  /**
   * Finds the most recent snapshot for a given schedule (for diff detection).
   */
  private QuerySnapshotModel findPreviousSnapshot(String scheduleId) {
    try {
      PersistentStore<QuerySnapshotModel> store = getSnapshotStore();
      if (store == null) {
        return null;
      }

      QuerySnapshotModel latest = null;
      String latestTime = "";
      Iterator<Map.Entry<String, QuerySnapshotModel>> iter = store.getAll();
      while (iter.hasNext()) {
        QuerySnapshotModel snap = iter.next().getValue();
        if (scheduleId.equals(snap.scheduleId) && !"skipped".equals(snap.status)) {
          String t = snap.executedAt != null ? snap.executedAt : "";
          if (t.compareTo(latestTime) > 0) {
            latestTime = t;
            latest = snap;
          }
        }
      }
      return latest;
    } catch (Exception e) {
      logger.debug("Failed to find previous snapshot for schedule {}", scheduleId, e);
      return null;
    }
  }

  // ==================== Expiration ====================

  private boolean isExpired(QueryScheduleModel schedule, Instant now, WorkflowConfig wfConfig) {
    if (wfConfig == null || !wfConfig.expirationEnabled) {
      return false;
    }
    if (schedule.expiresAt != null && !schedule.expiresAt.isEmpty()) {
      try {
        return Instant.parse(schedule.expiresAt).isBefore(now);
      } catch (Exception e) {
        // Fall through
      }
    }
    return false;
  }

  // ==================== Next Run ====================

  private String computeNextRunAt(QueryScheduleModel schedule, Instant fromTime) {
    try {
      ZonedDateTime base = fromTime.atZone(ZoneId.systemDefault());

      LocalTime scheduledTime = LocalTime.of(8, 0);
      if (schedule.timeOfDay != null && !schedule.timeOfDay.isEmpty()) {
        try {
          scheduledTime = LocalTime.parse(schedule.timeOfDay);
        } catch (Exception e) {
          // Use default
        }
      }

      ZonedDateTime next;
      String freq = schedule.frequency != null ? schedule.frequency : "daily";

      switch (freq) {
        case "hourly":
          next = base.plusHours(1).withMinute(scheduledTime.getMinute()).withSecond(0).withNano(0);
          break;
        case "weekly":
          DayOfWeek targetDay = mapDayOfWeek(schedule.dayOfWeek != null ? schedule.dayOfWeek : 1);
          next = base.plusDays(1).with(scheduledTime).with(TemporalAdjusters.nextOrSame(targetDay));
          if (!next.isAfter(base)) {
            next = next.plusWeeks(1);
          }
          break;
        case "monthly":
          int dom = schedule.dayOfMonth != null && schedule.dayOfMonth > 0 ? schedule.dayOfMonth : 1;
          next = base.plusMonths(1)
              .withDayOfMonth(Math.min(dom, base.plusMonths(1).toLocalDate().lengthOfMonth()))
              .with(scheduledTime);
          if (!next.isAfter(base)) {
            next = next.plusMonths(1);
          }
          break;
        case "daily":
        default:
          next = base.plusDays(1).with(scheduledTime);
          break;
      }

      return next.toInstant().toString();
    } catch (Exception e) {
      logger.error("Error computing next run for schedule {}", schedule.id, e);
      return fromTime.plusSeconds(86400).toString();
    }
  }

  private DayOfWeek mapDayOfWeek(int day) {
    switch (day) {
      case 0: return DayOfWeek.SUNDAY;
      case 2: return DayOfWeek.TUESDAY;
      case 3: return DayOfWeek.WEDNESDAY;
      case 4: return DayOfWeek.THURSDAY;
      case 5: return DayOfWeek.FRIDAY;
      case 6: return DayOfWeek.SATURDAY;
      case 1:
      default: return DayOfWeek.MONDAY;
    }
  }

  // ==================== Snapshots ====================

  /**
   * Builds a snapshot model (does not persist it).
   */
  private QuerySnapshotModel buildSnapshot(QueryScheduleModel schedule, Instant executedAt,
      boolean success, int rowCount, long duration, String errorMessage,
      String resultPath, String aiSummary, String triggeredAlerts,
      String previewRowsJson) {
    QuerySnapshotModel snapshot = new QuerySnapshotModel();
    snapshot.id = UUID.randomUUID().toString();
    snapshot.scheduleId = schedule.id;
    snapshot.savedQueryId = schedule.savedQueryId;
    snapshot.executedAt = executedAt.toString();
    snapshot.status = success ? "success" : "error";
    snapshot.rowCount = rowCount;
    snapshot.duration = duration;
    snapshot.errorMessage = errorMessage;
    snapshot.resultPath = resultPath;
    snapshot.aiSummary = aiSummary;
    snapshot.triggeredAlerts = triggeredAlerts;
    snapshot.previewRows = previewRowsJson;
    return snapshot;
  }

  /**
   * Persists a snapshot to the store.
   */
  private void persistSnapshot(QuerySnapshotModel snapshot) {
    try {
      PersistentStore<QuerySnapshotModel> store = getSnapshotStore();
      if (store != null) {
        store.put(snapshot.id, snapshot);
      }
    } catch (Exception e) {
      logger.error("Failed to record snapshot for schedule {}", snapshot.scheduleId, e);
    }
  }

  private void enforceRetention(String scheduleId, int retentionCount) {
    int limit = retentionCount > 0 ? retentionCount : 30;
    try {
      PersistentStore<QuerySnapshotModel> store = getSnapshotStore();
      if (store == null) {
        return;
      }

      List<QuerySnapshotModel> snapshots = new ArrayList<>();
      Iterator<Map.Entry<String, QuerySnapshotModel>> iter = store.getAll();
      while (iter.hasNext()) {
        QuerySnapshotModel snap = iter.next().getValue();
        if (scheduleId.equals(snap.scheduleId)) {
          snapshots.add(snap);
        }
      }

      if (snapshots.size() <= limit) {
        return;
      }

      // Sort newest first by executedAt string (ISO format sorts lexicographically)
      snapshots.sort((a, b) -> {
        String aTime = a.executedAt != null ? a.executedAt : "";
        String bTime = b.executedAt != null ? b.executedAt : "";
        return bTime.compareTo(aTime);
      });

      for (int i = limit; i < snapshots.size(); i++) {
        store.delete(snapshots.get(i).id);
      }
    } catch (Exception e) {
      logger.error("Error enforcing retention for schedule {}", scheduleId, e);
    }
  }

  // ==================== Connection ====================

  /**
   * Builds a WebUserConnection for scheduled query execution.
   * Runs under the schedule owner's identity so storage plugins and impersonation
   * enforce the owner's ACLs. Falls back to anonymous only for legacy schedules
   * with no recorded owner.
   */
  private WebUserConnection createSchedulerConnection(String userName) {
    DrillConfig config = workManager.getContext().getConfig();
    BufferAllocator allocator = workManager.getContext().getAllocator()
        .newChildAllocator("ScheduleManager:session",
            config.getLong(ExecConstants.HTTP_SESSION_MEMORY_RESERVATION),
            config.getLong(ExecConstants.HTTP_SESSION_MEMORY_MAXIMUM));
    String effectiveUser = (userName == null || userName.isEmpty())
        ? DrillUserPrincipal.ANONYMOUS_USER : userName;
    UserSession session = UserSession.Builder.newBuilder()
        .withCredentials(UserBitShared.UserCredentials.newBuilder()
            .setUserName(effectiveUser)
            .build())
        .withOptionManager(workManager.getContext().getOptionManager())
        .setSupportComplexTypes(config.getBoolean(ExecConstants.CLIENT_SUPPORT_COMPLEX_TYPES))
        .build();
    Promise<Void> closeFuture = new DefaultPromise<>(GlobalEventExecutor.INSTANCE);
    WebSessionResources wsr = new WebSessionResources(allocator, null, session, closeFuture);
    if (DrillUserPrincipal.ANONYMOUS_USER.equals(effectiveUser)) {
      return new WebUserConnection.AnonWebUserConnection(wsr);
    }
    return new WebUserConnection(wsr);
  }

  private String effectiveUserName(QueryScheduleModel schedule) {
    return (schedule.owner == null || schedule.owner.isEmpty())
        ? DrillUserPrincipal.ANONYMOUS_USER : schedule.owner;
  }

  // ==================== Helpers ====================

  private SavedQuery lookupSavedQuery(String savedQueryId) {
    try {
      PersistentStore<SavedQuery> store = getSavedQueryStore();
      if (store == null) {
        return null;
      }
      return store.get(savedQueryId);
    } catch (Exception e) {
      logger.error("Error looking up saved query {}", savedQueryId, e);
      return null;
    }
  }

  private WorkflowConfig loadWorkflowConfig() {
    try {
      PersistentStore<WorkflowConfig> store = getWorkflowConfigStore();
      if (store == null) {
        return new WorkflowConfig();
      }
      WorkflowConfig config = store.get(WORKFLOW_CONFIG_KEY);
      return config != null ? config : new WorkflowConfig();
    } catch (Exception e) {
      return new WorkflowConfig();
    }
  }

  // ==================== Store Accessors ====================

  private PersistentStore<QueryScheduleModel> getScheduleStore() {
    if (scheduleStore == null) {
      synchronized (this) {
        if (scheduleStore == null) {
          try {
            scheduleStore = storeProvider.getOrCreateStore(
                PersistentStoreConfig.newJacksonBuilder(
                    workManager.getContext().getLpPersistence().getMapper(),
                    QueryScheduleModel.class
                ).name(SCHEDULE_STORE).build()
            );
          } catch (Exception e) {
            logger.error("Failed to access schedule store", e);
          }
        }
      }
    }
    return scheduleStore;
  }

  private PersistentStore<QuerySnapshotModel> getSnapshotStore() {
    if (snapshotStore == null) {
      synchronized (this) {
        if (snapshotStore == null) {
          try {
            snapshotStore = storeProvider.getOrCreateStore(
                PersistentStoreConfig.newJacksonBuilder(
                    workManager.getContext().getLpPersistence().getMapper(),
                    QuerySnapshotModel.class
                ).name(SNAPSHOT_STORE).build()
            );
          } catch (Exception e) {
            logger.error("Failed to access snapshot store", e);
          }
        }
      }
    }
    return snapshotStore;
  }

  private PersistentStore<SavedQuery> getSavedQueryStore() {
    if (savedQueryStore == null) {
      synchronized (this) {
        if (savedQueryStore == null) {
          try {
            savedQueryStore = storeProvider.getOrCreateStore(
                PersistentStoreConfig.newJacksonBuilder(
                    workManager.getContext().getLpPersistence().getMapper(),
                    SavedQuery.class
                ).name(SAVED_QUERY_STORE).build()
            );
          } catch (Exception e) {
            logger.error("Failed to access saved query store", e);
          }
        }
      }
    }
    return savedQueryStore;
  }

  private PersistentStore<WorkflowConfig> getWorkflowConfigStore() {
    if (workflowConfigStore == null) {
      synchronized (this) {
        if (workflowConfigStore == null) {
          try {
            workflowConfigStore = storeProvider.getOrCreateStore(
                PersistentStoreConfig.newJacksonBuilder(
                    workManager.getContext().getLpPersistence().getMapper(),
                    WorkflowConfig.class
                ).name(WORKFLOW_CONFIG_STORE).build()
            );
          } catch (Exception e) {
            logger.error("Failed to access workflow config store", e);
          }
        }
      }
    }
    return workflowConfigStore;
  }

  // ==================== Lifecycle ====================

  @Override
  public void close() {
    running = false;
    scheduler.shutdown();
    queryExecutor.shutdown();
    try {
      if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
        logger.warn("ScheduleManager did not terminate gracefully, forcing shutdown");
        scheduler.shutdownNow();
      }
      if (!queryExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
        logger.warn("Query executor did not terminate gracefully, forcing shutdown");
        queryExecutor.shutdownNow();
      }
    } catch (InterruptedException e) {
      scheduler.shutdownNow();
      queryExecutor.shutdownNow();
      Thread.currentThread().interrupt();
    }
    instance = null;
    logger.info("ScheduleManager stopped");
  }
}
