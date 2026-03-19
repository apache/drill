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

import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.GlobalEventExecutor;
import io.netty.util.concurrent.Promise;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.rpc.user.UserSession;
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Background scheduler service that periodically checks for due scheduled
 * queries and executes them. Uses the same model classes as
 * {@link ScheduleResources} to ensure consistent serialization.
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
  private static final int DEFAULT_EXPIRATION_DAYS = 90;

  private final WorkManager workManager;
  private final PersistentStoreProvider storeProvider;
  private final ScheduledExecutorService scheduler;
  private volatile boolean running;

  private volatile PersistentStore<QueryScheduleModel> scheduleStore;
  private volatile PersistentStore<QuerySnapshotModel> snapshotStore;
  private volatile PersistentStore<SavedQuery> savedQueryStore;
  private volatile PersistentStore<WorkflowConfig> workflowConfigStore;

  private ScheduleManager(WorkManager workManager, PersistentStoreProvider storeProvider) {
    this.workManager = workManager;
    this.storeProvider = storeProvider;
    this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
      Thread t = new Thread(r, "drill-schedule-manager");
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

  // ==================== Core Logic ====================

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

  private void processSchedule(QueryScheduleModel schedule, Instant now,
      WorkflowConfig wfConfig, PersistentStore<QueryScheduleModel> store) {
    if (!schedule.enabled) {
      return;
    }

    // Check expiration
    if (isExpired(schedule, now, wfConfig)) {
      logger.info("Schedule {} has expired - auto-disabling", schedule.id);
      schedule.enabled = false;
      store.put(schedule.id, schedule);
      return;
    }

    // Check if due (nextRunAt is an ISO string)
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

    // Look up saved query
    SavedQuery savedQuery = lookupSavedQuery(schedule.savedQueryId);
    if (savedQuery == null) {
      logger.warn("Saved query {} not found for schedule {}", schedule.savedQueryId, schedule.id);
      recordSnapshot(schedule, now, false, 0, 0,
          "Saved query not found: " + schedule.savedQueryId);
      schedule.lastRunAt = now.toString();
      schedule.nextRunAt = computeNextRunAt(schedule, now);
      store.put(schedule.id, schedule);
      return;
    }

    logger.info("Executing scheduled query: {} (schedule: {})", savedQuery.getName(), schedule.id);

    // Execute
    long startTime = System.currentTimeMillis();
    boolean success = false;
    int rowCount = 0;
    String errorMessage = null;

    WebUserConnection conn = null;
    try {
      conn = createSchedulerConnection();
      String sql = savedQuery.getSql();
      QueryWrapper wrapper = new QueryWrapper.RestQueryBuilder()
          .query(sql)
          .queryType("SQL")
          .rowLimit(100)
          .build();
      RestQueryRunner runner = new RestQueryRunner(wrapper, workManager, conn);
      RestQueryRunner.QueryResult result = runner.run();

      success = true;
      if (result.rows != null) {
        rowCount = result.rows.size();
      }
      logger.info("Schedule {} executed successfully - {} rows", schedule.id, rowCount);
    } catch (Exception e) {
      errorMessage = e.getMessage();
      logger.error("Schedule {} execution failed: {}", schedule.id, errorMessage);
    } finally {
      if (conn != null) {
        try {
          conn.cleanupSession();
        } catch (Exception e) {
          logger.debug("Error cleaning up scheduler connection", e);
        }
      }
    }

    long duration = System.currentTimeMillis() - startTime;

    recordSnapshot(schedule, now, success, rowCount, duration, errorMessage);

    schedule.lastRunAt = now.toString();
    schedule.nextRunAt = computeNextRunAt(schedule, now);
    store.put(schedule.id, schedule);

    enforceRetention(schedule.id, schedule.retentionCount);
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
        // Fall through to computed expiration
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

  private void recordSnapshot(QueryScheduleModel schedule, Instant executedAt,
      boolean success, int rowCount, long duration, String errorMessage) {
    try {
      PersistentStore<QuerySnapshotModel> store = getSnapshotStore();
      if (store == null) {
        return;
      }

      QuerySnapshotModel snapshot = new QuerySnapshotModel();
      snapshot.id = UUID.randomUUID().toString();
      snapshot.scheduleId = schedule.id;
      snapshot.savedQueryId = schedule.savedQueryId;
      snapshot.executedAt = executedAt.toString();
      snapshot.status = success ? "success" : "error";
      snapshot.rowCount = rowCount;
      snapshot.duration = duration;
      snapshot.errorMessage = errorMessage;

      store.put(snapshot.id, snapshot);
    } catch (Exception e) {
      logger.error("Failed to record snapshot for schedule {}", schedule.id, e);
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

  private WebUserConnection createSchedulerConnection() {
    DrillConfig config = workManager.getContext().getConfig();
    BufferAllocator allocator = workManager.getContext().getAllocator()
        .newChildAllocator("ScheduleManager:session",
            config.getLong(ExecConstants.HTTP_SESSION_MEMORY_RESERVATION),
            config.getLong(ExecConstants.HTTP_SESSION_MEMORY_MAXIMUM));
    UserSession session = UserSession.Builder.newBuilder()
        .withCredentials(UserBitShared.UserCredentials.newBuilder()
            .setUserName("anonymous")
            .build())
        .withOptionManager(workManager.getContext().getOptionManager())
        .setSupportComplexTypes(config.getBoolean(ExecConstants.CLIENT_SUPPORT_COMPLEX_TYPES))
        .build();
    Promise<Void> closeFuture = new DefaultPromise<>(GlobalEventExecutor.INSTANCE);
    WebSessionResources wsr = new WebSessionResources(allocator, null, session, closeFuture);
    return new WebUserConnection.AnonWebUserConnection(wsr);
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
    try {
      if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
        logger.warn("ScheduleManager did not terminate gracefully, forcing shutdown");
        scheduler.shutdownNow();
      }
    } catch (InterruptedException e) {
      scheduler.shutdownNow();
      Thread.currentThread().interrupt();
    }
    instance = null;
    logger.info("ScheduleManager stopped");
  }
}
