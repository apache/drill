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

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.server.rest.WorkflowConfigResources.WorkflowConfig;
import org.apache.drill.exec.server.rest.auth.DrillUserPrincipal;
import org.apache.drill.exec.store.sys.PersistentStore;
import org.apache.drill.exec.store.sys.PersistentStoreConfig;
import org.apache.drill.exec.store.sys.PersistentStoreProvider;
import org.apache.drill.exec.work.WorkManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.annotation.security.RolesAllowed;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
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

/**
 * REST API for managing query schedules and their execution snapshots.
 * Schedules define when saved queries should be executed automatically.
 * Snapshots record the results of each scheduled execution.
 */
@Path("/api/v1/schedules")
@Tag(name = "Schedules", description = "APIs for managing scheduled query execution")
@RolesAllowed(DrillUserPrincipal.AUTHENTICATED_ROLE)
public class ScheduleResources {

  private static final Logger logger = LoggerFactory.getLogger(ScheduleResources.class);
  private static final String SCHEDULE_STORE_NAME = "drill.sqllab.schedules";
  private static final String SNAPSHOT_STORE_NAME = "drill.sqllab.snapshots";
  private static final String WORKFLOW_CONFIG_STORE_NAME = "drill.sqllab.workflow_config";
  private static final String WORKFLOW_CONFIG_KEY = "default";

  @Inject
  WorkManager workManager;

  @Inject
  DrillUserPrincipal principal;

  @Inject
  PersistentStoreProvider storeProvider;

  private static volatile PersistentStore<QueryScheduleModel> scheduleStore;
  private static volatile PersistentStore<QuerySnapshotModel> snapshotStore;
  private static volatile PersistentStore<WorkflowConfig> workflowConfigStore;

  // ==================== Model Classes ====================

  /**
   * Model representing a scheduled query execution configuration.
   */
  public static class QueryScheduleModel {
    @JsonProperty
    public String id;

    @JsonProperty
    public String savedQueryId;

    @JsonProperty
    public String description;

    @JsonProperty
    public String frequency;

    @JsonProperty
    public boolean enabled;

    @JsonProperty
    public String timeOfDay;

    @JsonProperty
    public Integer dayOfWeek;

    @JsonProperty
    public Integer dayOfMonth;

    @JsonProperty
    public boolean notifyOnSuccess;

    @JsonProperty
    public boolean notifyOnFailure;

    @JsonProperty
    public List<String> notifyEmails;

    @JsonProperty
    public int retentionCount = 30;

    @JsonProperty
    public String nextRunAt;

    @JsonProperty
    public String lastRunAt;

    @JsonProperty
    public String createdAt;

    @JsonProperty
    public String expiresAt;

    @JsonProperty
    public String renewedAt;

    public QueryScheduleModel() {
    }
  }

  /**
   * Model representing a snapshot of a scheduled query execution result.
   */
  public static class QuerySnapshotModel {
    @JsonProperty
    public String id;

    @JsonProperty
    public String scheduleId;

    @JsonProperty
    public String savedQueryId;

    @JsonProperty
    public String executedAt;

    @JsonProperty
    public String status;

    @JsonProperty
    public Integer rowCount;

    @JsonProperty
    public Long duration;

    @JsonProperty
    public String errorMessage;

    public QuerySnapshotModel() {
    }
  }

  // ==================== Schedule Endpoints ====================

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "List all schedules",
      description = "Returns all query schedules, auto-disabling any that have expired")
  public List<QueryScheduleModel> listSchedules() {
    try {
      PersistentStore<QueryScheduleModel> store = getScheduleStore();
      Iterator<Map.Entry<String, QueryScheduleModel>> iter = store.getAll();
      List<QueryScheduleModel> list = new ArrayList<>();
      String now = Instant.now().toString();
      while (iter.hasNext()) {
        QueryScheduleModel schedule = iter.next().getValue();
        if (schedule.enabled && schedule.expiresAt != null && schedule.expiresAt.compareTo(now) < 0) {
          schedule.enabled = false;
          store.put(schedule.id, schedule);
          logger.info("Auto-disabled expired schedule: {}", schedule.id);
        }
        list.add(schedule);
      }
      return list;
    } catch (Exception e) {
      logger.error("Failed to list schedules", e);
      throw new DrillRuntimeException("Failed to list schedules: " + e.getMessage(), e);
    }
  }

  @GET
  @Path("/{id}")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Get a schedule by ID",
      description = "Returns a single query schedule")
  public Response getSchedule(
      @Parameter(description = "Schedule ID") @PathParam("id") String id) {
    try {
      PersistentStore<QueryScheduleModel> store = getScheduleStore();
      QueryScheduleModel schedule = store.get(id);
      if (schedule == null) {
        return Response.status(Response.Status.NOT_FOUND)
            .entity(Map.of("error", "Schedule not found: " + id))
            .build();
      }
      return Response.ok(schedule).build();
    } catch (Exception e) {
      logger.error("Failed to get schedule: {}", id, e);
      throw new DrillRuntimeException("Failed to get schedule: " + e.getMessage(), e);
    }
  }

  @GET
  @Path("/query/{savedQueryId}")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Get schedule for a saved query",
      description = "Returns the schedule associated with a specific saved query")
  public Response getScheduleForQuery(
      @Parameter(description = "Saved query ID") @PathParam("savedQueryId") String savedQueryId) {
    try {
      PersistentStore<QueryScheduleModel> store = getScheduleStore();
      Iterator<Map.Entry<String, QueryScheduleModel>> iter = store.getAll();
      while (iter.hasNext()) {
        QueryScheduleModel schedule = iter.next().getValue();
        if (savedQueryId.equals(schedule.savedQueryId)) {
          return Response.ok(schedule).build();
        }
      }
      return Response.status(Response.Status.NOT_FOUND)
          .entity(Map.of("error", "No schedule found for saved query: " + savedQueryId))
          .build();
    } catch (Exception e) {
      logger.error("Failed to find schedule for query: {}", savedQueryId, e);
      throw new DrillRuntimeException("Failed to find schedule: " + e.getMessage(), e);
    }
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Create a new schedule",
      description = "Creates a new scheduled query execution")
  public Response createSchedule(QueryScheduleModel schedule) {
    try {
      schedule.id = UUID.randomUUID().toString();
      schedule.createdAt = Instant.now().toString();
      schedule.nextRunAt = computeNextRun(schedule);
      schedule.expiresAt = computeExpiresAt();

      PersistentStore<QueryScheduleModel> store = getScheduleStore();
      store.put(schedule.id, schedule);
      logger.info("Created schedule: {} for query: {}", schedule.id, schedule.savedQueryId);

      return Response.status(Response.Status.CREATED).entity(schedule).build();
    } catch (Exception e) {
      logger.error("Failed to create schedule", e);
      throw new DrillRuntimeException("Failed to create schedule: " + e.getMessage(), e);
    }
  }

  @PUT
  @Path("/{id}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Update a schedule",
      description = "Updates an existing query schedule")
  public Response updateSchedule(
      @Parameter(description = "Schedule ID") @PathParam("id") String id,
      QueryScheduleModel schedule) {
    try {
      PersistentStore<QueryScheduleModel> store = getScheduleStore();
      QueryScheduleModel existing = store.get(id);
      if (existing == null) {
        return Response.status(Response.Status.NOT_FOUND)
            .entity(Map.of("error", "Schedule not found: " + id))
            .build();
      }

      schedule.id = id;
      schedule.createdAt = existing.createdAt;
      if (schedule.expiresAt == null) {
        schedule.expiresAt = existing.expiresAt;
      }
      if (schedule.renewedAt == null) {
        schedule.renewedAt = existing.renewedAt;
      }
      schedule.nextRunAt = computeNextRun(schedule);

      store.put(id, schedule);
      logger.info("Updated schedule: {}", id);

      return Response.ok(schedule).build();
    } catch (Exception e) {
      logger.error("Failed to update schedule: {}", id, e);
      throw new DrillRuntimeException("Failed to update schedule: " + e.getMessage(), e);
    }
  }

  @POST
  @Path("/{id}/renew")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Renew a schedule",
      description = "Resets the expiration date, re-enables the schedule, and recomputes the next run time")
  public Response renewSchedule(
      @Parameter(description = "Schedule ID") @PathParam("id") String id) {
    try {
      PersistentStore<QueryScheduleModel> store = getScheduleStore();
      QueryScheduleModel schedule = store.get(id);
      if (schedule == null) {
        return Response.status(Response.Status.NOT_FOUND)
            .entity(Map.of("error", "Schedule not found: " + id))
            .build();
      }

      schedule.renewedAt = Instant.now().toString();
      schedule.expiresAt = computeExpiresAt();
      schedule.enabled = true;
      schedule.nextRunAt = computeNextRun(schedule);

      store.put(id, schedule);
      logger.info("Renewed schedule: {}, new expiration: {}", id, schedule.expiresAt);

      return Response.ok(schedule).build();
    } catch (Exception e) {
      logger.error("Failed to renew schedule: {}", id, e);
      throw new DrillRuntimeException("Failed to renew schedule: " + e.getMessage(), e);
    }
  }

  @DELETE
  @Path("/{id}")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Delete a schedule",
      description = "Deletes a schedule and all of its associated snapshots")
  public Response deleteSchedule(
      @Parameter(description = "Schedule ID") @PathParam("id") String id) {
    try {
      PersistentStore<QueryScheduleModel> store = getScheduleStore();
      QueryScheduleModel existing = store.get(id);
      if (existing == null) {
        return Response.status(Response.Status.NOT_FOUND)
            .entity(Map.of("error", "Schedule not found: " + id))
            .build();
      }

      // Delete all snapshots for this schedule
      PersistentStore<QuerySnapshotModel> snapStore = getSnapshotStore();
      Iterator<Map.Entry<String, QuerySnapshotModel>> iter = snapStore.getAll();
      List<String> snapshotKeysToDelete = new ArrayList<>();
      while (iter.hasNext()) {
        Map.Entry<String, QuerySnapshotModel> entry = iter.next();
        if (id.equals(entry.getValue().scheduleId)) {
          snapshotKeysToDelete.add(entry.getKey());
        }
      }
      for (String key : snapshotKeysToDelete) {
        snapStore.delete(key);
      }
      logger.info("Deleted {} snapshots for schedule: {}", snapshotKeysToDelete.size(), id);

      store.delete(id);
      logger.info("Deleted schedule: {}", id);

      return Response.ok(Map.of("message", "Schedule deleted", "id", id)).build();
    } catch (Exception e) {
      logger.error("Failed to delete schedule: {}", id, e);
      throw new DrillRuntimeException("Failed to delete schedule: " + e.getMessage(), e);
    }
  }

  // ==================== Snapshot Endpoints ====================

  @GET
  @Path("/{id}/snapshots")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "List snapshots for a schedule",
      description = "Returns all execution snapshots for a given schedule")
  public Response listSnapshots(
      @Parameter(description = "Schedule ID") @PathParam("id") String id) {
    try {
      PersistentStore<QuerySnapshotModel> store = getSnapshotStore();
      Iterator<Map.Entry<String, QuerySnapshotModel>> iter = store.getAll();
      List<QuerySnapshotModel> snapshots = new ArrayList<>();
      while (iter.hasNext()) {
        QuerySnapshotModel snapshot = iter.next().getValue();
        if (id.equals(snapshot.scheduleId)) {
          snapshots.add(snapshot);
        }
      }
      return Response.ok(snapshots).build();
    } catch (Exception e) {
      logger.error("Failed to list snapshots for schedule: {}", id, e);
      throw new DrillRuntimeException("Failed to list snapshots: " + e.getMessage(), e);
    }
  }

  // ==================== Helper Methods ====================

  /**
   * Computes the next run time based on the schedule's frequency, timeOfDay,
   * dayOfWeek, and dayOfMonth settings.
   */
  private String computeNextRun(QueryScheduleModel schedule) {
    if (schedule.frequency == null || !schedule.enabled) {
      return null;
    }

    ZonedDateTime now = ZonedDateTime.now(ZoneId.of("UTC"));
    LocalTime runTime = LocalTime.of(0, 0);
    if (schedule.timeOfDay != null && !schedule.timeOfDay.isEmpty()) {
      runTime = LocalTime.parse(schedule.timeOfDay);
    }

    ZonedDateTime next;
    switch (schedule.frequency.toLowerCase()) {
      case "hourly":
        next = now.plusHours(1).withMinute(0).withSecond(0).withNano(0);
        break;
      case "daily":
        next = now.toLocalDate().atTime(runTime).atZone(now.getZone());
        if (!next.isAfter(now)) {
          next = next.plusDays(1);
        }
        break;
      case "weekly":
        int targetDow = (schedule.dayOfWeek != null) ? schedule.dayOfWeek : 1;
        DayOfWeek dow = DayOfWeek.of(targetDow);
        next = now.toLocalDate().atTime(runTime).atZone(now.getZone())
            .with(TemporalAdjusters.nextOrSame(dow));
        if (!next.isAfter(now)) {
          next = next.with(TemporalAdjusters.next(dow));
        }
        break;
      case "monthly":
        int targetDom = (schedule.dayOfMonth != null) ? schedule.dayOfMonth : 1;
        next = now.toLocalDate().withDayOfMonth(Math.min(targetDom, now.toLocalDate().lengthOfMonth()))
            .atTime(runTime).atZone(now.getZone());
        if (!next.isAfter(now)) {
          next = next.plusMonths(1);
          next = next.withDayOfMonth(Math.min(targetDom, next.toLocalDate().lengthOfMonth()));
        }
        break;
      default:
        logger.warn("Unknown frequency '{}' for schedule {}", schedule.frequency, schedule.id);
        return null;
    }

    return next.toInstant().toString();
  }

  /**
   * Computes the expiration date for a schedule based on the workflow configuration.
   */
  private String computeExpiresAt() {
    WorkflowConfig config = loadWorkflowConfig();
    if (config == null || !config.expirationEnabled) {
      return null;
    }
    return Instant.now()
        .atZone(ZoneId.of("UTC"))
        .plusDays(config.expirationDays)
        .toInstant()
        .toString();
  }

  /**
   * Loads the workflow configuration from the workflow config store.
   */
  private WorkflowConfig loadWorkflowConfig() {
    try {
      PersistentStore<WorkflowConfig> store = getWorkflowConfigStore();
      WorkflowConfig config = store.get(WORKFLOW_CONFIG_KEY);
      if (config == null) {
        return new WorkflowConfig();
      }
      return config;
    } catch (Exception e) {
      logger.error("Error reading workflow config, using defaults", e);
      return new WorkflowConfig();
    }
  }

  // ==================== Store Accessors ====================

  private PersistentStore<QueryScheduleModel> getScheduleStore() {
    if (scheduleStore == null) {
      synchronized (ScheduleResources.class) {
        if (scheduleStore == null) {
          try {
            scheduleStore = storeProvider.getOrCreateStore(
                PersistentStoreConfig.newJacksonBuilder(
                    workManager.getContext().getLpPersistence().getMapper(),
                    QueryScheduleModel.class
                )
                .name(SCHEDULE_STORE_NAME)
                .build()
            );
          } catch (Exception e) {
            throw new DrillRuntimeException("Failed to access schedule store", e);
          }
        }
      }
    }
    return scheduleStore;
  }

  private PersistentStore<QuerySnapshotModel> getSnapshotStore() {
    if (snapshotStore == null) {
      synchronized (ScheduleResources.class) {
        if (snapshotStore == null) {
          try {
            snapshotStore = storeProvider.getOrCreateStore(
                PersistentStoreConfig.newJacksonBuilder(
                    workManager.getContext().getLpPersistence().getMapper(),
                    QuerySnapshotModel.class
                )
                .name(SNAPSHOT_STORE_NAME)
                .build()
            );
          } catch (Exception e) {
            throw new DrillRuntimeException("Failed to access snapshot store", e);
          }
        }
      }
    }
    return snapshotStore;
  }

  private PersistentStore<WorkflowConfig> getWorkflowConfigStore() {
    if (workflowConfigStore == null) {
      synchronized (ScheduleResources.class) {
        if (workflowConfigStore == null) {
          try {
            workflowConfigStore = storeProvider.getOrCreateStore(
                PersistentStoreConfig.newJacksonBuilder(
                    workManager.getContext().getLpPersistence().getMapper(),
                    WorkflowConfig.class
                )
                .name(WORKFLOW_CONFIG_STORE_NAME)
                .build()
            );
          } catch (Exception e) {
            throw new DrillRuntimeException("Failed to access workflow config store", e);
          }
        }
      }
    }
    return workflowConfigStore;
  }
}
