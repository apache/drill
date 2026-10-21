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

import io.swagger.v3.oas.annotations.tags.Tag;
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
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.server.rest.ProjectResources;
import org.apache.drill.exec.server.rest.QueryWrapper;
import org.apache.drill.exec.server.rest.RestQueryRunner;
import org.apache.drill.exec.server.rest.SavedQueryResources;
import org.apache.drill.exec.server.rest.WebUserConnection;
import org.apache.drill.exec.server.rest.auth.DrillUserPrincipal;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.dfs.FileSystemConfig;
import org.apache.drill.exec.store.dfs.WorkspaceConfig;
import org.apache.drill.exec.work.WorkManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Path("/api/v1/workflows")
@Tag(name = "Workflows", description = "Scheduled queries with action pipelines")
@RolesAllowed(DrillUserPrincipal.AUTHENTICATED_ROLE)
@Produces(MediaType.APPLICATION_JSON)
public class WorkflowResources {

  private static final Logger logger = LoggerFactory.getLogger(WorkflowResources.class);

  @Inject
  DrillUserPrincipal principal;

  @Inject
  WorkManager workManager;

  @Inject
  WebUserConnection webUserConnection;

  @Inject
  StoragePluginRegistry storageRegistry;

  // ---------------------------------------------------------------- CRUD

  @GET
  public List<Workflow> list(@QueryParam("projectId") String projectId) {
    List<Workflow> out = new ArrayList<>();
    if (projectId != null && !WorkflowPermissions.canView(stores().project(projectId), user(), admin())) {
      throw error(Response.Status.FORBIDDEN, "Access denied");
    }
    Iterator<Map.Entry<String, Workflow>> it = stores().workflows().getAll();
    while (it.hasNext()) {
      Workflow wf = it.next().getValue();
      boolean match = projectId == null ? canView(wf) : projectId.equals(wf.projectId);
      if (match) {
        out.add(wf);
      }
    }
    return out;
  }

  @GET
  @Path("/{id}")
  public Workflow get(@PathParam("id") String id) {
    return viewable(id);
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  public Response create(Workflow wf) {
    requireBody(wf);
    ProjectResources.Project p = stores().project(wf.projectId);
    requireCreator(p);
    List<WorkflowValidator.ValidationError> errors = validate(wf, p, user());
    if (!errors.isEmpty()) {
      return unprocessable(errors);
    }
    Instant now = Instant.now();
    WorkflowConfig cfg = stores().loadConfig();
    wf.id = UUID.randomUUID().toString();
    wf.owner = user();
    wf.createdAt = now.toString();
    wf.renewedAt = null;
    wf.status = "paused";
    wf.schedule.enabled = false;
    wf.isRunning = false;
    wf.runStartedAt = null;
    wf.consecutiveFailures = 0;
    wf.lastRunAt = null;
    wf.lastStatus = null;
    wf.schedule.expiresAt = cfg.expirationEnabled ? now.plus(Duration.ofDays(cfg.expirationDays)).toString() : null;
    wf.nextRunAt = WorkflowSchedule.nextRun(wf.schedule, now, ZoneId.systemDefault()).toString();
    stores().workflows().put(wf.id, wf);
    return Response.status(Response.Status.CREATED).entity(wf).build();
  }

  @PUT
  @Path("/{id}")
  @Consumes(MediaType.APPLICATION_JSON)
  public Response update(@PathParam("id") String id, Workflow wf) {
    requireBody(wf);
    Workflow existing = viewable(id);
    if (!WorkflowPermissions.canEdit(existing, user(), admin())) {
      throw error(Response.Status.FORBIDDEN, "Only the workflow owner or an admin can edit it");
    }
    wf.projectId = existing.projectId;
    List<WorkflowValidator.ValidationError> errors = validate(wf, stores().project(existing.projectId), existing.owner);
    if (!errors.isEmpty()) {
      return unprocessable(errors);
    }
    wf.id = existing.id;
    wf.owner = existing.owner;
    wf.createdAt = existing.createdAt;
    wf.renewedAt = existing.renewedAt;
    wf.status = existing.status;
    wf.schedule.enabled = existing.schedule.enabled;
    wf.schedule.expiresAt = existing.schedule.expiresAt;
    wf.nextRunAt = WorkflowSchedule.nextRun(wf.schedule, Instant.now(), ZoneId.systemDefault()).toString();
    save(wf);
    return Response.ok(wf).build();
  }

  @DELETE
  @Path("/{id}")
  public Response delete(@PathParam("id") String id) {
    Workflow wf = manageable(id);
    stores().workflows().delete(wf.id);
    List<String> runIds = new ArrayList<>();
    stores().runs().getAll().forEachRemaining(e -> {
      if (e.getValue() != null && id.equals(e.getValue().scheduleId)) {
        runIds.add(e.getKey());
      }
    });
    runIds.forEach(stores().runs()::delete);
    return Response.noContent().build();
  }

  public static class EnabledRequest {
    public boolean enabled;
  }

  @POST
  @Path("/{id}/enabled")
  @Consumes(MediaType.APPLICATION_JSON)
  public Workflow setEnabled(@PathParam("id") String id, EnabledRequest req) {
    requireBody(req);
    Workflow wf = manageable(id);
    if (req.enabled && "owner_lost_access".equals(wf.status)) {
      throw error(Response.Status.CONFLICT, "The workflow owner is not a member of this project. "
          + "An admin can open the workflow and click Take ownership");
    }
    if (req.enabled && WorkflowSchedule.isExpired(wf, Instant.now(), stores().loadConfig())) {
      throw error(Response.Status.CONFLICT, "Workflow has expired; renew it first");
    }
    if (req.enabled && wf.projectId == null) {
      throw error(Response.Status.CONFLICT, "Assign the workflow to a project first");
    }
    wf.schedule.enabled = req.enabled;
    wf.status = req.enabled ? "active" : "paused";
    wf.nextRunAt = WorkflowSchedule.nextRun(wf.schedule, Instant.now(), ZoneId.systemDefault()).toString();
    save(wf);
    return wf;
  }

  // ---------------------------------------------------------------- validation & dry run

  public static class ValidationResult {
    public List<WorkflowValidator.ValidationError> errors;
  }

  @POST
  @Path("/validate")
  @Consumes(MediaType.APPLICATION_JSON)
  public ValidationResult validateOnly(Workflow wf) {
    requireBody(wf);
    ProjectResources.Project p = stores().project(wf.projectId);
    if (wf.projectId != null && !WorkflowPermissions.canView(p, user(), admin())) {
      throw error(Response.Status.FORBIDDEN, "Access denied");
    }
    Workflow existing = wf.id == null ? null : stores().workflows().get(wf.id);
    // The id is client supplied: only trust it within the project the caller was checked against,
    // so the 422 cannot reveal the owner of a workflow the caller cannot see.
    boolean sameProject = existing != null && wf.projectId != null && wf.projectId.equals(existing.projectId);
    ValidationResult r = new ValidationResult();
    r.errors = validate(wf, p, sameProject ? existing.owner : user());
    return r;
  }

  public static class ParseRequest {
    public String expr;
  }

  @POST
  @Path("/parse-condition")
  @Consumes(MediaType.APPLICATION_JSON)
  public Map<String, Object> parseCondition(ParseRequest req) {
    if (req == null || req.expr == null) {
      throw error(Response.Status.BAD_REQUEST, "expr is required");
    }
    Map<String, Object> out = new LinkedHashMap<>();
    try {
      out.put("tree", ConditionParser.parse(req.expr));
    } catch (ConditionParser.ParseException e) {
      out.put("error", e.getMessage());
      out.put("position", e.position);
    }
    return out;
  }

  @POST
  @Path("/dry-run")
  @Consumes(MediaType.APPLICATION_JSON)
  public Response dryRunUnsaved(Workflow wf) {
    requireBody(wf);
    ProjectResources.Project p = stores().project(wf.projectId);
    requireCreator(p);
    List<WorkflowValidator.ValidationError> errors = validate(wf, p, user());
    if (!errors.isEmpty()) {
      return unprocessable(errors);
    }
    // Dry runs of unsaved drafts execute as the person drafting them.
    wf.owner = user();
    return Response.ok(runner().dryRun(wf)).build();
  }

  @POST
  @Path("/{id}/dry-run")
  public WorkflowRunner.DryRunResult dryRunSaved(@PathParam("id") String id) {
    Workflow wf = viewable(id);
    if (!WorkflowPermissions.canEdit(wf, user(), admin())) {
      throw error(Response.Status.FORBIDDEN, "Only the workflow owner or an admin can dry-run it");
    }
    return runner().dryRun(wf);
  }

  @POST
  @Path("/{id}/run")
  public WorkflowRun runNow(@PathParam("id") String id) {
    Workflow wf = manageable(id);
    if (wf.isRunning && !WorkflowRunner.isStale(wf, Instant.now(), stores().loadConfig())) {
      throw error(Response.Status.CONFLICT, "Workflow is already running");
    }
    if (wf.projectId != null && !WorkflowPermissions.isMember(stores().project(wf.projectId), wf.owner)) {
      throw error(Response.Status.CONFLICT, "The workflow runs as " + wf.owner + ", who is not a member of this project. An admin can open the workflow "
          + "and click Take ownership, or add " + wf.owner + " to the project");
    }
    if (WorkflowSchedule.isExpired(wf, Instant.now(), stores().loadConfig())) {
      throw error(Response.Status.CONFLICT, "Workflow has expired; renew it first");
    }
    return runner().runNow(wf);
  }

  @POST
  @Path("/{id}/renew")
  public Workflow renew(@PathParam("id") String id) {
    Workflow wf = manageable(id);
    WorkflowConfig cfg = stores().loadConfig();
    Instant now = Instant.now();
    wf.renewedAt = now.toString();
    wf.schedule.expiresAt = cfg.expirationEnabled ? now.plus(Duration.ofDays(cfg.expirationDays)).toString() : null;
    if ("expired".equals(wf.status)) {
      wf.status = "paused";
    }
    save(wf);
    return wf;
  }

  @POST
  @Path("/{id}/take-ownership")
  @RolesAllowed(DrillUserPrincipal.ADMIN_ROLE)
  public Workflow takeOwnership(@PathParam("id") String id) {
    Workflow wf = viewable(id);
    ProjectResources.Project p = stores().project(wf.projectId);
    if (p != null && !WorkflowPermissions.isMember(p, user())) {
      throw error(Response.Status.CONFLICT, "Add yourself to the project before taking ownership");
    }
    wf.owner = user();
    if ("owner_lost_access".equals(wf.status)) {
      wf.status = "paused";
    }
    save(wf);
    return wf;
  }

  public static class AssignRequest {
    public String projectId;
  }

  @POST
  @Path("/{id}/assign-project")
  @RolesAllowed(DrillUserPrincipal.ADMIN_ROLE)
  @Consumes(MediaType.APPLICATION_JSON)
  public Workflow assignProject(@PathParam("id") String id, AssignRequest req) {
    requireBody(req);
    Workflow wf = viewable(id);
    ProjectResources.Project p = stores().project(req.projectId);
    if (p == null) {
      throw error(Response.Status.NOT_FOUND, "Project not found");
    }
    if (!p.getSavedQueryIds().contains(wf.savedQueryId)) {
      throw error(Response.Status.CONFLICT, "Add the workflow's saved query to that project first");
    }
    wf.projectId = p.getId();
    save(wf);
    return wf;
  }

  // ---------------------------------------------------------------- runs, types, config

  @GET
  @Path("/{id}/runs")
  public List<WorkflowRun> runs(@PathParam("id") String id) {
    runReadable(id);
    List<WorkflowRun> out = new ArrayList<>();
    stores().runs().getAll().forEachRemaining(e -> {
      if (e.getValue() != null && id.equals(e.getValue().scheduleId)) {
        out.add(e.getValue());
      }
    });
    out.sort((a, b) -> b.executedAt.compareTo(a.executedAt));
    return out;
  }

  @GET
  @Path("/{id}/runs/{runId}")
  public WorkflowRun run(@PathParam("id") String id, @PathParam("runId") String runId) {
    runReadable(id);
    WorkflowRun r = stores().runs().get(runId);
    if (r == null || !id.equals(r.scheduleId)) {
      throw error(Response.Status.NOT_FOUND, "Run not found");
    }
    return r;
  }

  public static class ActionTypeInfo {
    public String id;
    public String displayName;
    public String description;
    public Object configSchema;
    public Map<String, String> outputs;
    public boolean hasSideEffects;
  }

  @GET
  @Path("/action-types")
  public List<ActionTypeInfo> actionTypes() {
    List<ActionTypeInfo> out = new ArrayList<>();
    for (ActionType t : runner().registry().all()) {
      ActionTypeInfo i = new ActionTypeInfo();
      i.id = t.id();
      i.displayName = t.displayName();
      i.description = t.description();
      i.configSchema = t.configSchema();
      i.outputs = t.outputs();
      i.hasSideEffects = t.hasSideEffects();
      out.add(i);
    }
    return out;
  }

  @GET
  @Path("/config")
  public WorkflowConfig getConfig() {
    return stores().loadConfig();
  }

  @PUT
  @Path("/config")
  @RolesAllowed(DrillUserPrincipal.ADMIN_ROLE)
  @Consumes(MediaType.APPLICATION_JSON)
  public WorkflowConfig putConfig(WorkflowConfig cfg) {
    cfg.legacyMigrated = stores().loadConfig().legacyMigrated;
    stores().saveConfig(cfg);
    return cfg;
  }

  // ---------------------------------------------------------------- observability

  @GET
  @Path("/observability/status")
  public Map<String, Object> observabilityStatus() throws Exception {
    String logDir = System.getenv("DRILL_LOG_DIR");
    StoragePluginConfig cfg = storageRegistry.getStoredConfig("dfs");
    FileSystemConfig fs = cfg instanceof FileSystemConfig ? (FileSystemConfig) cfg : null;
    Map<String, Object> out = new LinkedHashMap<>();
    if (admin()) {
      out.put("logDir", logDir);
    }
    out.put("logPresent", WorkflowObservability.logPresent(logDir));
    out.put("workspace", fs != null && fs.getWorkspaces().containsKey(WorkflowObservability.WORKSPACE));
    out.put("format", fs != null && fs.getFormats().containsKey(WorkflowObservability.FORMAT));
    out.put("ready", Boolean.TRUE.equals(out.get("workspace")) && Boolean.TRUE.equals(out.get("format")));
    return out;
  }

  /** Same steps as AiAnalyticsResources.setup, for the workflow run log. */
  @POST
  @Path("/observability/setup")
  @RolesAllowed(DrillUserPrincipal.ADMIN_ROLE)
  public Map<String, Object> observabilitySetup() throws Exception {
    String logDir = System.getenv("DRILL_LOG_DIR");
    if (logDir == null) {
      throw error(Response.Status.BAD_REQUEST, "DRILL_LOG_DIR is not set");
    }
    StoragePluginConfig cfg = storageRegistry.getStoredConfig("dfs");
    if (!(cfg instanceof FileSystemConfig)) {
      throw error(Response.Status.CONFLICT, "dfs storage plugin is not a file system plugin");
    }
    FileSystemConfig fs = (FileSystemConfig) cfg;
    if (!fs.getWorkspaces().containsKey(WorkflowObservability.WORKSPACE)) {
      FileSystemConfig copy = fs.copy();
      copy.getWorkspaces().put(WorkflowObservability.WORKSPACE,
          new WorkspaceConfig(logDir, false, WorkflowObservability.FORMAT, false));
      storageRegistry.put("dfs", copy);
    }
    FileSystemConfig fs2 = (FileSystemConfig) storageRegistry.getStoredConfig("dfs");
    if (!fs2.getFormats().containsKey(WorkflowObservability.FORMAT)) {
      FormatPluginConfig fmt = storageRegistry.mapper().readValue(
          "{\"type\":\"json\",\"extensions\":[\"log\"]}", FormatPluginConfig.class);
      storageRegistry.putFormatPlugin("dfs", WorkflowObservability.FORMAT, fmt);
    }
    return observabilityStatus();
  }

  @GET
  @Path("/observability/summary")
  public Map<String, Object> observabilitySummary(@QueryParam("projectId") String projectId) throws Exception {
    if (projectId == null) {
      if (!admin()) {
        throw error(Response.Status.BAD_REQUEST, "projectId is required");
      }
    } else {
      ProjectResources.Project p = stores().project(projectId);
      if (!admin() && !WorkflowPermissions.isMember(p, user())) {
        throw error(Response.Status.FORBIDDEN, "Run statistics are visible to project members only");
      }
    }
    String since = Instant.now().minus(Duration.ofDays(30)).toString();
    return sqlResult(Map.of("rows", WorkflowObservability.summarySql(projectId, since)));
  }

  @GET
  @Path("/{id}/health")
  public Map<String, Object> health(@PathParam("id") String id) throws Exception {
    runReadable(id);
    String since = Instant.now().minus(Duration.ofDays(30)).toString();
    Map<String, String> queries = new LinkedHashMap<>();
    queries.put("daily", WorkflowObservability.dailySql(id, since));
    queries.put("actions", WorkflowObservability.actionSql(id, since));
    return sqlResult(queries);
  }

  @GET
  @Path("/observability/running")
  @RolesAllowed(DrillUserPrincipal.ADMIN_ROLE)
  public List<Workflow> running() {
    List<Workflow> out = new ArrayList<>();
    Instant now = Instant.now();
    WorkflowConfig cfg = stores().loadConfig();
    stores().workflows().getAll().forEachRemaining(e -> {
      if (e.getValue() != null && e.getValue().isRunning && !WorkflowRunner.isStale(e.getValue(), now, cfg)) {
        out.add(e.getValue());
      }
    });
    return out;
  }

  /** Runs each named query; if the run-log workspace is missing, reports setupRequired instead. */
  private Map<String, Object> sqlResult(Map<String, String> queries) throws Exception {
    Map<String, Object> status = observabilityStatus();
    boolean ready = Boolean.TRUE.equals(status.get("ready")) && Boolean.TRUE.equals(status.get("logPresent"));
    Map<String, Object> out = new LinkedHashMap<>();
    out.put("setupRequired", !ready);
    for (Map.Entry<String, String> q : queries.entrySet()) {
      if (!ready) {
        out.put(q.getKey(), List.of());
        continue;
      }
      try {
        WebUserConnection conn = new WebUserConnection(webUserConnection.resources());
        QueryWrapper w = new QueryWrapper.RestQueryBuilder().query(q.getValue()).queryType("SQL")
            .rowLimit("100000").build();
        out.put(q.getKey(), new RestQueryRunner(w, workManager, conn).run().rows);
      } catch (Exception e) {
        logger.error("Workflow observability query '{}' failed", q.getKey(), e);
        out.put(q.getKey(), List.of());
        out.put("error", "Query failed: " + e.getMessage());
      }
    }
    return out;
  }

  // ---------------------------------------------------------------- helpers

  /** Members create workflows; an admin outside the project would own one that is disabled on its first tick. */
  private void requireCreator(ProjectResources.Project p) {
    if (!WorkflowPermissions.canCreate(p, user(), admin())) {
      throw error(Response.Status.FORBIDDEN, "You must be a member of the project");
    }
    if (p != null && !WorkflowPermissions.isMember(p, user())) {
      throw error(Response.Status.CONFLICT, "Add yourself to the project before creating a workflow");
    }
  }

  /** runAs is the user the workflow's queries run as: the creator on create, the existing owner on update. */
  private List<WorkflowValidator.ValidationError> validate(Workflow wf, ProjectResources.Project p, String runAs) {
    if (wf.schedule == null) {
      wf.schedule = new Workflow.Schedule();
    }
    if (wf.actions == null) {
      wf.actions = new ArrayList<>();
    }
    List<WorkflowValidator.ValidationError> errors = WorkflowValidator.validate(wf, runner().registry());
    boolean hasQuery = wf.savedQueryId != null && !wf.savedQueryId.isEmpty();
    if (hasQuery && (p == null || !p.getSavedQueryIds().contains(wf.savedQueryId))) {
      errors.add(new WorkflowValidator.ValidationError("savedQueryId", "Saved query is not in this project"));
    } else if (hasQuery) {
      // Its author can edit the SQL at any time, and the workflow runs it with the run-as user's access.
      SavedQueryResources.SavedQuery q = stores().savedQuery(wf.savedQueryId);
      String owner = runAs == null || runAs.isEmpty() ? DrillUserPrincipal.ANONYMOUS_USER : runAs;
      if (q != null && !owner.equals(q.getOwner())) {
        errors.add(new WorkflowValidator.ValidationError("savedQueryId",
            "The workflow runs as " + owner + ", so its saved query must be owned by " + owner));
      }
    }
    return errors;
  }

  private boolean canView(Workflow wf) {
    return WorkflowPermissions.canView(stores().project(wf.projectId), user(), admin());
  }

  private Workflow viewable(String id) {
    Workflow wf = stores().workflows().get(id);
    if (wf == null) {
      throw error(Response.Status.NOT_FOUND, "Workflow not found");
    }
    if (!canView(wf)) {
      throw error(Response.Status.FORBIDDEN, "Access denied");
    }
    return wf;
  }

  private Workflow manageable(String id) {
    Workflow wf = viewable(id);
    if (!WorkflowPermissions.canManage(wf, stores().project(wf.projectId), user(), admin())) {
      throw error(Response.Status.FORBIDDEN, "Access denied");
    }
    return wf;
  }

  /** Run output was fetched under the owner's identity, so it is shown to project members and admins only. */
  private void runReadable(String id) {
    Workflow wf = viewable(id);
    if (!admin() && !WorkflowPermissions.isMember(stores().project(wf.projectId), user())) {
      throw error(Response.Status.FORBIDDEN, "Run results are visible to project members only");
    }
  }

  /** Writes wf, taking run state from the store so a run finishing concurrently is not overwritten. */
  private void save(Workflow wf) {
    Workflow current = stores().workflows().get(wf.id);
    if (current != null) {
      wf.isRunning = current.isRunning;
      wf.runStartedAt = current.runStartedAt;
      wf.lastRunAt = current.lastRunAt;
      wf.lastStatus = current.lastStatus;
      wf.consecutiveFailures = current.consecutiveFailures;
    }
    stores().workflows().put(wf.id, wf);
  }

  private static void requireBody(Object body) {
    if (body == null) {
      throw error(Response.Status.BAD_REQUEST, "Request body is required");
    }
  }

  WorkflowRunner runner() {
    WorkflowRunner r = WorkflowRunner.getInstance();
    if (r == null) {
      throw error(Response.Status.SERVICE_UNAVAILABLE, "Workflow runner is not running");
    }
    return r;
  }

  WorkflowStores stores() {
    return runner().stores();
  }

  String user() {
    return principal.getName();
  }

  boolean admin() {
    return principal.isAdminUser();
  }

  static WebApplicationException error(Response.Status status, String message) {
    return new WebApplicationException(Response.status(status).entity(Map.of("error", message))
        .type(MediaType.APPLICATION_JSON).build());
  }

  private static Response unprocessable(List<WorkflowValidator.ValidationError> errors) {
    return Response.status(422).entity(Map.of("errors", errors)).build();
  }
}
