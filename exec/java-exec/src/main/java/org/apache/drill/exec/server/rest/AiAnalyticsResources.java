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
import io.swagger.v3.oas.annotations.tags.Tag;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.server.rest.RestQueryRunner.QueryResult;
import org.apache.drill.exec.server.rest.ai.AiPricing;
import org.apache.drill.exec.server.rest.auth.DrillUserPrincipal;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.dfs.FileSystemConfig;
import org.apache.drill.exec.store.dfs.WorkspaceConfig;
import org.apache.drill.exec.store.sys.PersistentStoreProvider;
import org.apache.drill.exec.work.WorkManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.annotation.security.RolesAllowed;
import jakarta.inject.Inject;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Admin-only REST resource powering the AI analytics dashboard.
 * Reads ai-events.log via Drill SQL through the registered dfs.ai_logs workspace.
 */
@Path("/api/v1/ai/analytics")
@Tag(name = "AI Analytics", description = "Admin-only AI usage analytics")
@RolesAllowed(DrillUserPrincipal.ADMIN_ROLE)
public class AiAnalyticsResources {

  private static final Logger logger = LoggerFactory.getLogger(AiAnalyticsResources.class);
  private static final String DFS_PLUGIN_NAME = "dfs";
  private static final String AI_LOGS_WORKSPACE = "ai_logs";
  private static final String AI_LOG_FORMAT = "ai_log";
  /** Active log file written by the AI_EVENTS logback appender. */
  private static final String EVENT_LOG_FILE = "ai-events.log";

  /**
   * Glob that matches the active file plus rolled archives produced by the
   * SizeAndTimeBasedRollingPolicy in logback.xml ("ai-events-YYYY-MM-DD.N.log").
   */
  private static final String EVENT_LOG_GLOB = "ai-events*.log";

  @Inject
  WorkManager workManager;

  @Inject
  WebUserConnection webUserConnection;

  @Inject
  StoragePluginRegistry storageRegistry;

  @Inject
  PersistentStoreProvider storeProvider;

  // ==================== Response models ====================

  public static class StatusResponse {
    @JsonProperty public boolean logDirConfigured;
    @JsonProperty public String logDir;
    @JsonProperty public boolean logFileExists;
    @JsonProperty public boolean workspaceExists;
    @JsonProperty public boolean formatExists;
    @JsonProperty public boolean ready;
  }

  public static class SetupResponse {
    @JsonProperty public final boolean success;
    @JsonProperty public final String message;

    public SetupResponse(boolean success, String message) {
      this.success = success;
      this.message = message;
    }
  }

  public static class SummaryResponse {
    @JsonProperty public Map<String, Object> totals;
    @JsonProperty public List<Map<String, Object>> series;
    @JsonProperty public List<Map<String, Object>> byFeature;
    @JsonProperty public List<Map<String, Object>> byModel;
    @JsonProperty public List<Map<String, Object>> byUser;
    @JsonProperty public List<Map<String, Object>> latencyByModel;
    @JsonProperty public Map<String, AiPricing> pricing;
    /** Current-calendar-month cost forecast — independent of the selected date range. */
    @JsonProperty public ProjectionResponse projection;
    /**
     * True when the data source isn't ready (no event log yet, or the
     * dfs.ai_logs workspace/format isn't set up). Lets the dashboard tell
     * "not configured" apart from "configured but genuinely zero usage".
     */
    @JsonProperty public boolean notConfigured;
  }

  /**
   * Cost projection for the current calendar month based on usage so far.
   * mtdCostUsd is computed by joining month-to-date events to the pricing snapshot;
   * projection is a linear extrapolation: (mtd / daysElapsed) * daysInMonth.
   */
  public static class ProjectionResponse {
    @JsonProperty public double mtdCostUsd;
    @JsonProperty public double projectedMonthEndCostUsd;
    @JsonProperty public int daysElapsed;
    @JsonProperty public int daysInMonth;
    @JsonProperty public String monthStart;
  }

  public static class EventsResponse {
    @JsonProperty public List<Map<String, String>> rows;
    @JsonProperty public List<String> columns;
    @JsonProperty public int limit;
    @JsonProperty public int offset;
    /** See {@link SummaryResponse#notConfigured}. */
    @JsonProperty public boolean notConfigured;
  }

  // ==================== Endpoints ====================

  @GET
  @Path("/status")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Status of the AI analytics data source")
  public Response status() {
    StatusResponse status = new StatusResponse();
    String logDir = System.getenv("DRILL_LOG_DIR");
    status.logDirConfigured = logDir != null;
    status.logDir = logDir;
    if (logDir != null) {
      status.logFileExists = new File(logDir, EVENT_LOG_FILE).isFile();
    }
    try {
      StoragePluginConfig cfg = storageRegistry.getStoredConfig(DFS_PLUGIN_NAME);
      if (cfg instanceof FileSystemConfig) {
        FileSystemConfig fs = (FileSystemConfig) cfg;
        status.workspaceExists = fs.getWorkspaces().containsKey(AI_LOGS_WORKSPACE);
        status.formatExists = fs.getFormats().containsKey(AI_LOG_FORMAT);
      }
    } catch (Exception e) {
      logger.debug("Error checking AI analytics status", e);
    }
    status.ready = status.workspaceExists && status.formatExists && status.logDirConfigured;
    return Response.ok(status).build();
  }

  @POST
  @Path("/setup")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Register the dfs.ai_logs workspace + JSON format")
  public Response setup() {
    String logDir = System.getenv("DRILL_LOG_DIR");
    if (logDir == null) {
      return Response.status(Response.Status.BAD_REQUEST)
          .entity(new SetupResponse(false, "DRILL_LOG_DIR is not set")).build();
    }
    try {
      StoragePluginConfig cfg = storageRegistry.getStoredConfig(DFS_PLUGIN_NAME);
      if (!(cfg instanceof FileSystemConfig)) {
        return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
            .entity(new SetupResponse(false, "dfs plugin is not a file system plugin"))
            .build();
      }
      FileSystemConfig fs = (FileSystemConfig) cfg;
      boolean modified = false;

      if (!fs.getWorkspaces().containsKey(AI_LOGS_WORKSPACE)) {
        FileSystemConfig copy = fs.copy();
        copy.getWorkspaces().put(AI_LOGS_WORKSPACE,
            new WorkspaceConfig(logDir, false, AI_LOG_FORMAT, false));
        storageRegistry.put(DFS_PLUGIN_NAME, copy);
        modified = true;
      }

      StoragePluginConfig refreshed = storageRegistry.getStoredConfig(DFS_PLUGIN_NAME);
      if (refreshed instanceof FileSystemConfig) {
        FileSystemConfig fs2 = (FileSystemConfig) refreshed;
        if (!fs2.getFormats().containsKey(AI_LOG_FORMAT)) {
          // JSON format scoped to the .log extension so JSONL ai-events.log files match.
          String formatJson = "{\"type\":\"json\",\"extensions\":[\"log\"]}";
          FormatPluginConfig fmt = storageRegistry.mapper().readValue(formatJson, FormatPluginConfig.class);
          storageRegistry.putFormatPlugin(DFS_PLUGIN_NAME, AI_LOG_FORMAT, fmt);
          modified = true;
        }
      }

      String msg = modified
          ? "AI analytics workspace and format configured"
          : "AI analytics workspace and format were already configured";
      return Response.ok(new SetupResponse(true, msg)).build();
    } catch (Exception e) {
      logger.error("Error setting up AI analytics workspace", e);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity(new SetupResponse(false, "Failed: " + e.getMessage())).build();
    }
  }

  @GET
  @Path("/summary")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Aggregated AI usage stats for a date range")
  public Response summary(@QueryParam("from") String from,
                          @QueryParam("to") String to) {
    SummaryResponse resp = new SummaryResponse();
    resp.pricing = AiPricingResources.snapshot(workManager, storeProvider);

    if (!isReady()) {
      resp.totals = new LinkedHashMap<>();
      resp.series = new ArrayList<>();
      resp.byFeature = new ArrayList<>();
      resp.byModel = new ArrayList<>();
      resp.byUser = new ArrayList<>();
      resp.latencyByModel = new ArrayList<>();
      resp.notConfigured = true;
      return Response.ok(resp).build();
    }
    resp.notConfigured = false;

    String where = buildWhereClause(from, to);
    // Glob picks up the active file plus rolled archives in one scan.
    String table = "dfs.ai_logs.`" + EVENT_LOG_GLOB + "`";

    try {
      // Cancelled calls are user-aborts (browser closed the SSE stream); exclude them
      // from the success-rate denominator so they don't masquerade as upstream failures.
      String totalsSql = "SELECT "
          + "COUNT(*) AS totalCalls, "
          + "SUM(CASE WHEN success = true THEN 1 ELSE 0 END) AS successCount, "
          + "SUM(CASE WHEN success = false AND (cancelled IS NULL OR cancelled = false) "
          + "         THEN 1 ELSE 0 END) AS failureCount, "
          + "SUM(CASE WHEN cancelled = true THEN 1 ELSE 0 END) AS cancelledCount, "
          + "AVG(CAST(durationMs AS DOUBLE)) AS avgDurationMs, "
          + "SUM(CAST(promptTokens AS BIGINT)) AS totalPromptTokens, "
          + "SUM(CAST(responseTokens AS BIGINT)) AS totalResponseTokens, "
          + "SUM(CAST(totalTokens AS BIGINT)) AS totalTokens, "
          + "COUNT(DISTINCT `user`) AS uniqueUsers "
          + "FROM " + table + where;
      List<Map<String, String>> totalsRows = runQuery(totalsSql);
      resp.totals = totalsRows.isEmpty() ? new LinkedHashMap<>() : new LinkedHashMap<>(totalsRows.get(0));

      String seriesSql = "SELECT "
          + "SUBSTR(ts, 1, 10) AS day, "
          + "COUNT(*) AS calls, "
          + "SUM(CASE WHEN success = true THEN 1 ELSE 0 END) AS successes, "
          + "SUM(CAST(promptTokens AS BIGINT)) AS inputTokens, "
          + "SUM(CAST(responseTokens AS BIGINT)) AS outputTokens "
          + "FROM " + table + where
          + " GROUP BY SUBSTR(ts, 1, 10) ORDER BY day";
      resp.series = toListOfMaps(runQuery(seriesSql));

      String byFeatureSql = "SELECT feature, COUNT(*) AS calls, "
          + "SUM(CAST(totalTokens AS BIGINT)) AS tokens, "
          + "AVG(CAST(durationMs AS DOUBLE)) AS avgDurationMs "
          + "FROM " + table + where
          + " GROUP BY feature ORDER BY calls DESC";
      resp.byFeature = toListOfMaps(runQuery(byFeatureSql));

      String byModelSql = "SELECT provider, model, COUNT(*) AS calls, "
          + "SUM(CAST(promptTokens AS BIGINT)) AS inputTokens, "
          + "SUM(CAST(responseTokens AS BIGINT)) AS outputTokens, "
          + "SUM(CAST(totalTokens AS BIGINT)) AS tokens "
          + "FROM " + table + where
          + " GROUP BY provider, model ORDER BY calls DESC";
      resp.byModel = toListOfMaps(runQuery(byModelSql));

      String byUserSql = "SELECT `user`, COUNT(*) AS calls, "
          + "SUM(CAST(totalTokens AS BIGINT)) AS tokens "
          + "FROM " + table + where
          + " GROUP BY `user` ORDER BY calls DESC LIMIT 50";
      resp.byUser = toListOfMaps(runQuery(byUserSql));

      // Latency p50/p95 via PERCENTILE_CONT — Drill supports this aggregate.
      String latencySql = "SELECT provider, model, "
          + "AVG(CAST(durationMs AS DOUBLE)) AS avgDurationMs, "
          + "MIN(CAST(durationMs AS BIGINT)) AS minDurationMs, "
          + "MAX(CAST(durationMs AS BIGINT)) AS maxDurationMs "
          + "FROM " + table + where
          + " GROUP BY provider, model ORDER BY avgDurationMs DESC";
      resp.latencyByModel = toListOfMaps(runQuery(latencySql));

      resp.projection = computeProjection(table, resp.pricing);

      return Response.ok(resp).build();
    } catch (Exception e) {
      logger.error("Error running AI analytics summary", e);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity(new SetupResponse(false, "Query failed: " + e.getMessage())).build();
    }
  }

  @GET
  @Path("/events")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Paginated raw AI events for inspection")
  public Response events(@QueryParam("from") String from,
                         @QueryParam("to") String to,
                         @QueryParam("user") String user,
                         @QueryParam("feature") String feature,
                         @QueryParam("provider") String provider,
                         @QueryParam("model") String model,
                         @QueryParam("success") String success,
                         @QueryParam("limit") @DefaultValue("100") int limit,
                         @QueryParam("offset") @DefaultValue("0") int offset) {
    EventsResponse resp = new EventsResponse();
    resp.limit = clamp(limit, 1, 1000);
    resp.offset = Math.max(offset, 0);
    resp.rows = new ArrayList<>();
    resp.columns = new ArrayList<>();

    if (!isReady()) {
      resp.notConfigured = true;
      return Response.ok(resp).build();
    }
    resp.notConfigured = false;

    StringBuilder where = new StringBuilder(buildWhereClause(from, to));
    appendEqClause(where, "`user`", user);
    appendEqClause(where, "feature", feature);
    appendEqClause(where, "provider", provider);
    appendEqClause(where, "model", model);
    if ("true".equalsIgnoreCase(success)) {
      where.append(" AND success = true");
    } else if ("false".equalsIgnoreCase(success)) {
      where.append(" AND success = false AND (cancelled IS NULL OR cancelled = false)");
    } else if ("cancelled".equalsIgnoreCase(success)) {
      where.append(" AND cancelled = true");
    }

    String sql = "SELECT ts, `user`, feature, source, provider, model, "
        + "promptTokens, responseTokens, totalTokens, durationMs, success, cancelled, "
        + "errorClass, error, userMessage, prompt, response "
        + "FROM dfs.ai_logs.`" + EVENT_LOG_GLOB + "` "
        + where + " ORDER BY ts DESC LIMIT " + resp.limit + " OFFSET " + resp.offset;

    try {
      QueryResult qr = executeQuery(sql);
      resp.columns = new ArrayList<>(qr.columns);
      resp.rows = qr.rows;
    } catch (Exception e) {
      logger.error("Error fetching AI events", e);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity(new SetupResponse(false, "Query failed: " + e.getMessage())).build();
    }
    return Response.ok(resp).build();
  }

  // ==================== Helpers ====================

  /**
   * Computes month-to-date cost across all configured (provider, model) pairs and
   * extrapolates linearly to a month-end estimate. Returns zeros when no pricing
   * is configured — token counts alone aren't enough to forecast cost.
   */
  private ProjectionResponse computeProjection(String table, Map<String, AiPricing> pricing) {
    ProjectionResponse projection = new ProjectionResponse();
    java.time.YearMonth ym = java.time.YearMonth.now(java.time.ZoneOffset.UTC);
    java.time.Instant now = java.time.Instant.now();
    java.time.Instant monthStart = ym.atDay(1)
        .atStartOfDay(java.time.ZoneOffset.UTC).toInstant();
    java.time.Instant nextMonth = ym.plusMonths(1).atDay(1)
        .atStartOfDay(java.time.ZoneOffset.UTC).toInstant();

    projection.monthStart = monthStart.toString();
    projection.daysInMonth = ym.lengthOfMonth();
    long secondsPerDay = 86_400L;
    long elapsedSec = Math.max(1, now.getEpochSecond() - monthStart.getEpochSecond());
    projection.daysElapsed = (int) Math.min(projection.daysInMonth,
        Math.max(1, (elapsedSec + secondsPerDay - 1) / secondsPerDay));

    if (pricing == null || pricing.isEmpty()) {
      return projection;
    }

    String mtdSql = "SELECT provider, model, "
        + "SUM(CAST(promptTokens AS BIGINT)) AS inputTokens, "
        + "SUM(CAST(responseTokens AS BIGINT)) AS outputTokens "
        + "FROM " + table
        + " WHERE ts >= '" + monthStart.toString() + "'"
        + "   AND ts < '" + nextMonth.toString() + "'"
        + "   AND success = true "
        + " GROUP BY provider, model";
    try {
      double mtdCost = 0;
      for (Map<String, String> row : runQuery(mtdSql)) {
        AiPricing p = pricing.get(AiPricing.key(row.get("provider"), row.get("model")));
        if (p == null) {
          continue;
        }
        long in = parseLongOrZero(row.get("inputTokens"));
        long out = parseLongOrZero(row.get("outputTokens"));
        mtdCost += (in / 1_000_000.0) * p.getInputPricePerMTokens()
            + (out / 1_000_000.0) * p.getOutputPricePerMTokens();
      }
      projection.mtdCostUsd = round4(mtdCost);
      projection.projectedMonthEndCostUsd = round4(
          (mtdCost / projection.daysElapsed) * projection.daysInMonth);
    } catch (Exception e) {
      logger.warn("Failed to compute month-to-date projection", e);
    }
    return projection;
  }

  private static long parseLongOrZero(String s) {
    if (s == null || s.isEmpty()) {
      return 0;
    }
    try {
      return Long.parseLong(s);
    } catch (NumberFormatException ignored) {
      return 0;
    }
  }

  private static double round4(double v) {
    return Math.round(v * 10000.0) / 10000.0;
  }

  /**
   * True when the log directory holds any AI event log, including rolled
   * archives. Mirrors the ai-events*.log glob used when querying.
   */
  static boolean hasEventLog(String logDir) {
    if (logDir == null) {
      return false;
    }
    File dir = new File(logDir);
    if (!dir.isDirectory()) {
      return false;
    }
    File[] matches = dir.listFiles((d, name) ->
        name.startsWith("ai-events") && name.endsWith(".log"));
    return matches != null && matches.length > 0;
  }

  private boolean isReady() {
    String logDir = System.getenv("DRILL_LOG_DIR");
    if (!hasEventLog(logDir)) {
      return false;
    }
    try {
      StoragePluginConfig cfg = storageRegistry.getStoredConfig(DFS_PLUGIN_NAME);
      if (cfg instanceof FileSystemConfig) {
        FileSystemConfig fs = (FileSystemConfig) cfg;
        return fs.getWorkspaces().containsKey(AI_LOGS_WORKSPACE)
            && fs.getFormats().containsKey(AI_LOG_FORMAT);
      }
    } catch (Exception ignored) {
      // fall through
    }
    return false;
  }

  private static String buildWhereClause(String from, String to) {
    StringBuilder sb = new StringBuilder(" WHERE 1=1");
    if (isIsoLike(from)) {
      sb.append(" AND ts >= '").append(from).append("'");
    }
    if (isIsoLike(to)) {
      sb.append(" AND ts < '").append(to).append("'");
    }
    return sb.toString();
  }

  private static void appendEqClause(StringBuilder where, String column, String value) {
    if (value == null || value.isEmpty()) {
      return;
    }
    where.append(" AND ").append(column).append(" = '").append(escapeSql(value)).append("'");
  }

  /**
   * Permissive ISO-8601 sniff that rejects anything containing single quotes or
   * other characters that could break out of the SQL literal.
   */
  private static boolean isIsoLike(String s) {
    if (s == null || s.isEmpty() || s.length() > 32) {
      return false;
    }
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      boolean ok = Character.isLetterOrDigit(c) || c == '-' || c == ':' || c == '.' || c == '+';
      if (!ok) {
        return false;
      }
    }
    return true;
  }

  private static String escapeSql(String s) {
    return s.replace("'", "''");
  }

  private static int clamp(int v, int min, int max) {
    return Math.max(min, Math.min(max, v));
  }

  private List<Map<String, String>> runQuery(String sql) throws Exception {
    return executeQuery(sql).rows;
  }

  private static List<Map<String, Object>> toListOfMaps(List<Map<String, String>> rows) {
    List<Map<String, Object>> out = new ArrayList<>(rows.size());
    for (Map<String, String> row : rows) {
      out.add(new LinkedHashMap<>(coerce(row)));
    }
    return out;
  }

  private static Map<String, Object> coerce(Map<String, String> row) {
    Map<String, Object> m = new HashMap<>();
    for (Map.Entry<String, String> e : row.entrySet()) {
      String v = e.getValue();
      if (v == null) {
        m.put(e.getKey(), null);
        continue;
      }
      try {
        m.put(e.getKey(), Long.parseLong(v));
        continue;
      } catch (NumberFormatException ignored) {
        // not a long
      }
      try {
        m.put(e.getKey(), Double.parseDouble(v));
        continue;
      } catch (NumberFormatException ignored) {
        // not a double
      }
      m.put(e.getKey(), v);
    }
    return m;
  }

  private QueryResult executeQuery(String sql) throws Exception {
    WebUserConnection conn = new WebUserConnection(webUserConnection.webSessionResources);
    QueryWrapper wrapper = new QueryWrapper.RestQueryBuilder()
        .query(sql)
        .queryType("SQL")
        .rowLimit("100000")
        .build();
    return new RestQueryRunner(wrapper, workManager, conn).run();
  }
}
