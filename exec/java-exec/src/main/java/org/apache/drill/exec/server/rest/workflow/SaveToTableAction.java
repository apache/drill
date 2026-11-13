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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/** Persists the full query result with CTAS / INSERT. */
public class SaveToTableAction implements ActionType {
  private static final String LOCATION_PATTERN = "^[A-Za-z0-9_]+(\\.[A-Za-z0-9_]+)?$";
  private static final Pattern LOCATION = Pattern.compile(LOCATION_PATTERN);
  private static final Pattern WORKFLOW_ID = Pattern.compile("[A-Za-z0-9_-]+");
  private static final Set<String> FORMATS = Set.of("parquet", "json", "csv");
  private static final Set<String> MODES = Set.of("overwrite", "append", "newTablePerRun");

  private static final String SCHEMA = "{\"type\":\"object\",\"properties\":{"
      + "\"location\":{\"type\":\"string\",\"title\":\"Workspace\",\"default\":\"dfs.tmp\","
      + "\"pattern\":\"" + LOCATION_PATTERN.replace("\\", "\\\\") + "\"},"
      + "\"format\":{\"type\":\"string\",\"title\":\"Format\",\"default\":\"parquet\",\"enum\":[\"parquet\",\"json\",\"csv\"]},"
      + "\"mode\":{\"type\":\"string\",\"title\":\"Mode\",\"default\":\"overwrite\","
      + "\"enum\":[\"overwrite\",\"append\",\"newTablePerRun\"]}}}";

  @Override public String id() { return "save_to_table"; }

  @Override public String displayName() { return "Save to table"; }

  @Override public String description() { return "Write the full query result to a table (CTAS or INSERT)."; }

  @Override public ObjectNode configSchema() { return RecordAlertAction.parse(SCHEMA); }

  @Override public Map<String, String> outputs() { return Map.of("path", "string"); }

  @Override public boolean hasSideEffects() { return true; }

  /** Lets the inner query timeout and its cancel grace finish before the outer action timer fires. */
  static final int MARGIN_SECONDS = 5;

  /** The CTAS runs the whole query, so it gets the workflow's query timeout (plus the cancel grace). */
  @Override
  public long timeoutMs(Workflow wf, long configuredMs) {
    return Math.max(configuredMs, (wf.schedule.timeoutSeconds + WorkflowRunner.CANCEL_GRACE_SECONDS + MARGIN_SECONDS) * 1000L);
  }

  @Override
  public Map<String, Object> execute(ActionContext ctx, JsonNode config) throws Exception {
    Workflow wf = ctx.workflow();
    long now = System.currentTimeMillis();
    List<String> sql = statements(wf.id, ctx.sql(), config, now);
    ctx.services().executeStatements(sql, wf.owner, wf.schedule.timeoutSeconds);
    return Map.of("path", targetPath(wf.id, config, now));
  }

  /** The table this run writes to. Callers must have validated the config via {@link #statements}. */
  private static String targetPath(String workflowId, JsonNode config, long nowMillis) {
    String location = config.path("location").asText("dfs.tmp");
    String mode = config.path("mode").asText("overwrite");
    return tablePath(location, workflowId, "newTablePerRun".equals(mode) ? "run_" + nowMillis : null);
  }

  /** Builds the SQL; validates every interpolated value itself because legacy configs are migrated unvalidated. */
  static List<String> statements(String workflowId, String sql, JsonNode config, long nowMillis) {
    String location = config.path("location").asText("dfs.tmp");
    String format = config.path("format").asText("parquet");
    String mode = config.path("mode").asText("overwrite");
    if (workflowId == null || !WORKFLOW_ID.matcher(workflowId).matches()) {
      throw new IllegalArgumentException("Invalid workflow id");
    }
    if (!LOCATION.matcher(location).matches()) {
      throw new IllegalArgumentException("Invalid save_to_table location: " + location);
    }
    if (!FORMATS.contains(format)) {
      throw new IllegalArgumentException("Invalid save_to_table format: " + format);
    }
    if (!MODES.contains(mode)) {
      throw new IllegalArgumentException("Invalid save_to_table mode: " + mode);
    }
    // Newline keeps a trailing line comment in the query from swallowing the closing paren.
    String query = "(" + stripSemicolon(sql) + "\n)";
    String path = targetPath(workflowId, config, nowMillis);
    List<String> out = new ArrayList<>();
    // Same session as the CTAS, so the format option actually applies.
    out.add("ALTER SESSION SET `store.format` = '" + format + "'");
    switch (mode) {
      case "append":
        out.add("INSERT INTO " + path + " SELECT * FROM " + query);
        break;
      case "newTablePerRun":
        out.add("CREATE TABLE " + path + " AS SELECT * FROM " + query);
        break;
      default:
        out.add("DROP TABLE IF EXISTS " + path);
        out.add("CREATE TABLE " + path + " AS SELECT * FROM " + query);
        break;
    }
    return out;
  }

  /**
   * Keeps the legacy `schedules` folder so migrated workflows keep writing to the same tables.
   * Drill rejects nested backtick segments, so the folder and table share one quoted path.
   */
  static String tablePath(String location, String workflowId, String run) {
    String table = "schedules/" + workflowId.replace('-', '_') + (run == null ? "" : "/" + run);
    return location + ".`" + table + "`";
  }

  public static String stripSemicolon(String sql) {
    String s = sql.trim();
    while (s.endsWith(";")) {
      s = s.substring(0, s.length() - 1).trim();
    }
    return s;
  }
}
