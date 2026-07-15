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
package org.apache.ranger.services.drill;

import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.service.RangerBaseService;
import org.apache.ranger.plugin.service.ResourceLookupContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Ranger service plugin for Apache Drill.
 *
 * <p>Deployed into Ranger Admin (NOT into the Drillbit). Provides:
 * <ul>
 *   <li>{@link #validateConfig()} - tests connectivity to the Drill cluster
 *       by calling the Drill REST API ({@code POST /query.json}).</li>
 *   <li>{@link #lookupResource(ResourceLookupContext)} - enumerates
 *       datasource / schema / table / column resources via
 *       {@code INFORMATION_SCHEMA} queries through the REST API, so the
 *       Ranger policy editor can auto-complete resource paths.</li>
 * </ul>
 *
 * <p>Uses Drill's REST API (default port 8047) instead of JDBC, so this
 * module can be compiled with JDK 8 and deployed into a JDK 8 Ranger Admin
 * without pulling in Drill's JDK 11+ dependencies.
 *
 * <p>Connection configuration (must match the service-def JSON
 * {@code serviceConfigOptions}):
 * <ul>
 *   <li>{@code username}             - Drill user name (required)</li>
 *   <li>{@code password}             - Drill user password (optional)</li>
 *   <li>{@code drill.connection.url} - Drill REST endpoint, e.g.
 *       {@code http://host:8047} (required).</li>
 * </ul>
 */
public class RangerServiceDrill extends RangerBaseService {

  private static final Logger LOG = LoggerFactory.getLogger(RangerServiceDrill.class);

  // Service config keys (must match ranger-servicedef-drill.json)
  private static final String CONFIG_USERNAME = "username";
  private static final String CONFIG_PASSWORD = "password";
  private static final String CONFIG_DRILL_URL = "drill.connection.url";

  // Resource names (must match ranger-servicedef-drill.json, lowercase per Ranger naming rules)
  private static final String RESOURCE_DATASOURCE = "datasource";
  private static final String RESOURCE_SCHEMA = "schema";
  private static final String RESOURCE_TABLE = "table";
  private static final String RESOURCE_COLUMN = "column";

  // HTTP connect / read timeout (milliseconds)
  private static final int CONNECT_TIMEOUT_MS = 10_000;
  private static final int READ_TIMEOUT_MS = 30_000;

  // SQL templates for resource lookup. Each %s is filled via String.format
  // with the corresponding escaped resource value. TABLES and COLUMNS are
  // reserved keywords in Drill SQL and must be backtick-quoted.
  private static final String SQL_VALIDATE_CONNECTION = "SELECT 1";

  private static final String SQL_LOOKUP_DATASOURCE =
      "SELECT DISTINCT SPLIT_PART(SCHEMA_NAME, '.', 1) AS DATASOURCE "
          + "FROM INFORMATION_SCHEMA.SCHEMATA "
          + "WHERE SCHEMA_NAME LIKE '%.%' "
          + "ORDER BY 1";

  // %s = datasource (e.g. "mysql")
  private static final String SQL_LOOKUP_SCHEMA =
      "SELECT SPLIT_PART(SCHEMA_NAME, '.', 2) AS SCHEMA "
          + "FROM INFORMATION_SCHEMA.SCHEMATA "
          + "WHERE SCHEMA_NAME LIKE '%s.%%' "
          + "ORDER BY 1";

  // %s = full table schema (e.g. "mysql.shf")
  private static final String SQL_LOOKUP_TABLE =
      "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.`TABLES` "
          + "WHERE TABLE_SCHEMA = '%s' "
          + "ORDER BY 1";

  // %1$s = full table schema, %2$s = table name
  private static final String SQL_LOOKUP_COLUMN =
      "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.`COLUMNS` "
          + "WHERE TABLE_SCHEMA = '%s' "
          + "AND TABLE_NAME = '%s' "
          + "ORDER BY 1";

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override
  public void init(RangerServiceDef serviceDef, RangerService service) {
    super.init(serviceDef, service);
    LOG.debug("RangerServiceDrill initialized for service={}",
        service != null ? service.getName() : "null");
  }

  /**
   * Validates the service configuration by testing connectivity to Drill.
   *
   * @return a map with {@code status} = {@code SUCCESS} or {@code FAILURE}
   *         and a human-readable {@code message}.
   */
  @Override
  public Map<String, Object> validateConfig() throws Exception {
    Map<String, Object> result = new HashMap<>();

    String username = getConfig(CONFIG_USERNAME);
    if (isBlank(username)) {
      return failure(result, "Drill user name is required");
    }
    String baseUrl = getConfig(CONFIG_DRILL_URL);
    if (isBlank(baseUrl)) {
      return failure(result, "Drill connection URL is required");
    }
    String password = getConfig(CONFIG_PASSWORD);

    String normalizedUrl;
    try {
      normalizedUrl = buildBaseUrl(baseUrl);
    } catch (IllegalArgumentException e) {
      return failure(result, "Invalid drill.connection.url: " + e.getMessage());
    }

    LOG.info("Validating Drill service connection to {}", normalizedUrl);
    try {
      String response = executeQuery(normalizedUrl, username, password, SQL_VALIDATE_CONNECTION);
      // A successful query returns JSON with a "rows" array
      JsonNode root = MAPPER.readTree(response);
      if (root != null && root.has("rows") && root.get("rows").isArray()) {
        result.put("status", "SUCCESS");
        result.put("message", "Connection test succeeded");
        LOG.info("Drill connection validation succeeded for {}", normalizedUrl);
      } else {
        return failure(result, "Unexpected response from Drill: " + response);
      }
    } catch (Exception e) {
      LOG.error("Drill connection validation failed for url={}", normalizedUrl, e);
      return failure(result, "Connection test failed: " + e.getMessage());
    }
    return result;
  }

  /**
   * Lists Drill resources for the Ranger policy editor autocomplete.
   *
   * <p>Supported resource levels (must match the service-def JSON):
   * <ul>
   *   <li>{@code datasource} - distinct storage plugins from
   *       {@code INFORMATION_SCHEMA.SCHEMATA}</li>
   *   <li>{@code schema} - schema names filtered by the selected datasource</li>
   *   <li>{@code table} - table names filtered by datasource + schema</li>
   *   <li>{@code column} - column names filtered by datasource + schema + table</li>
   * </ul>
   *
   * @param context carries the requested resource name and the already-selected
   *                parent resources in {@link ResourceLookupContext#getResources()}
   * @return a list of matching resource names (never {@code null})
   */
  @Override
  public List<String> lookupResource(ResourceLookupContext context) throws Exception {
    if (context == null) {
      return Collections.emptyList();
    }
    String resourceName = context.getResourceName();
    // getResources() returns Map<String, List<String>> in Ranger 2.8.0:
    // each parent resource name maps to a list of selected values.
    Map<String, List<String>> hints = context.getResources() != null
        ? context.getResources() : Collections.emptyMap();

    if (isBlank(resourceName)) {
      return Collections.emptyList();
    }

    String username = getConfig(CONFIG_USERNAME);
    String password = getConfig(CONFIG_PASSWORD);
    String baseUrl = buildBaseUrl(getConfig(CONFIG_DRILL_URL));

    LOG.debug("lookupResource: resource={}, hints={}", resourceName, hints);

    try {
      switch (resourceName) {
        case RESOURCE_DATASOURCE:
          // Drill's INFORMATION_SCHEMA.SCHEMATA has no STORAGE_PLUGIN column.
          // The datasource (storage plugin name) is the first segment of
          // SCHEMA_NAME (e.g. "mysql.shf" -> "mysql"). Use SUBSTR_INDEX to
          // extract it, then DISTINCT to deduplicate.
          return extractFirstColumnValues(executeQuery(baseUrl, username, password,
              SQL_LOOKUP_DATASOURCE));
        case RESOURCE_SCHEMA: {
          String datasource = firstHint(hints, RESOURCE_DATASOURCE);
          if (isBlank(datasource)) {
            return Collections.emptyList();
          }
          // For a given datasource, list schema names by stripping the
          // "datasource." prefix from SCHEMA_NAME (e.g. "mysql.shf" -> "shf").
          // Schemas without a dot (e.g. plain "mysql") are filtered out.
          String sql = String.format(SQL_LOOKUP_SCHEMA, escapeSql(datasource));
          return extractFirstColumnValues(executeQuery(baseUrl, username, password, sql));
        }
        case RESOURCE_TABLE: {
          String datasource = firstHint(hints, RESOURCE_DATASOURCE);
          String schema = firstHint(hints, RESOURCE_SCHEMA);
          if (isBlank(datasource) || isBlank(schema)) {
            return Collections.emptyList();
          }
          // TABLE_SCHEMA in INFORMATION_SCHEMA.`TABLES` uses the full qualified
          // form "datasource.schema" (e.g. "mysql.shf"), so concatenate the
          // selected datasource and schema before filtering.
          // NOTE: TABLES is a reserved keyword in Drill SQL and must be
          // backtick-quoted; without quotes the parser rejects the query.
          String tableSchema = escapeSql(datasource) + "." + escapeSql(schema);
          String sql = String.format(SQL_LOOKUP_TABLE, tableSchema);
          return extractFirstColumnValues(executeQuery(baseUrl, username, password, sql));
        }
        case RESOURCE_COLUMN: {
          String datasource = firstHint(hints, RESOURCE_DATASOURCE);
          String schema = firstHint(hints, RESOURCE_SCHEMA);
          String table = firstHint(hints, RESOURCE_TABLE);
          if (isBlank(datasource) || isBlank(schema) || isBlank(table)) {
            return Collections.emptyList();
          }
          // COLUMNS is also a reserved keyword in Drill SQL — backtick-quote it.
          String tableSchema = escapeSql(datasource) + "." + escapeSql(schema);
          String sql = String.format(SQL_LOOKUP_COLUMN, tableSchema, escapeSql(table));
          return extractFirstColumnValues(executeQuery(baseUrl, username, password, sql));
        }
        default:
          LOG.warn("Unknown resource name: {}", resourceName);
          return Collections.emptyList();
      }
    } catch (Exception e) {
      LOG.error("lookupResource failed for resource={}, hints={}", resourceName, hints, e);
      throw e;
    }
  }

  @Override
  public List<RangerPolicy> getDefaultRangerPolicies() throws Exception {
    return super.getDefaultRangerPolicies();
  }

  // ========================================================================
  // REST API helpers
  // ========================================================================

  /**
   * Normalizes the user-supplied {@code drill.connection.url} to a base URL
   * like {@code http://host:8047}. Trims trailing slashes.
   */
  static String buildBaseUrl(String configuredUrl) {
    if (isBlank(configuredUrl)) {
      throw new IllegalArgumentException("drill.connection.url is empty");
    }
    String url = configuredUrl.trim();
    // Strip trailing slashes
    while (url.endsWith("/")) {
      url = url.substring(0, url.length() - 1);
    }
    return url;
  }

  /**
   * Executes a SQL query via the Drill REST API.
   * <p>POSTs to {@code <baseUrl>/query.json} with a JSON body
   * {@code {"queryType":"SQL","query":"<sql>"}} using HTTP Basic auth.
   *
   * @param baseUrl  Drill REST endpoint, e.g. {@code http://host:8047}
   * @param username Drill user name
   * @param password Drill password (may be null or empty)
   * @param sql      the SQL statement to execute
   * @return the raw JSON response string from Drill
   */
  private static String executeQuery(String baseUrl, String username, String password, String sql)
      throws Exception {
    String endpoint = baseUrl + "/query.json";
    Map<String, String> payload = new HashMap<>();
    payload.put("queryType", "SQL");
    payload.put("query", sql);
    String body = MAPPER.writeValueAsString(payload);

    HttpURLConnection conn = null;
    try {
      URL url = new URL(endpoint);
      conn = (HttpURLConnection) url.openConnection();
      conn.setRequestMethod("POST");
      conn.setConnectTimeout(CONNECT_TIMEOUT_MS);
      conn.setReadTimeout(READ_TIMEOUT_MS);
      conn.setDoOutput(true);
      conn.setRequestProperty("Content-Type", "application/json");
      conn.setRequestProperty("Accept", "application/json");

      // HTTP Basic auth
      String creds = username + ":" + (password == null ? "" : password);
      String encoded = Base64.getEncoder().encodeToString(
          creds.getBytes(StandardCharsets.UTF_8));
      conn.setRequestProperty("Authorization", "Basic " + encoded);

      // Write request body
      try (OutputStream os = conn.getOutputStream()) {
        os.write(body.getBytes(StandardCharsets.UTF_8));
      }

      int code = conn.getResponseCode();
      InputStream is = (code >= 200 && code < 300) ? conn.getInputStream() : conn.getErrorStream();
      String response = readAll(is);

      if (code < 200 || code >= 300) {
        throw new RuntimeException("Drill REST API returned HTTP " + code
            + ": " + truncate(response, 500));
      }
      return response;
    } finally {
      if (conn != null) {
        conn.disconnect();
      }
    }
  }

  /**
   * Extracts the first column value from each row of a Drill REST API response.
   * <p>Drill returns JSON like:
   * <pre>{@code
   * {"columns":["COL1"],"rows":[{"COL1":"value1"},{"COL1":"value2"}]}
   * }</pre>
   *
   * <p>This method is used for ALL resource levels (datasource/schema/table/column)
   * because every lookup SQL selects exactly one column. The "column" in the
   * method name refers to the JSON result column, not the Ranger column resource.
   *
   * @param jsonResponse the raw JSON response from {@code POST /query.json}
   * @return list of string values from the first column
   */
  private static List<String> extractFirstColumnValues(String jsonResponse) throws Exception {
    JsonNode root = MAPPER.readTree(jsonResponse);
    if (root == null || !root.has("rows") || !root.get("rows").isArray()) {
      return Collections.emptyList();
    }

    // Determine the first column name from the "columns" array
    String firstColumn = null;
    if (root.has("columns") && root.get("columns").isArray()) {
      JsonNode cols = root.get("columns");
      if (cols.size() > 0) {
        firstColumn = cols.get(0).asText();
      }
    }

    List<String> result = new ArrayList<>();
    for (JsonNode row : root.get("rows")) {
      if (firstColumn != null && row.has(firstColumn)) {
        String value = row.get(firstColumn).asText();
        if (value != null && !value.trim().isEmpty()) {
          result.add(value.trim());
        }
      } else if (row.isObject() && row.size() > 0) {
        // Fallback: use the first field in the row
        String value = row.elements().next().asText();
        if (value != null && !value.trim().isEmpty()) {
          result.add(value.trim());
        }
      }
    }
    return result;
  }

  private static String readAll(InputStream is) throws Exception {
    if (is == null) {
      return "";
    }
    StringBuilder sb = new StringBuilder();
    try (BufferedReader reader = new BufferedReader(
        new InputStreamReader(is, StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        sb.append(line).append('\n');
      }
    }
    return sb.toString();
  }

  private static String truncate(String s, int maxLen) {
    if (s == null) {
      return "";
    }
    return s.length() <= maxLen ? s : s.substring(0, maxLen) + "...";
  }

  // ========================================================================
  // Common helpers
  // ========================================================================

  private String getConfig(String key) {
    if (configs == null) {
      return null;
    }
    Object value = configs.get(key);
    return value == null ? null : value.toString().trim();
  }

  private static String firstHint(Map<String, List<String>> hints, String resourceName) {
    if (hints == null) {
      return null;
    }
    List<String> values = hints.get(resourceName);
    if (values == null || values.isEmpty()) {
      return null;
    }
    return values.get(0);
  }

  private static String escapeSql(String value) {
    return value == null ? "" : value.replace("'", "''");
  }

  private static boolean isBlank(String s) {
    return s == null || s.trim().isEmpty();
  }

  private static Map<String, Object> failure(Map<String, Object> result, String message) {
    result.put("status", "FAILURE");
    result.put("message", message);
    return result;
  }
}
