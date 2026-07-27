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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.drill.common.util.JacksonUtils;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.ClusterTest;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests for the MetadataResources REST API endpoints.
 * Verifies schema, table, column browsing, plugin listing, and function listing.
 */
public class TestMetadataResources extends ClusterTest {

  private static final int TIMEOUT = 30;
  private static int portNumber;

  private static final MediaType JSON = MediaType.parse("application/json");

  private static final OkHttpClient httpClient = new OkHttpClient.Builder()
      .connectTimeout(TIMEOUT, TimeUnit.SECONDS)
      .writeTimeout(TIMEOUT, TimeUnit.SECONDS)
      .readTimeout(TIMEOUT, TimeUnit.SECONDS)
      .build();

  private static final ObjectMapper mapper = JacksonUtils.createObjectMapper();

  private static String url(String path) {
    return String.format("http://localhost:%d%s", portNumber, path);
  }

  private static JsonNode post(String path, String jsonBody) throws Exception {
    RequestBody body = RequestBody.create(jsonBody == null ? "" : jsonBody, JSON);
    Request request = new Request.Builder().url(url(path)).post(body).build();
    try (Response response = httpClient.newCall(request).execute()) {
      assertTrue(response.code() >= 200 && response.code() < 300,
          "Expected 2xx from POST " + path + " but got " + response.code());
      return mapper.readTree(response.body().string());
    }
  }

  private static String get(String path) throws Exception {
    Request request = new Request.Builder().url(url(path)).build();
    try (Response response = httpClient.newCall(request).execute()) {
      assertEquals(200, response.code());
      return response.body().string();
    }
  }

  @BeforeClass
  public static void setup() throws Exception {
    ClusterFixtureBuilder builder = new ClusterFixtureBuilder(dirTestWatcher)
        .configProperty(ExecConstants.HTTP_ENABLE, true)
        .configProperty(ExecConstants.HTTP_PORT_HUNT, true);
    startCluster(builder);
    portNumber = cluster.drillbit().getWebServerPort();
  }

  @Test
  public void testGetSchemas() throws Exception {
    String url = String.format("http://localhost:%d/api/v1/metadata/schemas", portNumber);
    Request request = new Request.Builder().url(url).build();
    try (Response response = httpClient.newCall(request).execute()) {
      assertEquals(200, response.code());
      String body = response.body().string();
      JsonNode json = mapper.readTree(body);
      assertTrue(json.has("schemas"));
      JsonNode schemas = json.get("schemas");
      assertTrue(schemas.isArray());
      assertTrue(schemas.size() > 0, "Should return at least one schema");

      // Verify excluded plugins are not present
      for (JsonNode schema : schemas) {
        String name = schema.get("name").asText();
        assertFalse("cp".equals(name), "Should not include cp plugin");
        assertFalse("sys".equals(name), "Should not include sys plugin");
        assertFalse("information_schema".equals(name),
            "Should not include information_schema");
      }

      // dfs should be present since it's a default plugin
      boolean hasDfs = false;
      for (JsonNode schema : schemas) {
        if ("dfs".equals(schema.get("name").asText())) {
          hasDfs = true;
          break;
        }
      }
      assertTrue(hasDfs, "Should include dfs schema");
    }
  }

  @Test
  public void testGetPlugins() throws Exception {
    String url = String.format("http://localhost:%d/api/v1/metadata/plugins", portNumber);
    Request request = new Request.Builder().url(url).build();
    try (Response response = httpClient.newCall(request).execute()) {
      assertEquals(200, response.code());
      String body = response.body().string();
      JsonNode json = mapper.readTree(body);
      assertTrue(json.has("plugins"));
      JsonNode plugins = json.get("plugins");
      assertTrue(plugins.isArray());
      assertTrue(plugins.size() > 0, "Should return at least one plugin");

      // Verify plugin structure and exclusions
      for (JsonNode plugin : plugins) {
        assertNotNull(plugin.get("name"));
        assertNotNull(plugin.get("type"));
        assertTrue(plugin.get("enabled").asBoolean(),
            "All returned plugins should be enabled");

        String name = plugin.get("name").asText();
        assertFalse("cp".equals(name), "Should not include cp plugin");
        assertFalse("sys".equals(name), "Should not include sys plugin");
        assertFalse("information_schema".equals(name),
            "Should not include information_schema");
      }
    }
  }

  @Test
  public void testGetPluginSchemas() throws Exception {
    String url = String.format("http://localhost:%d/api/v1/metadata/plugins/dfs/schemas",
        portNumber);
    Request request = new Request.Builder().url(url).build();
    try (Response response = httpClient.newCall(request).execute()) {
      assertEquals(200, response.code());
      String body = response.body().string();
      JsonNode json = mapper.readTree(body);
      assertTrue(json.has("schemas"));
      JsonNode schemas = json.get("schemas");
      assertTrue(schemas.size() > 0, "dfs plugin should have at least one schema");

      // All schemas should reference the dfs plugin
      for (JsonNode schema : schemas) {
        assertEquals("dfs", schema.get("plugin").asText());
      }
    }
  }

  @Test
  public void testGetTablesForCp() throws Exception {
    String url = String.format("http://localhost:%d/api/v1/metadata/schemas/cp/tables",
        portNumber);
    Request request = new Request.Builder().url(url).build();
    try (Response response = httpClient.newCall(request).execute()) {
      assertEquals(200, response.code());
      String body = response.body().string();
      JsonNode json = mapper.readTree(body);
      assertTrue(json.has("tables"));
      assertTrue(json.get("tables").isArray());
    }
  }

  @Test
  public void testGetColumnsEndpoint() throws Exception {
    // The columns endpoint uses INFORMATION_SCHEMA.COLUMNS which requires tables
    // to have been previously queried. Verify the endpoint returns valid structure.
    String url = String.format(
        "http://localhost:%d/api/v1/metadata/schemas/cp/tables/employee.json/columns",
        portNumber);
    Request request = new Request.Builder().url(url).build();
    try (Response response = httpClient.newCall(request).execute()) {
      assertEquals(200, response.code());
      String body = response.body().string();
      JsonNode json = mapper.readTree(body);
      assertTrue(json.has("columns"));
      assertTrue(json.get("columns").isArray());

      // If columns are returned, verify their structure
      JsonNode columns = json.get("columns");
      for (JsonNode col : columns) {
        assertNotNull(col.get("name"), "Column name should not be null");
        assertNotNull(col.get("type"), "Column type should not be null");
      }
    }
  }

  @Test
  public void testGetFunctions() throws Exception {
    String url = String.format("http://localhost:%d/api/v1/metadata/functions", portNumber);
    Request request = new Request.Builder().url(url).build();
    try (Response response = httpClient.newCall(request).execute()) {
      assertEquals(200, response.code());
      String body = response.body().string();
      JsonNode json = mapper.readTree(body);
      assertTrue(json.has("functions"));
      JsonNode functions = json.get("functions");
      assertTrue(functions.isArray());
      assertTrue(functions.size() > 0, "Should return at least one function");

      // Should include common SQL functions
      boolean hasCount = false;
      boolean hasSum = false;
      for (JsonNode func : functions) {
        String name = func.asText();
        if ("count".equals(name)) {
          hasCount = true;
        }
        if ("sum".equals(name)) {
          hasSum = true;
        }
      }
      assertTrue(hasCount, "Should include COUNT function");
      assertTrue(hasSum, "Should include SUM function");
    }
  }

  @Test
  public void testGetFilesForDfsTmp() throws Exception {
    String url = String.format(
        "http://localhost:%d/api/v1/metadata/schemas/dfs.tmp/files",
        portNumber);
    Request request = new Request.Builder().url(url).build();
    try (Response response = httpClient.newCall(request).execute()) {
      assertEquals(200, response.code());
      String body = response.body().string();
      JsonNode json = mapper.readTree(body);
      assertTrue(json.has("files"));
      assertTrue(json.get("files").isArray());
    }
  }

  @Test
  public void testGetFilesFiltersUnrecognizedExtensions() throws Exception {
    // Create test files in the dfs.tmp workspace directory
    java.io.File tmpDir = dirTestWatcher.getDfsTestTmpDir();
    java.io.File jsonFile = new java.io.File(tmpDir, "test_recognized.json");
    java.io.File unrecognized = new java.io.File(tmpDir, "test_unrecognized.xyz");
    java.nio.file.Files.writeString(jsonFile.toPath(), "[{\"a\":1}]");
    java.nio.file.Files.writeString(unrecognized.toPath(), "unknown data");

    try {
      String url = String.format(
          "http://localhost:%d/api/v1/metadata/schemas/dfs.tmp/files",
          portNumber);
      Request request = new Request.Builder().url(url).build();
      try (Response response = httpClient.newCall(request).execute()) {
        assertEquals(200, response.code());
        String body = response.body().string();
        JsonNode json = mapper.readTree(body);
        JsonNode files = json.get("files");

        boolean hasJson = false;
        boolean hasXyz = false;
        for (JsonNode file : files) {
          String name = file.get("name").asText();
          if ("test_recognized.json".equals(name)) {
            hasJson = true;
          }
          if ("test_unrecognized.xyz".equals(name)) {
            hasXyz = true;
          }
        }
        assertTrue(hasJson, "Should include .json files (recognized extension)");
        assertFalse(hasXyz, "Should exclude .xyz files (unrecognized extension)");
      }
    } finally {
      jsonFile.delete();
      unrecognized.delete();
    }
  }

  @Test
  public void testHasRecognizedExtension() {
    Set<String> extensions = new HashSet<>(Arrays.asList("json", "csv", "parquet"));

    // Direct matches
    assertTrue(MetadataResources.hasRecognizedExtension("data.json", extensions));
    assertTrue(MetadataResources.hasRecognizedExtension("data.csv", extensions));
    assertTrue(MetadataResources.hasRecognizedExtension("data.parquet", extensions));
    assertTrue(MetadataResources.hasRecognizedExtension("DATA.JSON", extensions));

    // Compressed files
    assertTrue(MetadataResources.hasRecognizedExtension("data.csv.gz", extensions));
    assertTrue(MetadataResources.hasRecognizedExtension("data.json.bz2", extensions));
    assertTrue(MetadataResources.hasRecognizedExtension("data.parquet.snappy", extensions));
    assertTrue(MetadataResources.hasRecognizedExtension("data.csv.zip", extensions));

    // Unrecognized extensions
    assertFalse(MetadataResources.hasRecognizedExtension("data.xyz", extensions));
    assertFalse(MetadataResources.hasRecognizedExtension("readme.txt", extensions));
    assertFalse(MetadataResources.hasRecognizedExtension("image.png", extensions));

    // Edge cases
    assertTrue(MetadataResources.hasRecognizedExtension(null, extensions));
    assertTrue(MetadataResources.hasRecognizedExtension("data.json", new HashSet<>()));
  }

  @Test
  public void testPreviewTable() throws Exception {
    String url = String.format(
        "http://localhost:%d/api/v1/metadata/schemas/cp/tables/employee.json/preview?limit=5",
        portNumber);
    Request request = new Request.Builder().url(url).build();
    try (Response response = httpClient.newCall(request).execute()) {
      assertEquals(200, response.code());
      String body = response.body().string();
      JsonNode json = mapper.readTree(body);
      assertTrue(json.has("columns"));
      assertTrue(json.has("rows"));
      assertTrue(json.get("columns").size() > 0, "Preview should have columns");
      assertTrue(json.get("rows").size() <= 5, "Preview should respect the limit");
      assertTrue(json.get("rows").size() > 0, "Preview should return rows");
    }
  }

  /**
   * dfs.tmp is writable and file-based, so it must be offered. cp is file-based but not
   * writable, and sys is neither — both must be absent.
   */
  @Test
  public void testGetViewTargets() throws Exception {
    String url = String.format("http://localhost:%d/api/v1/metadata/view-targets", portNumber);
    Request request = new Request.Builder().url(url).build();
    try (Response response = httpClient.newCall(request).execute()) {
      assertEquals(200, response.code());
      JsonNode json = mapper.readTree(response.body().string());
      assertTrue(json.has("schemas"));

      Set<String> names = new HashSet<>();
      for (JsonNode schema : json.get("schemas")) {
        names.add(schema.get("name").asText());
      }
      assertTrue(names.contains("dfs.tmp"), "dfs.tmp is writable and must be a view target");
      assertFalse(names.contains("sys"), "sys is not file-based and must not be a view target");
      assertFalse(names.contains("cp"), "cp is not writable and must not be a view target");
    }
  }

  /**
   * Read-through: with a projectId that references dfs.tmp and a refreshed cache, the
   * tables endpoint is served from the cache. Proven by dropping the underlying view
   * after the scan: the live (no-projectId) call no longer sees it, but the cached
   * (projectId) call still returns it — which can only happen if the read-through fires.
   */
  @Test
  public void testTablesServedFromProjectCache() throws Exception {
    String viewName = "metadata_cache_view";
    client.runSqlSilently("CREATE OR REPLACE VIEW dfs.tmp." + viewName + " AS SELECT 1 AS n");
    try {
      // Create a project referencing dfs.tmp and populate its schema cache.
      JsonNode project = post("/api/v1/projects", "{\"name\":\"Metadata Cache Test\"}");
      String id = project.get("id").asText();
      post("/api/v1/projects/" + id + "/datasets",
          "{\"type\":\"schema\",\"schema\":\"dfs.tmp\",\"label\":\"tmp\"}");
      JsonNode refreshed = post("/api/v1/projects/" + id + "/schema-cache/refresh", null);
      assertTrue(refreshed.get("schemas").has("dfs.tmp"),
          "Refresh should have scanned dfs.tmp");

      // Cached read returns 200 with a tables array that includes the view.
      JsonNode cachedBefore = mapper.readTree(
          get("/api/v1/metadata/schemas/dfs.tmp/tables?projectId=" + id));
      assertTrue(cachedBefore.has("tables"), "Response should contain a tables array");
      assertTrue(cachedBefore.get("tables").isArray());
      assertTrue(containsTable(cachedBefore, viewName),
          "Cached tables should include the freshly scanned view");

      // Drop the view: the live path must stop reporting it.
      client.runSqlSilently("DROP VIEW dfs.tmp." + viewName);

      JsonNode liveAfter = mapper.readTree(
          get("/api/v1/metadata/schemas/dfs.tmp/tables"));
      assertFalse(containsTable(liveAfter, viewName),
          "Live query must not list the dropped view");

      // The cached (projectId) call still returns it — read-through, not a live query.
      JsonNode cachedAfter = mapper.readTree(
          get("/api/v1/metadata/schemas/dfs.tmp/tables?projectId=" + id));
      assertTrue(containsTable(cachedAfter, viewName),
          "Cached read-through should still return the view after it was dropped");
    } finally {
      client.runSqlSilently("DROP VIEW IF EXISTS dfs.tmp." + viewName);
    }
  }

  /**
   * A schema that is not referenced by the project must not be served from the cache:
   * the projectId call must be byte-identical to the plain live call (fall-through).
   */
  @Test
  public void testUnreferencedSchemaFallsThroughToLive() throws Exception {
    JsonNode project = post("/api/v1/projects", "{\"name\":\"Fallthrough Test\"}");
    String id = project.get("id").asText();
    post("/api/v1/projects/" + id + "/datasets",
        "{\"type\":\"schema\",\"schema\":\"dfs.tmp\",\"label\":\"tmp\"}");
    post("/api/v1/projects/" + id + "/schema-cache/refresh", null);

    // cp is not referenced by the project, so the projectId call must equal the live call.
    String withProject = get("/api/v1/metadata/schemas/cp/tables?projectId=" + id);
    String live = get("/api/v1/metadata/schemas/cp/tables");
    assertEquals(live, withProject,
        "Unreferenced schema must fall through to the identical live result");
  }

  private static boolean containsTable(JsonNode tablesResponse, String name) {
    for (JsonNode t : tablesResponse.get("tables")) {
      if (name.equals(t.get("name").asText())) {
        return true;
      }
    }
    return false;
  }
}
