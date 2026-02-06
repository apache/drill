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

import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.OkHttpClient;
import okhttp3.Request;
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

  private static final OkHttpClient httpClient = new OkHttpClient.Builder()
      .connectTimeout(TIMEOUT, TimeUnit.SECONDS)
      .writeTimeout(TIMEOUT, TimeUnit.SECONDS)
      .readTimeout(TIMEOUT, TimeUnit.SECONDS)
      .build();

  private static final ObjectMapper mapper = JacksonUtils.createObjectMapper();

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
}
