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
import static org.junit.jupiter.api.Assertions.assertTrue;

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
 * Integration tests for the project schema-cache REST endpoints
 * ({@code POST /{id}/schema-cache/refresh} and {@code GET /{id}/schema-cache}).
 * These exercise the scan-on-refresh path wired into {@link ProjectResources}
 * and verify the cached schema metadata is persisted and returned.
 */
public class TestProjectSchemaCacheEndpoints extends ClusterTest {

  private static final int TIMEOUT = 30;
  private static final MediaType JSON = MediaType.parse("application/json");
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

  private String url(String path) {
    return String.format("http://localhost:%d%s", portNumber, path);
  }

  private JsonNode post(String path, String jsonBody) throws Exception {
    RequestBody body = RequestBody.create(jsonBody == null ? "" : jsonBody, JSON);
    Request request = new Request.Builder().url(url(path)).post(body).build();
    try (Response response = httpClient.newCall(request).execute()) {
      assertTrue(response.code() >= 200 && response.code() < 300,
          "Expected 2xx from POST " + path + " but got " + response.code());
      return mapper.readTree(response.body().string());
    }
  }

  @Test
  public void testRefreshAndGetSchemaCache() throws Exception {
    // 1. Create a project.
    JsonNode project = post("/api/v1/projects",
        "{\"name\":\"Schema Cache Test\"}");
    String id = project.get("id").asText();
    assertTrue(id != null && !id.isEmpty(), "Created project should have an id");

    // 2. Add a dataset referencing a real, scannable schema (dfs.tmp).
    post("/api/v1/projects/" + id + "/datasets",
        "{\"type\":\"schema\",\"schema\":\"dfs.tmp\",\"label\":\"tmp\"}");

    // 3. Refresh the schema cache; this triggers a live INFORMATION_SCHEMA scan.
    JsonNode refreshed = post("/api/v1/projects/" + id + "/schema-cache/refresh", null);
    assertTrue(refreshed.has("schemas"), "Refresh response should contain a schemas object");
    assertTrue(refreshed.get("schemas").has("dfs.tmp"),
        "Refresh response should have scanned the dfs.tmp schema");

    // 4. Read the cache back and assert the scan was persisted.
    Request getReq = new Request.Builder()
        .url(url("/api/v1/projects/" + id + "/schema-cache")).build();
    try (Response response = httpClient.newCall(getReq).execute()) {
      assertEquals(200, response.code());
      JsonNode json = mapper.readTree(response.body().string());
      assertTrue(json.has("schemas"), "Cache entry should contain a schemas object");
      JsonNode schemas = json.get("schemas");
      assertTrue(schemas.has("dfs.tmp"),
          "Cached schemas should contain the added dfs.tmp key");
      JsonNode dfsTmp = schemas.get("dfs.tmp");
      assertTrue(dfsTmp.get("scannedAt").asLong() > 0,
          "Cached schema dfs.tmp should have a scannedAt timestamp > 0");
    }
  }
}
