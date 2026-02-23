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
 * Integration tests for the {@link TranspileResources} REST API endpoints.
 * Uses an embedded Drillbit with HTTP enabled and OkHttp for request execution.
 * Java sqlglot is always available, so no tests are conditionally skipped.
 */
public class TestTranspileResources extends ClusterTest {

  private static final int TIMEOUT = 30;
  private static final MediaType JSON_MEDIA_TYPE = MediaType.parse("application/json");
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
  public void testStatusEndpoint() throws Exception {
    String url = String.format("http://localhost:%d/api/v1/transpile/status", portNumber);
    Request request = new Request.Builder().url(url).build();
    try (Response response = httpClient.newCall(request).execute()) {
      assertEquals(200, response.code());
      String body = response.body().string();
      JsonNode json = mapper.readTree(body);
      assertTrue(json.has("available"), "Response should contain 'available' field");
      assertTrue(json.get("available").asBoolean(),
          "Transpiler should always be available with Java sqlglot");
    }
  }

  @Test
  public void testTranspileEndpoint() throws Exception {
    String url = String.format("http://localhost:%d/api/v1/transpile", portNumber);
    String requestJson = mapper.writeValueAsString(
        new TranspileResources.TranspileRequest(
            "SELECT CAST(x AS TEXT) FROM t", "mysql", "drill", null));
    Request request = new Request.Builder()
        .url(url)
        .post(RequestBody.create(requestJson, JSON_MEDIA_TYPE))
        .build();
    try (Response response = httpClient.newCall(request).execute()) {
      assertEquals(200, response.code());
      String body = response.body().string();
      JsonNode json = mapper.readTree(body);
      assertTrue(json.has("sql"), "Response should contain 'sql' field");
      assertTrue(json.get("success").asBoolean(), "Transpilation should succeed");
      assertTrue(json.get("sql").asText().toUpperCase().contains("CAST"),
          "MySQL CAST should be preserved in transpilation");
    }
  }

  @Test
  public void testTranspileWithDefaults() throws Exception {
    String url = String.format("http://localhost:%d/api/v1/transpile", portNumber);
    String requestJson = "{\"sql\": \"SELECT CAST(x AS TEXT) FROM t\"}";
    Request request = new Request.Builder()
        .url(url)
        .post(RequestBody.create(requestJson, JSON_MEDIA_TYPE))
        .build();
    try (Response response = httpClient.newCall(request).execute()) {
      assertEquals(200, response.code());
      String body = response.body().string();
      JsonNode json = mapper.readTree(body);
      assertTrue(json.get("success").asBoolean(), "Transpilation should succeed");
      assertTrue(json.get("sql").asText().toUpperCase().contains("CAST"),
          "Default mysql->drill should preserve CAST");
    }
  }

  @Test
  public void testTranspileEmptySql() throws Exception {
    String url = String.format("http://localhost:%d/api/v1/transpile", portNumber);
    String requestJson = mapper.writeValueAsString(
        new TranspileResources.TranspileRequest("", "mysql", "drill", null));
    Request request = new Request.Builder()
        .url(url)
        .post(RequestBody.create(requestJson, JSON_MEDIA_TYPE))
        .build();
    try (Response response = httpClient.newCall(request).execute()) {
      assertEquals(200, response.code());
      String body = response.body().string();
      JsonNode json = mapper.readTree(body);
      assertEquals("", json.get("sql").asText(), "Empty SQL should return empty string");
      assertTrue(json.get("success").asBoolean(), "Empty SQL should still succeed");
    }
  }

  @Test
  public void testTranspilePassthrough() throws Exception {
    String url = String.format("http://localhost:%d/api/v1/transpile", portNumber);
    String drillSql = "SELECT * FROM dfs.`test.csv`";
    String requestJson = mapper.writeValueAsString(
        new TranspileResources.TranspileRequest(drillSql, "drill", "drill", null));
    Request request = new Request.Builder()
        .url(url)
        .post(RequestBody.create(requestJson, JSON_MEDIA_TYPE))
        .build();
    try (Response response = httpClient.newCall(request).execute()) {
      assertEquals(200, response.code());
      String body = response.body().string();
      JsonNode json = mapper.readTree(body);
      assertTrue(json.get("success").asBoolean(), "Passthrough should succeed");
    }
  }
}
