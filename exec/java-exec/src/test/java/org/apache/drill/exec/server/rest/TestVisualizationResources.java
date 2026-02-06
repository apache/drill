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
import java.util.HashMap;
import java.util.Map;
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
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * Tests for the VisualizationResources REST API endpoints.
 * Tests are ordered to ensure CRUD operations are executed in sequence.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestVisualizationResources extends ClusterTest {

  private static final int TIMEOUT = 30;
  private static final MediaType JSON_MEDIA_TYPE = MediaType.parse("application/json");
  private static int portNumber;
  private static String createdVizId;

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
  public void test1_ListVisualizationsEmpty() throws Exception {
    String url = String.format("http://localhost:%d/api/v1/visualizations", portNumber);
    Request request = new Request.Builder().url(url).build();
    try (Response response = httpClient.newCall(request).execute()) {
      assertEquals(200, response.code());
      String body = response.body().string();
      JsonNode json = mapper.readTree(body);
      assertTrue(json.has("visualizations"));
      assertTrue(json.get("visualizations").isArray());
    }
  }

  @Test
  public void test2_CreateVisualization() throws Exception {
    String url = String.format("http://localhost:%d/api/v1/visualizations", portNumber);

    Map<String, Object> config = new HashMap<>();
    config.put("xAxis", "department_id");
    config.put("metrics", Arrays.asList("employee_id"));
    config.put("dimensions", Arrays.asList("department_id"));
    config.put("colorScheme", "default");

    Map<String, Object> requestBody = new HashMap<>();
    requestBody.put("name", "Employee Bar Chart");
    requestBody.put("description", "Bar chart of employees by department");
    requestBody.put("chartType", "bar");
    requestBody.put("config", config);
    requestBody.put("isPublic", true);
    requestBody.put("sql", "SELECT department_id, COUNT(*) as employee_id "
        + "FROM cp.`employee.json` GROUP BY department_id");
    requestBody.put("defaultSchema", "cp");

    String jsonStr = mapper.writeValueAsString(requestBody);
    Request request = new Request.Builder()
        .url(url)
        .post(RequestBody.create(jsonStr, JSON_MEDIA_TYPE))
        .build();
    try (Response response = httpClient.newCall(request).execute()) {
      assertEquals(201, response.code());
      String body = response.body().string();
      JsonNode json = mapper.readTree(body);

      assertNotNull(json.get("id").asText());
      assertFalse(json.get("id").asText().isEmpty());
      assertEquals("Employee Bar Chart", json.get("name").asText());
      assertEquals("Bar chart of employees by department", json.get("description").asText());
      assertEquals("bar", json.get("chartType").asText());
      assertTrue(json.get("isPublic").asBoolean());
      assertNotNull(json.get("config"));
      assertEquals("department_id", json.get("config").get("xAxis").asText());
      assertNotNull(json.get("sql"));
      assertEquals("cp", json.get("defaultSchema").asText());
      assertTrue(json.get("createdAt").asLong() > 0);
      assertTrue(json.get("updatedAt").asLong() > 0);

      createdVizId = json.get("id").asText();
    }
  }

  @Test
  public void test3_CreateVisualizationValidation() throws Exception {
    String url = String.format("http://localhost:%d/api/v1/visualizations", portNumber);

    // Test missing name
    Map<String, Object> requestBody = new HashMap<>();
    requestBody.put("chartType", "bar");
    String jsonStr = mapper.writeValueAsString(requestBody);
    Request request = new Request.Builder()
        .url(url)
        .post(RequestBody.create(jsonStr, JSON_MEDIA_TYPE))
        .build();
    try (Response response = httpClient.newCall(request).execute()) {
      assertEquals(400, response.code());
    }

    // Test missing chart type
    Map<String, Object> requestBody2 = new HashMap<>();
    requestBody2.put("name", "Missing Chart Type");
    String jsonStr2 = mapper.writeValueAsString(requestBody2);
    Request request2 = new Request.Builder()
        .url(url)
        .post(RequestBody.create(jsonStr2, JSON_MEDIA_TYPE))
        .build();
    try (Response response = httpClient.newCall(request2).execute()) {
      assertEquals(400, response.code());
    }
  }

  @Test
  public void test4_GetVisualization() throws Exception {
    assertNotNull(createdVizId, "Visualization should have been created in previous test");

    String url = String.format("http://localhost:%d/api/v1/visualizations/%s",
        portNumber, createdVizId);
    Request request = new Request.Builder().url(url).build();
    try (Response response = httpClient.newCall(request).execute()) {
      assertEquals(200, response.code());
      String body = response.body().string();
      JsonNode json = mapper.readTree(body);
      assertEquals(createdVizId, json.get("id").asText());
      assertEquals("Employee Bar Chart", json.get("name").asText());
      assertEquals("bar", json.get("chartType").asText());
    }
  }

  @Test
  public void test5_GetVisualizationNotFound() throws Exception {
    String url = String.format("http://localhost:%d/api/v1/visualizations/nonexistent-id",
        portNumber);
    Request request = new Request.Builder().url(url).build();
    try (Response response = httpClient.newCall(request).execute()) {
      assertEquals(404, response.code());
    }
  }

  @Test
  public void test6_UpdateVisualization() throws Exception {
    assertNotNull(createdVizId, "Visualization should have been created in previous test");

    String url = String.format("http://localhost:%d/api/v1/visualizations/%s",
        portNumber, createdVizId);

    Map<String, Object> updateBody = new HashMap<>();
    updateBody.put("name", "Updated Bar Chart");
    updateBody.put("description", "Updated description");
    updateBody.put("chartType", "line");
    updateBody.put("isPublic", false);

    String jsonStr = mapper.writeValueAsString(updateBody);
    Request request = new Request.Builder()
        .url(url)
        .put(RequestBody.create(jsonStr, JSON_MEDIA_TYPE))
        .build();
    try (Response response = httpClient.newCall(request).execute()) {
      assertEquals(200, response.code());
      String body = response.body().string();
      JsonNode json = mapper.readTree(body);
      assertEquals("Updated Bar Chart", json.get("name").asText());
      assertEquals("Updated description", json.get("description").asText());
      assertEquals("line", json.get("chartType").asText());
      assertFalse(json.get("isPublic").asBoolean());
    }
  }

  @Test
  public void test7_ListVisualizationsAfterCreate() throws Exception {
    String url = String.format("http://localhost:%d/api/v1/visualizations", portNumber);
    Request request = new Request.Builder().url(url).build();
    try (Response response = httpClient.newCall(request).execute()) {
      assertEquals(200, response.code());
      String body = response.body().string();
      JsonNode json = mapper.readTree(body);
      assertTrue(json.has("visualizations"));
      assertTrue(json.get("visualizations").size() > 0,
          "Should have at least one visualization");

      boolean found = false;
      for (JsonNode node : json.get("visualizations")) {
        if (createdVizId.equals(node.get("id").asText())) {
          found = true;
          break;
        }
      }
      assertTrue(found, "Should contain the created visualization");
    }
  }

  @Test
  public void test8_DeleteVisualization() throws Exception {
    assertNotNull(createdVizId, "Visualization should have been created in previous test");

    String url = String.format("http://localhost:%d/api/v1/visualizations/%s",
        portNumber, createdVizId);
    Request request = new Request.Builder()
        .url(url)
        .delete()
        .build();
    try (Response response = httpClient.newCall(request).execute()) {
      assertEquals(200, response.code());
    }

    // Verify the visualization is deleted
    Request getRequest = new Request.Builder().url(url).build();
    try (Response response = httpClient.newCall(getRequest).execute()) {
      assertEquals(404, response.code());
    }
  }

  @Test
  public void test9_DeleteNonExistent() throws Exception {
    String url = String.format("http://localhost:%d/api/v1/visualizations/nonexistent-id",
        portNumber);
    Request request = new Request.Builder()
        .url(url)
        .delete()
        .build();
    try (Response response = httpClient.newCall(request).execute()) {
      assertEquals(404, response.code());
    }
  }
}
