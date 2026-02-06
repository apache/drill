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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
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
 * Tests for the DashboardResources REST API endpoints.
 * Tests are ordered to ensure CRUD operations are executed in sequence.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestDashboardResources extends ClusterTest {

  private static final int TIMEOUT = 30;
  private static final MediaType JSON_MEDIA_TYPE = MediaType.parse("application/json");
  private static int portNumber;
  private static String createdDashboardId;

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
  public void test01_ListDashboardsEmpty() throws Exception {
    String url = String.format("http://localhost:%d/api/v1/dashboards", portNumber);
    Request request = new Request.Builder().url(url).build();
    try (Response response = httpClient.newCall(request).execute()) {
      assertEquals(200, response.code());
      String body = response.body().string();
      JsonNode json = mapper.readTree(body);
      assertTrue(json.has("dashboards"));
      assertTrue(json.get("dashboards").isArray());
    }
  }

  @Test
  public void test02_CreateDashboard() throws Exception {
    String url = String.format("http://localhost:%d/api/v1/dashboards", portNumber);

    Map<String, Object> requestBody = new HashMap<>();
    requestBody.put("name", "Test Dashboard");
    requestBody.put("description", "A test dashboard");
    requestBody.put("isPublic", true);
    requestBody.put("refreshInterval", 30);

    List<Map<String, Object>> panels = new ArrayList<>();
    Map<String, Object> panel1 = new HashMap<>();
    panel1.put("id", UUID.randomUUID().toString());
    panel1.put("visualizationId", "viz-1");
    panel1.put("x", 0);
    panel1.put("y", 0);
    panel1.put("width", 6);
    panel1.put("height", 3);
    panels.add(panel1);
    requestBody.put("panels", panels);

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
      assertEquals("Test Dashboard", json.get("name").asText());
      assertEquals("A test dashboard", json.get("description").asText());
      assertTrue(json.get("isPublic").asBoolean());
      assertEquals(30, json.get("refreshInterval").asInt());
      assertTrue(json.get("panels").isArray());
      assertEquals(1, json.get("panels").size());

      JsonNode firstPanel = json.get("panels").get(0);
      assertEquals("viz-1", firstPanel.get("visualizationId").asText());
      assertEquals(0, firstPanel.get("x").asInt());
      assertEquals(0, firstPanel.get("y").asInt());
      assertEquals(6, firstPanel.get("width").asInt());
      assertEquals(3, firstPanel.get("height").asInt());
      assertTrue(json.get("createdAt").asLong() > 0);
      assertTrue(json.get("updatedAt").asLong() > 0);

      createdDashboardId = json.get("id").asText();
    }
  }

  @Test
  public void test03_CreateDashboardMinimal() throws Exception {
    String url = String.format("http://localhost:%d/api/v1/dashboards", portNumber);

    Map<String, Object> requestBody = new HashMap<>();
    requestBody.put("name", "Minimal Dashboard");

    String jsonStr = mapper.writeValueAsString(requestBody);
    Request request = new Request.Builder()
        .url(url)
        .post(RequestBody.create(jsonStr, JSON_MEDIA_TYPE))
        .build();
    try (Response response = httpClient.newCall(request).execute()) {
      assertEquals(201, response.code());
      String body = response.body().string();
      JsonNode json = mapper.readTree(body);
      assertEquals("Minimal Dashboard", json.get("name").asText());
      assertTrue(json.get("panels").isArray());
      assertEquals(0, json.get("panels").size());

      // Clean up
      String deleteUrl = String.format("http://localhost:%d/api/v1/dashboards/%s",
          portNumber, json.get("id").asText());
      Request deleteRequest = new Request.Builder().url(deleteUrl).delete().build();
      httpClient.newCall(deleteRequest).execute().close();
    }
  }

  @Test
  public void test04_CreateDashboardValidation() throws Exception {
    String url = String.format("http://localhost:%d/api/v1/dashboards", portNumber);

    // Test missing name
    Map<String, Object> requestBody = new HashMap<>();
    requestBody.put("description", "No name dashboard");
    String jsonStr = mapper.writeValueAsString(requestBody);
    Request request = new Request.Builder()
        .url(url)
        .post(RequestBody.create(jsonStr, JSON_MEDIA_TYPE))
        .build();
    try (Response response = httpClient.newCall(request).execute()) {
      assertEquals(400, response.code());
    }

    // Test empty name
    Map<String, Object> requestBody2 = new HashMap<>();
    requestBody2.put("name", "   ");
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
  public void test05_GetDashboard() throws Exception {
    assertNotNull(createdDashboardId, "Dashboard should have been created in previous test");

    String url = String.format("http://localhost:%d/api/v1/dashboards/%s",
        portNumber, createdDashboardId);
    Request request = new Request.Builder().url(url).build();
    try (Response response = httpClient.newCall(request).execute()) {
      assertEquals(200, response.code());
      String body = response.body().string();
      JsonNode json = mapper.readTree(body);
      assertEquals(createdDashboardId, json.get("id").asText());
      assertEquals("Test Dashboard", json.get("name").asText());
      assertEquals(1, json.get("panels").size());
    }
  }

  @Test
  public void test06_GetDashboardNotFound() throws Exception {
    String url = String.format("http://localhost:%d/api/v1/dashboards/nonexistent-id",
        portNumber);
    Request request = new Request.Builder().url(url).build();
    try (Response response = httpClient.newCall(request).execute()) {
      assertEquals(404, response.code());
    }
  }

  @Test
  public void test07_UpdateDashboardMetadata() throws Exception {
    assertNotNull(createdDashboardId, "Dashboard should have been created in previous test");

    String url = String.format("http://localhost:%d/api/v1/dashboards/%s",
        portNumber, createdDashboardId);

    Map<String, Object> updateBody = new HashMap<>();
    updateBody.put("name", "Updated Dashboard");
    updateBody.put("description", "Updated description");
    updateBody.put("isPublic", false);
    updateBody.put("refreshInterval", 60);

    String jsonStr = mapper.writeValueAsString(updateBody);
    Request request = new Request.Builder()
        .url(url)
        .put(RequestBody.create(jsonStr, JSON_MEDIA_TYPE))
        .build();
    try (Response response = httpClient.newCall(request).execute()) {
      assertEquals(200, response.code());
      String body = response.body().string();
      JsonNode json = mapper.readTree(body);
      assertEquals("Updated Dashboard", json.get("name").asText());
      assertEquals("Updated description", json.get("description").asText());
      assertFalse(json.get("isPublic").asBoolean());
      assertEquals(60, json.get("refreshInterval").asInt());
      // Panels should be unchanged
      assertEquals(1, json.get("panels").size());
    }
  }

  @Test
  public void test08_UpdateDashboardPanels() throws Exception {
    assertNotNull(createdDashboardId, "Dashboard should have been created in previous test");

    String url = String.format("http://localhost:%d/api/v1/dashboards/%s",
        portNumber, createdDashboardId);

    List<Map<String, Object>> panels = new ArrayList<>();

    Map<String, Object> panel1 = new HashMap<>();
    panel1.put("id", UUID.randomUUID().toString());
    panel1.put("visualizationId", "viz-1");
    panel1.put("x", 0);
    panel1.put("y", 0);
    panel1.put("width", 12);
    panel1.put("height", 4);
    panels.add(panel1);

    Map<String, Object> panel2 = new HashMap<>();
    panel2.put("id", UUID.randomUUID().toString());
    panel2.put("visualizationId", "viz-2");
    panel2.put("x", 0);
    panel2.put("y", 4);
    panel2.put("width", 6);
    panel2.put("height", 3);
    panels.add(panel2);

    Map<String, Object> panel3 = new HashMap<>();
    panel3.put("id", UUID.randomUUID().toString());
    panel3.put("visualizationId", "viz-3");
    panel3.put("x", 6);
    panel3.put("y", 4);
    panel3.put("width", 6);
    panel3.put("height", 3);
    panels.add(panel3);

    Map<String, Object> updateBody = new HashMap<>();
    updateBody.put("panels", panels);

    String jsonStr = mapper.writeValueAsString(updateBody);
    Request request = new Request.Builder()
        .url(url)
        .put(RequestBody.create(jsonStr, JSON_MEDIA_TYPE))
        .build();
    try (Response response = httpClient.newCall(request).execute()) {
      assertEquals(200, response.code());
      String body = response.body().string();
      JsonNode json = mapper.readTree(body);
      assertEquals(3, json.get("panels").size());

      JsonNode firstPanel = json.get("panels").get(0);
      assertEquals("viz-1", firstPanel.get("visualizationId").asText());
      assertEquals(12, firstPanel.get("width").asInt());
      assertEquals(4, firstPanel.get("height").asInt());

      JsonNode secondPanel = json.get("panels").get(1);
      assertEquals("viz-2", secondPanel.get("visualizationId").asText());
      assertEquals(0, secondPanel.get("x").asInt());
      assertEquals(4, secondPanel.get("y").asInt());

      JsonNode thirdPanel = json.get("panels").get(2);
      assertEquals("viz-3", thirdPanel.get("visualizationId").asText());
      assertEquals(6, thirdPanel.get("x").asInt());
      assertEquals(4, thirdPanel.get("y").asInt());
    }
  }

  @Test
  public void test09_UpdateNonExistent() throws Exception {
    String url = String.format("http://localhost:%d/api/v1/dashboards/nonexistent-id",
        portNumber);
    Map<String, Object> updateBody = new HashMap<>();
    updateBody.put("name", "Ghost");
    String jsonStr = mapper.writeValueAsString(updateBody);
    Request request = new Request.Builder()
        .url(url)
        .put(RequestBody.create(jsonStr, JSON_MEDIA_TYPE))
        .build();
    try (Response response = httpClient.newCall(request).execute()) {
      assertEquals(404, response.code());
    }
  }

  @Test
  public void test10_ListDashboardsAfterCreate() throws Exception {
    String url = String.format("http://localhost:%d/api/v1/dashboards", portNumber);
    Request request = new Request.Builder().url(url).build();
    try (Response response = httpClient.newCall(request).execute()) {
      assertEquals(200, response.code());
      String body = response.body().string();
      JsonNode json = mapper.readTree(body);
      assertTrue(json.has("dashboards"));
      assertTrue(json.get("dashboards").size() > 0, "Should have at least one dashboard");

      boolean found = false;
      for (JsonNode node : json.get("dashboards")) {
        if (createdDashboardId.equals(node.get("id").asText())) {
          found = true;
          break;
        }
      }
      assertTrue(found, "Should contain the created dashboard");
    }
  }

  @Test
  public void test11_DeleteDashboard() throws Exception {
    assertNotNull(createdDashboardId, "Dashboard should have been created in previous test");

    String url = String.format("http://localhost:%d/api/v1/dashboards/%s",
        portNumber, createdDashboardId);
    Request request = new Request.Builder()
        .url(url)
        .delete()
        .build();
    try (Response response = httpClient.newCall(request).execute()) {
      assertEquals(200, response.code());
    }

    // Verify the dashboard is deleted
    Request getRequest = new Request.Builder().url(url).build();
    try (Response response = httpClient.newCall(getRequest).execute()) {
      assertEquals(404, response.code());
    }
  }

  @Test
  public void test12_DeleteNonExistent() throws Exception {
    String url = String.format("http://localhost:%d/api/v1/dashboards/nonexistent-id",
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
