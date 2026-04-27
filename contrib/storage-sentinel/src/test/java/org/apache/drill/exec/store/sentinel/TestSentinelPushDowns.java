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

package org.apache.drill.exec.store.sentinel;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.apache.drill.common.logical.StoragePluginConfig.AuthMode;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.assertTrue;

public class TestSentinelPushDowns extends SentinelTestBase {
  private static final int MOCK_SERVER_PORT = 18889;
  private static final ObjectMapper mapper = new ObjectMapper();

  @BeforeClass
  public static void setupPlugin() throws Exception {
    String mockServerUrl = "http://localhost:" + MOCK_SERVER_PORT;
    SentinelStoragePluginConfig config = new SentinelStoragePluginConfig(
        "workspace-id",
        "tenant-id",
        "client-id",
        "client-secret",
        "P1D",
        10000,
        new ArrayList<>(),
        AuthMode.SHARED_USER,
        null,
        mockServerUrl
    );
    config.setEnabled(true);
    cluster.defineStoragePlugin("sentinel", config);
  }

  @Test
  public void testFilterPushdown() throws Exception {
    String responseJson = createResponse(new String[]{"Severity", "AlertName"},
        new String[]{"string", "string"},
        new Object[][]{{"High", "Alert1"}, {"Critical", "Alert2"}});

    try (MockWebServer server = startMockServer()) {
      server.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

      String sql = "SELECT Severity, AlertName FROM sentinel.SecurityAlert WHERE Severity = 'High'";
      String plan = client.queryBuilder().sql(sql).explainJson();

      assertTrue("Filter should be pushed down to Sentinel", containsKqlOperation(plan, "where"));
      assertTrue("WHERE clause should contain Severity filter", containsKqlOperation(plan, "Severity"));
    }
  }

  @Test
  public void testProjectionPushdown() throws Exception {
    String responseJson = createResponse(new String[]{"AlertName", "Severity"},
        new String[]{"string", "string"},
        new Object[][]{{"Alert1", "High"}});

    try (MockWebServer server = startMockServer()) {
      server.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

      String sql = "SELECT AlertName, Severity FROM sentinel.SecurityAlert";
      String plan = client.queryBuilder().sql(sql).explainJson();

      assertTrue("Projection should be pushed down", containsKqlOperation(plan, "project"));
      assertTrue("Project should contain AlertName", containsKqlOperation(plan, "AlertName"));
      assertTrue("Project should contain Severity", containsKqlOperation(plan, "Severity"));
    }
  }

  @Test
  public void testLimitPushdown() throws Exception {
    String responseJson = createResponse(new String[]{"AlertName"},
        new String[]{"string"},
        new Object[][]{{"Alert1"}, {"Alert2"}});

    try (MockWebServer server = startMockServer()) {
      server.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

      String sql = "SELECT AlertName FROM sentinel.SecurityAlert LIMIT 10";
      String plan = client.queryBuilder().sql(sql).explainJson();

      assertTrue("LIMIT should be pushed down as take", containsKqlOperation(plan, "take"));
    }
  }

  @Test
  public void testSortPushdown() throws Exception {
    String responseJson = createResponse(new String[]{"AlertName", "Count"},
        new String[]{"string", "int"},
        new Object[][]{{"Alert1", 5}, {"Alert2", 3}});

    try (MockWebServer server = startMockServer()) {
      server.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

      String sql = "SELECT AlertName, Count FROM sentinel.SecurityAlert ORDER BY Count DESC";
      String plan = client.queryBuilder().sql(sql).explainJson();

      assertTrue("ORDER BY should be pushed down as sort", containsKqlOperation(plan, "sort by"));
      assertTrue("Sort should specify column", containsKqlOperation(plan, "Count"));
    }
  }

  @Test
  public void testAggregatePushdown() throws Exception {
    String responseJson = createResponse(new String[]{"Severity", "Count"},
        new String[]{"string", "long"},
        new Object[][]{{"High", 5}, {"Critical", 3}});

    try (MockWebServer server = startMockServer()) {
      server.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

      String sql = "SELECT Severity, COUNT(*) as Count FROM sentinel.SecurityAlert GROUP BY Severity";
      String plan = client.queryBuilder().sql(sql).explainJson();

      assertTrue("GROUP BY should be pushed down as summarize", containsKqlOperation(plan, "summarize"));
      assertTrue("Summarize should have count function", containsKqlOperation(plan, "count()"));
    }
  }

  @Test
  public void testMultiplePushdowns() throws Exception {
    String responseJson = createResponse(new String[]{"AlertName"},
        new String[]{"string"},
        new Object[][]{{"Alert1"}});

    try (MockWebServer server = startMockServer()) {
      server.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

      String sql = "SELECT AlertName FROM sentinel.SecurityAlert WHERE Severity = 'High' " +
          "ORDER BY AlertName LIMIT 5";
      String plan = client.queryBuilder().sql(sql).explainJson();

      assertTrue("Filter should be pushed down", containsKqlOperation(plan, "where"));
      assertTrue("Projection should be pushed down", containsKqlOperation(plan, "project"));
      assertTrue("Sort should be pushed down", containsKqlOperation(plan, "sort"));
      assertTrue("Limit should be pushed down", containsKqlOperation(plan, "take"));
    }
  }

  private static String createResponse(String[] columnNames, String[] columnTypes, Object[][] rows) {
    StringBuilder json = new StringBuilder();
    json.append("{\n  \"tables\": [\n    {\n      \"columns\": [\n");

    for (int i = 0; i < columnNames.length; i++) {
      json.append("        {\"name\": \"").append(columnNames[i]).append("\", \"type\": \"")
          .append(columnTypes[i]).append("\"}");
      if (i < columnNames.length - 1) json.append(",");
      json.append("\n");
    }

    json.append("      ],\n      \"rows\": [\n");

    for (int i = 0; i < rows.length; i++) {
      json.append("        [");
      for (int j = 0; j < rows[i].length; j++) {
        Object val = rows[i][j];
        if (val instanceof String) {
          json.append("\"").append(val).append("\"");
        } else {
          json.append(val);
        }
        if (j < rows[i].length - 1) json.append(", ");
      }
      json.append("]");
      if (i < rows.length - 1) json.append(",");
      json.append("\n");
    }

    json.append("      ]\n    }\n  ]\n}");
    return json.toString();
  }

  private static boolean containsKqlOperation(String plan, String operation) throws Exception {
    JsonNode root = mapper.readTree(plan);
    return plan.contains(operation) || findInPlan(root, operation);
  }

  private static boolean findInPlan(JsonNode node, String target) {
    if (node == null) {
      return false;
    }

    if (node.isTextual() && node.asText().contains(target)) {
      return true;
    }

    if (node.isObject()) {
      for (JsonNode child : node) {
        if (findInPlan(child, target)) {
          return true;
        }
      }
    }

    if (node.isArray()) {
      for (JsonNode child : node) {
        if (findInPlan(child, target)) {
          return true;
        }
      }
    }

    return false;
  }

  private static MockWebServer startMockServer() throws Exception {
    MockWebServer server = new MockWebServer();
    server.start(MOCK_SERVER_PORT);
    return server;
  }
}

