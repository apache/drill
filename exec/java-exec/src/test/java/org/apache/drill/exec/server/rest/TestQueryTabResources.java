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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.drill.common.util.JacksonUtils;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.ClusterTest;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * HTTP-level tests for the query tab endpoints.
 *
 * <p>The listing and locking assertions here encode two rules from
 * {@code docs/dev/TabPersistence.md}: a hidden tab is still listed, which is what makes
 * closing non-destructive, and a locked tab cannot be deleted, which has to be enforced
 * server-side because a client-side guard on a durable object is not a guard.
 */
public class TestQueryTabResources extends ClusterTest {

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

  private static String url(String path) {
    return String.format("http://localhost:%d%s", portNumber, path);
  }

  private static JsonNode get(String path) throws Exception {
    Request request = new Request.Builder().url(url(path)).build();
    try (Response response = httpClient.newCall(request).execute()) {
      assertTrue("Expected 2xx from GET " + path + " but got " + response.code(),
          response.code() >= 200 && response.code() < 300);
      return mapper.readTree(response.body().string());
    }
  }

  private static JsonNode post(String path, String jsonBody) throws Exception {
    RequestBody body = RequestBody.create(jsonBody, JSON);
    Request request = new Request.Builder().url(url(path)).post(body).build();
    try (Response response = httpClient.newCall(request).execute()) {
      assertTrue("Expected 2xx from POST " + path + " but got " + response.code(),
          response.code() >= 200 && response.code() < 300);
      return mapper.readTree(response.body().string());
    }
  }

  private static JsonNode put(String path, String jsonBody) throws Exception {
    RequestBody body = RequestBody.create(jsonBody, JSON);
    Request request = new Request.Builder().url(url(path)).put(body).build();
    try (Response response = httpClient.newCall(request).execute()) {
      assertTrue("Expected 2xx from PUT " + path + " but got " + response.code(),
          response.code() >= 200 && response.code() < 300);
      return mapper.readTree(response.body().string());
    }
  }

  private static int delete(String path) throws Exception {
    Request request = new Request.Builder().url(url(path)).delete().build();
    try (Response response = httpClient.newCall(request).execute()) {
      return response.code();
    }
  }

  @Test
  public void testCreateListUpdateDelete() throws Exception {
    JsonNode created = post("/api/v1/tabs",
        "{\"name\":\"Query 1\",\"sql\":\"SELECT 1\",\"projectId\":\"crud\"}");
    String id = created.get("id").asText();
    assertFalse(id.isEmpty());

    assertEquals(1, get("/api/v1/tabs?projectId=crud").get("tabs").size());

    put("/api/v1/tabs/" + id,
        "{\"name\":\"Renamed\",\"sql\":\"SELECT 2\",\"projectId\":\"crud\"}");
    JsonNode afterUpdate = get("/api/v1/tabs?projectId=crud").get("tabs").get(0);
    assertEquals("Renamed", afterUpdate.get("name").asText());
    assertEquals("SELECT 2", afterUpdate.get("sql").asText());

    assertEquals(200, delete("/api/v1/tabs/" + id));
    assertEquals(0, get("/api/v1/tabs?projectId=crud").get("tabs").size());
  }

  /** The server generates the id so the client never has to invent a unique one. */
  @Test
  public void testCreateGeneratesAnIdWhenAbsent() throws Exception {
    JsonNode created = post("/api/v1/tabs",
        "{\"name\":\"No id\",\"sql\":\"SELECT 1\",\"projectId\":\"genid\"}");
    assertTrue(created.get("id").asText().length() > 10);
    assertTrue(created.get("createdAt").asLong() > 0);
  }

  /**
   * Locking is enforced here because a client-side-only guard on a durable object is
   * not a guard.
   */
  @Test
  public void testLockedTabCannotBeDeleted() throws Exception {
    JsonNode created = post("/api/v1/tabs",
        "{\"name\":\"Locked\",\"sql\":\"SELECT 1\",\"projectId\":\"locked\",\"locked\":true}");
    String id = created.get("id").asText();

    assertEquals(409, delete("/api/v1/tabs/" + id));
    assertEquals(1, get("/api/v1/tabs?projectId=locked").get("tabs").size());
  }

  /** A locked tab may still be hidden — hiding is harmless, deletion is not. */
  @Test
  public void testLockedTabCanBeHidden() throws Exception {
    JsonNode created = post("/api/v1/tabs",
        "{\"name\":\"Locked\",\"sql\":\"SELECT 1\",\"projectId\":\"lockhide\",\"locked\":true}");
    String id = created.get("id").asText();

    put("/api/v1/tabs/" + id,
        "{\"name\":\"Locked\",\"sql\":\"SELECT 1\",\"projectId\":\"lockhide\","
            + "\"locked\":true,\"hidden\":true}");

    assertTrue(get("/api/v1/tabs?projectId=lockhide").get("tabs").get(0).get("hidden").asBoolean());
  }

  /** A hidden tab is still listed. That is the whole point of hide-versus-delete. */
  @Test
  public void testHiddenTabsAreStillListed() throws Exception {
    post("/api/v1/tabs",
        "{\"name\":\"Hidden\",\"sql\":\"SELECT 1\",\"projectId\":\"hid\",\"hidden\":true}");

    JsonNode tabs = get("/api/v1/tabs?projectId=hid").get("tabs");
    assertEquals(1, tabs.size());
    assertTrue(tabs.get(0).get("hidden").asBoolean());
  }

  @Test
  public void testGlobalTabsAreSeparateFromProjectTabs() throws Exception {
    post("/api/v1/tabs", "{\"name\":\"Global\",\"sql\":\"SELECT 1\"}");
    post("/api/v1/tabs", "{\"name\":\"Scoped\",\"sql\":\"SELECT 2\",\"projectId\":\"sep\"}");

    assertEquals("Global", get("/api/v1/tabs").get("tabs").get(0).get("name").asText());
    assertEquals(1, get("/api/v1/tabs?projectId=sep").get("tabs").size());
    assertEquals("Scoped", get("/api/v1/tabs?projectId=sep").get("tabs").get(0).get("name").asText());
  }

  @Test
  public void testDeletingAnUnknownTabIsNotFound() throws Exception {
    assertEquals(404, delete("/api/v1/tabs/no-such-tab"));
  }

  /**
   * The owner comes from the authenticated principal, never the request body.
   * Otherwise any user could write into another user's namespace.
   */
  @Test
  public void testOwnerInTheBodyIsIgnored() throws Exception {
    JsonNode created = post("/api/v1/tabs",
        "{\"name\":\"Spoof\",\"sql\":\"SELECT 1\",\"projectId\":\"spoof\","
            + "\"owner\":\"someone-else\"}");
    assertFalse("someone-else".equals(created.get("owner").asText()));
  }
}
