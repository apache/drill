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
import static org.junit.Assert.assertTrue;

/**
 * The link from a published API back to the tab it came from.
 *
 * <p>Before this existed the association lived only in React state, so a reload lost
 * it entirely and the delete-tab warning had nothing to read.
 *
 * <p>The link is provenance, not a foreign key: {@code SharedQueryApi} holds its own
 * copy of the SQL and {@code /{id}/data} re-executes that copy, so deleting the tab
 * leaves the endpoint serving. {@link #testApiKeepsServingAfterItsTabIsDeleted} is what
 * makes it truthful for the UI to say the endpoint stays live.
 */
public class TestSharedQueryApiTabLink extends ClusterTest {

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

  private static int delete(String path) throws Exception {
    Request request = new Request.Builder().url(url(path)).delete().build();
    try (Response response = httpClient.newCall(request).execute()) {
      return response.code();
    }
  }

  @Test
  public void testCreatedApiRemembersItsTab() throws Exception {
    JsonNode created = post("/api/v1/shared-queries",
        "{\"name\":\"Sales feed\",\"sql\":\"SELECT 1\",\"apiEnabled\":true,"
            + "\"tabId\":\"tab-abc\"}");
    assertEquals("tab-abc", created.get("tabId").asText());
  }

  @Test
  public void testApisCanBeListedByTab() throws Exception {
    post("/api/v1/shared-queries",
        "{\"name\":\"A\",\"sql\":\"SELECT 1\",\"apiEnabled\":true,\"tabId\":\"tab-filter-a\"}");
    post("/api/v1/shared-queries",
        "{\"name\":\"B\",\"sql\":\"SELECT 2\",\"apiEnabled\":true,\"tabId\":\"tab-filter-b\"}");

    JsonNode found = get("/api/v1/shared-queries?tabId=tab-filter-a").get("queries");
    assertEquals(1, found.size());
    assertEquals("A", found.get(0).get("name").asText());
  }

  /** Without a tabId filter the listing still returns everything the user owns. */
  @Test
  public void testListingWithoutATabFilterIsUnchanged() throws Exception {
    post("/api/v1/shared-queries",
        "{\"name\":\"Unfiltered\",\"sql\":\"SELECT 1\",\"apiEnabled\":true,"
            + "\"tabId\":\"tab-unfiltered\"}");

    assertTrue(get("/api/v1/shared-queries").get("queries").size() > 0);
  }

  /** An API published before this field existed has no tabId, and must still list. */
  @Test
  public void testApiWithoutATabIdIsStillListed() throws Exception {
    JsonNode created = post("/api/v1/shared-queries",
        "{\"name\":\"Legacy\",\"sql\":\"SELECT 1\",\"apiEnabled\":true}");
    assertTrue(created.get("tabId") == null || created.get("tabId").isNull());
    assertTrue(get("/api/v1/shared-queries").get("queries").size() > 0);
  }

  /**
   * The endpoint holds its own copy of the SQL, so removing the tab must not disturb
   * it. This is what lets the delete warning say "stays live" truthfully.
   */
  @Test
  public void testApiKeepsServingAfterItsTabIsDeleted() throws Exception {
    String tabId = post("/api/v1/tabs",
        "{\"name\":\"T\",\"sql\":\"SELECT 1\",\"projectId\":\"apilink\"}").get("id").asText();
    String apiId = post("/api/v1/shared-queries",
        "{\"name\":\"Feed\",\"sql\":\"SELECT 1\",\"apiEnabled\":true,\"tabId\":\"" + tabId + "\"}")
        .get("id").asText();

    assertEquals(200, delete("/api/v1/tabs/" + tabId));

    Request request = new Request.Builder()
        .url(url("/api/v1/shared-queries/" + apiId + "/data")).build();
    try (Response response = httpClient.newCall(request).execute()) {
      assertEquals(200, response.code());
    }
  }
}
