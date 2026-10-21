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
 * Basemap tile configuration for the geospatial filter builder.
 *
 * <p>The default is deliberately empty: with no tile URL the map draws over the vector
 * basemaps already bundled in the webapp, so nothing about a deployment's data leaves
 * the network unless an administrator opts in.
 *
 * <p>Note that role enforcement is not covered here — the test cluster runs without
 * authentication, so every request is an unauthenticated "anonymous" principal. The
 * annotations are on the resource; only their effect under a real security setup is
 * untested.
 */
public class TestMapConfigResources extends ClusterTest {

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

  private static int post(String path, String jsonBody) throws Exception {
    RequestBody body = RequestBody.create(jsonBody, JSON);
    Request request = new Request.Builder().url(url(path)).post(body).build();
    try (Response response = httpClient.newCall(request).execute()) {
      return response.code();
    }
  }

  @Test
  public void testDefaultsToNoTileServer() throws Exception {
    JsonNode config = get("/api/v1/map/config");
    assertEquals("", config.get("tileUrl").asText());
    assertEquals("", config.get("attribution").asText());
  }

  @Test
  public void testTileUrlRoundTrips() throws Exception {
    assertEquals(200, post("/api/v1/map/config",
        "{\"tileUrl\":\"https://tiles.example.com/{z}/{x}/{y}.png\","
            + "\"attribution\":\"© Example\"}"));

    JsonNode config = get("/api/v1/map/config");
    assertEquals("https://tiles.example.com/{z}/{x}/{y}.png", config.get("tileUrl").asText());
    assertEquals("© Example", config.get("attribution").asText());
  }

  /**
   * Most tile providers require credit as a condition of use — OpenStreetMap's policy
   * does — so a URL with no attribution would put the administrator in breach without
   * their noticing. Rejected rather than defaulted: only they know what the provider
   * requires.
   */
  @Test
  public void testTileUrlWithoutAttributionIsRejected() throws Exception {
    assertEquals(400, post("/api/v1/map/config",
        "{\"tileUrl\":\"https://tiles.example.com/{z}/{x}/{y}.png\"}"));
  }

  @Test
  public void testTileUrlWithBlankAttributionIsRejected() throws Exception {
    assertEquals(400, post("/api/v1/map/config",
        "{\"tileUrl\":\"https://tiles.example.com/{z}/{x}/{y}.png\",\"attribution\":\"   \"}"));
  }

  /** Clearing the URL returns to bundled vectors and needs no attribution. */
  @Test
  public void testClearingTheTileUrlIsAllowed() throws Exception {
    post("/api/v1/map/config",
        "{\"tileUrl\":\"https://tiles.example.com/{z}/{x}/{y}.png\",\"attribution\":\"c\"}");

    assertEquals(200, post("/api/v1/map/config", "{\"tileUrl\":\"\",\"attribution\":\"\"}"));
    assertEquals("", get("/api/v1/map/config").get("tileUrl").asText());
  }

  /**
   * An XYZ template without the placeholders would silently request the same tile
   * forever, which looks like a blank map rather than a misconfiguration.
   */
  @Test
  public void testTileUrlMustLookLikeAnXyzTemplate() throws Exception {
    assertEquals(400, post("/api/v1/map/config",
        "{\"tileUrl\":\"https://tiles.example.com/map.png\",\"attribution\":\"c\"}"));
  }
}
