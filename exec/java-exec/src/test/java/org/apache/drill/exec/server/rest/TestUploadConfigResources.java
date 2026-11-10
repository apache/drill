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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.ClusterTest;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * The upload target setting. The cluster has two Drillbits so the multi-node warning
 * can fire against the stock local {@code dfs} plugin. The stored config is cleared
 * before each test.
 */
public class TestUploadConfigResources extends ClusterTest {

  private static final MediaType JSON = MediaType.parse("application/json");
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static int portNumber;
  private static final OkHttpClient client = new OkHttpClient.Builder()
      .connectTimeout(30, TimeUnit.SECONDS)
      .readTimeout(30, TimeUnit.SECONDS)
      .build();

  @BeforeClass
  public static void setup() throws Exception {
    startCluster(new ClusterFixtureBuilder(dirTestWatcher)
        .clusterSize(2)
        .configProperty(ExecConstants.HTTP_ENABLE, true)
        .configProperty(ExecConstants.HTTP_PORT_HUNT, true));
    portNumber = cluster.drillbit().getWebServerPort();
    cluster.defineImmutableWorkspace("dfs", "readonly", dirTestWatcher.getRootDir().getAbsolutePath(),
        "parquet", null);
  }

  @Before
  public void clearStoredConfig() {
    UploadConfigResources.clearForTest(cluster.drillbit().getContext().getStoreProvider());
  }

  private static JsonNode call(String method, String path, String json, int expected)
      throws Exception {
    Request.Builder b = new Request.Builder().url("http://localhost:" + portNumber
        + "/api/v1/uploads" + path);
    if ("PUT".equals(method)) {
      b.put(RequestBody.create(json, JSON));
    }
    try (Response r = client.newCall(b.build()).execute()) {
      String body = r.body().string();
      assertEquals(body, expected, r.code());
      return MAPPER.readTree(body);
    }
  }

  @Test
  public void testStatusReportsUnconfiguredWhenNothingStored() throws Exception {
    assertNull(UploadConfigResources.current(
        cluster.drillbit().getContext().getStoreProvider()));
    JsonNode status = call("GET", "/status", null, 200);
    assertFalse(status.get("configured").asBoolean());
    assertTrue(status.get("plugin").isNull());
  }

  @Test
  public void testPutRejectsNonFileSystemPlugin() throws Exception {
    JsonNode body = call("PUT", "/config", "{\"plugin\":\"sys\",\"workspace\":\"x\"}", 400);
    assertTrue(body.get("message").asText().contains("sys"));
  }

  @Test
  public void testPutRejectsNonWritableWorkspace() throws Exception {
    JsonNode body = call("PUT", "/config", "{\"plugin\":\"dfs\",\"workspace\":\"readonly\"}", 403);
    assertTrue(body.get("message").asText().contains("readonly"));
  }

  @Test
  public void testPutThenStatusReportsConfigured() throws Exception {
    call("PUT", "/config", "{\"plugin\":\"dfs\",\"workspace\":\"tmp\",\"maxSizeBytes\":5000}", 200);
    JsonNode status = call("GET", "/status", null, 200);
    assertTrue(status.get("configured").asBoolean());
    assertEquals("dfs", status.get("plugin").asText());
    assertEquals("tmp", status.get("workspace").asText());
    assertEquals(5000, status.get("maxSizeBytes").asLong());
    UploadConfigResources.UploadConfig stored = UploadConfigResources.current(
        cluster.drillbit().getContext().getStoreProvider());
    assertNotNull(stored);
    assertEquals("tmp", stored.workspace);
  }

  @Test
  public void testPutWarnsOnLocalConnectionWithMultipleBits() throws Exception {
    JsonNode body = call("PUT", "/config", "{\"plugin\":\"dfs\",\"workspace\":\"tmp\"}", 200);
    assertFalse(body.get("warning").asText().isEmpty());
    assertEquals(UploadConfigResources.DEFAULT_MAX_SIZE_BYTES,
        body.get("config").get("maxSizeBytes").asLong());
  }

  @Test
  public void testWarningOnlyForLocalConnectionOnMultipleBits() {
    assertNull(UploadConfigResources.multiNodeWarning("file:///", 1));
    assertNull(UploadConfigResources.multiNodeWarning("s3a://bucket", 3));
    assertNotNull(UploadConfigResources.multiNodeWarning("file:///", 2));
  }
}
