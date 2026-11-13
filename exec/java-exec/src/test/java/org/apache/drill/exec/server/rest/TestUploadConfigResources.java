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
import jakarta.annotation.security.RolesAllowed;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.server.rest.auth.DrillUserPrincipal;
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

  @Test
  public void testPutRejectsMissingPlugin() throws Exception {
    JsonNode body = call("PUT", "/config", "{\"plugin\":\"nosuchplugin\",\"workspace\":\"tmp\"}", 404);
    assertTrue(body.get("message").asText().contains("nosuchplugin"));
  }

  @Test
  public void testPutRejectsMissingWorkspace() throws Exception {
    JsonNode body = call("PUT", "/config", "{\"plugin\":\"dfs\",\"workspace\":\"nosuchws\"}", 404);
    assertTrue(body.get("message").asText().contains("nosuchws"));
  }

  @Test
  public void testPutRejectsBlankOrAbsentInput() throws Exception {
    call("PUT", "/config", "{\"workspace\":\"tmp\"}", 400);
    call("PUT", "/config", "{\"plugin\":\"dfs\"}", 400);
    call("PUT", "/config", "{\"plugin\":\"  \",\"workspace\":\"tmp\"}", 400);
    call("PUT", "/config", "{\"plugin\":\"dfs\",\"workspace\":\"\"}", 400);
  }

  /**
   * The harness requests as the anonymous user, so the role split is pinned on the
   * annotations Jersey enforces: status is open to any authenticated user, the rest
   * inherits the admin-only class annotation.
   */
  @Test
  public void testRoleSplitBetweenStatusAndConfig() throws Exception {
    Class<?> c = UploadConfigResources.class;
    assertEquals(DrillUserPrincipal.ADMIN_ROLE,
        c.getAnnotation(RolesAllowed.class).value()[0]);
    assertEquals(DrillUserPrincipal.AUTHENTICATED_ROLE,
        c.getMethod("getStatus").getAnnotation(RolesAllowed.class).value()[0]);
    assertNull(c.getMethod("getConfig").getAnnotation(RolesAllowed.class));
    assertNull(c.getMethod("putConfig", UploadConfigResources.UploadConfig.class)
        .getAnnotation(RolesAllowed.class));
  }

  @Test
  public void testStoredEntryWithUnknownPropertyStillReads() throws Exception {
    UploadConfigResources.UploadConfig read = new ObjectMapper().readValue(
        "{\"plugin\":\"dfs\",\"workspace\":\"tmp\",\"maxSizeBytes\":7,\"futureField\":true}",
        UploadConfigResources.UploadConfig.class);
    assertEquals("dfs", read.plugin);
    assertEquals(7, read.maxSizeBytes);
  }
}
