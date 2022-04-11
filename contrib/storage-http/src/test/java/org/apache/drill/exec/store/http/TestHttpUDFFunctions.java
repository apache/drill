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

package org.apache.drill.exec.store.http;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.apache.drill.common.logical.StoragePluginConfig.AuthMode;
import org.apache.drill.common.logical.security.PlainCredentialsProvider;
import org.apache.drill.common.util.DrillFileUtils;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.store.http.util.SimpleHttp;
import org.apache.drill.exec.store.security.UsernamePasswordCredentials;
import org.apache.drill.shaded.guava.com.google.common.base.Charsets;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableMap;
import org.apache.drill.shaded.guava.com.google.common.io.Files;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestHttpUDFFunctions extends ClusterTest {

  private static final int MOCK_SERVER_PORT = 47771;
  private static String TEST_JSON_RESPONSE;
  private static String DUMMY_URL = "http://localhost:" + MOCK_SERVER_PORT;


  @BeforeClass
  public static void setup() throws Exception {
    startCluster(ClusterFixture.builder(dirTestWatcher));
    TEST_JSON_RESPONSE = Files.asCharSource(DrillFileUtils.getResourceAsFile("/data/simple.json"), Charsets.UTF_8).read();

    HttpApiConfig mockGithubWithDuplicateParam = HttpApiConfig.builder()
      .url(String.format("%s/orgs/{org}/repos", DUMMY_URL))
      .method("GET")
      .params(Arrays.asList("org", "lng", "date"))
      .dataPath("results")
      .requireTail(false)
      .build();

    Map<String, HttpApiConfig> configs = new HashMap<>();
    configs.put("github", mockGithubWithDuplicateParam);

    HttpStoragePluginConfig mockStorageConfigWithWorkspace =
      new HttpStoragePluginConfig(false, configs, 2, "globaluser", "globalpass", "",
        80, "", "", "", null, new PlainCredentialsProvider(ImmutableMap.of(
        UsernamePasswordCredentials.USERNAME, "globaluser",
        UsernamePasswordCredentials.PASSWORD, "globalpass")), AuthMode.SHARED_USER.name());
    mockStorageConfigWithWorkspace.setEnabled(true);
    cluster.defineStoragePlugin("local", mockStorageConfigWithWorkspace);
  }

  @Test
  public void testHttpGetWithNoParams() throws Exception {
    try (MockWebServer server = startServer()) {
      server.enqueue(new MockResponse().setResponseCode(200).setBody(TEST_JSON_RESPONSE));
      String sql = "SELECT http_get('" + DUMMY_URL + "') AS result FROM (values(1))";

      RowSet results = client.queryBuilder().sql(sql).rowSet();
      assertEquals(1, results.rowCount());
      results.clear();

      RecordedRequest recordedRequest = server.takeRequest();
      assertEquals("GET", recordedRequest.getMethod());
      assertEquals(String.format("%s/", DUMMY_URL), recordedRequest.getRequestUrl().toString());
    }
  }

  @Test
  public void testHttpGetWithParams() throws Exception {
    try (MockWebServer server = startServer()) {
      server.enqueue(new MockResponse().setResponseCode(200).setBody(TEST_JSON_RESPONSE));
      String sql = "SELECT http_get('" + DUMMY_URL + "/{p1}/{p2}', 'param1', 'param2') AS result FROM (values(1))";

      RowSet results = client.queryBuilder().sql(sql).rowSet();
      assertEquals(1, results.rowCount());
      results.clear();

      RecordedRequest recordedRequest = server.takeRequest();
      assertEquals("GET", recordedRequest.getMethod());
      assertEquals(String.format("%s/param1/param2", DUMMY_URL), recordedRequest.getRequestUrl().toString());
    }
  }

  @Test
  public void testHttpGetFromPlugin() throws Exception {
    try (MockWebServer server = startServer()) {
      server.enqueue(new MockResponse().setResponseCode(200).setBody(TEST_JSON_RESPONSE));
      String sql = "SELECT http_request('local.github', 'apache') AS result FROM (values(1))";

      RowSet results = client.queryBuilder().sql(sql).rowSet();
      assertEquals(1, results.rowCount());
      results.clear();

      RecordedRequest recordedRequest = server.takeRequest();
      assertEquals("GET", recordedRequest.getMethod());
      assertEquals(String.format("%s/orgs/apache/repos", DUMMY_URL), recordedRequest.getRequestUrl().toString());
    }
  }

  @Test
  public void testHttpGetWithInvalidPlugin() {
    try {
      String sql = "SELECT http_request('nope.nothere', 'apache') AS result FROM (values(1))";
      client.queryBuilder().sql(sql).run();
      fail();
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("FUNCTION ERROR: nope is not a valid plugin."));
    }
  }

  @Test
  public void testNullParam() throws Exception {
    // any null parameter results in an empty map
    String sql = "SELECT http_get('" + DUMMY_URL + "/{p1}/{p2}', 'param1', null) AS result FROM (values(1))";
    RowSet results = client.queryBuilder().sql(sql).rowSet();
    assertEquals(1, results.rowCount());
    assertEquals(0, results.container().getLast().getField().getChildCount());
    results.clear();

    sql = "SELECT http_request('local.github', null) AS result FROM (values(1))";
    results = client.queryBuilder().sql(sql).rowSet();
    assertEquals(1, results.rowCount());
    assertEquals(0, results.container().getLast().getField().getChildCount());
    results.clear();
  }

  @Test
  public void testPositionalReplacement() {
    String url = "http://somesite.com/{p1}/{p2}/path/{}";
    List<String> params = new ArrayList<>();
    params.add("foo");
    params.add("bar");
    params.add("baz");
    assertEquals("http://somesite.com/foo/bar/path/baz", SimpleHttp.mapPositionalParameters(url, params));
  }

  /**
   * Helper function to start the MockHTTPServer
   * @return Started Mock server
   * @throws IOException If the server cannot start, throws IOException
   */
  public static MockWebServer startServer() throws IOException {
    MockWebServer server = new MockWebServer();
    server.start(MOCK_SERVER_PORT);
    return server;
  }
}
