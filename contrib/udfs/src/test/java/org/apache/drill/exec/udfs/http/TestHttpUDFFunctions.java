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

package org.apache.drill.exec.udfs.http;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.apache.drill.common.util.DrillFileUtils;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.util.HttpUtils;
import org.apache.drill.shaded.guava.com.google.common.base.Charsets;
import org.apache.drill.shaded.guava.com.google.common.io.Files;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.ClusterTest;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestHttpUDFFunctions extends ClusterTest {

  private static final int MOCK_SERVER_PORT = 47770;
  private static String TEST_JSON_RESPONSE;
  private static String TEST_COMPLEX_JSON_RESPONSE;
  private static String DUMMY_URL = "http://localhost:" + MOCK_SERVER_PORT + "/";


  @BeforeClass
  public static void setup() throws Exception {
    ClusterFixtureBuilder builder = ClusterFixture.builder(dirTestWatcher);
    TEST_JSON_RESPONSE = Files.asCharSource(DrillFileUtils.getResourceAsFile("/json/simple.json"), Charsets.UTF_8).read();
    TEST_COMPLEX_JSON_RESPONSE = Files.asCharSource(DrillFileUtils.getResourceAsFile("/json/weather.json"), Charsets.UTF_8).read();
    startCluster(builder);
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
      assertEquals("http://localhost:47770/", recordedRequest.getRequestUrl().toString());
    }
  }

  @Test
  public void testPositionalReplacement() {
    String url = "http://somesite.com/{p1}/{p2}/path/{}";
    List<String> params = new ArrayList<>();
    params.add("foo");
    params.add("bar");
    params.add("baz");
    assertEquals("http://somesite.com/foo/bar/path/baz", HttpUtils.mapPositionalParameters(url, params));
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
