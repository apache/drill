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

import static org.apache.drill.test.rowSet.RowSetUtilities.mapValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.util.DrillFileUtils;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.physical.rowSet.RowSetBuilder;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.shaded.guava.com.google.common.base.Charsets;
import org.apache.drill.shaded.guava.com.google.common.io.Files;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import okio.Buffer;
import okio.Okio;

/**
 * Tests the HTTP Storage plugin. Since the plugin makes use of REST requests,
 * this test class makes use of the okhttp3 MockWebServer to simulate a remote
 * web server. There are two unit tests that make remote REST calls, however
 * these tests are ignored by default.
 * <p>
 * The HTTP reader uses Drill's existing JSON reader class, so the unit tests
 * focus on testing the plugin configurations rather than how well it parses the
 * JSON as this is tested elsewhere.
 */
public class TestHttpPlugin extends ClusterTest {

  private static final int MOCK_SERVER_PORT = 8091;
  private static String TEST_JSON_RESPONSE;

  @BeforeClass
  public static void setup() throws Exception {
    startCluster(ClusterFixture.builder(dirTestWatcher));

    TEST_JSON_RESPONSE = Files.asCharSource(DrillFileUtils.getResourceAsFile("/data/response.json"), Charsets.UTF_8).read();

    dirTestWatcher.copyResourceToRoot(Paths.get("data/"));

    Map<String, String> headers = new HashMap<>();
    headers.put("header1", "value1");
    headers.put("header2", "value2");

    HttpAPIConfig mockConfig = new HttpAPIConfig("http://localhost:8091/", "GET", headers, "basic", "user", "pass",null);

    HttpAPIConfig sunriseConfig = new HttpAPIConfig("https://api.sunrise-sunset.org/", "GET", null, null, null, null, null);

    HttpAPIConfig stockConfig = new HttpAPIConfig("https://api.worldtradingdata.com/api/v1/stock?symbol=SNAP,TWTR,VOD" +
      ".L&api_token=zuHlu2vZaehdZN6GmJdTiVlp7xgZn6gl6sfgmI4G6TY4ej0NLOzvy0TUl4D4", "get", null, null, null, null, null);

    HttpAPIConfig mockPostConfig = new HttpAPIConfig("http://localhost:8091/", "POST", headers, null, null, null,"key1=value1\nkey2=value2");

    Map<String, HttpAPIConfig> configs = new HashMap<>();
    configs.put("stock", stockConfig);
    configs.put("sunrise", sunriseConfig);
    configs.put("mock", mockConfig);
    configs.put("mockpost", mockPostConfig);

    HttpStoragePluginConfig mockStorageConfigWithWorkspace = new HttpStoragePluginConfig(false, configs, 2, "", 80, "", "", "");
    mockStorageConfigWithWorkspace.setEnabled(true);
    cluster.defineStoragePlugin("api", mockStorageConfigWithWorkspace);
  }

  @Test
  public void verifyPluginConfig() throws Exception {
    String sql = "SELECT SCHEMA_NAME, TYPE FROM INFORMATION_SCHEMA.`SCHEMATA` WHERE TYPE='http'";

    RowSet results = client.queryBuilder().sql(sql).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
      .add("SCHEMA_NAME", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
      .add("TYPE", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
      .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
      .addRow("api.mock", "http")
      .addRow("api.mockpost", "http")
      .addRow("api.stock", "http")
      .addRow("api.sunrise", "http")
      .addRow("api", "http")
      .build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  /**
   * Evaluates the HTTP plugin with the results from an API that returns the
   * sunrise/sunset times for a given lat/long and date. API documentation is
   * available here: https://sunrise-sunset.org/api
   *
   * The API returns results in the following format:
   * <pre><code>
   * {
   *       "results":
   *       {
   *         "sunrise":"7:27:02 AM",
   *         "sunset":"5:05:55 PM",
   *         "solar_noon":"12:16:28 PM",
   *         "day_length":"9:38:53",
   *         "civil_twilight_begin":"6:58:14 AM",
   *         "civil_twilight_end":"5:34:43 PM",
   *         "nautical_twilight_begin":"6:25:47 AM",
   *         "nautical_twilight_end":"6:07:10 PM",
   *         "astronomical_twilight_begin":"5:54:14 AM",
   *         "astronomical_twilight_end":"6:38:43 PM"
   *       },
   *        "status":"OK"
   *     }
   * }</code></pre>
   *
   * @throws Exception
   *           Throws exception if something goes awry
   */
  @Test
  @Ignore("Requires Remote Server")
  public void simpleStarQuery() throws Exception {
    String sql = "SELECT * FROM api.sunrise.`/json?lat=36.7201600&lng=-4.4203400&date=2019-10-02`";

    RowSet results = client.queryBuilder().sql(sql).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
      .addMap("results")
      .add("sunrise", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
      .add("sunset", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
      .add("solar_noon", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
      .add("day_length", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
      .add("civil_twilight_begin", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
      .add("civil_twilight_end", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
      .add("nautical_twilight_begin", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
      .add("nautical_twilight_end", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
      .add("astronomical_twilight_begin", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
      .add("astronomical_twilight_end", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
      .resumeSchema()
      .add("status", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
      .build();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
      .addRow( mapValue("6:13:58 AM", "5:59:55 PM", "12:06:56 PM", "11:45:57", "5:48:14 AM", "6:25:38 PM", "5:18:16 AM", "6:55:36 PM", "4:48:07 AM", "7:25:45 PM"), "OK")
      .build();

    int resultCount =  results.rowCount();
    new RowSetComparison(expected).verifyAndClearAll(results);

    assertEquals(1,  resultCount);
  }

  @Test
  @Ignore("Requires Remote Server")
  public void simpleSpecificQuery() throws Exception {
    String sql = "SELECT t1.results.sunrise AS sunrise, t1.results.sunset AS sunset FROM api.sunrise.`/json?lat=36.7201600&lng=-4.4203400&date=2019-10-02` AS t1";

    RowSet results = client.queryBuilder().sql(sql).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
      .add("sunrise", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
      .add("sunset", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
      .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
      .addRow("6:13:58 AM", "5:59:55 PM")
      .build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
   public void testSerDe() throws Exception {
    try (MockWebServer server = startServer()) {

      server.enqueue(
        new MockResponse().setResponseCode(200)
          .setBody(TEST_JSON_RESPONSE)
      );

      String sql = "SELECT COUNT(*) FROM api.mock.`/json?lat=36.7201600&lng=-4.4203400&date=2019-10-02`";
      String plan = queryBuilder().sql(sql).explainJson();
      long cnt = queryBuilder().physical(plan).singletonLong();
      assertEquals("Counts should match",1L, cnt);
    }
  }

  @Test
  public void simpleTestWithMockServer() throws Exception {
    try (MockWebServer server = startServer()) {

      server.enqueue(
        new MockResponse().setResponseCode(200)
          .setBody(TEST_JSON_RESPONSE)
      );

      String sql = "SELECT * FROM api.mock.`json?lat=36.7201600&lng=-4.4203400&date=2019-10-02`";
      RowSet results = client.queryBuilder().sql(sql).rowSet();

      TupleMetadata expectedSchema = new SchemaBuilder()
        .addMap("results")
        .add("sunrise", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
        .add("sunset", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
        .add("solar_noon", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
        .add("day_length", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
        .add("civil_twilight_begin", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
        .add("civil_twilight_end", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
        .add("nautical_twilight_begin", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
        .add("nautical_twilight_end", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
        .add("astronomical_twilight_begin", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
        .add("astronomical_twilight_end", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
        .resumeSchema()
        .add("status", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
        .build();

      RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow(mapValue("6:13:58 AM", "5:59:55 PM", "12:06:56 PM", "11:45:57", "5:48:14 AM", "6:25:38 PM", "5:18:16 AM", "6:55:36 PM", "4:48:07 AM", "7:25:45 PM"), "OK")
        .build();

      int resultCount =  results.rowCount();
      new RowSetComparison(expected).verifyAndClearAll(results);

      assertEquals(1,  resultCount);
    }
  }

  @Test
  public void testPostWithMockServer() throws Exception {
    try (MockWebServer server = startServer()) {

      server.enqueue(
        new MockResponse()
          .setResponseCode(200)
          .setBody(TEST_JSON_RESPONSE)
      );

      String sql = "SELECT * FROM api.mockPost.`json?lat=36.7201600&lng=-4.4203400&date=2019-10-02`";
      RowSet results = client.queryBuilder().sql(sql).rowSet();

      TupleMetadata expectedSchema = new SchemaBuilder()
        .addMap("results")
        .add("sunrise", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
        .add("sunset", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
        .add("solar_noon", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
        .add("day_length", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
        .add("civil_twilight_begin", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
        .add("civil_twilight_end", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
        .add("nautical_twilight_begin", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
        .add("nautical_twilight_end", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
        .add("astronomical_twilight_begin", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
        .add("astronomical_twilight_end", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
        .resumeSchema()
        .add("status", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
        .build();

      RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow(mapValue("6:13:58 AM", "5:59:55 PM", "12:06:56 PM", "11:45:57", "5:48:14 AM", "6:25:38 PM", "5:18:16 AM", "6:55:36 PM", "4:48:07 AM", "7:25:45 PM"), "OK")
        .build();

      int resultCount =  results.rowCount();
      new RowSetComparison(expected).verifyAndClearAll(results);

      RecordedRequest recordedRequest = server.takeRequest();
      assertEquals("POST", recordedRequest.getMethod());
      assertEquals(recordedRequest.getHeader("header1"), "value1");
      assertEquals(recordedRequest.getHeader("header2"), "value2");
      assertEquals(1,  resultCount);
    }
  }

  @Test
  public void specificTestWithMockServer() throws Exception {
    try (MockWebServer server = startServer()) {

      server.enqueue(
        new MockResponse().setResponseCode(200)
          .setBody(TEST_JSON_RESPONSE)
      );

      String sql = "SELECT t1.results.sunrise AS sunrise, t1.results.sunset AS sunset FROM api.mock.`/json?lat=36.7201600&lng=-4.4203400&date=2019-10-02` AS t1";

      RowSet results = client.queryBuilder().sql(sql).rowSet();

      TupleMetadata expectedSchema = new SchemaBuilder()
        .add("sunrise", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
        .add("sunset", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
        .buildSchema();

      RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow("6:13:58 AM", "5:59:55 PM")
        .build();

      new RowSetComparison(expected).verifyAndClearAll(results);
    }
  }

  @Test
  public void testSlowResponse() throws Exception {
    try (MockWebServer server = startServer()) {

      server.enqueue(
        new MockResponse().setResponseCode(200)
          .setBody(TEST_JSON_RESPONSE)
          .throttleBody(64, 4, TimeUnit.SECONDS)
      );

      String sql = "SELECT t1.results.sunrise AS sunrise, t1.results.sunset AS sunset FROM api.mock.`/json?lat=36.7201600&lng=-4.4203400&date=2019-10-02` AS t1";

      try {
        client.queryBuilder().sql(sql).rowSet();
        fail();
      } catch (Exception e) {
        assertTrue(e.getMessage().contains("DATA_READ ERROR: timeout"));
      }
    }
  }

  @Test
  public void testZeroByteResponse() throws Exception {
    try (MockWebServer server = startServer()) {

      server.enqueue(
        new MockResponse().setResponseCode(200)
          .setBody("")
      );

      String sql = "SELECT * FROM api.mock.`/json?lat=36.7201600&lng=-4.4203400&date=2019-10-02`";

      RowSet results = client.queryBuilder().sql(sql).rowSet();
      assertNull(results);
    }
  }

  // Note that, in this test, the response is not empty. Instead, the
  // response has a single row with no columns.
  @Test
  public void testEmptyJSONObjectResponse() throws Exception {
    try (MockWebServer server = startServer()) {

      server.enqueue(
        new MockResponse().setResponseCode(200)
          .setBody("{}")
      );

      String sql = "SELECT * FROM api.mock.`/json?lat=36.7201600&lng=-4.4203400&date=2019-10-02`";

      RowSet results = client.queryBuilder().sql(sql).rowSet();

      TupleMetadata expectedSchema = new SchemaBuilder()
        .buildSchema();

      RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow()
        .build();

      new RowSetComparison(expected).verifyAndClearAll(results);
    }
  }

  @Test
  public void testErrorResponse() throws Exception {
    try (MockWebServer server = startServer()) {

      server.enqueue(
        new MockResponse().setResponseCode(404)
          .setBody("{}")
      );

      String sql = "SELECT * FROM api.mock.`/json?lat=36.7201600&lng=-4.4203400&date=2019-10-02`";

      try {
        client.queryBuilder().sql(sql).rowSet();
        fail();
      } catch (Exception e) {
        assertTrue(e.getMessage().contains("DATA_READ ERROR: Error retrieving data from HTTP Storage Plugin: 404 Client Error"));
      }
    }
  }

  @Test
  public void testHeaders() throws Exception {
    try (MockWebServer server = startServer()) {

      server.enqueue(
        new MockResponse().setResponseCode(200)
          .setBody(TEST_JSON_RESPONSE)
      );

      String sql = "SELECT * FROM api.mock.`json?lat=36.7201600&lng=-4.4203400&date=2019-10-02`";
      RowSet results = client.queryBuilder().sql(sql).rowSet();


      TupleMetadata expectedSchema = new SchemaBuilder()
        .addMap("results")
        .add("sunrise", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
        .add("sunset", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
        .add("solar_noon", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
        .add("day_length", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
        .add("civil_twilight_begin", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
        .add("civil_twilight_end", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
        .add("nautical_twilight_begin", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
        .add("nautical_twilight_end", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
        .add("astronomical_twilight_begin", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
        .add("astronomical_twilight_end", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
        .resumeSchema()
        .add("status", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
        .build();

      RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow( mapValue("6:13:58 AM", "5:59:55 PM", "12:06:56 PM", "11:45:57", "5:48:14 AM", "6:25:38 PM", "5:18:16 AM", "6:55:36 PM", "4:48:07 AM", "7:25:45 PM"), "OK")
        .build();

      int resultCount =  results.rowCount();
      new RowSetComparison(expected).verifyAndClearAll(results);

      assertEquals(1,  resultCount);

      RecordedRequest request = server.takeRequest();
      assertEquals("value1", request.getHeader("header1"));
      assertEquals("value2", request.getHeader("header2"));
      assertEquals("Basic dXNlcjpwYXNz", request.getHeader("Authorization"));
    }
  }

  /**
   * Helper function to convert files to a readable input steam.
   * @param file The input file to be read
   * @return A buffer to the file
   * @throws IOException If the file is unreadable, throws an IOException
   */
  private Buffer fileToBytes(File file) throws IOException {
    Buffer result = new Buffer();
    result.writeAll(Okio.source(file));
    return result;
  }

  /**
   * Helper function to start the MockHTTPServer
   * @return Started Mock server
   * @throws IOException If the server cannot start, throws IOException
   */
  private MockWebServer startServer() throws IOException {
    MockWebServer server = new MockWebServer();
    server.start(MOCK_SERVER_PORT);
    return server;
  }
}
