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

import java.util.concurrent.TimeUnit;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.ClusterTest;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestQueryProfiles extends ClusterTest {

  private static final int TIMEOUT = 3000; // for debugging
  private static final String GOOD_SQL = "SELECT * FROM cp.`employee.json` LIMIT 20";
  private static final String BAD_SQL = "SELECT cast(first_name as int)  FROM cp.`employee.json` where LIMIT 20";
  private static int portNumber;

  private static final OkHttpClient httpClient = new OkHttpClient.Builder()
      .connectTimeout(TIMEOUT, TimeUnit.SECONDS)
      .writeTimeout(TIMEOUT, TimeUnit.SECONDS)
      .readTimeout(TIMEOUT, TimeUnit.SECONDS).build();

  @BeforeClass
  public static void setup() throws Exception {
    ClusterFixtureBuilder builder = new ClusterFixtureBuilder(dirTestWatcher)
        .configProperty(ExecConstants.HTTP_ENABLE, true)
        .configProperty(ExecConstants.HTTP_PORT_HUNT, true);
    startCluster(builder);
    portNumber = cluster.drillbit().getWebServerPort();

    client.runSqlSilently(GOOD_SQL);
    try {
      client.runSqlSilently(BAD_SQL);
    } catch (Exception ex) {
      // "Error: SYSTEM ERROR: NumberFormatException: Sheri", as intended.
    }
  }

  @Test
  public void testCompletedProfiles() throws Exception {
    String url = String.format("http://localhost:%d/profiles/completed.json", portNumber);
    Request request = new Request.Builder().url(url).build();
    try (Response response = httpClient.newCall(request).execute()) {
      String responseBody = response.body().string();
      JSONObject jsonData = (JSONObject) new JSONParser().parse(responseBody);
      JSONArray finishedQueries = (JSONArray) jsonData.get("finishedQueries");
      JSONObject firstData = (JSONObject) finishedQueries.get(0);
      JSONObject secondData = (JSONObject) finishedQueries.get(1);

      assertEquals(2, finishedQueries.size());
      assertEquals(BAD_SQL, firstData.get("query").toString());
      assertEquals("Failed", firstData.get("state").toString());
      assertEquals(GOOD_SQL, secondData.get("query").toString());
      assertEquals("Succeeded", secondData.get("state").toString());
    }
  }

  @Test
  public void testQueryProfiles() throws Exception {
    String url = String.format("http://localhost:%d/profiles.json", portNumber);
    Request request = new Request.Builder().url(url).build();
    try (Response response = httpClient.newCall(request).execute()) {
      String responseBody = response.body().string();
      JSONObject jsonBody = (JSONObject) new JSONParser().parse(responseBody);
      JSONArray finishedQueries = (JSONArray) jsonBody.get("finishedQueries");
      JSONObject firstData = (JSONObject) finishedQueries.get(0);
      JSONObject secondData = (JSONObject) finishedQueries.get(1);

      assertEquals(5, jsonBody.size());
      assertEquals("[]", jsonBody.get("runningQueries").toString());
      assertEquals(2, finishedQueries.size());
      assertEquals(BAD_SQL, firstData.get("query").toString());
      assertEquals("Failed", firstData.get("state").toString());
      assertEquals(GOOD_SQL, secondData.get("query").toString());
      assertEquals("Succeeded", secondData.get("state").toString());
    }
  }

  @Test
  public void testRunningProfiles() throws Exception {
    String url = String.format("http://localhost:%d/profiles/running.json", portNumber);
    Request request = new Request.Builder().url(url).build();
    try (Response response = httpClient.newCall(request).execute()) {
      String responseBody = response.body().string();
      JSONObject jsonData = (JSONObject) new JSONParser().parse(responseBody);
      assertEquals(4, jsonData.size());
      assertEquals("[]", jsonData.get("runningQueries").toString());
    }
  }
}
