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

import okhttp3.Call;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.ClusterTest;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class TestFileExtensions extends ClusterTest {
  
  private static final int TIMEOUT = 30;
  private static String hostname;

  private final OkHttpClient httpClient = new OkHttpClient.Builder()
    .connectTimeout(TIMEOUT, TimeUnit.SECONDS)
    .writeTimeout(TIMEOUT, TimeUnit.SECONDS)
    .readTimeout(TIMEOUT, TimeUnit.SECONDS).build();


  @BeforeClass
  public static void setup() throws Exception {
    ClusterFixtureBuilder builder = new ClusterFixtureBuilder(dirTestWatcher)
      .configProperty(ExecConstants.HTTP_ENABLE, true)
      .configProperty(ExecConstants.HTTP_PORT_HUNT, true);
    startCluster(builder);
    int portNumber = cluster.drillbit().getWebServerPort();
    hostname = "http://localhost:" + portNumber + "/storage/";
  }

  @Test
  public void testGetAllFileExtensions() throws Exception {
    Request request = new Request.Builder()
      .url(hostname + "extension_list.json")
      .build();

    Call call = httpClient.newCall(request);
    Response response = call.execute();
    assertEquals(response.code(), 200);
    assertEquals("{\n" + "  \"result\" : \"[seq, txt, tbl, tsv, csv, ssv, csvh-test, json, csvh, avro]\"\n" + "}", response.body().string());
  }

  @Test
  public void testGetPluginFileExtensions() throws Exception {
    Request request = new Request.Builder()
      .url(hostname + "dfs/extension_list.json")
      .build();

    Call call = httpClient.newCall(request);
    Response response = call.execute();
    assertEquals(response.code(), 200);
    assertEquals("{\n" + "  \"result\" : \"[seq, txt, tbl, tsv, csv, ssv, csvh-test, json, csvh, avro]\"\n" + "}", response.body().string());
  }
}
