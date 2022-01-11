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

package org.apache.drill.exec.store.http.oauth;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.apache.drill.common.logical.security.CredentialsProvider;
import org.apache.drill.common.logical.security.PlainCredentialsProvider;
import org.apache.drill.common.util.DrillFileUtils;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.store.http.HttpApiConfig;
import org.apache.drill.exec.store.http.HttpOAuthConfig;
import org.apache.drill.exec.store.http.HttpStoragePluginConfig;
import org.apache.drill.exec.store.security.OAuthTokenCredentials;
import org.apache.drill.shaded.guava.com.google.common.base.Charsets;
import org.apache.drill.shaded.guava.com.google.common.io.Files;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.ClusterTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestOAuthProcess extends ClusterTest {

  private static final Logger logger = LoggerFactory.getLogger(TestOAuthProcess.class);
  private static final int MOCK_SERVER_PORT = 47770;

  private static final int TIMEOUT = 30;
  private static final String CONNECTION_NAME = "localOauth";
  private final OkHttpClient httpClient = new OkHttpClient.Builder()
    .connectTimeout(TIMEOUT, TimeUnit.SECONDS)
    .writeTimeout(TIMEOUT, TimeUnit.SECONDS)
    .readTimeout(TIMEOUT, TimeUnit.SECONDS).build();

  private static String ACCESS_TOKEN_RESPONSE;
  private static int portNumber;
  private static String hostname;


  @BeforeClass
  public static void setup() throws Exception {

    ACCESS_TOKEN_RESPONSE = Files.asCharSource(DrillFileUtils.getResourceAsFile("/data/oauth_access_token_response.json"), Charsets.UTF_8).read();

    ClusterFixtureBuilder builder = new ClusterFixtureBuilder(dirTestWatcher)
      .configProperty(ExecConstants.HTTP_ENABLE, true)
      .configProperty(ExecConstants.HTTP_PORT_HUNT, true);
    startCluster(builder);
    portNumber = cluster.drillbit().getWebServerPort();
    hostname = "http://localhost:" + portNumber + "/storage/" + CONNECTION_NAME;

    Map<String, String> creds = new HashMap<>();
    creds.put("clientID", "12345");
    creds.put("clientSecret", "54321");
    creds.put("accessToken", null);
    creds.put("refreshToken", null);
    creds.put(OAuthTokenCredentials.TOKEN_URI, "http://localhost:" + MOCK_SERVER_PORT + "/update_oath2_authtoken");

    CredentialsProvider credentialsProvider = new PlainCredentialsProvider(creds);

    HttpApiConfig connectionConfig = HttpApiConfig.builder()
      .url(hostname + "/update_oath2_authtoken")
      .method("get")
      .requireTail(false)
      .inputType("json")
      .build();

    HttpOAuthConfig oAuthConfig = HttpOAuthConfig.builder()
      .callbackURL(hostname + "/update_oath2_authtoken")
      .build();

    Map<String, HttpApiConfig> configs = new HashMap<>();
    configs.put(CONNECTION_NAME, connectionConfig);

    // Add storage plugin for test OAuth
    HttpStoragePluginConfig mockStorageConfigWithWorkspace =
      new HttpStoragePluginConfig(false, configs, TIMEOUT, "", 80, "", "", "",
        oAuthConfig, credentialsProvider);
    mockStorageConfigWithWorkspace.setEnabled(true);
    cluster.defineStoragePlugin("localOauth", mockStorageConfigWithWorkspace);
  }

  @Test
  public void testAccessToken() throws Exception {
    String url = hostname + "/update_oath2_authtoken?code=ABCDEF";
    Request request = new Request.Builder().url(url).build();

    try (MockWebServer server = startServer()) {
      server.enqueue(new MockResponse().setResponseCode(200).setBody(ACCESS_TOKEN_RESPONSE));
      Response response = httpClient.newCall(request).execute();

      // Verify that the request succeeded w/o error
      assertEquals(200, response.code());

      // Verify that the access and refresh tokens were saved
      HttpStoragePluginConfig updatedConfig = (HttpStoragePluginConfig) cluster.storageRegistry().getPlugin("localOauth").getConfig();
      CredentialsProvider credentialsProvider = updatedConfig.getCredentialsProvider();

      assertEquals("you_have_access", credentialsProvider.getCredentials().get(OAuthTokenCredentials.ACCESS_TOKEN));
      assertEquals("refresh_me", credentialsProvider.getCredentials().get(OAuthTokenCredentials.REFRESH_TOKEN));

    } catch (Exception e) {
      System.out.println(e.getMessage());
      fail();
    }
  }

  /**
   * Helper function to start the MockHTTPServer
   * @return Started Mock server
   * @throws IOException If the server cannot start, throws IOException
   */
  public static MockWebServer startServer () throws IOException {
    MockWebServer server = new MockWebServer();
    server.start(MOCK_SERVER_PORT);
    return server;
  }
}
