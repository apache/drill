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

import okhttp3.Call;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.drill.common.logical.security.CredentialsProvider;
import org.apache.drill.common.logical.security.PlainCredentialsProvider;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.oauth.PersistentTokenTable;
import org.apache.drill.exec.store.StoragePluginRegistry.PluginException;
import org.apache.drill.exec.store.security.oauth.OAuthTokenCredentials;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.ClusterTest;
import org.json.simple.JSONObject;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class TestOAuthTokenUpdate extends ClusterTest {

  public static final MediaType JSON_MEDIA_TYPE = MediaType.get("application/json; charset=utf-8");
  private static final String CONNECTION_NAME = "localOauth";
  private static final int MOCK_SERVER_PORT = 47770;
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
    hostname = "http://localhost:" + portNumber + "/storage/" + CONNECTION_NAME;

    Map<String, String> creds = new HashMap<>();
    creds.put("clientID", "12345");
    creds.put("clientSecret", "54321");
    creds.put("accessToken", null);
    creds.put("refreshToken", null);
    creds.put(OAuthTokenCredentials.TOKEN_URI, "http://localhost:" + MOCK_SERVER_PORT + "/get_access_token");

    CredentialsProvider credentialsProvider = new PlainCredentialsProvider(creds);

    HttpApiConfig connectionConfig = HttpApiConfig.builder()
      .url("http://localhost:" + MOCK_SERVER_PORT + "/getdata")
      .method("get")
      .requireTail(false)
      .inputType("json")
      .build();

    HttpOAuthConfig oAuthConfig = HttpOAuthConfig.builder()
      .callbackURL(hostname + "/update_ouath2_authtoken")
      .build();

    Map<String, HttpApiConfig> configs = new HashMap<>();
    configs.put("test", connectionConfig);

    // Add storage plugin for test OAuth
    HttpStoragePluginConfig mockStorageConfigWithWorkspace =
      new HttpStoragePluginConfig(false, configs, TIMEOUT,null, null, "", 80, "", "", "",
        oAuthConfig, credentialsProvider);
    mockStorageConfigWithWorkspace.setEnabled(true);
    cluster.defineStoragePlugin("localOauth", mockStorageConfigWithWorkspace);
  }

  @Test
  public void testUpdateAccessToken() throws Exception {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put("accessToken", "access_approved");

    RequestBody requestBody = RequestBody.create(jsonObject.toString(), JSON_MEDIA_TYPE);
    Request request = new Request.Builder()
      .url(hostname + "/update_access_token")
      .post(requestBody)
      .build();

    Call call = httpClient.newCall(request);
    Response response = call.execute();
    assertEquals(response.code(), 200);

    PersistentTokenTable tokenTable = getTokenTable();
    assertEquals("access_approved", tokenTable.getAccessToken());
  }

  @Test
  public void testUpdateRefreshToken() throws Exception {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put("refreshToken", "refresh_me");

    RequestBody requestBody = RequestBody.create(jsonObject.toString(), JSON_MEDIA_TYPE);

    Request request = new Request.Builder()
      .url(hostname + "/update_refresh_token")
      .post(requestBody)
      .build();

    Call call = httpClient.newCall(request);
    Response response = call.execute();
    assertEquals(response.code(), 200);

    PersistentTokenTable tokenTable = getTokenTable();
    assertEquals(tokenTable.getRefreshToken(), "refresh_me");
  }


  @Test
  public void testUpdateAllTokens() throws Exception {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put("accessToken", "access_approved");
    jsonObject.put("refreshToken", "refresh_me");

    RequestBody requestBody = RequestBody.create(jsonObject.toString(), JSON_MEDIA_TYPE);

    Request request = new Request.Builder()
      .url(hostname + "/update_oauth_tokens")
      .post(requestBody)
      .build();

    Call call = httpClient.newCall(request);
    Response response = call.execute();
    assertEquals(response.code(), 200);

    PersistentTokenTable tokenTable = getTokenTable();
    assertEquals(tokenTable.getAccessToken(), "access_approved");
    assertEquals(tokenTable.getRefreshToken(), "refresh_me");
  }

  private PersistentTokenTable getTokenTable() throws PluginException {
    PersistentTokenTable tokenTable = ((HttpStoragePlugin) cluster.storageRegistry()
      .getPlugin("localOauth"))
      .getTokenRegistry()
      .getTokenTable("localOauth");

    return tokenTable;
  }
}
