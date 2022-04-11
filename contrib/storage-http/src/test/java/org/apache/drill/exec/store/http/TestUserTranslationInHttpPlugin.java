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

import okhttp3.Cookie;
import okhttp3.CookieJar;
import okhttp3.FormBody;
import okhttp3.Headers;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.apache.commons.io.FileUtils;
import org.apache.commons.net.util.Base64;
import org.apache.drill.common.config.DrillProperties;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.logical.CredentialedStoragePluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig.AuthMode;
import org.apache.drill.common.logical.security.PlainCredentialsProvider;
import org.apache.drill.common.util.DrillFileUtils;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl;
import org.apache.drill.exec.store.StoragePlugin;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.shaded.guava.com.google.common.base.Charsets;
import org.apache.drill.shaded.guava.com.google.common.io.Files;
import org.apache.drill.test.BaseDirTestWatcher;
import org.apache.drill.test.ClientFixture;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.test.QueryBuilder.QuerySummary;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl.TEST_USER_1;
import static org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl.TEST_USER_1_PASSWORD;
import static org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl.TEST_USER_2;
import static org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl.TEST_USER_2_PASSWORD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


public class TestUserTranslationInHttpPlugin extends ClusterTest {

  private static final int MOCK_SERVER_PORT = 47775;

  private static final int TIMEOUT = 30;
  private final OkHttpClient httpClient = new OkHttpClient.Builder()
    .connectTimeout(TIMEOUT, TimeUnit.SECONDS)
    .writeTimeout(TIMEOUT, TimeUnit.SECONDS)
    .readTimeout(TIMEOUT, TimeUnit.SECONDS)
    .cookieJar(new TestCookieJar())
    .build();

  private static String TEST_JSON_RESPONSE_WITH_DATATYPES;
  private static int portNumber;


  @ClassRule
  public static final BaseDirTestWatcher dirTestWatcher = new BaseDirTestWatcher();

  @After
  public void cleanup() throws Exception {
    FileUtils.cleanDirectory(dirTestWatcher.getStoreDir());
  }

  @BeforeClass
  public static void setup() throws Exception {
    TEST_JSON_RESPONSE_WITH_DATATYPES = Files.asCharSource(DrillFileUtils.getResourceAsFile("/data/response2.json"), Charsets.UTF_8).read();

    ClusterFixtureBuilder builder = new ClusterFixtureBuilder(dirTestWatcher)
      .configProperty(ExecConstants.HTTP_ENABLE, true)
      .configProperty(ExecConstants.HTTP_PORT_HUNT, true)
      .configProperty(ExecConstants.USER_AUTHENTICATOR_IMPL, UserAuthenticatorTestImpl.TYPE)
      .configProperty(ExecConstants.IMPERSONATION_ENABLED, true);

    startCluster(builder);

    portNumber = cluster.drillbit().getWebServerPort();

    HttpApiConfig testEndpoint = HttpApiConfig.builder()
      .url(makeUrl("http://localhost:%d/json"))
      .method("GET")
      .requireTail(false)
      .authType("basic")
      .errorOn400(true)
      .build();

    Map<String, HttpApiConfig> configs = new HashMap<>();
    configs.put("sharedEndpoint", testEndpoint);


    Map<String, String> credentials = new HashMap<>();
    credentials.put("username", "user2user");
    credentials.put("password", "user2pass");

    PlainCredentialsProvider credentialsProvider = new PlainCredentialsProvider(TEST_USER_2, credentials);


    HttpStoragePluginConfig mockStorageConfigWithWorkspace =
      new HttpStoragePluginConfig(false, configs, 2, null, null, "",
        80, "", "", "", null, credentialsProvider, AuthMode.USER_TRANSLATION.name());
    mockStorageConfigWithWorkspace.setEnabled(true);
    cluster.defineStoragePlugin("local", mockStorageConfigWithWorkspace);
  }

  @Test
  public void testEmptyUserCredentials() throws Exception {
    ClientFixture client = cluster.clientBuilder()
      .property(DrillProperties.USER, TEST_USER_1)
      .property(DrillProperties.PASSWORD, TEST_USER_1_PASSWORD)
      .build();

    // First verify that the user has no credentials
    StoragePluginRegistry registry = cluster.storageRegistry();
    StoragePlugin plugin = registry.getPlugin("local");
    PlainCredentialsProvider credentialsProvider = (PlainCredentialsProvider)((CredentialedStoragePluginConfig)plugin.getConfig()).getCredentialsProvider();
    Map<String, String> credentials = credentialsProvider.getCredentials(TEST_USER_1);
    assertNotNull(credentials);
    assertNull(credentials.get("username"));
    assertNull(credentials.get("password"));
  }

  @Test
  public void testQueryWithValidCredentials() throws Exception {
    // This test validates that the correct credentials are sent down to the HTTP API.
    ClientFixture client = cluster.clientBuilder()
      .property(DrillProperties.USER, TEST_USER_2)
      .property(DrillProperties.PASSWORD, TEST_USER_2_PASSWORD)
      .build();

    try (MockWebServer server = startServer()) {
      server.enqueue(new MockResponse()
        .setResponseCode(200)
        .setBody(TEST_JSON_RESPONSE_WITH_DATATYPES));

      String sql = "SELECT * FROM local.sharedEndpoint";
      RowSet results = client.queryBuilder().sql(sql).rowSet();
      assertEquals(results.rowCount(), 2);
      results.clear();

      // Verify correct username/password from endpoint configuration
      RecordedRequest recordedRequest = server.takeRequest();
      Headers headers = recordedRequest.getHeaders();
      assertEquals(headers.get("Authorization"), createEncodedText("user2user", "user2pass") );
    }
  }

  @Test
  public void testQueryWithMissingCredentials() throws Exception {
    // This test validates that the correct credentials are sent down to the HTTP API.
    ClientFixture client = cluster.clientBuilder()
      .property(DrillProperties.USER, TEST_USER_1)
      .property(DrillProperties.PASSWORD, TEST_USER_1_PASSWORD)
      .build();

    try (MockWebServer server = startServer()) {
      server.enqueue(new MockResponse()
        .setResponseCode(200)
        .setBody(TEST_JSON_RESPONSE_WITH_DATATYPES));

      String sql = "SELECT * FROM local.sharedEndpoint";
      try {
        client.queryBuilder().sql(sql).run();
        fail();
      } catch (UserException e) {
        assertTrue(e.getMessage().contains("You do not have valid credentials for this API."));
      }
    }
  }

  private boolean makeLoginRequest(String username, String password) throws IOException {
    String loginURL =  "http://localhost:" + portNumber + "/j_security_check";

    RequestBody formBody = new FormBody.Builder()
      .add("j_username", username)
      .add("j_password", password)
      .build();

    Request request = new Request.Builder()
      .url(loginURL)
      .post(formBody)
      .addHeader("Content-Type", "application/x-www-form-urlencoded")
      .addHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
      .build();

    Response response = httpClient.newCall(request).execute();
    return response.code() == 200;
  }

  @Test
  public void testUnrelatedQueryWithUser() throws Exception {
    // This test verifies that a query with a user that does NOT have credentials
    // for a plugin using user translation will still execute.
    ClientFixture client = cluster.clientBuilder()
      .property(DrillProperties.USER, TEST_USER_1)
      .property(DrillProperties.PASSWORD, TEST_USER_1_PASSWORD)
      .build();

    String sql = "SHOW FILES IN dfs";
    QuerySummary result = client.queryBuilder().sql(sql).run();
    assertTrue(result.succeeded());
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

  public static String makeUrl(String url) {
    return String.format(url, MOCK_SERVER_PORT);
  }

  private static String createEncodedText(String username, String password) {
    String pair = username + ":" + password;
    byte[] encodedBytes = Base64.encodeBase64(pair.getBytes());
    return "Basic " + new String(encodedBytes);
  }

  public static class TestCookieJar implements CookieJar {

    private List<Cookie> cookies;

    @Override
    public void saveFromResponse(HttpUrl url, List<Cookie> cookies) {
      this.cookies =  cookies;
    }

    @Override
    public List<Cookie> loadForRequest(HttpUrl url) {
      if (cookies != null) {
        return cookies;
      }
      return new ArrayList<>();
    }
  }


}
