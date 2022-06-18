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

import okhttp3.Headers;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.apache.commons.io.FileUtils;
import org.apache.commons.net.util.Base64;
import org.apache.drill.common.config.DrillProperties;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.logical.OAuthConfig;
import org.apache.drill.common.logical.StoragePluginConfig.AuthMode;
import org.apache.drill.common.logical.security.CredentialsProvider;
import org.apache.drill.common.logical.security.PlainCredentialsProvider;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.util.DrillFileUtils;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.oauth.PersistentTokenTable;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.physical.rowSet.RowSetBuilder;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.StoragePlugin;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.security.oauth.OAuthTokenCredentials;
import org.apache.drill.shaded.guava.com.google.common.base.Charsets;
import org.apache.drill.shaded.guava.com.google.common.io.Files;
import org.apache.drill.test.BaseDirTestWatcher;
import org.apache.drill.test.ClientFixture;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.test.QueryBuilder.QuerySummary;
import org.apache.drill.test.rowSet.RowSetUtilities;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

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
  private static final Logger logger = LoggerFactory.getLogger(TestUserTranslationInHttpPlugin.class);

  private static final int MOCK_SERVER_PORT = 47778;
  private static String TEST_JSON_RESPONSE_WITH_DATATYPES;
  private static String ACCESS_TOKEN_RESPONSE;
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
    ACCESS_TOKEN_RESPONSE = Files.asCharSource(DrillFileUtils.getResourceAsFile("/data/oauth_access_token_response.json"), Charsets.UTF_8).read();

    ClusterFixtureBuilder builder = new ClusterFixtureBuilder(dirTestWatcher)
      .configProperty(ExecConstants.HTTP_ENABLE, true)
      .configProperty(ExecConstants.HTTP_PORT_HUNT, true)
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

    OAuthConfig oAuthConfig = OAuthConfig.builder()
      .callbackURL(makeUrl("http://localhost:%d") + "/update_oauth2_authtoken")
      .build();

    Map<String, String> oauthCreds = new HashMap<>();
    oauthCreds.put("clientID", "12345");
    oauthCreds.put("clientSecret", "54321");
    oauthCreds.put(OAuthTokenCredentials.TOKEN_URI, "http://localhost:" + MOCK_SERVER_PORT + "/get_access_token");

    CredentialsProvider oauthCredentialProvider = new PlainCredentialsProvider(oauthCreds);



    Map<String, HttpApiConfig> configs = new HashMap<>();
    configs.put("sharedEndpoint", testEndpoint);

    Map<String, String> credentials = new HashMap<>();
    credentials.put("username", "user2user");
    credentials.put("password", "user2pass");

    PlainCredentialsProvider credentialsProvider = new PlainCredentialsProvider(TEST_USER_2, credentials);

    HttpStoragePluginConfig mockStorageConfigWithWorkspace = new HttpStoragePluginConfig(false, configs, 2, null, null, "", 80, "", "", "", null, credentialsProvider,
      AuthMode.USER_TRANSLATION.name());
    mockStorageConfigWithWorkspace.setEnabled(true);

    HttpStoragePluginConfig mockOAuthPlugin = new HttpStoragePluginConfig(false, configs, 2, null, null, "", 80, "", "", "", oAuthConfig, oauthCredentialProvider,
      AuthMode.USER_TRANSLATION.name());
    mockOAuthPlugin.setEnabled(true);

    cluster.defineStoragePlugin("local", mockStorageConfigWithWorkspace);
    cluster.defineStoragePlugin("oauth", mockOAuthPlugin);
  }

  @Test
  public void testEmptyUserCredentials() throws Exception {
    ClientFixture client = cluster
      .clientBuilder()
      .property(DrillProperties.USER, TEST_USER_1)
      .property(DrillProperties.PASSWORD, TEST_USER_1_PASSWORD)
      .build();

    // First verify that the user has no credentials
    StoragePluginRegistry registry = cluster.storageRegistry();
    StoragePlugin plugin = registry.getPlugin("local");
    PlainCredentialsProvider credentialsProvider = (PlainCredentialsProvider) plugin.getConfig().getCredentialsProvider();
    Map<String, String> credentials = credentialsProvider.getUserCredentials(TEST_USER_1);
    assertNotNull(credentials);
    assertNull(credentials.get("username"));
    assertNull(credentials.get("password"));
  }

  @Test
  public void testQueryWithValidCredentials() throws Exception {
    // This test validates that the correct credentials are sent down to the HTTP API.
    ClientFixture client = cluster
      .clientBuilder()
      .property(DrillProperties.USER, TEST_USER_2)
      .property(DrillProperties.PASSWORD, TEST_USER_2_PASSWORD)
      .build();

    try (MockWebServer server = startServer()) {
      server.enqueue(new MockResponse().setResponseCode(200).setBody(TEST_JSON_RESPONSE_WITH_DATATYPES));

      String sql = "SELECT * FROM local.sharedEndpoint";
      RowSet results = client.queryBuilder().sql(sql).rowSet();
      assertEquals(results.rowCount(), 2);
      results.clear();

      // Verify correct username/password from endpoint configuration
      RecordedRequest recordedRequest = server.takeRequest();
      Headers headers = recordedRequest.getHeaders();
      assertEquals(headers.get("Authorization"), createEncodedText("user2user", "user2pass"));
    }
  }

  @Test
  public void testQueryWithMissingCredentials() throws Exception {
    // This test validates that the correct credentials are sent down to the HTTP API.
    ClientFixture client = cluster
      .clientBuilder()
      .property(DrillProperties.USER, TEST_USER_1)
      .property(DrillProperties.PASSWORD, TEST_USER_1_PASSWORD)
      .build();

    try (MockWebServer server = startServer()) {
      server.enqueue(new MockResponse().setResponseCode(200).setBody(TEST_JSON_RESPONSE_WITH_DATATYPES));

      String sql = "SELECT * FROM local.sharedEndpoint";
      try {
        client.queryBuilder().sql(sql).run();
        fail();
      } catch (UserException e) {
        assertTrue(e.getMessage().contains("You do not have valid credentials for this API."));
      }
    }
  }

  @Test
  public void testQueryWithOAuth() throws Exception {
    ClientFixture client = cluster
      .clientBuilder()
      .property(DrillProperties.USER, TEST_USER_2)
      .property(DrillProperties.PASSWORD, TEST_USER_2_PASSWORD)
      .build();

    try (MockWebServer server = startServer()) {
      // Get the token table for test user 2, which should be empty
      PersistentTokenTable tokenTable = ((HttpStoragePlugin) cluster.storageRegistry()
        .getPlugin("oauth"))
        .getTokenRegistry(TEST_USER_2)
        .getTokenTable("oauth");

      // Add the access tokens for user 2
      tokenTable.setAccessToken("you_have_access_2");
      tokenTable.setRefreshToken("refresh_me_2");

      assertEquals("you_have_access_2", tokenTable.getAccessToken());
      assertEquals("refresh_me_2", tokenTable.getRefreshToken());

      // Now execute a query and get query results.
      server.enqueue(new MockResponse()
        .setResponseCode(200)
        .setBody(TEST_JSON_RESPONSE_WITH_DATATYPES));

      String sql = "SELECT * FROM oauth.sharedEndpoint";
      RowSet results = queryBuilder().sql(sql).rowSet();

      TupleMetadata expectedSchema = new SchemaBuilder()
        .add("col_1", MinorType.FLOAT8, DataMode.OPTIONAL)
        .add("col_2", MinorType.BIGINT, DataMode.OPTIONAL)
        .add("col_3", MinorType.VARCHAR, DataMode.OPTIONAL)
        .build();

      RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow(1.0, 2, "3.0")
        .addRow(4.0, 5, "6.0")
        .build();

      RowSetUtilities.verify(expected, results);

      // Verify the correct tokens were passed
      RecordedRequest recordedRequest = server.takeRequest();
      String authToken = recordedRequest.getHeader("Authorization");
      assertEquals("you_have_access_2", authToken);
    } catch (Exception e) {
      logger.debug(e.getMessage());
      fail();
    }
  }

  @Test
  public void testUnrelatedQueryWithUser() throws Exception {
    // This test verifies that a query with a user that does NOT have credentials
    // for a plugin using user translation will still execute.
    ClientFixture client = cluster
      .clientBuilder()
      .property(DrillProperties.USER, TEST_USER_1)
      .property(DrillProperties.PASSWORD, TEST_USER_1_PASSWORD)
      .build();

    String sql = "SHOW FILES IN dfs";
    QuerySummary result = client.queryBuilder().sql(sql).run();
    assertTrue(result.succeeded());
  }

  /**
   * Helper function to start the MockHTTPServer
   *
   * @return Started Mock server
   * @throws IOException If the server cannot start, throws IOException
   */
  private static MockWebServer startServer() throws IOException {
    MockWebServer server = new MockWebServer();
    server.start(MOCK_SERVER_PORT);
    return server;
  }

  private static String makeUrl(String url) {
    return String.format(url, MOCK_SERVER_PORT);
  }

  private static String createEncodedText(String username, String password) {
    String pair = username + ":" + password;
    byte[] encodedBytes = Base64.encodeBase64(pair.getBytes());
    return "Basic " + new String(encodedBytes);
  }
}
