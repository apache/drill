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

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.util.DrillFileUtils;
import org.apache.drill.exec.store.http.HttpOAuthConfig;
import org.apache.drill.exec.store.http.TestHttpPlugin;
import org.apache.drill.shaded.guava.com.google.common.base.Charsets;
import org.apache.drill.shaded.guava.com.google.common.io.Files;

import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.junit.BeforeClass;
import org.junit.Test;


import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class TestOAuthAccessTokenRepository extends ClusterTest {

  private static String TEST_OAUTH_TOKEN;
  private static String TEST_OAUTH_TOKEN2;

  @BeforeClass
  public static void setup() throws Exception {
    TEST_OAUTH_TOKEN = Files.asCharSource(DrillFileUtils.getResourceAsFile("/data/oauth-1.json"), Charsets.UTF_8).read();
    TEST_OAUTH_TOKEN2 = Files.asCharSource(DrillFileUtils.getResourceAsFile("/data/oauth-2.json"), Charsets.UTF_8).read();
    startCluster(ClusterFixture.builder(dirTestWatcher));
  }

  @Test
  public void testGetAccessToken() throws Exception {
    try (MockWebServer server = TestHttpPlugin.startServer()) {
      server.enqueue(new MockResponse()
        .setResponseCode(200)
        .setBody(TEST_OAUTH_TOKEN));

      Map<String,String> tokens = new HashMap<>();
      tokens.put("authorizationCode", "DJSFLKSJDLFKJSLDKFJLSKDJFLS");

      HttpOAuthConfig localOAuthConfig = HttpOAuthConfig.builder()
        .clientID("SDJFHJKSHDKFHKSDJHFEWER")
        .clientSecret("DSKFLJLSKDJFLKSDJFLCJKDLEWIURFJCNDSJDHFWEF")
        .callbackURL("http://localhost:8091/storage/clickup" +
          "/update_oath2_authtoken")
        .baseURL("http://localhost:8091/api")
        .tokens(tokens)
        .accessTokenPath("/api/v2/oauth/token").build();

      AccessTokenRepository repo = new AccessTokenRepository(localOAuthConfig, null, null, null);
      String accessToken = repo.refreshAccessToken();
      assertEquals("123456789", accessToken);

      // Get the token again, it should be the same
      accessToken = repo.getAccessToken();
      assertEquals("123456789", accessToken);

      server.enqueue(new MockResponse()
        .setResponseCode(200)
        .setBody(TEST_OAUTH_TOKEN2));

      // Refresh the access token.  It should be different this time.
      accessToken = repo.refreshAccessToken();
      assertEquals("987654321", accessToken);

      // Get the access token again.  It should be the second token.
      accessToken = repo.getAccessToken();
      assertEquals("987654321", accessToken);
    }
  }

  @Test
  public void testMissingConfigItems() {

    Map<String,String> tokens = new HashMap<>();
    tokens.put("authorizationCode", "DJSFLKSJDLFKJSLDKFJLSKDJFLS");

    HttpOAuthConfig oAuthConfig = HttpOAuthConfig.builder()
      .clientSecret("DSKFLJLSKDJFLKSDJFLCJKDLEWIURFJCNDSJDHFWEF")
      .callbackURL("http://localhost:8091/storage/clickup" +
        "/update_oath2_authtoken")
      .baseURL("http://localhost:8091/api")
      .tokens(tokens)
      .accessTokenPath("/v2/oauth/token").build();

    AccessTokenRepository repo = new AccessTokenRepository(oAuthConfig, null, null, null);

    try {
      String token = repo.getAccessToken();
      // Token should be null
      assertNull(token);
      repo.refreshAccessToken();
      fail();
    } catch (UserException e) {
      assert(e.getMessage().contains("The client ID field is missing"));
    }

    oAuthConfig = HttpOAuthConfig.builder()
      .clientID("SDJFHJKSHDKFHKSDJHFEWER")
      .callbackURL("http://localhost:8091/storage/clickup" +
        "/update_oath2_authtoken")
      .baseURL("http://localhost:8091/api")
      .authorizationPath("http://localhost:8091/api")
      .tokens(tokens)
      .accessTokenPath("/v2/oauth/token")
      .build();

    repo = new AccessTokenRepository(oAuthConfig, null, null, null);
    try {
      repo.refreshAccessToken();
      fail();
    } catch (UserException e) {
      assert(e.getMessage().contains("The client secret field is missing"));
    }

    oAuthConfig = HttpOAuthConfig.builder()
      .clientID("SDJFHJKSHDKFHKSDJHFEWER")
      .clientSecret("DSKFLJLSKDJFLKSDJFLCJKDLEWIURFJCNDSJDHFWEF")
      .callbackURL("http://localhost:8091/storage/clickup" +
        "/update_oath2_authtoken")
      .tokens(tokens)
      .build();

    repo = new AccessTokenRepository(oAuthConfig, null, null, null);
    try {
      repo.refreshAccessToken();
      fail();
    } catch (UserException e) {
      assert(e.getMessage().contains("The access token path field is missing"));
    }

    oAuthConfig = HttpOAuthConfig.builder()
      .clientID("SDJFHJKSHDKFHKSDJHFEWER")
      .clientSecret("DSKFLJLSKDJFLKSDJFLCJKDLEWIURFJCNDSJDHFWEF")
      .callbackURL("http://localhost:8091/storage/clickup" +
        "/update_oath2_authtoken")
      .baseURL("http://localhost:8091/api")
      .accessTokenPath("/v2/oauth/token").build();

    repo = new AccessTokenRepository(oAuthConfig, null, null, null);
    try {
      repo.refreshAccessToken();
      fail();
    } catch (UserException e) {
      assert(e.getMessage().contains("The authorization code is missing"));
    }
  }
}
