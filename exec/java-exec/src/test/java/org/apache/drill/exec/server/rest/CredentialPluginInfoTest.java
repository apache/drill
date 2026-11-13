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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.drill.common.logical.OAuthConfig;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.common.logical.security.CredentialsProvider;
import org.apache.drill.common.logical.security.PlainCredentialsProvider;
import org.apache.drill.categories.SecurityTest;
import org.apache.drill.test.BaseTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * {@code GET /credentials.json} is reachable by any authenticated user, because
 * supplying one's own data source credentials is a non-admin action. These tests
 * pin down that the response carries only what the caller needs for that, and in
 * particular never another user's credentials nor the plugin config they live in.
 */
@Category(SecurityTest.class)
public class CredentialPluginInfoTest extends BaseTest {

  private static final String ALICE = "alice";
  private static final String BOB = "bob";

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /**
   * Minimal concrete config. Mirrors a real user translation plugin: credentials
   * live in the provider rather than on the config, so {@code directCredentials}
   * is false and the provider would be serialized if the config ever were.
   */
  private static class TestPluginConfig extends StoragePluginConfig {
    TestPluginConfig(CredentialsProvider credentialsProvider, AuthMode authMode, OAuthConfig oAuthConfig) {
      super(credentialsProvider, false, authMode, oAuthConfig);
    }

    @Override
    public String getValue(String key) {
      return null;
    }

    @Override
    public boolean equals(Object o) {
      return this == o;
    }

    @Override
    public int hashCode() {
      return System.identityHashCode(this);
    }
  }

  /**
   * A username/password user translation plugin with two users' credentials
   * stored, plus a shared fallback secret in the provider's shared map.
   *
   * <p>Note the shared map deliberately holds no {@code clientID}: Drill defines
   * an OAuth plugin as one whose credentials carry a client ID, so adding one
   * here would make this an OAuth plugin instead.
   */
  private static StoragePluginConfig twoUserConfig() {
    Map<String, String> shared = new HashMap<>();
    shared.put("password", "shared-fallback-password");

    PlainCredentialsProvider provider = new PlainCredentialsProvider(shared, new HashMap<>());
    provider.setUserCredentials("alice_at_source", "alice_secret", ALICE);
    provider.setUserCredentials("bob_at_source", "bob_secret", BOB);

    StoragePluginConfig config = new TestPluginConfig(provider, StoragePluginConfig.AuthMode.USER_TRANSLATION, null);
    config.setEnabled(true);
    return config;
  }

  private static String serialize(String name, StoragePluginConfig config, String queryUser) throws Exception {
    return MAPPER.writeValueAsString(CredentialPluginInfo.from(name, config, queryUser));
  }

  @Test
  public void testResponseOmitsPluginConfigEntirely() throws Exception {
    String json = serialize("h2", twoUserConfig(), ALICE);

    assertFalse("the plugin config must not be serialized", json.contains("\"config\""));
    assertFalse(json.contains("credentialsProvider"));
    assertFalse(json.contains("userCredentials"));
  }

  @Test
  public void testResponseLeaksNoPasswordsOrOtherUsers() throws Exception {
    String json = serialize("h2", twoUserConfig(), ALICE);

    // The caller's own password is as sensitive as anyone else's: they supplied
    // it, so there is no reason to hand it back.
    assertFalse(json.contains("alice_secret"));
    assertFalse("another user's password must never be disclosed", json.contains("bob_secret"));
    assertFalse("another user's source username must never be disclosed", json.contains("bob_at_source"));
    assertFalse(json.contains(BOB));
  }

  /**
   * A user translation plugin's provider still has a shared credentials map
   * alongside the per-user one. It is admin-owned configuration and must not
   * reach a non-admin either.
   */
  @Test
  public void testResponseLeaksNoSharedSecrets() throws Exception {
    String json = serialize("h2", twoUserConfig(), ALICE);

    assertFalse("a shared secret must never be disclosed to a non-admin",
      json.contains("shared-fallback-password"));
  }

  @Test
  public void testReportsTheCallersOwnCredentialStateOnly() {
    StoragePluginConfig config = twoUserConfig();

    CredentialPluginInfo forAlice = CredentialPluginInfo.from("h2", config, ALICE);
    assertFalse("fixture must be a username/password plugin, not OAuth", forAlice.isOauth());
    assertTrue(forAlice.isHasCredentials());
    assertEquals("alice_at_source", forAlice.getUserName());

    CredentialPluginInfo forBob = CredentialPluginInfo.from("h2", config, BOB);
    assertTrue(forBob.isHasCredentials());
    assertEquals("bob_at_source", forBob.getUserName());
  }

  @Test
  public void testUserWithNoStoredCredentials() {
    CredentialPluginInfo info = CredentialPluginInfo.from("h2", twoUserConfig(), "carol");

    assertFalse(info.isHasCredentials());
    assertNull(info.getUserName());
    assertEquals("h2", info.getName());
    assertTrue(info.isEnabled());
  }

  @Test
  public void testUnresolvedUserReportsNoCredentialState() {
    CredentialPluginInfo info = CredentialPluginInfo.from("h2", twoUserConfig(), null);

    assertFalse(info.isHasCredentials());
    assertNull(info.getUserName());
  }

  /**
   * A credentials provider is not obliged to support per-user credentials; the
   * interface default throws. Listing the plugin must still succeed, or one such
   * plugin would blank the whole page.
   */
  @Test
  public void testProviderWithoutPerUserSupportIsNotFatal() {
    CredentialsProvider noPerUser = new CredentialsProvider() {
      @Override
      public Map<String, String> getCredentials() {
        return new HashMap<>();
      }
    };

    StoragePluginConfig config =
      new TestPluginConfig(noPerUser, StoragePluginConfig.AuthMode.USER_TRANSLATION, null);
    config.setEnabled(true);

    CredentialPluginInfo info = CredentialPluginInfo.from("env_backed", config, ALICE);

    assertEquals("env_backed", info.getName());
    assertFalse(info.isHasCredentials());
    assertNull(info.getUserName());
  }

  /**
   * OAuth plugins keep per-user state as tokens in the token registry rather than
   * as a username, so no credential state is reported. The authorize URL is still
   * built, since the browser needs it to start the flow, and it carries the client
   * ID and callback but no secret.
   */
  @Test
  public void testOauthPluginExposesAuthorizeUrlButNoSecret() throws Exception {
    OAuthConfig oAuthConfig = OAuthConfig.builder()
      .authorizationURL("https://example.com/authorize")
      .callbackURL("https://drill.example.com/callback")
      .scope("read")
      .build();

    Map<String, String> shared = new HashMap<>();
    shared.put("clientID", "shared-client-id");
    shared.put("clientSecret", "shared-client-secret");

    StoragePluginConfig config = new TestPluginConfig(
      new PlainCredentialsProvider(shared, new HashMap<>()),
      StoragePluginConfig.AuthMode.USER_TRANSLATION,
      oAuthConfig);
    config.setEnabled(true);

    CredentialPluginInfo info = CredentialPluginInfo.from("oauth_plugin", config, ALICE);

    assertTrue(info.isOauth());
    assertTrue(info.getAuthorizationUrl().startsWith("https://example.com/authorize?client_id=shared-client-id"));
    assertFalse(info.isHasCredentials());
    assertNull(info.getUserName());

    assertFalse(MAPPER.writeValueAsString(info).contains("shared-client-secret"));
  }
}
