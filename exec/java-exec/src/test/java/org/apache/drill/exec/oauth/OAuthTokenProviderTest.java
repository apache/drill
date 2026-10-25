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
package org.apache.drill.exec.oauth;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import org.apache.drill.categories.SecurityTest;
import org.apache.drill.exec.server.rest.auth.DrillUserPrincipal;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.ClusterTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Under {@code USER_TRANSLATION} each user's OAuth tokens live in their own
 * registry, keyed by user name in the persistent store. These tests pin down that
 * the provider resolves a registry by the identity asked for, because handing back
 * a registry belonging to a different user would make that user's queries run with
 * someone else's tokens at the data source.
 */
@Category(SecurityTest.class)
public class OAuthTokenProviderTest extends ClusterTest {

  /**
   * Token tables are held in the cluster's persistent store and outlive each test
   * method, and test order is not fixed. Each test therefore uses its own plugin
   * name so that one test's writes cannot be observed by another.
   */
  private static final String LEAK_PLUGIN = "leak_test_plugin";
  private static final String OVERWRITE_PLUGIN = "overwrite_test_plugin";
  private static final String PATH_PLUGIN = "path_test_plugin";

  @BeforeClass
  public static void setUp() throws Exception {
    ClusterFixtureBuilder builder = ClusterFixture.builder(dirTestWatcher);
    startCluster(builder);
  }

  private static OAuthTokenProvider provider() {
    return cluster.drillbit().getContext().getOauthTokenProvider();
  }

  @Test
  public void testEachUserGetsTheirOwnRegistry() {
    OAuthTokenProvider provider = provider();

    TokenRegistry alice = provider.getOauthTokenRegistry("alice");
    TokenRegistry bob = provider.getOauthTokenRegistry("bob");

    assertNotSame("each user must get their own token registry", alice, bob);
  }

  @Test
  public void testUserRegistryIsNotTheSharedRegistry() {
    OAuthTokenProvider provider = provider();

    TokenRegistry shared = provider.getOauthTokenRegistry(null);
    TokenRegistry carol = provider.getOauthTokenRegistry("carol");

    assertNotSame("a user's registry must not be the shared registry", shared, carol);
  }

  @Test
  public void testRegistryIsStableForTheSameUser() {
    OAuthTokenProvider provider = provider();

    assertSame(provider.getOauthTokenRegistry("dave"), provider.getOauthTokenRegistry("dave"));
    assertSame(provider.getOauthTokenRegistry(null), provider.getOauthTokenRegistry(null));
  }

  /** A blank user name means "no user", i.e. the shared registry. */
  @Test
  public void testBlankUserSelectsTheSharedRegistry() {
    OAuthTokenProvider provider = provider();

    assertSame(provider.getOauthTokenRegistry(null), provider.getOauthTokenRegistry(""));
  }

  /**
   * The behaviour that matters: a token stored for one user must not be readable
   * as another user, nor through the shared registry.
   */
  @Test
  public void testTokensDoNotLeakBetweenUsers() {
    OAuthTokenProvider provider = provider();

    TokenRegistry erinRegistry = provider.getOauthTokenRegistry("erin");
    erinRegistry.createTokenTable(LEAK_PLUGIN);
    erinRegistry.getTokenTable(LEAK_PLUGIN).setAccessToken("erin-access-token");

    TokenRegistry frankRegistry = provider.getOauthTokenRegistry("frank");
    frankRegistry.createTokenTable(LEAK_PLUGIN);

    assertEquals("erin-access-token", erinRegistry.getTokenTable(LEAK_PLUGIN).getAccessToken());
    assertNull("another user must not read these tokens",
      frankRegistry.getTokenTable(LEAK_PLUGIN).getAccessToken());

    TokenRegistry sharedRegistry = provider.getOauthTokenRegistry(null);
    sharedRegistry.createTokenTable(LEAK_PLUGIN);
    assertNull("the shared registry must not expose a user's tokens",
      sharedRegistry.getTokenTable(LEAK_PLUGIN).getAccessToken());
  }

  /**
   * With authentication disabled, queries run as
   * {@link org.apache.drill.exec.server.rest.auth.DrillUserPrincipal#ANONYMOUS_USER}
   * (see {@code DrillRestServer}, which builds the anonymous session's user
   * credentials from that principal). A user translation plugin's REST writes must
   * therefore target the anonymous user's registry and not the shared one, or the
   * tokens would be written where the query path never reads them.
   */
  @Test
  public void testAnonymousUserIsNotTheSharedRegistry() {
    OAuthTokenProvider provider = provider();

    TokenRegistry anonymous = provider.getOauthTokenRegistry(DrillUserPrincipal.ANONYMOUS_USER);
    TokenRegistry shared = provider.getOauthTokenRegistry(null);

    assertNotSame(
      "the anonymous user's registry is a per-user registry, not the shared one",
      shared, anonymous);
  }

  /**
   * A user name becomes part of the store path, so a name carrying a path
   * separator or a parent reference must not be able to reach another identity's
   * path. A name ending in {@code /..} is the dangerous one: unencoded it walks
   * back up to the shared registry's own path.
   */
  @Test
  public void testUserNamesCannotEscapeTheirOwnPath() {
    OAuthTokenProvider provider = provider();

    TokenRegistry traversal = provider.getOauthTokenRegistry("ivan/..");
    traversal.createTokenTable(PATH_PLUGIN);
    traversal.getTokenTable(PATH_PLUGIN).setAccessToken("traversal-access-token");

    TokenRegistry shared = provider.getOauthTokenRegistry(null);
    shared.createTokenTable(PATH_PLUGIN);
    assertNull("a user name must not be able to walk up into the shared registry",
      shared.getTokenTable(PATH_PLUGIN).getAccessToken());

    // A full Kerberos principal contains a separator and must still work, and
    // must not share a path with the user name it starts with.
    TokenRegistry principal = provider.getOauthTokenRegistry("heidi/host@EXAMPLE.COM");
    principal.createTokenTable(PATH_PLUGIN);
    principal.getTokenTable(PATH_PLUGIN).setAccessToken("heidi-access-token");

    TokenRegistry heidi = provider.getOauthTokenRegistry("heidi");
    heidi.createTokenTable(PATH_PLUGIN);
    assertNull("a principal's tokens must not be readable as the bare user name",
      heidi.getTokenTable(PATH_PLUGIN).getAccessToken());
    assertEquals("heidi-access-token",
      principal.getTokenTable(PATH_PLUGIN).getAccessToken());
  }

  /**
   * Writing the shared token table must not disturb a user's own tokens. Before
   * registries were resolved per user, whichever identity was looked up first won
   * and every later lookup returned that same registry.
   */
  @Test
  public void testSharedTokensDoNotOverwriteUserTokens() {
    OAuthTokenProvider provider = provider();

    TokenRegistry graceRegistry = provider.getOauthTokenRegistry("grace");
    graceRegistry.createTokenTable(OVERWRITE_PLUGIN);
    graceRegistry.getTokenTable(OVERWRITE_PLUGIN).setAccessToken("grace-access-token");

    TokenRegistry sharedRegistry = provider.getOauthTokenRegistry(null);
    sharedRegistry.createTokenTable(OVERWRITE_PLUGIN);
    sharedRegistry.getTokenTable(OVERWRITE_PLUGIN).setAccessToken("shared-access-token");

    assertEquals("a shared token write must not clobber a user's tokens",
      "grace-access-token", graceRegistry.getTokenTable(OVERWRITE_PLUGIN).getAccessToken());
    assertEquals("the shared table must hold its own value",
      "shared-access-token", sharedRegistry.getTokenTable(OVERWRITE_PLUGIN).getAccessToken());
  }
}
