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

import org.apache.commons.lang3.StringUtils;
import org.apache.drill.common.AutoCloseables;
import org.apache.drill.exec.server.DrillbitContext;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Class for managing oauth tokens.  Storage plugins will have to manage obtaining the tokens, but
 * these classes handle the storage of access and refresh tokens.
 *
 * <p>Tokens are held in one registry per identity: a registry for each user of a
 * {@code USER_TRANSLATION} plugin, plus one shared registry for plugins that
 * authenticate as a single service account. Each maps to its own path in the
 * persistent store, so a registry must be looked up by the identity whose tokens
 * are wanted and never cached across identities.
 */
public class OAuthTokenProvider implements AutoCloseable {
  private static final String STORAGE_REGISTRY_PATH = "oauth_tokens";

  /**
   * Map key standing in for the shared registry, which has no user. The empty
   * string cannot collide with a user name because a blank name is treated as
   * absent.
   */
  private static final String SHARED_REGISTRY_KEY = "";

  private final DrillbitContext context;

  private final Map<String, PersistentTokenRegistry> oauthTokenRegistries = new ConcurrentHashMap<>();

  public OAuthTokenProvider(DrillbitContext context) {
    this.context = context;
  }

  /**
   * Returns the token registry holding {@code username}'s tokens, creating it on
   * first use. A null or empty user name selects the shared registry used by
   * plugins that do not translate users.
   *
   * <p>The registry is resolved per call: returning a registry belonging to
   * another user would let one user's queries run with another user's tokens,
   * which is precisely what user translation exists to prevent.
   */
  public TokenRegistry getOauthTokenRegistry(String username) {
    String key = StringUtils.isNotEmpty(username) ? username : SHARED_REGISTRY_KEY;

    // computeIfAbsent so that concurrent callers for the same identity share one
    // registry, and so a failure to build one is not cached.
    return oauthTokenRegistries.computeIfAbsent(
      key,
      k -> new PersistentTokenRegistry(context, registryPath(username))
    );
  }

  /**
   * The persistent store path for a user's tokens, or the root path for the
   * shared registry.
   */
  private static String registryPath(String username) {
    return StringUtils.isNotEmpty(username)
      ? STORAGE_REGISTRY_PATH + "/" + encodePathSegment(username)
      : STORAGE_REGISTRY_PATH;
  }

  /**
   * Percent-encodes a user name so that it forms a single path segment. A user
   * name is not restricted to what a store path accepts: a full Kerberos
   * principal ({@code user/host@REALM}) carries a separator, and a name of
   * {@code ..} would walk up the tree, so either could place one identity's
   * tokens in a path belonging to another. {@code .} is encoded as well, which
   * {@link URLEncoder} leaves untouched.
   */
  private static String encodePathSegment(String username) {
    return URLEncoder.encode(username, StandardCharsets.UTF_8).replace(".", "%2E");
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.closeSilently(oauthTokenRegistries.values().toArray(new AutoCloseable[0]));
    oauthTokenRegistries.clear();
  }
}
