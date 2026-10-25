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

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.xml.bind.annotation.XmlRootElement;

import org.apache.commons.lang3.StringUtils;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Read-only, per-user view of a {@code USER_TRANSLATION} storage plugin, as
 * returned by {@code GET /credentials.json}.
 *
 * <p>This endpoint is reachable by any authenticated user, not just admins,
 * because supplying one's own data source credentials is a non-admin action.
 * It therefore must not expose the plugin configuration: a
 * {@link StoragePluginConfig} serializes its {@code credentialsProvider}, and
 * for a {@code PlainCredentialsProvider} that includes <em>every</em> user's
 * stored username and password plus any shared secrets such as an OAuth
 * client secret. Only the fields the caller needs to act on their own
 * credentials are carried here.
 *
 * <p>Admin-facing plugin configuration is served separately by
 * {@link StorageResources}, which is gated on {@code ADMIN_ROLE} and uses
 * {@link PluginConfigWrapper}.
 */
@XmlRootElement
public class CredentialPluginInfo {
  private static final Logger logger = LoggerFactory.getLogger(CredentialPluginInfo.class);

  private final String name;
  private final boolean enabled;
  private final boolean oauth;
  private final String authorizationUrl;
  private final boolean hasCredentials;
  private final String userName;

  public CredentialPluginInfo(String name, boolean enabled, boolean oauth,
                              String authorizationUrl, boolean hasCredentials,
                              String userName) {
    this.name = name;
    this.enabled = enabled;
    this.oauth = oauth;
    this.authorizationUrl = authorizationUrl;
    this.hasCredentials = hasCredentials;
    this.userName = userName;
  }

  /**
   * Builds the view of {@code config} that {@code queryUser} is allowed to see.
   *
   * @param name the storage config name
   * @param config the stored plugin config, which is <em>not</em> serialized
   * @param queryUser the authenticated caller, or null if not resolvable
   * @return a config-free summary of the plugin for this caller
   */
  public static CredentialPluginInfo from(String name, StoragePluginConfig config, String queryUser) {
    PluginConfigWrapper wrapper = new PluginConfigWrapper(name, config);
    boolean isOauth = wrapper.isOauth();

    // Only built for OAuth plugins; the wrapper logs a warning otherwise. The URL
    // carries the client ID and callback but no secret, and exists precisely so the
    // browser can start the OAuth flow without reading the plugin config.
    String authorizationUrl = isOauth ? wrapper.getAuthorizationURIWithParams() : "";

    // For OAuth plugins the caller's per-user state is a token in the token
    // registry rather than a username, so there is nothing to report here.
    String ownUserName = null;
    boolean credentialsPresent = false;
    if (!isOauth && queryUser != null) {
      try {
        ownUserName = wrapper.getUserName(queryUser);
        credentialsPresent = StringUtils.isNotEmpty(ownUserName);
      } catch (RuntimeException e) {
        // A credentials provider need not support per-user credentials, in which
        // case it throws. That is not fatal: the plugin is still listed, just
        // without an indication of whether this caller has credentials stored.
        logger.warn("Could not read per-user credentials of {} for {}: {}",
          name, queryUser, e.getMessage());
      }
    }

    return new CredentialPluginInfo(name, config.isEnabled(), isOauth,
      authorizationUrl, credentialsPresent, ownUserName);
  }

  public String getName() { return name; }

  /** Whether the plugin is currently enabled in the storage registry. */
  public boolean isEnabled() { return enabled; }

  @JsonProperty("isOauth")
  public boolean isOauth() { return oauth; }

  /**
   * The provider authorize URL, pre-built with client ID, callback and scope,
   * or an empty string for a non-OAuth plugin.
   */
  public String getAuthorizationUrl() { return authorizationUrl; }

  /** Whether the calling user already has credentials stored for this plugin. */
  public boolean isHasCredentials() { return hasCredentials; }

  /**
   * The calling user's own stored username for this plugin, or null. Never
   * another user's, and never a password.
   */
  public String getUserName() { return userName; }
}
