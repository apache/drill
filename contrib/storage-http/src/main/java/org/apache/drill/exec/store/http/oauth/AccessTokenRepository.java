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

import okhttp3.OkHttpClient.Builder;
import okhttp3.OkHttpClient;
import okhttp3.Request;

import org.apache.commons.lang3.StringUtils;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.http.HttpOAuthConfig;
import org.apache.drill.exec.store.http.HttpStoragePluginConfig;
import org.apache.drill.exec.store.http.util.HttpOAuthUtils;
import org.apache.drill.exec.store.http.util.HttpProxyConfig;
import org.apache.drill.exec.store.http.util.SimpleHttp;
import org.apache.parquet.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;


public class AccessTokenRepository {

  private static final Logger logger = LoggerFactory.getLogger(AccessTokenRepository.class);
  private String accessToken;
  private final OkHttpClient client;
  private final HttpOAuthConfig oAuthConfig;
  private final StoragePluginRegistry registry;
  private final HttpStoragePluginConfig pluginConfig;

  public AccessTokenRepository(HttpOAuthConfig oAuthConfig,
                               HttpProxyConfig proxyConfig,
                               HttpStoragePluginConfig pluginConfig,
                               StoragePluginRegistry registry) {
    Builder builder = new OkHttpClient.Builder();
    this.oAuthConfig = oAuthConfig;
    this.registry = registry;
    this.pluginConfig = pluginConfig;

    if (oAuthConfig.tokens() != null && oAuthConfig.tokens().containsKey("accessToken")) {
      accessToken = oAuthConfig.tokens().get("accessToken");
    }

    // Add proxy info
    SimpleHttp.addProxyInfo(builder, proxyConfig);
    client = builder.build();
  }

  /**
   * Returns the current access token.  Does not perform an HTTP request.
   * @return The current access token.
   */
  public String getAccessToken() {
    logger.debug("Getting Access token");
    if (accessToken == null) {
      return refreshAccessToken();
    }
    return accessToken;
  }

  /**
   * Refreshes the access token using the code and other information from the HTTP OAuthConfig.
   * This executes a POST request.  This method will throw exceptions if any of the required fields
   * are empty.  This plugin also updates the configuration in the storage plugin registry.
   *
   * In the event that a user submits a request and the access token is expired, the API will
   * return a 401 non-authorized response.  In the event of a 401 response, the AccessTokenAuthenticator will
   * create additional calls to obtain an updated token. This process should be transparent to the user.
   *
   * @return String of the new access token.
   */
  public String refreshAccessToken() {
    Request request;
    logger.debug("Refreshing Access Token.");
    validateKeys();

    // If the refresh token is present process with that
    if (oAuthConfig.tokens().containsKey("refreshToken") &&
      StringUtils.isNotEmpty(oAuthConfig.tokens().get("refreshToken"))) {
      request = HttpOAuthUtils.getAccessTokenRequestFromRefreshToken(oAuthConfig);
    } else {
      request = HttpOAuthUtils.getAccessTokenRequest(oAuthConfig);
    }

    // Update/Refresh the tokens
    Map<String, String> tokens = HttpOAuthUtils.getOAuthTokens(client, request);
    HttpOAuthConfig updatedConfig = new HttpOAuthConfig(oAuthConfig, tokens);

    if (tokens.containsKey("accessToken")) {
      accessToken = tokens.get("accessToken");
    }

    // This null check is here for testing only.  In actual Drill, the registry will not be null.
    if (registry != null) {
      HttpOAuthUtils.updateOAuthTokens(registry, updatedConfig, pluginConfig);
    }
    return accessToken;
  }

  /**
   * Validate the key parts of the OAuth request and throw helpful error messages
   * if anything is missing.
   */
  private void validateKeys() {
    if (Strings.isNullOrEmpty(oAuthConfig.clientID())) {
      throw UserException.validationError()
        .message("The client ID field is missing in your OAuth configuration.")
        .build(logger);
    }

    if (Strings.isNullOrEmpty(oAuthConfig.clientSecret())) {
      throw UserException.validationError()
        .message("The client secret field is missing in your OAuth configuration.")
        .build(logger);
    }

    if (Strings.isNullOrEmpty(oAuthConfig.accessTokenPath())) {
      throw UserException.validationError()
        .message("The access token path field is missing in your OAuth configuration.")
        .build(logger);
    }

    if ( oAuthConfig.tokens() == null ||
      (! oAuthConfig.tokens().containsKey("authorization_code") &&
      Strings.isNullOrEmpty(oAuthConfig.tokens().get("authorizationCode")))) {
      throw UserException.validationError()
        .message("The authorization code is missing in your OAuth configuration.  Please go back to the Drill configuration for this connection" +
          " and get the authorization code.")
        .build(logger);
    }
  }
}
