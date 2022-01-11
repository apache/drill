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

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.logical.security.CredentialsProvider;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.StoragePluginRegistry.PluginException;
import org.apache.drill.exec.store.http.HttpStoragePluginConfig;
import org.apache.drill.exec.store.http.util.HttpProxyConfig;
import org.apache.drill.exec.store.http.util.SimpleHttp;
import org.apache.drill.exec.store.security.OAuthTokenCredentials;
import org.apache.parquet.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;


public class AccessTokenRepository {

  private static final Logger logger = LoggerFactory.getLogger(AccessTokenRepository.class);
  private String accessToken;
  private final String refreshToken;
  private HttpStoragePluginConfig pluginConfig;
  private final OkHttpClient client;
  private final StoragePluginRegistry registry;
  private final OAuthTokenCredentials credentials;
  private final CredentialsProvider credentialsProvider;

  public AccessTokenRepository(HttpProxyConfig proxyConfig,
                               HttpStoragePluginConfig pluginConfig,
                               StoragePluginRegistry registry) {
    Builder builder = new OkHttpClient.Builder();
    this.registry = registry;
    this.pluginConfig = pluginConfig;
    this.credentialsProvider = pluginConfig.getCredentialsProvider();
    this.credentials = new OAuthTokenCredentials(credentialsProvider);
    accessToken = credentials.getAccessToken();
    refreshToken = credentials.getRefreshToken();

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
      try {
        return refreshAccessToken();
      } catch (PluginException e) {
        throw UserException.internalError(e)
          .message("Unable to access storage plugin: " + e.getMessage())
          .build(logger);
      }
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
  public String refreshAccessToken() throws PluginException {
    Request request;
    logger.debug("Refreshing Access Token.");
    validateKeys();

    // If the refresh token is present process with that
    if (! Strings.isNullOrEmpty(refreshToken)) {
      request = OAuthUtils.getAccessTokenRequestFromRefreshToken(pluginConfig.getCredentialsProvider());
    } else {
      throw UserException.connectionError()
        .message("Your connection expired. Please refresh your access token in the Drill configuration.")
        .build(logger);
    }

    // Update/Refresh the tokens
    Map<String, String> updatedTokens = OAuthUtils.getOAuthTokens(client, request);
    credentialsProvider.updateCredentials(OAuthTokenCredentials.ACCESS_TOKEN, updatedTokens.get(OAuthTokenCredentials.ACCESS_TOKEN));

    // If we get a new refresh token, update it as well
    if (updatedTokens.containsKey(OAuthTokenCredentials.REFRESH_TOKEN)) {
      credentialsProvider.updateCredentials(OAuthTokenCredentials.REFRESH_TOKEN, updatedTokens.get(OAuthTokenCredentials.REFRESH_TOKEN));
    }

    if (updatedTokens.containsKey("accessToken")) {
      accessToken = updatedTokens.get("accessToken");
    }

    // This null check is here for testing only.  In actual Drill, the registry will not be null.
    if (registry != null) {
      String name = registry.getPluginByConfig(pluginConfig).getName();
      HttpStoragePluginConfig updatedConfig = new HttpStoragePluginConfig(pluginConfig, pluginConfig.oAuthConfig);
      registry.validatedPut(name, updatedConfig);
      pluginConfig = updatedConfig;
    }
    return accessToken;
  }

  /**
   * Validate the key parts of the OAuth request and throw helpful error messages
   * if anything is missing.
   */
  private void validateKeys() {
    if (Strings.isNullOrEmpty(credentials.getClientID())) {
      throw UserException.validationError()
        .message("The client ID field is missing in your OAuth configuration.")
        .build(logger);
    }

    if (Strings.isNullOrEmpty(credentials.getClientSecret())) {
      throw UserException.validationError()
        .message("The client secret field is missing in your OAuth configuration.")
        .build(logger);
    }

    if (Strings.isNullOrEmpty(credentials.getTokenUri())) {
      throw UserException.validationError()
        .message("The access token path field is missing in your OAuth configuration.")
        .build(logger);
    }
  }
}
