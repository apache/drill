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

import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.FormBody;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.StoragePluginRegistry.PluginException;
import org.apache.drill.exec.store.http.HttpOAuthConfig;
import org.apache.drill.exec.store.http.HttpStoragePluginConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class OAuthUtils {
  private static final Logger logger = LoggerFactory.getLogger(OAuthUtils.class);

  /**
   * Crafts a POST response for obtaining an access token and refresh token.
   * @param oAuthConfig The oAuthConfiguration containing the client_id, client_secret, and auth_code.
   * @return A Request Body with the correct parameters for obtaining an access token
   */
  public static RequestBody getPostResponse(HttpOAuthConfig oAuthConfig) {
    return new FormBody.Builder()
      .add("grant_type", "authorization_code")
      .add("client_id", oAuthConfig.clientID())
      .add("client_secret", oAuthConfig.clientSecret())
      .add("code", oAuthConfig.tokens().get("authorizationCode"))
      .build();
  }

  /**
   * Crafts a POST response for refreshing an access token when a refresh token is present.
   * @param oAuthConfig The oAuthConfiguration containing the client_id, client_secret, and auth_code.
   * @return A Request Body with the correct parameters for obtaining an access token
   */
  public static RequestBody getPostResponseForTokenRefresh(HttpOAuthConfig oAuthConfig) {
    return new FormBody.Builder()
      .add("grant_type", "refresh_token")
      .add("client_id", oAuthConfig.clientID())
      .add("client_secret", oAuthConfig.clientSecret())
      .add("refresh_token", oAuthConfig.tokens().get("refreshToken"))
      .build();
  }

  /**
   * Helper method for building the access token URL.
   * @param oAuthConfig The oAuthConfig.
   * @return The URL string for obtaining an Auth Code.
   */
  public static String buildAccessTokenURL(HttpOAuthConfig oAuthConfig) {
    return oAuthConfig.baseURL() + oAuthConfig.accessTokenPath();
  }

  /**
   * Crafts a POST request to obtain an access token.  This method should be used for the initial call
   * to the OAuth API when you are exchanging the authorization code for an access token.
   * @param oAuthConfig The oAuthConfiguration containing the client_id, client_secret, and auth_code.
   * @return A request to obtain the access token.
   */
  public static Request getAccessTokenRequest(HttpOAuthConfig oAuthConfig ) {
    return new Request.Builder()
      .url(buildAccessTokenURL(oAuthConfig))
      .header("Content-Type", "application/json")
      .addHeader("Accept", "application/json")
      .post(getPostResponse(oAuthConfig))
      .build();
  }

  /**
   * Crafts a POST request to obtain an access token.  This method should be used for the additional calls
   * to the OAuth API when you are refreshing the access token. The refresh token must be populated for this
   * to be successful.
   * @param oAuthConfig The oAuthConfiguration containing the client_id, client_secret, and refresh token.
   * @return A request to obtain the access token.
   */
  public static Request getAccessTokenRequestFromRefreshToken(HttpOAuthConfig oAuthConfig) {
    return new Request.Builder()
      .url(buildAccessTokenURL(oAuthConfig))
      .header("Content-Type", "application/json")
      .addHeader("Accept", "application/json")
      .post(getPostResponseForTokenRefresh(oAuthConfig))
      .build();
  }

  /**
   * This function is called in after the user has obtained an OAuth Authorization Code.
   * It returns a map of any tokens returned which should be an access_token and an optional
   * refresh_token.
   * @param client The OkHTTP3 client.
   * @param request The finalized Request to obtain the tokens.  This request should be a POST request
   *                containing a client_id, client_secret, authorization code, and grant type.
   * @return a Map of any tokens returned.
   */
  public static Map<String, String> getOAuthTokens(OkHttpClient client, Request request) {
    String accessToken;
    String refreshToken;
    Map<String, String> tokens = new HashMap<>();

    try {
      Response response = client.newCall(request).execute();
      String responseBody = response.body().string();

      if (!response.isSuccessful()) {
        throw UserException.connectionError()
          .message("Error obtaining access tokens: ")
          .addContext(response.message())
          .addContext("Response code: " + response.code())
          .addContext(response.body().string())
          .build(logger);
      }

      logger.debug("Response: {}", responseBody);
      ObjectMapper mapper = new ObjectMapper();
      Map<String, Object> parsedJson = mapper.readValue(responseBody, Map.class);

      if (parsedJson.containsKey("access_token")) {
        accessToken = (String) parsedJson.get("access_token");
        tokens.put("accessToken", accessToken);
        logger.debug("Successfully added access token");
      } else {
        // Something went wrong here.
        throw UserException.connectionError()
          .message("Error obtaining access token.")
          .addContext(parsedJson.toString())
          .build(logger);
      }

      // Some APIs will return an access token AND a refresh token at the same time. In that case,
      // we will get both tokens and store them in a HashMap.  The refresh token is used when the
      // access token expires.
      if (parsedJson.containsKey("refresh_token")) {
        refreshToken = (String) parsedJson.get("refresh_token");
        tokens.put("refreshToken", refreshToken);
      }
      return tokens;

    } catch (NullPointerException | IOException e) {
      throw UserException.connectionError()
        .message("Error refreshing access OAuth2 access token. " + e.getMessage())
        .build(logger);

    }
  }

  /**
   * This method updates the OAuth tokens which are stored in the plugin config.  It should be called
   * AFTER any token has been refreshed to make sure the tokens are preserved for future use.
   * @param plugins The current StoragePluginRegistry
   * @param oAuthConfig The oAuthConfig with new tokens
   * @param pluginConfig The storage plugin config
   */
  public static void updateOAuthTokens(StoragePluginRegistry plugins, HttpOAuthConfig oAuthConfig, HttpStoragePluginConfig pluginConfig) {
    HttpStoragePluginConfig newConfig = new HttpStoragePluginConfig(pluginConfig, oAuthConfig);
    try {
      String name = plugins.getPluginByConfig(pluginConfig).getName();
      plugins.validatedPut(name, newConfig);
    } catch (PluginException e) {
      throw UserException.connectionError(e)
        .message("Error updating OAuth tokens: " + e.getMessage())
        .build(logger);
    }
  }
}
