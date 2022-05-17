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

import okhttp3.OkHttpClient;
import okhttp3.OkHttpClient.Builder;
import okhttp3.Request;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig.AuthMode;
import org.apache.drill.common.logical.security.CredentialsProvider;
import org.apache.drill.exec.oauth.OAuthTokenProvider;
import org.apache.drill.exec.oauth.PersistentTokenTable;
import org.apache.drill.exec.oauth.TokenRegistry;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.rest.DrillRestServer.UserAuthEnabled;
import org.apache.drill.exec.server.rest.StorageResources.JsonResult;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.StoragePluginRegistry.PluginException;
import org.apache.drill.exec.store.http.oauth.OAuthUtils;
import org.apache.drill.exec.store.security.oauth.OAuthTokenCredentials;
import org.eclipse.jetty.util.resource.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.SecurityContext;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.stream.Collectors;

public class OAuthRequests {

  private static final Logger logger = LoggerFactory.getLogger(OAuthRequests.class);
  private static final String OAUTH_SUCCESS_PAGE = "/rest/storage/success.html";

  public static Response updateAccessToken(String name,
                                           OAuthTokenContainer tokens,
                                           StoragePluginRegistry storage,
                                           UserAuthEnabled authEnabled,
                                           SecurityContext sc) {
    try {
      DrillbitContext context = ((AbstractStoragePlugin) storage.getPlugin(name)).getContext();
      OAuthTokenProvider tokenProvider = context.getoAuthTokenProvider();
      PersistentTokenTable tokenTable = tokenProvider.getOauthTokenRegistry(getQueryUser(storage.getPlugin(name).getConfig(), authEnabled, sc)).getTokenTable(name);

      // Set the access token
      tokenTable.setAccessToken(tokens.getAccessToken());

      return Response.status(Status.OK)
        .entity("Access tokens have been updated.")
        .build();
    } catch (PluginException e) {
      logger.error("Error when adding tokens to {}", name);
      return Response.status(Status.INTERNAL_SERVER_ERROR)
        .entity(message("Unable to add tokens: %s", e.getMessage()))
        .build();
    }
  }

  public static Response updateRefreshToken(String name, OAuthTokenContainer tokens,
                                            StoragePluginRegistry storage, UserAuthEnabled authEnabled,
                                            SecurityContext sc) {
    try {
      DrillbitContext context = ((AbstractStoragePlugin) storage.getPlugin(name)).getContext();
      OAuthTokenProvider tokenProvider = context.getoAuthTokenProvider();
      PersistentTokenTable tokenTable = tokenProvider.getOauthTokenRegistry(
        getQueryUser(storage.getPlugin(name).getConfig(), authEnabled, sc)).getTokenTable(name);

      // Set the access token
      tokenTable.setRefreshToken(tokens.getRefreshToken());

      return Response.status(Status.OK)
        .entity("Refresh token have been updated.")
        .build();
    } catch (PluginException e) {
      logger.error("Error when adding tokens to {}", name);
      return Response.status(Status.INTERNAL_SERVER_ERROR)
        .entity(message("Unable to add tokens: %s", e.getMessage()))
        .build();
    }
  }

  public static Response updateOAuthTokens(String name, OAuthTokenContainer tokenContainer, StoragePluginRegistry storage,
                                           UserAuthEnabled authEnabled, SecurityContext sc) {
    try {
      DrillbitContext context = ((AbstractStoragePlugin) storage.getPlugin(name)).getContext();
      OAuthTokenProvider tokenProvider = context.getoAuthTokenProvider();
      PersistentTokenTable tokenTable = tokenProvider
        .getOauthTokenRegistry(getQueryUser(storage.getPlugin(name).getConfig(), authEnabled, sc))
        .getTokenTable(name);

      // Set the access and refresh token
      tokenTable.setAccessToken(tokenContainer.getAccessToken());
      tokenTable.setRefreshToken(tokenContainer.getRefreshToken());

      return Response.status(Status.OK)
        .entity("Access tokens have been updated.")
        .build();
    } catch (PluginException e) {
      logger.error("Error when adding tokens to {}", name);
      return Response.status(Status.INTERNAL_SERVER_ERROR)
        .entity(message("Unable to add tokens: %s", e.getMessage()))
        .build();
    }
  }

  public static Response updateAuthToken(String name, String code, HttpServletRequest request,
                                         StoragePluginRegistry storage, UserAuthEnabled authEnabled,
                                         SecurityContext sc) {
    try {
      CredentialsProvider credentialsProvider = storage.getPlugin(name).getConfig().getCredentialsProvider();
      String callbackURL = request.getRequestURL().toString();

      // Now exchange the authorization token for an access token
      Builder builder = new OkHttpClient.Builder();
      OkHttpClient client = builder.build();

      Request accessTokenRequest = OAuthUtils.getAccessTokenRequest(credentialsProvider, code, callbackURL);
      Map<String, String> updatedTokens = OAuthUtils.getOAuthTokens(client, accessTokenRequest);

      // Add to token registry
      // If USER_TRANSLATION is enabled, Drill will create a token table for each user.
      TokenRegistry tokenRegistry = ((AbstractStoragePlugin) storage.getPlugin(name))
        .getContext()
        .getoAuthTokenProvider()
        .getOauthTokenRegistry(getQueryUser(storage.getPlugin(name).getConfig(), authEnabled, sc));

      // Add a token registry table if none exists
      tokenRegistry.createTokenTable(name);
      PersistentTokenTable tokenTable = tokenRegistry.getTokenTable(name);

      // Add tokens to persistent storage
      tokenTable.setAccessToken(updatedTokens.get(OAuthTokenCredentials.ACCESS_TOKEN));
      tokenTable.setRefreshToken(updatedTokens.get(OAuthTokenCredentials.REFRESH_TOKEN));

      // Get success page
      String successPage = null;
      try (InputStream inputStream = Resource.newClassPathResource(OAUTH_SUCCESS_PAGE).getInputStream()) {
        InputStreamReader reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
        BufferedReader bufferedReader = new BufferedReader(reader);
        successPage = bufferedReader.lines()
          .collect(Collectors.joining("\n"));
        bufferedReader.close();
        reader.close();
      } catch (IOException e) {
        return Response.status(Status.OK).entity("You may close this window.").build();
      }

      return Response.status(Status.OK).entity(successPage).build();
    } catch (PluginException e) {
      logger.error("Error when adding auth token to {}", name);
      return Response.status(Status.INTERNAL_SERVER_ERROR)
        .entity(message("Unable to add authorization code: %s", e.getMessage()))
        .build();
    }
  }

  private static JsonResult message(String message, Object... args) {
    return new JsonResult(String.format(message, args));  // lgtm [java/tainted-format-string]
  }

  /**
   * This function checks to see if a given storage plugin is using USER_TRANSLATION mode and if user
   * authentication is enabled.  If so, it will return the active user name.  If not it will return null.
   * @param config {@link StoragePluginConfig} The current plugin configuration
   * @return If USER_TRANSLATION is enabled, returns the active user.  If not, returns null.
   */
  private static String getQueryUser(StoragePluginConfig config,
                                     UserAuthEnabled authEnabled,
                                     SecurityContext sc) {
    if (config.getAuthMode() == AuthMode.USER_TRANSLATION && authEnabled.get()) {
      return sc.getUserPrincipal().getName();
    } else {
      return null;
    }
  }
}
