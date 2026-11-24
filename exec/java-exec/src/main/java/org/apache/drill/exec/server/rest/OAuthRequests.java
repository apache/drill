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
import org.apache.drill.exec.server.rest.StorageResources.JsonResult;
import org.apache.drill.exec.server.rest.auth.DrillUserPrincipal;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.StoragePluginRegistry.PluginException;
import org.apache.drill.exec.store.http.oauth.OAuthUtils;
import org.apache.drill.exec.store.security.oauth.OAuthTokenCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;
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
                                           DrillUserPrincipal principal) {
    try {
      DrillbitContext context = ((AbstractStoragePlugin) storage.getPlugin(name)).getContext();
      OAuthTokenProvider tokenProvider = context.getOauthTokenProvider();
      PersistentTokenTable tokenTable = tokenProvider.getOauthTokenRegistry(getQueryUser(storage.getPlugin(name).getConfig(), principal)).getTokenTable(name);

      // Set the access token
      tokenTable.setAccessToken(tokens.getAccessToken());

      return Response.status(Status.OK)
        .entity("Access tokens have been updated.")
        .build();
    } catch (TokenAccessDeniedException e) {
      return forbidden(name, e);
    } catch (PluginException e) {
      logger.error("Error when adding tokens to {}", name);
      return Response.status(Status.INTERNAL_SERVER_ERROR)
        .entity(message("Unable to add tokens: %s", e.getMessage()))
        .build();
    }
  }

  public static Response updateRefreshToken(String name, OAuthTokenContainer tokens,
                                            StoragePluginRegistry storage,
                                            DrillUserPrincipal principal) {
    try {
      DrillbitContext context = ((AbstractStoragePlugin) storage.getPlugin(name)).getContext();
      OAuthTokenProvider tokenProvider = context.getOauthTokenProvider();
      PersistentTokenTable tokenTable = tokenProvider.getOauthTokenRegistry(
        getQueryUser(storage.getPlugin(name).getConfig(), principal)).getTokenTable(name);

      // Set the access token
      tokenTable.setRefreshToken(tokens.getRefreshToken());

      return Response.status(Status.OK)
        .entity("Refresh token have been updated.")
        .build();
    } catch (TokenAccessDeniedException e) {
      return forbidden(name, e);
    } catch (PluginException e) {
      logger.error("Error when adding tokens to {}", name);
      return Response.status(Status.INTERNAL_SERVER_ERROR)
        .entity(message("Unable to add tokens: %s", e.getMessage()))
        .build();
    }
  }

  public static Response updateOAuthTokens(String name, OAuthTokenContainer tokenContainer,
                                           StoragePluginRegistry storage,
                                           DrillUserPrincipal principal) {
    try {
      DrillbitContext context = ((AbstractStoragePlugin) storage.getPlugin(name)).getContext();
      OAuthTokenProvider tokenProvider = context.getOauthTokenProvider();
      PersistentTokenTable tokenTable = tokenProvider
        .getOauthTokenRegistry(getQueryUser(storage.getPlugin(name).getConfig(), principal))
        .getTokenTable(name);

      // Set the access and refresh token
      tokenTable.setAccessToken(tokenContainer.getAccessToken());
      tokenTable.setRefreshToken(tokenContainer.getRefreshToken());
      tokenTable.setExpiresIn(tokenContainer.getExpiresIn());

      return Response.status(Status.OK)
        .entity("Access tokens have been updated.")
        .build();
    } catch (TokenAccessDeniedException e) {
      return forbidden(name, e);
    } catch (PluginException e) {
      logger.error("Error when adding tokens to {}", name);
      return Response.status(Status.INTERNAL_SERVER_ERROR)
        .entity(message("Unable to add tokens: %s", e.getMessage()))
        .build();
    }
  }

  public static Response updateAuthToken(String name, String code, HttpServletRequest request,
                                         StoragePluginRegistry storage,
                                         DrillUserPrincipal principal) {
    try {
      // Establish who may store these tokens before exchanging the code. Doing
      // this first means a refused caller cannot make Drill spend the client
      // secret on an outbound request, nor consume a single-use auth code.
      String queryUser = getQueryUser(storage.getPlugin(name).getConfig(), principal);

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
        .getOauthTokenProvider()
        .getOauthTokenRegistry(queryUser);

      // Add a token registry table if none exists
      tokenRegistry.createTokenTable(name);
      PersistentTokenTable tokenTable = tokenRegistry.getTokenTable(name);

      // Add tokens to persistent storage
      tokenTable.setAccessToken(updatedTokens.get(OAuthTokenCredentials.ACCESS_TOKEN));
      tokenTable.setRefreshToken(updatedTokens.get(OAuthTokenCredentials.REFRESH_TOKEN));
      if (updatedTokens.containsKey(OAuthTokenCredentials.EXPIRES_IN)) {
        tokenTable.setExpiresIn(updatedTokens.get(OAuthTokenCredentials.EXPIRES_IN));
      }

      // Get success page
      String successPage = null;
      try (InputStream inputStream = OAuthRequests.class.getResourceAsStream(OAUTH_SUCCESS_PAGE)) {
        if (inputStream == null) {
          return Response.status(Status.OK).entity("You may close this window.").build();
        }
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
    } catch (TokenAccessDeniedException e) {
      return forbidden(name, e);
    } catch (PluginException e) {
      logger.error("Error when adding auth token to {}", name);
      return Response.status(Status.INTERNAL_SERVER_ERROR)
        .entity(message("Unable to add authorization code: %s", e.getMessage()))
        .build();
    }
  }

  private static JsonResult message(String message, Object... args) {
    return new JsonResult(String.format(message, args));
  }

  /**
   * Resolves which token table the caller may write to, and refuses the write if
   * they may not.
   *
   * <p>A {@code USER_TRANSLATION} plugin gives each user their own token table,
   * so the caller gets their own name and can only ever touch their own tokens.
   * Every other auth mode shares one token table across the whole cluster, and
   * those tokens are admin-owned configuration: overwriting them changes the
   * identity that <em>all</em> users' queries run under at the data source. The
   * credential endpoints that reach this code are open to any authenticated
   * user, so the shared table has to be gated on the admin role here rather than
   * left to the resource class.
   *
   * <p>{@link DrillUserPrincipal} is used rather than the {@code SecurityContext}
   * because it is bound to the anonymous admin principal when authentication is
   * disabled, where the security context carries no principal at all.
   *
   * @param config {@link StoragePluginConfig} The current plugin configuration
   * @param principal the caller, never null in practice
   * @return the caller's user name for a user translation plugin, or null to
   *         select the shared token table
   * @throws TokenAccessDeniedException if the caller may not write these tokens
   */
  private static String getQueryUser(StoragePluginConfig config, DrillUserPrincipal principal) {
    if (principal == null) {
      // Only reachable with authentication on but no authenticated principal;
      // with it off the anonymous principal is always supplied.
      throw new TokenAccessDeniedException("Could not determine the current user.");
    }

    if (config.getAuthMode() == AuthMode.USER_TRANSLATION) {
      // The name must be the one queries run under, or tokens would be written
      // to a table the query path never reads. With authentication disabled that
      // is the anonymous user, which is also what the query session is given.
      return principal.getName();
    }

    if (!principal.isAdminUser()) {
      throw new TokenAccessDeniedException(
        "Only an administrator may change the shared OAuth tokens of a storage plugin.");
    }
    return null;
  }

  /**
   * Thrown when the caller is not permitted to write the token table they asked
   * for. Carries the message sent back to the client.
   */
  private static class TokenAccessDeniedException extends RuntimeException {
    TokenAccessDeniedException(String message) {
      super(message);
    }
  }

  private static Response forbidden(String name, TokenAccessDeniedException e) {
    logger.warn("Refused an OAuth token update for {}: {}", name, e.getMessage());
    return Response.status(Status.FORBIDDEN)
      .entity(message("%s", e.getMessage()))
      .build();
  }
}
