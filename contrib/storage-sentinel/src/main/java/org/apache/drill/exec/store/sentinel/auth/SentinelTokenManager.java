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

package org.apache.drill.exec.store.sentinel.auth;

import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.FormBody;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.logical.StoragePluginConfig.AuthMode;
import org.apache.drill.common.logical.security.CredentialsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class SentinelTokenManager {
  private static final Logger logger = LoggerFactory.getLogger(SentinelTokenManager.class);
  private static final ObjectMapper mapper = new ObjectMapper();

  private static class TokenCache {
    volatile String accessToken;
    volatile long tokenExpiryTime;
  }

  private final String tenantId;
  private final String clientId;
  private final String clientSecret;
  private final AuthMode authMode;
  private final CredentialsProvider credentialsProvider;
  private final String tokenEndpoint;
  private final OkHttpClient httpClient;

  private volatile String accessToken;
  private volatile long tokenExpiryTime;

  private final ConcurrentHashMap<String, TokenCache> userTokens = new ConcurrentHashMap<>();

  public SentinelTokenManager(String tenantId, String clientId, String clientSecret,
                            AuthMode authMode, CredentialsProvider credentialsProvider) {
    this(tenantId, clientId, clientSecret, authMode, credentialsProvider, null);
  }

  public SentinelTokenManager(String tenantId, String clientId, String clientSecret,
                            AuthMode authMode, CredentialsProvider credentialsProvider,
                            String tokenEndpoint) {
    this.tenantId = tenantId;
    this.clientId = clientId;
    this.clientSecret = clientSecret;
    this.authMode = authMode;
    this.credentialsProvider = credentialsProvider;
    this.tokenEndpoint = tokenEndpoint;
    this.httpClient = new OkHttpClient.Builder()
        .connectTimeout(10, TimeUnit.SECONDS)
        .readTimeout(30, TimeUnit.SECONDS)
        .build();
  }

  public synchronized String getBearerToken() {
    if (accessToken != null && System.currentTimeMillis() < tokenExpiryTime) {
      return accessToken;
    }
    refreshToken(null);
    return accessToken;
  }

  public synchronized String getBearerToken(String username) {
    if (authMode != AuthMode.USER_TRANSLATION || username == null) {
      return getBearerToken();
    }

    TokenCache cache = userTokens.computeIfAbsent(username, k -> new TokenCache());
    if (cache.accessToken != null && System.currentTimeMillis() < cache.tokenExpiryTime) {
      return cache.accessToken;
    }
    refreshToken(username);
    return userTokens.get(username).accessToken;
  }

  private void refreshToken(String username) {
    try {
      String tokenUrl = tokenEndpoint != null
          ? tokenEndpoint
          : String.format("https://login.microsoftonline.com/%s/oauth2/v2.0/token", tenantId);

      FormBody.Builder bodyBuilder = new FormBody.Builder()
          .add("client_id", clientId)
          .add("scope", "https://api.loganalytics.io/.default");

      if (authMode == AuthMode.USER_TRANSLATION) {
        if (username == null || credentialsProvider == null) {
          throw UserException.dataReadError()
              .message("USER_TRANSLATION mode requires user credentials to be configured")
              .build(logger);
        }
        Map<String, String> userCreds = credentialsProvider.getUserCredentials(username);
        if (userCreds == null || userCreds.isEmpty()) {
          throw UserException.dataReadError()
              .message("No credentials found for user %s in USER_TRANSLATION mode", username)
              .build(logger);
        }
        String userPassword = userCreds.get("password");
        if (userPassword == null) {
          throw UserException.dataReadError()
              .message("User password not found for user %s in USER_TRANSLATION mode", username)
              .build(logger);
        }
        bodyBuilder.add("grant_type", "password")
            .add("username", username)
            .add("password", userPassword);
      } else {
        bodyBuilder.add("grant_type", "client_credentials")
            .add("client_secret", clientSecret);
      }

      FormBody body = bodyBuilder.build();

      Request request = new Request.Builder()
          .url(tokenUrl)
          .post(body)
          .build();

      try (Response response = httpClient.newCall(request).execute()) {
        if (!response.isSuccessful()) {
          throw UserException.dataReadError()
              .message("Failed to obtain Azure AD token: HTTP %d", response.code())
              .build(logger);
        }

        String responseBody = response.body().string();
        @SuppressWarnings("unchecked")
        Map<String, Object> tokenResponse = mapper.readValue(responseBody, Map.class);

        String token = (String) tokenResponse.get("access_token");
        int expiresIn = ((Number) tokenResponse.getOrDefault("expires_in", 3600)).intValue();
        long expiryTime = System.currentTimeMillis() + (expiresIn - 60) * 1000L;

        if (authMode == AuthMode.USER_TRANSLATION && username != null) {
          TokenCache cache = userTokens.computeIfAbsent(username, k -> new TokenCache());
          cache.accessToken = token;
          cache.tokenExpiryTime = expiryTime;
          logger.debug("Azure AD token obtained for user {}, expires in {} seconds", username, expiresIn);
        } else {
          this.accessToken = token;
          this.tokenExpiryTime = expiryTime;
          logger.debug("Azure AD token obtained (shared), expires in {} seconds", expiresIn);
        }
      }
    } catch (IOException e) {
      throw UserException.dataReadError(e)
          .message("Error obtaining Azure AD token")
          .build(logger);
    }
  }
}
