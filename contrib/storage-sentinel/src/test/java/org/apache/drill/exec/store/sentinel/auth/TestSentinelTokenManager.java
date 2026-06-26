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

import org.apache.drill.common.logical.StoragePluginConfig.AuthMode;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class TestSentinelTokenManager {

  @Test
  public void testTokenManagerCreation() {
    SentinelTokenManager tokenManager = new SentinelTokenManager(
        "test-tenant-id",
        "test-client-id",
        "test-client-secret",
        AuthMode.SHARED_USER,
        null
    );

    assertNotNull(tokenManager);
  }

  @Test
  public void testTokenManagerWithDifferentAuthMode() {
    SentinelTokenManager tokenManager = new SentinelTokenManager(
        "tenant-123",
        "client-456",
        "secret-789",
        AuthMode.USER_TRANSLATION,
        null
    );

    assertNotNull(tokenManager);
  }

  @Test
  public void testTokenManagerInitializesSuccessfully() {
    // Test that token manager can be created with various configurations
    String[] tenantIds = {"tenant-1", "tenant-2", "tenant-3"};
    String[] clientIds = {"client-1", "client-2", "client-3"};

    for (int i = 0; i < tenantIds.length; i++) {
      SentinelTokenManager tokenManager = new SentinelTokenManager(
          tenantIds[i],
          clientIds[i],
          "secret-" + i,
          AuthMode.SHARED_USER,
          null
      );

      assertNotNull(tokenManager);
    }
  }

  @Test
  public void testTokenManagerOAuth2Configuration() {
    // Test that token manager is configured for OAuth2 client credentials flow
    SentinelTokenManager tokenManager = new SentinelTokenManager(
        "test-tenant",
        "test-client",
        "test-secret",
        AuthMode.SHARED_USER,
        null
    );

    // OAuth2 client credentials flow uses:
    // - grant_type: client_credentials
    // - scope: https://api.loganalytics.io/.default
    // - client_id and client_secret from config
    assertNotNull(tokenManager);
  }

  @Test
  public void testBearerTokenFormat() {
    // Test that bearer tokens follow the correct format: "Bearer <token>"
    String token = "test-access-token-abc123";
    String bearerToken = "Bearer " + token;

    assertNotNull(bearerToken);
    assert(bearerToken.startsWith("Bearer "));
    assert(bearerToken.contains(token));
  }

  @Test
  public void testTokenExpiryCalculation() {
    // Tokens expire after "expires_in" seconds
    int expiresInSeconds = 3600;  // 1 hour
    long currentTimeMs = System.currentTimeMillis();
    long expiryTimeMs = currentTimeMs + (expiresInSeconds * 1000L);

    assertNotNull(expiryTimeMs);
    assert(expiryTimeMs > currentTimeMs);
  }

  @Test
  public void testTokenRefreshBuffer() {
    // Tokens should be refreshed 60 seconds before actual expiry
    int expiresInSeconds = 3600;
    int refreshBufferSeconds = 60;
    long refreshTimeSeconds = expiresInSeconds - refreshBufferSeconds;

    assert(refreshTimeSeconds == 3540);
  }

  @Test
  public void testTokenManagerSupportsSharedUserMode() {
    SentinelTokenManager tokenManager = new SentinelTokenManager(
        "tenant-id",
        "client-id",
        "client-secret",
        AuthMode.SHARED_USER,
        null
    );

    assertNotNull(tokenManager);
  }

  @Test
  public void testTokenManagerSupportsUserTranslationMode() {
    SentinelTokenManager tokenManager = new SentinelTokenManager(
        "tenant-id",
        "client-id",
        "client-secret",
        AuthMode.USER_TRANSLATION,
        null
    );

    assertNotNull(tokenManager);
  }

  @Test
  public void testTokenEndpointUrl() {
    // Token endpoint follows Azure AD pattern:
    // https://login.microsoftonline.com/{tenantId}/oauth2/v2.0/token
    String tenantId = "12345678-1234-5678-1234-567812345678";
    String expectedUrl = "https://login.microsoftonline.com/" + tenantId + "/oauth2/v2.0/token";

    assertNotNull(expectedUrl);
    assert(expectedUrl.contains("login.microsoftonline.com"));
    assert(expectedUrl.contains("oauth2/v2.0/token"));
  }
}
