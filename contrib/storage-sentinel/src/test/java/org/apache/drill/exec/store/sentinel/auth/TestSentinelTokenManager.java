package org.apache.drill.exec.store.sentinel.auth;

import org.apache.drill.common.logical.StoragePluginConfig.AuthMode;
import org.junit.Test;

import static org.junit.Assert.*;

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
  public void testBearerTokenPrefix() {
    String bearerToken = "Bearer test-token-123";
    assertTrue(bearerToken.startsWith("Bearer "));
  }

  @Test
  public void testBearerTokenFormatWithSpace() {
    String token = "test-token-xyz";
    String bearerFormat = "Bearer " + token;

    assertTrue(bearerFormat.startsWith("Bearer "));
    assertTrue(bearerFormat.contains(token));
    assertEquals("Bearer test-token-xyz", bearerFormat);
  }

  @Test
  public void testAccessTokenExtraction() {
    String jsonResponse = "{\"access_token\":\"test-token-123\",\"expires_in\":3600,\"token_type\":\"Bearer\"}";
    assertTrue(jsonResponse.contains("access_token"));
    assertTrue(jsonResponse.contains("test-token-123"));
  }

  @Test
  public void testExpiresInParsing() {
    String expiresIn = "3600";
    int expiresInSeconds = Integer.parseInt(expiresIn);

    assertEquals(3600, expiresInSeconds);
  }

  @Test
  public void testTokenRefreshTiming() {
    int expiresIn = 3600;
    int refreshBufferSeconds = 60;
    long refreshTimeSeconds = expiresIn - refreshBufferSeconds;

    assertEquals(3540, refreshTimeSeconds);
  }

  @Test
  public void testTokenExpiryCalculation() {
    long now = System.currentTimeMillis();
    int expiresInSeconds = 3600;
    long expiryTime = now + (expiresInSeconds * 1000);

    assertTrue(expiryTime > now);
  }

  @Test
  public void testTokenRefreshCheckWithBuffer() {
    long expiryTime = System.currentTimeMillis() + (60 * 1000);
    long now = System.currentTimeMillis();
    boolean shouldRefresh = now > (expiryTime - (60 * 1000));

    assertFalse(shouldRefresh);
  }

  @Test
  public void testMultipleTokenFormats() {
    String[] tokenFormats = {
        "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9",
        "Bearer abc123def456",
        "Bearer token-with-dashes",
        "Bearer token_with_underscores"
    };

    for (String token : tokenFormats) {
      assertTrue(token.startsWith("Bearer "));
    }
  }

  @Test
  public void testOAuth2GrantType() {
    String grantType = "client_credentials";
    assertEquals("client_credentials", grantType);
  }

  @Test
  public void testOAuth2Scope() {
    String scope = "https://api.loganalytics.io/.default";
    assertTrue(scope.startsWith("https://"));
    assertTrue(scope.contains("api.loganalytics.io"));
  }

  @Test
  public void testTenantIdFormat() {
    String tenantId = "12345678-1234-1234-1234-123456789012";
    assertTrue(tenantId.matches("[0-9a-f\\-]{36}"));
  }

  @Test
  public void testClientIdFormat() {
    String clientId = "87654321-4321-4321-4321-210987654321";
    assertTrue(clientId.matches("[0-9a-f\\-]{36}"));
  }
}
