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
package org.apache.drill.exec.server.rest.spnego;

import com.google.common.collect.Lists;
import com.typesafe.config.ConfigValueFactory;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.commons.codec.binary.Base64;
import org.apache.drill.categories.SecurityTest;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.rpc.security.KerberosHelper;
import org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl;
import org.apache.drill.exec.server.rest.WebServerConstants;
import org.apache.drill.exec.server.rest.auth.SpnegoConfig;
import org.apache.drill.test.BaseDirTestWatcher;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.ClusterTest;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.hadoop.security.authentication.util.KerberosUtil;
import org.apache.kerby.kerberos.kerb.client.JaasKrbUtil;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import javax.security.auth.Subject;
import java.lang.reflect.Field;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Integration test for validating SPNEGO authentication using a real Drill server and HTTP client.
 * This test starts a real Drill cluster with SPNEGO enabled and uses OkHttpClient to make actual HTTP requests.
 */
@Category(SecurityTest.class)
public class TestDrillSpnegoAuthenticator extends ClusterTest {

  private static KerberosHelper spnegoHelper;
  private static final String primaryName = "HTTP";
  private static int portNumber;
  private static final int TIMEOUT = 3000;

  private static final OkHttpClient httpClient = new OkHttpClient.Builder()
      .connectTimeout(TIMEOUT, TimeUnit.MILLISECONDS)
      .writeTimeout(TIMEOUT, TimeUnit.MILLISECONDS)
      .readTimeout(TIMEOUT, TimeUnit.MILLISECONDS)
      .followRedirects(false) // Don't follow redirects automatically for SPNEGO testing
      .build();

  @BeforeClass
  public static void setupTest() throws Exception {
    spnegoHelper = new KerberosHelper(TestDrillSpnegoAuthenticator.class.getSimpleName(), primaryName);
    spnegoHelper.setupKdc(BaseDirTestWatcher.createTempDir(dirTestWatcher.getTmpDir()));

    // Reset the default realm
    final Field defaultRealm = KerberosName.class.getDeclaredField("defaultRealm");
    defaultRealm.setAccessible(true);
    defaultRealm.set(null, KerberosUtil.getDefaultRealm());

    // Start Drill cluster with SPNEGO authentication enabled for HTTP
    // We also need to enable user authentication and provide an RPC authenticator
    // even though we're only testing HTTP authentication
    ClusterFixtureBuilder builder = new ClusterFixtureBuilder(dirTestWatcher)
        .configProperty(ExecConstants.HTTP_ENABLE, true)
        .configProperty(ExecConstants.HTTP_PORT_HUNT, true)
        .configProperty(ExecConstants.USER_AUTHENTICATION_ENABLED, true)
        .configProperty(ExecConstants.USER_AUTHENTICATOR_IMPL, UserAuthenticatorTestImpl.TYPE)
        .configNonStringProperty(ExecConstants.HTTP_AUTHENTICATION_MECHANISMS,
            ConfigValueFactory.fromIterable(Lists.newArrayList("spnego")))
        .configProperty(ExecConstants.HTTP_SPNEGO_PRINCIPAL, spnegoHelper.SERVER_PRINCIPAL)
        .configProperty(ExecConstants.HTTP_SPNEGO_KEYTAB, spnegoHelper.serverKeytab.toString());

    // Build the cluster
    cluster = builder.build();
    portNumber = cluster.drillbit().getWebServerPort();

    // Create a client with authentication credentials
    // UserAuthenticatorTestImpl accepts specific hardcoded username/password combinations
    client = cluster.clientBuilder()
        .property(org.apache.drill.common.config.DrillProperties.USER, UserAuthenticatorTestImpl.TEST_USER_1)
        .property(org.apache.drill.common.config.DrillProperties.PASSWORD, UserAuthenticatorTestImpl.TEST_USER_1_PASSWORD)
        .build();
  }

  @AfterClass
  public static void cleanTest() throws Exception {
    spnegoHelper.stopKdc();
  }

  /**
   * Helper method to generate a valid SPNEGO token for authentication.
   */
  private String generateSpnegoToken() throws Exception {
    final Subject clientSubject = JaasKrbUtil.loginUsingKeytab(spnegoHelper.CLIENT_PRINCIPAL,
        spnegoHelper.clientKeytab.getAbsoluteFile());

    return Subject.doAs(clientSubject, (PrivilegedExceptionAction<String>) () -> {
      final GSSManager gssManager = GSSManager.getInstance();
      GSSContext gssContext = null;
      try {
        final Oid oid = new Oid(SpnegoConfig.GSS_SPNEGO_MECH_OID);
        final GSSName serviceName = gssManager.createName(spnegoHelper.SERVER_PRINCIPAL, GSSName.NT_USER_NAME, oid);

        gssContext = gssManager.createContext(serviceName, oid, null, GSSContext.DEFAULT_LIFETIME);
        gssContext.requestCredDeleg(true);
        gssContext.requestMutualAuth(true);

        byte[] outToken = new byte[0];
        outToken = gssContext.initSecContext(outToken, 0, outToken.length);
        return Base64.encodeBase64String(outToken);
      } finally {
        if (gssContext != null) {
          gssContext.dispose();
        }
      }
    });
  }

  /**
   * Test to verify response when request is sent for {@link WebServerConstants#SPENGO_LOGIN_RESOURCE_PATH} from
   * an unauthenticated session. Expectation is client will receive 401 response with WWW-Authenticate: Negotiate header.
   */
  @Test
  public void testNewSessionReqForSpnegoLogin() throws Exception {
    // Send request without authentication header
    String url = String.format("http://localhost:%d%s", portNumber, WebServerConstants.SPENGO_LOGIN_RESOURCE_PATH);
    Request request = new Request.Builder()
        .url(url)
        .build();

    try (Response response = httpClient.newCall(request).execute()) {
      // Verify server challenges for authentication
      assertEquals("Expected 401 Unauthorized for unauthenticated request",
          401, response.code());

      // Verify the server sends back a WWW-Authenticate header with Negotiate challenge
      String wwwAuthenticate = response.header("WWW-Authenticate");
      assertTrue("Expected WWW-Authenticate: Negotiate header",
          wwwAuthenticate != null && wwwAuthenticate.contains("Negotiate"));
    }
  }

  /**
   * Test to verify response when request is sent for {@link WebServerConstants#SPENGO_LOGIN_RESOURCE_PATH} with
   * valid SPNEGO credentials. Expectation is server will authenticate successfully and return 200 OK.
   */
  @Test
  public void testAuthClientRequestForSpnegoLoginResource() throws Exception {
    // Generate valid SPNEGO token
    String token = generateSpnegoToken();

    // Send authenticated request to SPNEGO login endpoint
    String url = String.format("http://localhost:%d%s", portNumber, WebServerConstants.SPENGO_LOGIN_RESOURCE_PATH);
    Request request = new Request.Builder()
        .url(url)
        .header("Authorization", "Negotiate " + token)
        .build();

    try (Response response = httpClient.newCall(request).execute()) {
      // Verify successful authentication
      assertEquals("Expected 200 OK for valid SPNEGO authentication",
          200, response.code());

      // Verify we received a Set-Cookie header to establish a session
      String setCookie = response.header("Set-Cookie");
      assertTrue("Expected Set-Cookie header to establish session, but got: " + setCookie,
          setCookie != null && (setCookie.contains("JSESSIONID") || setCookie.contains("Drill-Session-Id")));
    }
  }

  /**
   * Test to verify that once authenticated via SPNEGO, the session can be used to access other resources
   * without re-authenticating. This validates session persistence after initial SPNEGO authentication.
   */
  @Test
  public void testAuthClientRequestForOtherPage() throws Exception {
    // First, authenticate via SPNEGO login endpoint
    String token = generateSpnegoToken();
    String loginUrl = String.format("http://localhost:%d%s", portNumber, WebServerConstants.SPENGO_LOGIN_RESOURCE_PATH);
    Request loginRequest = new Request.Builder()
        .url(loginUrl)
        .header("Authorization", "Negotiate " + token)
        .build();

    String sessionCookie;
    try (Response loginResponse = httpClient.newCall(loginRequest).execute()) {
      assertEquals("Expected successful authentication", 200, loginResponse.code());

      // Extract the session cookie
      sessionCookie = loginResponse.header("Set-Cookie");
      assertTrue("Expected session cookie, but got: " + sessionCookie,
          sessionCookie != null && (sessionCookie.contains("JSESSIONID") || sessionCookie.contains("Drill-Session-Id")));

      // Extract just the session cookie part (either JSESSIONID or Drill-Session-Id)
      sessionCookie = sessionCookie.split(";")[0];
    }

    // Now access a different resource using the session cookie (no SPNEGO token needed)
    String otherUrl = String.format("http://localhost:%d/", portNumber);
    Request otherRequest = new Request.Builder()
        .url(otherUrl)
        .header("Cookie", sessionCookie)
        .build();

    try (Response otherResponse = httpClient.newCall(otherRequest).execute()) {
      // Verify we can access the resource with just the session cookie
      assertEquals("Expected 200 OK when accessing resource with valid session",
          200, otherResponse.code());
    }
  }

  /**
   * Test to verify that logout properly invalidates the session. After logout, attempts to access
   * protected resources with the old session cookie should fail with 401 Unauthorized.
   */
  @Test
  public void testAuthClientRequestForLogOut() throws Exception {
    // First, authenticate via SPNEGO
    String token = generateSpnegoToken();
    String loginUrl = String.format("http://localhost:%d%s", portNumber, WebServerConstants.SPENGO_LOGIN_RESOURCE_PATH);
    Request loginRequest = new Request.Builder()
        .url(loginUrl)
        .header("Authorization", "Negotiate " + token)
        .build();

    String sessionCookie;
    try (Response loginResponse = httpClient.newCall(loginRequest).execute()) {
      assertEquals("Expected successful authentication", 200, loginResponse.code());
      sessionCookie = loginResponse.header("Set-Cookie");
      assertTrue("Expected session cookie, but got: " + sessionCookie,
          sessionCookie != null && (sessionCookie.contains("JSESSIONID") || sessionCookie.contains("Drill-Session-Id")));
      sessionCookie = sessionCookie.split(";")[0];
    }

    // Verify we can access a protected resource with the session
    String protectedUrl = String.format("http://localhost:%d/", portNumber);
    Request beforeLogoutRequest = new Request.Builder()
        .url(protectedUrl)
        .header("Cookie", sessionCookie)
        .build();

    try (Response beforeLogoutResponse = httpClient.newCall(beforeLogoutRequest).execute()) {
      assertEquals("Expected 200 OK before logout", 200, beforeLogoutResponse.code());
    }

    // Now logout
    String logoutUrl = String.format("http://localhost:%d%s", portNumber, WebServerConstants.LOGOUT_RESOURCE_PATH);
    Request logoutRequest = new Request.Builder()
        .url(logoutUrl)
        .header("Cookie", sessionCookie)
        .build();

    try (Response logoutResponse = httpClient.newCall(logoutRequest).execute()) {
      // Logout should succeed
      assertTrue("Expected successful logout (200 or redirect)",
          logoutResponse.code() == 200 || logoutResponse.code() == 302 || logoutResponse.code() == 303);
    }

    // Try to access protected resource with the old session cookie - should fail
    Request afterLogoutRequest = new Request.Builder()
        .url(protectedUrl)
        .header("Cookie", sessionCookie)
        .build();

    try (Response afterLogoutResponse = httpClient.newCall(afterLogoutRequest).execute()) {
      // After logout, the session should be invalidated
      assertEquals("Expected 401 Unauthorized after logout with old session",
          401, afterLogoutResponse.code());
    }
  }

  /**
   * Test to verify authentication fails when client sends an invalid SPNEGO token.
   * This test uses a real HTTP client to send a malformed token and verifies the server returns 401 Unauthorized.
   */
  @Test
  public void testSpnegoLoginInvalidToken() throws Exception {
    // Generate a valid token and then corrupt it
    String validToken = generateSpnegoToken();
    String invalidToken = validToken + "INVALID_SUFFIX";

    // Send HTTP request with the corrupted token
    String url = String.format("http://localhost:%d%s", portNumber, WebServerConstants.SPENGO_LOGIN_RESOURCE_PATH);
    Request request = new Request.Builder()
        .url(url)
        .header("Authorization", "Negotiate " + invalidToken)
        .build();

    try (Response response = httpClient.newCall(request).execute()) {
      // Verify authentication failed with 401 Unauthorized
      assertEquals("Expected 401 Unauthorized for invalid SPNEGO token",
          401, response.code());

      // Verify the server sends back a WWW-Authenticate header with Negotiate challenge
      String wwwAuthenticate = response.header("WWW-Authenticate");
      assertTrue("Expected WWW-Authenticate header with Negotiate challenge",
          wwwAuthenticate != null && wwwAuthenticate.startsWith("Negotiate"));
    }
  }
}
