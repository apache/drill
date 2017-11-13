/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.server;


import com.google.common.collect.Lists;
import com.typesafe.config.ConfigValueFactory;
import org.apache.commons.codec.binary.Base64;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.DrillException;
import org.apache.drill.common.scanner.ClassPathScanner;
import org.apache.drill.common.scanner.persistence.ScanResult;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.DrillbitStartupException;
import org.apache.drill.exec.rpc.security.AuthenticatorProviderImpl;
import org.apache.drill.exec.rpc.security.KerberosHelper;
import org.apache.drill.exec.rpc.security.plain.PlainFactory;
import org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl;
import org.apache.drill.exec.server.options.SystemOptionManager;
import org.apache.drill.exec.server.rest.WebServerConstants;
import org.apache.drill.exec.server.rest.auth.DrillHttpSecurityHandlerProvider;
import org.apache.drill.exec.server.rest.auth.DrillSpnegoAuthenticator;
import org.apache.drill.exec.server.rest.auth.DrillSpnegoLoginService;
import org.apache.drill.exec.server.rest.auth.SpnegoUtil;
import org.apache.drill.test.BaseDirTestWatcher;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.hadoop.security.authentication.util.KerberosUtil;
import org.apache.kerby.kerberos.kerb.client.JaasKrbUtil;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.security.Authenticator;
import org.eclipse.jetty.security.DefaultIdentityService;
import org.eclipse.jetty.security.UserAuthentication;
import org.eclipse.jetty.server.Authentication;
import org.eclipse.jetty.server.UserIdentity;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import sun.security.jgss.GSSUtil;

import javax.security.auth.Subject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import java.lang.reflect.Field;
import java.security.PrivilegedExceptionAction;

import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * Test {@link SpnegoUtil}, {@link DrillSpnegoAuthenticator} and {@link DrillSpnegoLoginService}
 */
public class TestSpnegoAuthentication {

  private static KerberosHelper spnegoHelper;

  private static final String primaryName = "HTTP";

  private static final BaseDirTestWatcher dirTestWatcher = new BaseDirTestWatcher();


  @BeforeClass
  public static void setupTest() throws Exception {
    spnegoHelper = new KerberosHelper(TestSpnegoAuthentication.class.getSimpleName(), primaryName);
    spnegoHelper.setupKdc(dirTestWatcher.getTmpDir());


    sun.security.krb5.Config.refresh();

    // (2) Reset the default realm.
    final Field defaultRealm = KerberosName.class.getDeclaredField("defaultRealm");
    defaultRealm.setAccessible(true);
    defaultRealm.set(null, KerberosUtil.getDefaultRealm());
  }

  /**
   * Both SPNEGO and FORM mechanism is enabled for WebServer in configuration. Test to see if the respective security
   * handlers are created successfully or not.
   * @throws Exception
   */
  @Test
  public void testSPNEGOAndFORMEnabled() throws Exception {

    final DrillConfig newConfig = new DrillConfig(DrillConfig.create()
        .withValue(ExecConstants.USER_AUTHENTICATION_ENABLED,
            ConfigValueFactory.fromAnyRef(true))
        .withValue(ExecConstants.HTTP_AUTHENTICATION_MECHANISMS,
            ConfigValueFactory.fromIterable(Lists.newArrayList("form", "spnego")))
        .withValue(ExecConstants.HTTP_SPNEGO_PRINCIPAL,
            ConfigValueFactory.fromAnyRef(spnegoHelper.SERVER_PRINCIPAL))
        .withValue(ExecConstants.HTTP_SPNEGO_KEYTAB,
            ConfigValueFactory.fromAnyRef(spnegoHelper.serverKeytab.toString())),
        false);

    final ScanResult scanResult = ClassPathScanner.fromPrescan(newConfig);
    final AuthenticatorProviderImpl authenticatorProvider = Mockito.mock(AuthenticatorProviderImpl.class);
    Mockito.when(authenticatorProvider.containsFactory(PlainFactory.SIMPLE_NAME)).thenReturn(true);

    final DrillbitContext context = Mockito.mock(DrillbitContext.class);
    Mockito.when(context.getClasspathScan()).thenReturn(scanResult);
    Mockito.when(context.getConfig()).thenReturn(newConfig);
    Mockito.when(context.getAuthProvider()).thenReturn(authenticatorProvider);

    final DrillHttpSecurityHandlerProvider securityProvider = new DrillHttpSecurityHandlerProvider(newConfig, context);
    assertTrue(securityProvider.isFormEnabled());
    assertTrue(securityProvider.isSpnegoEnabled());
  }

  /**
   * Validate if FORM security handler is created successfully when only form is configured as auth mechanism
   * @throws Exception
   */
  @Test
  public void testOnlyFORMEnabled() throws Exception {

    final DrillConfig newConfig = new DrillConfig(DrillConfig.create()
        .withValue(ExecConstants.HTTP_AUTHENTICATION_MECHANISMS,
            ConfigValueFactory.fromIterable(Lists.newArrayList("form")))
        .withValue(ExecConstants.USER_AUTHENTICATION_ENABLED,
            ConfigValueFactory.fromAnyRef(true))
        .withValue(ExecConstants.HTTP_SPNEGO_PRINCIPAL,
            ConfigValueFactory.fromAnyRef(spnegoHelper.SERVER_PRINCIPAL))
        .withValue(ExecConstants.HTTP_SPNEGO_KEYTAB,
            ConfigValueFactory.fromAnyRef(spnegoHelper.serverKeytab.toString())),
        false);

    final ScanResult scanResult = ClassPathScanner.fromPrescan(newConfig);
    final AuthenticatorProviderImpl authenticatorProvider = Mockito.mock(AuthenticatorProviderImpl.class);
    Mockito.when(authenticatorProvider.containsFactory(PlainFactory.SIMPLE_NAME)).thenReturn(true);

    final DrillbitContext context = Mockito.mock(DrillbitContext.class);
    Mockito.when(context.getClasspathScan()).thenReturn(scanResult);
    Mockito.when(context.getConfig()).thenReturn(newConfig);
    Mockito.when(context.getAuthProvider()).thenReturn(authenticatorProvider);

    final DrillHttpSecurityHandlerProvider securityProvider = new DrillHttpSecurityHandlerProvider(newConfig, context);
    assertTrue(securityProvider.isFormEnabled());
    assertTrue(!securityProvider.isSpnegoEnabled());
  }

  /**
   * Validate failure in creating FORM security handler when PAM authenticator is absent. PAM authenticator is provided
   * via {@link PlainFactory#getAuthenticator()}
   * @throws Exception
   */
  @Test
  public void testFORMEnabledWithPlainDisabled() throws Exception {
    try {
      final DrillConfig newConfig = new DrillConfig(DrillConfig.create()
          .withValue(ExecConstants.USER_AUTHENTICATION_ENABLED,
              ConfigValueFactory.fromAnyRef(true))
          .withValue(ExecConstants.HTTP_AUTHENTICATION_MECHANISMS,
              ConfigValueFactory.fromIterable(Lists.newArrayList("form")))
          .withValue(ExecConstants.HTTP_SPNEGO_PRINCIPAL,
              ConfigValueFactory.fromAnyRef(spnegoHelper.SERVER_PRINCIPAL))
          .withValue(ExecConstants.HTTP_SPNEGO_KEYTAB,
              ConfigValueFactory.fromAnyRef(spnegoHelper.serverKeytab.toString())),
          false);

      final ScanResult scanResult = ClassPathScanner.fromPrescan(newConfig);
      final AuthenticatorProviderImpl authenticatorProvider = Mockito.mock(AuthenticatorProviderImpl.class);
      Mockito.when(authenticatorProvider.containsFactory(PlainFactory.SIMPLE_NAME)).thenReturn(false);

      final DrillbitContext context = Mockito.mock(DrillbitContext.class);
      Mockito.when(context.getClasspathScan()).thenReturn(scanResult);
      Mockito.when(context.getConfig()).thenReturn(newConfig);
      Mockito.when(context.getAuthProvider()).thenReturn(authenticatorProvider);

      final DrillHttpSecurityHandlerProvider securityProvider =
          new DrillHttpSecurityHandlerProvider(newConfig, context);
      fail();
    } catch(Exception ex) {
      assertTrue(ex instanceof DrillbitStartupException);
    }
  }

  /**
   * Validate only SPNEGO security handler is configured properly when enabled via configuration
   * @throws Exception
   */
  @Test
  public void testOnlySPNEGOEnabled() throws Exception {

    final DrillConfig newConfig = new DrillConfig(DrillConfig.create()
        .withValue(ExecConstants.HTTP_AUTHENTICATION_MECHANISMS,
            ConfigValueFactory.fromIterable(Lists.newArrayList("spnego")))
        .withValue(ExecConstants.USER_AUTHENTICATION_ENABLED,
            ConfigValueFactory.fromAnyRef(true))
        .withValue(ExecConstants.HTTP_SPNEGO_PRINCIPAL,
            ConfigValueFactory.fromAnyRef(spnegoHelper.SERVER_PRINCIPAL))
        .withValue(ExecConstants.HTTP_SPNEGO_KEYTAB,
            ConfigValueFactory.fromAnyRef(spnegoHelper.serverKeytab.toString())),
        false);

    final ScanResult scanResult = ClassPathScanner.fromPrescan(newConfig);
    final AuthenticatorProviderImpl authenticatorProvider = Mockito.mock(AuthenticatorProviderImpl.class);
    Mockito.when(authenticatorProvider.containsFactory(PlainFactory.SIMPLE_NAME)).thenReturn(false);

    final DrillbitContext context = Mockito.mock(DrillbitContext.class);
    Mockito.when(context.getClasspathScan()).thenReturn(scanResult);
    Mockito.when(context.getConfig()).thenReturn(newConfig);
    Mockito.when(context.getAuthProvider()).thenReturn(authenticatorProvider);

    final DrillHttpSecurityHandlerProvider securityProvider = new DrillHttpSecurityHandlerProvider(newConfig, context);

    assertTrue(!securityProvider.isFormEnabled());
    assertTrue(securityProvider.isSpnegoEnabled());
  }

  /**
   * Validate when none of the security mechanism is specified in the
   * {@link ExecConstants#HTTP_AUTHENTICATION_MECHANISMS}, FORM security handler is still configured correctly when
   * authentication is enabled along with PAM authenticator module.
   * @throws Exception
   */
  @Test
  public void testConfigBackwardCompatibility() throws Exception {

    final DrillConfig newConfig = new DrillConfig(DrillConfig.create()
        .withValue(ExecConstants.USER_AUTHENTICATION_ENABLED,
            ConfigValueFactory.fromAnyRef(true)),
        false);

    final ScanResult scanResult = ClassPathScanner.fromPrescan(newConfig);
    final AuthenticatorProviderImpl authenticatorProvider = Mockito.mock(AuthenticatorProviderImpl.class);
    Mockito.when(authenticatorProvider.containsFactory(PlainFactory.SIMPLE_NAME)).thenReturn(true);

    final DrillbitContext context = Mockito.mock(DrillbitContext.class);
    Mockito.when(context.getClasspathScan()).thenReturn(scanResult);
    Mockito.when(context.getConfig()).thenReturn(newConfig);
    Mockito.when(context.getAuthProvider()).thenReturn(authenticatorProvider);

    final DrillHttpSecurityHandlerProvider securityProvider = new DrillHttpSecurityHandlerProvider(newConfig, context);

    assertTrue(securityProvider.isFormEnabled());
    assertTrue(!securityProvider.isSpnegoEnabled());
  }

  /**
   * Validate behavior of {@link SpnegoUtil} class when provided with different configuration's for SPNEGO via
   * DrillConfig
   * @throws Exception
   */
  @Test
  public void testSpnegoUtil() throws Exception {

    DrillConfig newConfig;
    SpnegoUtil spnegoUtil;

    // Invalid configuration for SPNEGO
    try {
      newConfig = new DrillConfig(DrillConfig.create()
          .withValue(ExecConstants.USER_AUTHENTICATION_ENABLED,
              ConfigValueFactory.fromAnyRef(true))
          .withValue(ExecConstants.AUTHENTICATION_MECHANISMS,
              ConfigValueFactory.fromIterable(Lists.newArrayList("plain")))
          .withValue(ExecConstants.USER_AUTHENTICATOR_IMPL,
              ConfigValueFactory.fromAnyRef(UserAuthenticatorTestImpl.TYPE)),
          false);

      spnegoUtil = new SpnegoUtil(newConfig);
      spnegoUtil.validateSpnegoConfig();
      fail();
    } catch (Exception ex) {
      assertTrue(ex instanceof DrillException);
    }

    // Configuration with keytab only
    try {
      newConfig = new DrillConfig(DrillConfig.create()
          .withValue(ExecConstants.USER_AUTHENTICATION_ENABLED,
              ConfigValueFactory.fromAnyRef(true))
          .withValue(ExecConstants.AUTHENTICATION_MECHANISMS,
              ConfigValueFactory.fromIterable(Lists.newArrayList("plain")))
          .withValue(ExecConstants.HTTP_SPNEGO_KEYTAB,
              ConfigValueFactory.fromAnyRef(spnegoHelper.serverKeytab.toString()))
          .withValue(ExecConstants.USER_AUTHENTICATOR_IMPL,
              ConfigValueFactory.fromAnyRef(UserAuthenticatorTestImpl.TYPE)),
          false);

      spnegoUtil = new SpnegoUtil(newConfig);
      spnegoUtil.validateSpnegoConfig();
      fail();
    } catch (Exception ex) {
      assertTrue(ex instanceof DrillException);
    }

    // Configuration with principal only
    try {
      newConfig = new DrillConfig(DrillConfig.create()
          .withValue(ExecConstants.USER_AUTHENTICATION_ENABLED,
              ConfigValueFactory.fromAnyRef(true))
          .withValue(ExecConstants.AUTHENTICATION_MECHANISMS,
              ConfigValueFactory.fromIterable(Lists.newArrayList("plain")))
          .withValue(ExecConstants.HTTP_SPNEGO_PRINCIPAL,
              ConfigValueFactory.fromAnyRef(spnegoHelper.SERVER_PRINCIPAL))
          .withValue(ExecConstants.USER_AUTHENTICATOR_IMPL,
              ConfigValueFactory.fromAnyRef(UserAuthenticatorTestImpl.TYPE)),
          false);

      spnegoUtil = new SpnegoUtil(newConfig);
      spnegoUtil.validateSpnegoConfig();
      fail();
    } catch (Exception ex) {
      assertTrue(ex instanceof DrillException);
    }

    // Valid Configuration with both keytab & principal
    try {
      newConfig = new DrillConfig(DrillConfig.create()
          .withValue(ExecConstants.USER_AUTHENTICATION_ENABLED,
              ConfigValueFactory.fromAnyRef(true))
          .withValue(ExecConstants.AUTHENTICATION_MECHANISMS,
              ConfigValueFactory.fromIterable(Lists.newArrayList("plain")))
          .withValue(ExecConstants.HTTP_SPNEGO_PRINCIPAL,
              ConfigValueFactory.fromAnyRef(spnegoHelper.SERVER_PRINCIPAL))
          .withValue(ExecConstants.HTTP_SPNEGO_KEYTAB,
              ConfigValueFactory.fromAnyRef(spnegoHelper.serverKeytab.toString()))
          .withValue(ExecConstants.USER_AUTHENTICATOR_IMPL,
              ConfigValueFactory.fromAnyRef(UserAuthenticatorTestImpl.TYPE)),
          false);

      spnegoUtil = new SpnegoUtil(newConfig);
      spnegoUtil.validateSpnegoConfig();
      UserGroupInformation ugi = spnegoUtil.getLoggedInUgi();
      assertEquals(primaryName, ugi.getShortUserName());
      assertEquals(spnegoHelper.SERVER_PRINCIPAL, ugi.getUserName());
    } catch (Exception ex) {
      fail();
    }
  }

  /**
   * Validate successful {@link DrillSpnegoLoginService#login(String, Object)} when provided with client token for a
   * configured service principal.
   * @throws Exception
   */
  @Test
  public void testDrillSpnegoLoginService() throws Exception {

    // Create client subject using it's principal and keytab
    final Subject clientSubject = JaasKrbUtil.loginUsingKeytab(spnegoHelper.CLIENT_PRINCIPAL,
            spnegoHelper.clientKeytab.getAbsoluteFile());

    // Generate a SPNEGO token for the peer SERVER_PRINCIPAL from this CLIENT_PRINCIPAL
    final String token = Subject.doAs(clientSubject, new PrivilegedExceptionAction<String>() {
      @Override
      public String run() throws Exception {

        final GSSManager gssManager = GSSManager.getInstance();
        GSSContext gssContext = null;
        try {
          final Oid oid = GSSUtil.GSS_SPNEGO_MECH_OID;
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
      }
    });

    // Create a DrillbitContext with service principal and keytab for DrillSpnegoLoginService
    final DrillConfig newConfig = new DrillConfig(DrillConfig.create()
        .withValue(ExecConstants.HTTP_AUTHENTICATION_MECHANISMS,
            ConfigValueFactory.fromIterable(Lists.newArrayList("spnego")))
        .withValue(ExecConstants.HTTP_SPNEGO_PRINCIPAL,
            ConfigValueFactory.fromAnyRef(spnegoHelper.SERVER_PRINCIPAL))
        .withValue(ExecConstants.HTTP_SPNEGO_KEYTAB,
            ConfigValueFactory.fromAnyRef(spnegoHelper.serverKeytab.toString())),
        false);


    final SystemOptionManager optionManager = Mockito.mock(SystemOptionManager.class);
    Mockito.when(optionManager.getOption(ExecConstants.ADMIN_USERS_VALIDATOR))
        .thenReturn(ExecConstants.ADMIN_USERS_VALIDATOR.DEFAULT_ADMIN_USERS);
    Mockito.when(optionManager.getOption(ExecConstants.ADMIN_USER_GROUPS_VALIDATOR))
        .thenReturn(ExecConstants.ADMIN_USER_GROUPS_VALIDATOR.DEFAULT_ADMIN_USER_GROUPS);

    final DrillbitContext drillbitContext = Mockito.mock(DrillbitContext.class);
    Mockito.when(drillbitContext.getConfig()).thenReturn(newConfig);
    Mockito.when(drillbitContext.getOptionManager()).thenReturn(optionManager);

    final DrillSpnegoLoginService loginService = new DrillSpnegoLoginService(drillbitContext);

    // Authenticate the client using its SPNEGO token
    final UserIdentity user = loginService.login(null, token);

    // Validate the UserIdentity of authenticated client
    assertTrue(user != null);
    assertTrue(user.getUserPrincipal().getName().equals(spnegoHelper.CLIENT_PRINCIPAL));
    assertTrue(user.isUserInRole("authenticated", null));
  }

  /**
   * Validate {@link DrillSpnegoAuthenticator} with request of different nature and from authenticated and
   * unauthenticated session.
   * @throws Exception
   */
  @Test
  public void testDrillSpnegoAuthenticator() throws Exception {

    // Create a DrillbitContext with service principal and keytab for DrillSpnegoLoginService
    final DrillConfig newConfig = new DrillConfig(DrillConfig.create()
        .withValue(ExecConstants.HTTP_AUTHENTICATION_MECHANISMS,
            ConfigValueFactory.fromIterable(Lists.newArrayList("spnego")))
        .withValue(ExecConstants.HTTP_SPNEGO_PRINCIPAL,
            ConfigValueFactory.fromAnyRef(spnegoHelper.SERVER_PRINCIPAL))
        .withValue(ExecConstants.HTTP_SPNEGO_KEYTAB,
            ConfigValueFactory.fromAnyRef(spnegoHelper.serverKeytab.toString())),
        false);

    // Create mock objects for optionManager and AuthConfiguration
    final SystemOptionManager optionManager = Mockito.mock(SystemOptionManager.class);
    Mockito.when(optionManager.getOption(ExecConstants.ADMIN_USERS_VALIDATOR))
        .thenReturn(ExecConstants.ADMIN_USERS_VALIDATOR.DEFAULT_ADMIN_USERS);
    Mockito.when(optionManager.getOption(ExecConstants.ADMIN_USER_GROUPS_VALIDATOR))
        .thenReturn(ExecConstants.ADMIN_USER_GROUPS_VALIDATOR.DEFAULT_ADMIN_USER_GROUPS);

    final DrillbitContext drillbitContext = Mockito.mock(DrillbitContext.class);
    Mockito.when(drillbitContext.getConfig()).thenReturn(newConfig);
    Mockito.when(drillbitContext.getOptionManager()).thenReturn(optionManager);

    Authenticator.AuthConfiguration authConfiguration = Mockito.mock(Authenticator.AuthConfiguration.class);

    DrillSpnegoAuthenticator spnegoAuthenticator = new DrillSpnegoAuthenticator("SPNEGO");
    DrillSpnegoLoginService spnegoLoginService = new DrillSpnegoLoginService(drillbitContext);

    Mockito.when(authConfiguration.getLoginService()).thenReturn(spnegoLoginService);
    Mockito.when(authConfiguration.getIdentityService()).thenReturn(new DefaultIdentityService());
    Mockito.when(authConfiguration.isSessionRenewedOnAuthentication()).thenReturn(true);

    // Set the login service and identity service inside SpnegoAuthenticator
    spnegoAuthenticator.setConfiguration(authConfiguration);

    //
    // Request sent for SPNEGO_LOGIN_RESOURCE without client authenticated session
    //
    HttpServletRequest request1 = Mockito.mock(HttpServletRequest.class);
    HttpServletResponse response1 = Mockito.mock(HttpServletResponse.class);
    HttpSession session1 = Mockito.mock(HttpSession.class);

    Mockito.when(request1.getSession(true)).thenReturn(session1);
    Mockito.when(request1.getRequestURI()).thenReturn(WebServerConstants.SPENGO_LOGIN_RESOURCE_PATH);


    Authentication authentication1 = spnegoAuthenticator.validateRequest(request1, response1, false);

    assertEquals(authentication1, Authentication.SEND_CONTINUE);
    verify(response1).sendError(401);
    verify(response1).setHeader(HttpHeader.WWW_AUTHENTICATE.asString(), HttpHeader.NEGOTIATE.asString());

    //
    // Request sent for SPNEGO_LOGIN_RESOURCE with already authenticated session
    //
    HttpServletRequest request2 = Mockito.mock(HttpServletRequest.class);
    HttpServletResponse response2 = Mockito.mock(HttpServletResponse.class);
    HttpSession session2 = Mockito.mock(HttpSession.class);
    Authentication authentication2 = Mockito.mock(UserAuthentication.class);

    Mockito.when(request2.getSession(true)).thenReturn(session2);
    Mockito.when(request2.getRequestURI()).thenReturn(WebServerConstants.SPENGO_LOGIN_RESOURCE_PATH);
    Mockito.when(session2.getAttribute("org.eclipse.jetty.security.UserIdentity")).thenReturn(authentication2);

    UserAuthentication authentication =
        (UserAuthentication) spnegoAuthenticator.validateRequest(request2, response2, false);
    assertEquals(authentication, authentication2);
    verify(response2, never()).sendError(401);
    verify(response2, never()).setHeader(HttpHeader.WWW_AUTHENTICATE.asString(), HttpHeader.NEGOTIATE.asString());

    //
    // Request sent for OTHER PAGE with already authenticate session. There is no authentication happening again for
    // each of different requested resources.
    //
    HttpServletRequest request3 = Mockito.mock(HttpServletRequest.class);
    HttpServletResponse response3 = Mockito.mock(HttpServletResponse.class);
    HttpSession session3 = Mockito.mock(HttpSession.class);
    Authentication authentication3 = Mockito.mock(UserAuthentication.class);

    Mockito.when(request3.getSession(true)).thenReturn(session3);
    Mockito.when(request3.getRequestURI()).thenReturn(WebServerConstants.WEBSERVER_ROOT_PATH);
    Mockito.when(session3.getAttribute("org.eclipse.jetty.security.UserIdentity")).thenReturn(authentication3);

    authentication = (UserAuthentication) spnegoAuthenticator.validateRequest(request3, response3, false);
    assertEquals(authentication, authentication3);
    verify(response3, never()).sendError(401);
    verify(response3, never()).setHeader(HttpHeader.WWW_AUTHENTICATE.asString(), HttpHeader.NEGOTIATE.asString());

    //
    // Request sent for LOGOUT_RESOURCE_PATH for an authenticated session
    //
    HttpServletRequest request4 = Mockito.mock(HttpServletRequest.class);
    HttpServletResponse response4 = Mockito.mock(HttpServletResponse.class);
    HttpSession session4 = Mockito.mock(HttpSession.class);
    Authentication authentication4 = Mockito.mock(UserAuthentication.class);

    Mockito.when(request4.getSession(true)).thenReturn(session4);
    Mockito.when(request4.getRequestURI()).thenReturn(WebServerConstants.LOGOUT_RESOURCE_PATH);
    Mockito.when(session4.getAttribute("org.eclipse.jetty.security.UserIdentity")).thenReturn(authentication4);

    authentication = (UserAuthentication) spnegoAuthenticator.validateRequest(request4, response4, false);
    assertNull(authentication);
    verify(session4).removeAttribute("org.eclipse.jetty.security.UserIdentity");
    verify(response4, never()).sendError(401);
    verify(response4, never()).setHeader(HttpHeader.WWW_AUTHENTICATE.asString(), HttpHeader.NEGOTIATE.asString());

    //
    // Request sent for SPENGO_LOGIN_RESOURCE_PATH with a invalid NEGOTIATE token
    //
    HttpServletRequest request5 = Mockito.mock(HttpServletRequest.class);
    HttpServletResponse response5 = Mockito.mock(HttpServletResponse.class);
    HttpSession session5 = Mockito.mock(HttpSession.class);

    // Create client subject using it's principal and keytab
    final Subject clientSubject = JaasKrbUtil.loginUsingKeytab(spnegoHelper.CLIENT_PRINCIPAL,
        spnegoHelper.clientKeytab.getAbsoluteFile());

    // Generate a SPNEGO token for the peer SERVER_PRINCIPAL from this CLIENT_PRINCIPAL
    final String token = Subject.doAs(clientSubject, new PrivilegedExceptionAction<String>() {
      @Override
      public String run() throws Exception {

        final GSSManager gssManager = GSSManager.getInstance();
        GSSContext gssContext = null;
        try {
          final Oid oid = GSSUtil.GSS_SPNEGO_MECH_OID;
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
      }
    });

    Mockito.when(request5.getSession(true)).thenReturn(session5);

    final String httpReqAuthHeader = String.format("%s:%s", HttpHeader.NEGOTIATE.asString(), String.format
        ("%s%s","1234", token));
    Mockito.when(request5.getHeader(HttpHeader.AUTHORIZATION.asString())).thenReturn(httpReqAuthHeader);
    Mockito.when(request5.getRequestURI()).thenReturn(WebServerConstants.SPENGO_LOGIN_RESOURCE_PATH);

    assertEquals(spnegoAuthenticator.validateRequest(request5, response5, false), Authentication.UNAUTHENTICATED);

    verify(session5, never()).setAttribute("org.eclipse.jetty.security.UserIdentity", null);
    verify(response5, never()).sendError(401);
    verify(response5, never()).setHeader(HttpHeader.WWW_AUTHENTICATE.asString(), HttpHeader.NEGOTIATE.asString());
  }

  @AfterClass
  public static void cleanTest() throws Exception {
    spnegoHelper.stopKdc();
  }
}
