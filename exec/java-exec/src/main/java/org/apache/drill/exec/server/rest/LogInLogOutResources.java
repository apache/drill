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

import org.apache.commons.lang3.StringUtils;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.server.rest.auth.AuthDynamicFeature;
import org.apache.drill.exec.server.rest.auth.DrillHttpSecurityHandlerProvider;
import org.apache.drill.exec.work.WorkManager;
import com.google.common.annotations.VisibleForTesting;
import org.eclipse.jetty.security.Authenticator;
import org.eclipse.jetty.security.authentication.FormAuthenticator;
import org.eclipse.jetty.security.authentication.SessionAuthentication;
import org.glassfish.jersey.server.mvc.Viewable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.annotation.security.PermitAll;
import jakarta.inject.Inject;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpSession;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.SecurityContext;
import jakarta.ws.rs.core.UriBuilder;
import jakarta.ws.rs.core.UriInfo;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Set;

@Path(WebServerConstants.WEBSERVER_ROOT_PATH)
@PermitAll
public class LogInLogOutResources {

  private static final Logger logger = LoggerFactory.getLogger(LogInLogOutResources.class);

  @Inject
  WorkManager workManager;


  /**
   * Update the destination URI to be redirect URI if specified in the request URL so that after the login is
   * successful, request is forwarded to redirect page.
   *
   * @param redirect - Redirect parameter in the request URI
   * @param request  - Http Servlet Request
   * @throws Exception
   */
  private void updateSessionRedirectInfo(String redirect, HttpServletRequest request) throws Exception {
    if (!StringUtils.isEmpty(redirect)) {
      // If the URL has redirect in it, set the redirect URI in session, so that after the login is successful, request
      // is forwarded to the redirect page.
      final HttpSession session = request.getSession(true);
      final URI destURI = UriBuilder.fromUri(URLDecoder.decode(redirect, StandardCharsets.UTF_8)).build();
      session.setAttribute(FormAuthenticator.__J_URI, destURI.getPath());
    }
  }

  @GET
  @Path(WebServerConstants.FORM_LOGIN_RESOURCE_PATH)
  @Produces(MediaType.TEXT_HTML)
  public Viewable getLoginPage(@Context HttpServletRequest request, @Context HttpServletResponse response,
                               @Context SecurityContext sc, @Context UriInfo uriInfo,
                               @QueryParam(WebServerConstants.REDIRECT_QUERY_PARM) String redirect) throws Exception {

    if (AuthDynamicFeature.isUserLoggedIn(sc)) {
      // if the user is already login, forward the request to homepage.
      request.getRequestDispatcher(WebServerConstants.WEBSERVER_ROOT_PATH).forward(request, response);
      return null;
    }

    updateSessionRedirectInfo(redirect, request);
    return ViewableWithPermissions.createLoginPage(null);
  }

  @GET
  @Path(WebServerConstants.SPENGO_LOGIN_RESOURCE_PATH)
  @Produces(MediaType.TEXT_HTML)
  public Viewable getSpnegoLogin(@Context HttpServletRequest request, @Context HttpServletResponse response,
                                 @Context SecurityContext sc, @Context UriInfo uriInfo,
                                 @QueryParam(WebServerConstants.REDIRECT_QUERY_PARM) String redirect) throws Exception {
    if (AuthDynamicFeature.isUserLoggedIn(sc)) {
      request.getRequestDispatcher(WebServerConstants.WEBSERVER_ROOT_PATH).forward(request, response);
      return null;
    }

    final String errorString = "Invalid SPNEGO credentials or SPNEGO is not configured";
    final MainLoginPageModel model = new MainLoginPageModel(errorString);
    return ViewableWithPermissions.createMainLoginPage(model);
  }

  // Request type is POST because POST request which contains the login credentials are invalid and the request is
  // dispatched here directly.
  @POST
  @Path(WebServerConstants.FORM_LOGIN_RESOURCE_PATH)
  @Produces(MediaType.TEXT_HTML)
  public Viewable getLoginPageAfterValidationError() {
    return ViewableWithPermissions.createLoginPage("Invalid username/password credentials.");
  }

  @GET
  @Path(WebServerConstants.LOGOUT_RESOURCE_PATH)
  public void logout(@Context HttpServletRequest req, @Context HttpServletResponse resp) throws Exception {
    final HttpSession session = req.getSession();
    if (session != null) {
      final Object authCreds = session.getAttribute(SessionAuthentication.AUTHENTICATED_ATTRIBUTE);
      if (authCreds != null) {
        final SessionAuthentication sessionAuth = (SessionAuthentication) authCreds;
        logger.info("WebUser {} logged out from {}:{}", sessionAuth.getUserIdentity().getUserPrincipal().getName(), req
          .getRemoteHost(), req.getRemotePort());
      }
      session.invalidate();
    }

    req.getRequestDispatcher(WebServerConstants.WEBSERVER_ROOT_PATH).forward(req, resp);
  }

  @GET
  @Path(WebServerConstants.MAIN_LOGIN_RESOURCE_PATH)
  @Produces(MediaType.TEXT_HTML)
  public Viewable getMainLoginPage(@Context HttpServletRequest request, @Context HttpServletResponse response,
                                   @Context SecurityContext sc, @Context UriInfo uriInfo,
                                   @QueryParam(WebServerConstants.REDIRECT_QUERY_PARM) String redirect) throws Exception {
    updateSessionRedirectInfo(redirect, request);
    final MainLoginPageModel model = new MainLoginPageModel(null);
    return ViewableWithPermissions.createMainLoginPage(model);
  }

  /**
   * This class should be public for it's method's to be accessible by mainLogin.ftl file
   */
  @VisibleForTesting
  public class MainLoginPageModel {

    private final String error;

    private final boolean authEnabled;

    private final Set<String> configuredMechs;

    MainLoginPageModel(String error) {
      this.error = error;
      final DrillConfig config = workManager.getContext().getConfig();
      authEnabled = config.getBoolean(ExecConstants.USER_AUTHENTICATION_ENABLED);
      configuredMechs = DrillHttpSecurityHandlerProvider.getHttpAuthMechanisms(config);
    }

    public boolean isSpnegoEnabled() {
      return authEnabled && configuredMechs.contains(Authenticator.SPNEGO_AUTH);
    }

    public boolean isFormEnabled() {
      return authEnabled && configuredMechs.contains(Authenticator.FORM_AUTH);
    }

    public String getError() {
      return error;
    }
  }
}
