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

import javax.annotation.security.PermitAll;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;

import org.apache.commons.lang3.StringUtils;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.rpc.security.AuthStringUtil;
import org.apache.drill.exec.server.rest.auth.AuthDynamicFeature;
import org.apache.drill.exec.server.rest.auth.FORMSecurityHanlder;
import org.apache.drill.exec.server.rest.auth.SPNEGOSecurityHandler;
import org.apache.drill.exec.work.WorkManager;
import org.eclipse.jetty.security.authentication.FormAuthenticator;
import org.glassfish.jersey.server.mvc.Viewable;

import java.net.URI;
import java.net.URLDecoder;
import java.util.Set;

@Path(WebServerConstants.WEBSERVER_ROOT_PATH)
@PermitAll
public class LogInLogOutResources {

  @Inject
  WorkManager workManager;

  @GET
  @Path(WebServerConstants.FORM_LOGIN_RESOURCE_PATH)
  @Produces(MediaType.TEXT_HTML)
  public Viewable getLoginPage(@Context HttpServletRequest request, @Context HttpServletResponse response,
                               @Context SecurityContext sc, @Context UriInfo uriInfo,
                               @QueryParam(WebServerConstants.REDIRECT_QUERY_PARM) String redirect) throws Exception {

    if (AuthDynamicFeature.isUserLoggedIn(sc)) {
      // if the user is already login, forward the request to homepage.
      request.getRequestDispatcher("/").forward(request, response);
      return null;
    }

    if (!StringUtils.isEmpty(redirect)) {
      // If the URL has redirect in it, set the redirect URI in session, so that after the login is successful, request
      // is forwarded to the redirect page.
      final HttpSession session = request.getSession(true);
      final URI destURI = UriBuilder.fromUri(URLDecoder.decode(redirect, "UTF-8")).build();
      session.setAttribute(FormAuthenticator.__J_URI, destURI.toString());
    }

    return ViewableWithPermissions.createLoginPage(null);
  }

  @GET
  @Path(WebServerConstants.SPENGO_LOGIN_RESOURCE_PATH)
  @Produces(MediaType.TEXT_HTML)
  public Viewable getSpnegologin(@Context HttpServletRequest request, @Context HttpServletResponse response,
                                 @Context SecurityContext sc, @Context UriInfo uriInfo,
                                 @QueryParam(WebServerConstants.REDIRECT_QUERY_PARM) String redirect) throws Exception {
    if (AuthDynamicFeature.isUserLoggedIn(sc)) {
      request.getRequestDispatcher("/").forward(request, response);
      return null;
    }

    final String errorString = "Invalid SPNEGO credentials or SPNEGO is not configured";
    MainLoginPageModel model = new MainLoginPageModel(errorString);
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
    if (!StringUtils.isEmpty(redirect)) {
      // If the URL has redirect in it, set the redirect URI in session, so that after the login is successful, request
      // is forwarded to the redirect page.
      final HttpSession session = request.getSession(true);
      final URI destURI = UriBuilder.fromUri(URLDecoder.decode(redirect, "UTF-8")).build();
      session.setAttribute(FormAuthenticator.__J_URI, destURI.toString());
    }

    MainLoginPageModel model = new MainLoginPageModel(null);
    return ViewableWithPermissions.createMainLoginPage(model);
  }

  public class MainLoginPageModel {

    public String error;

    MainLoginPageModel(String error) {
      this.error = error;
    }

    public boolean isSpnegoEnabled() {
      return LogInLogOutResources.isSpnegoEnabled(workManager.getContext().getConfig());
    }

    public boolean isFormEnabled() {
      return LogInLogOutResources.isFormEnabled(workManager.getContext().getConfig());
    }

    public String getError() {
      return error;
    }
  }

  private static boolean isSpnegoEnabled(DrillConfig config) {
    Set<String> configuredMechs = AuthStringUtil.asSet(
        config.getStringList(ExecConstants.HTTP_AUTHENTICATION_MECHANISMS));

    final boolean authEnabled = config.getBoolean(ExecConstants.USER_AUTHENTICATION_ENABLED);
    return authEnabled && configuredMechs.contains(SPNEGOSecurityHandler.HANDLER_NAME);
  }

  private static boolean isFormEnabled(DrillConfig config) {
    Set<String> configuredMechs = AuthStringUtil.asSet(
        config.getStringList(ExecConstants.HTTP_AUTHENTICATION_MECHANISMS));

    final boolean authEnabled = config.getBoolean(ExecConstants.USER_AUTHENTICATION_ENABLED);
    return authEnabled && configuredMechs.contains(FORMSecurityHanlder.HANDLER_NAME);
  }
}
