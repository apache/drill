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
import org.apache.drill.exec.server.rest.auth.AuthDynamicFeature;
import org.eclipse.jetty.security.authentication.FormAuthenticator;
import org.eclipse.jetty.security.authentication.SessionAuthentication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.annotation.security.PermitAll;
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
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;
import jakarta.ws.rs.core.UriBuilder;
import jakarta.ws.rs.core.UriInfo;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

/**
 * Login / logout endpoints. The HTML pages themselves are rendered by the
 * React SPA; this resource keeps the session side-effects Jetty's
 * FormAuthenticator depends on (storing the post-login redirect target on
 * the session under {@link FormAuthenticator#__J_URI}).
 *
 * <p>The form action {@code /j_security_check} is handled directly by
 * Jetty's FormAuthenticator and does not appear here.</p>
 */
@Path(WebServerConstants.WEBSERVER_ROOT_PATH)
@PermitAll
public class LogInLogOutResources {

  private static final Logger logger = LoggerFactory.getLogger(LogInLogOutResources.class);

  /**
   * Store the post-login redirect target on the session so Jetty's
   * FormAuthenticator can bounce the user there after successful auth.
   */
  private void updateSessionRedirectInfo(String redirect, HttpServletRequest request) {
    if (StringUtils.isEmpty(redirect)) {
      return;
    }
    HttpSession session = request.getSession(true);
    URI destURI = UriBuilder.fromUri(URLDecoder.decode(redirect, StandardCharsets.UTF_8)).build();
    session.setAttribute(FormAuthenticator.__J_URI, destURI.getPath());
  }

  @GET
  @Path(WebServerConstants.FORM_LOGIN_RESOURCE_PATH)
  @Produces(MediaType.TEXT_HTML)
  public Response getLoginPage(@Context HttpServletRequest request, @Context HttpServletResponse response,
                               @Context SecurityContext sc, @Context UriInfo uriInfo,
                               @QueryParam(WebServerConstants.REDIRECT_QUERY_PARM) String redirect) {
    if (AuthDynamicFeature.isUserLoggedIn(sc)) {
      return Response.seeOther(URI.create(WebServerConstants.WEBSERVER_ROOT_PATH)).build();
    }
    updateSessionRedirectInfo(redirect, request);
    return SpaResponseUtil.serveSpaIndex();
  }

  /**
   * Jetty's FormAuthenticator dispatches POST /login when form auth fails.
   * Return a tiny HTML stub that redirects the browser to /login?error=1
   * so the React LoginPage can surface the error message.
   */
  @POST
  @Path(WebServerConstants.FORM_LOGIN_RESOURCE_PATH)
  @Produces(MediaType.TEXT_HTML)
  public Response getLoginPageAfterValidationError() {
    String html = "<!DOCTYPE html><html><head><meta charset=\"UTF-8\">"
        + "<meta http-equiv=\"refresh\" content=\"0; url=/login?error=1\">"
        + "</head><body><script>window.location.replace('/login?error=1');</script>"
        + "Redirecting…</body></html>";
    return Response.ok(html).type(MediaType.TEXT_HTML).build();
  }

  @GET
  @Path(WebServerConstants.SPENGO_LOGIN_RESOURCE_PATH)
  public Response getSpnegoLogin(@Context HttpServletRequest request, @Context HttpServletResponse response,
                                 @Context SecurityContext sc, @Context UriInfo uriInfo,
                                 @QueryParam(WebServerConstants.REDIRECT_QUERY_PARM) String redirect) {
    if (AuthDynamicFeature.isUserLoggedIn(sc)) {
      updateSessionRedirectInfo(redirect, request);
      return Response.seeOther(URI.create(WebServerConstants.WEBSERVER_ROOT_PATH)).build();
    }
    return Response.seeOther(URI.create(WebServerConstants.MAIN_LOGIN_RESOURCE_PATH + "?error=spnego")).build();
  }

  @GET
  @Path(WebServerConstants.LOGOUT_RESOURCE_PATH)
  public Response logout(@Context HttpServletRequest req, @Context HttpServletResponse resp) {
    HttpSession session = req.getSession(false);
    if (session != null) {
      Object authCreds = session.getAttribute(SessionAuthentication.AUTHENTICATED_ATTRIBUTE);
      if (authCreds != null) {
        SessionAuthentication sessionAuth = (SessionAuthentication) authCreds;
        logger.info("WebUser {} logged out from {}:{}",
            sessionAuth.getUserIdentity().getUserPrincipal().getName(),
            req.getRemoteHost(), req.getRemotePort());
      }
      session.invalidate();
    }
    return Response.seeOther(URI.create(WebServerConstants.WEBSERVER_ROOT_PATH)).build();
  }

  @GET
  @Path(WebServerConstants.MAIN_LOGIN_RESOURCE_PATH)
  @Produces(MediaType.TEXT_HTML)
  public Response getMainLoginPage(@Context HttpServletRequest request, @Context HttpServletResponse response,
                                   @Context SecurityContext sc, @Context UriInfo uriInfo,
                                   @QueryParam(WebServerConstants.REDIRECT_QUERY_PARM) String redirect) {
    updateSessionRedirectInfo(redirect, request);
    return SpaResponseUtil.serveSpaIndex();
  }
}
