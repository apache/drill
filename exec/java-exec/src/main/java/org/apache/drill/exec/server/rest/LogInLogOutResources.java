/**
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
import org.apache.drill.exec.server.rest.auth.AuthDynamicFeature;
import org.eclipse.jetty.security.authentication.FormAuthenticator;
import org.glassfish.jersey.server.mvc.Viewable;

import java.net.URI;
import java.net.URLDecoder;

@Path("/")
@PermitAll
public class LogInLogOutResources {
  public static final String REDIRECT_QUERY_PARM = "redirect";
  public static final String LOGIN_RESOURCE = "login";

  @GET
  @Path("/login")
  @Produces(MediaType.TEXT_HTML)
  public Viewable getLoginPage(@Context HttpServletRequest request, @Context HttpServletResponse response,
      @Context SecurityContext sc, @Context UriInfo uriInfo, @QueryParam(REDIRECT_QUERY_PARM) String redirect)
      throws Exception {
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

  // Request type is POST because POST request which contains the login credentials are invalid and the request is
  // dispatched here directly.
  @POST
  @Path("/login")
  @Produces(MediaType.TEXT_HTML)
  public Viewable getLoginPageAfterValidationError() {
    return ViewableWithPermissions.createLoginPage("Invalid username/password credentials.");
  }

  @GET
  @Path("/logout")
  public void logout(@Context HttpServletRequest req, @Context HttpServletResponse resp) throws Exception {
    final HttpSession session = req.getSession();
    if (session != null) {
      session.invalidate();
    }

    req.getRequestDispatcher("/").forward(req, resp);
  }
}
