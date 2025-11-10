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
package org.apache.drill.exec.server.rest.auth;


import org.apache.drill.exec.server.rest.WebServerConstants;
import org.apache.parquet.Strings;
import org.eclipse.jetty.ee10.servlet.ServletContextRequest;
import org.eclipse.jetty.http.HttpField;
import org.eclipse.jetty.http.HttpFields;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.security.AuthenticationState;
import org.eclipse.jetty.security.ServerAuthException;
import org.eclipse.jetty.security.UserIdentity;
import org.eclipse.jetty.security.authentication.LoginAuthenticator;
import org.eclipse.jetty.security.authentication.SessionAuthentication;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.util.Callback;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpSession;

/**
 * Custom SpnegoAuthenticator for Drill - Jetty 12 version
 *
 * This class extends LoginAuthenticator and provides SPNEGO authentication support.
 */
public class DrillSpnegoAuthenticator extends LoginAuthenticator {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillSpnegoAuthenticator.class);
  private static final String AUTH_METHOD = "SPNEGO";

  public DrillSpnegoAuthenticator() {
    super();
  }


  /**
   * Jetty 12 validateRequest implementation using core Request/Response/Callback API.
   * Handles:
   * 1) Perform SPNEGO authentication only when spnegoLogin resource is requested
   * 2) Redirect to target URL after authentication
   * 3) Clear session information on logout
   */
  @Override
  public AuthenticationState validateRequest(Request request, Response response, Callback callback)
      throws ServerAuthException {

    // Get the servlet request from the core request
    ServletContextRequest servletContextRequest = Request.as(request, ServletContextRequest.class);
    if (servletContextRequest == null) {
      return AuthenticationState.CHALLENGE;
    }

    HttpServletRequest httpReq = servletContextRequest.getServletApiRequest();
    final HttpSession session = httpReq.getSession(true);
    final String uri = httpReq.getRequestURI();

    // Check if already authenticated
    final AuthenticationState authentication = (AuthenticationState) session.getAttribute(SessionAuthentication.AUTHENTICATED_ATTRIBUTE);

    // If the Request URI is for /spnegoLogin then perform login
    final boolean mandatory = uri.equals(WebServerConstants.SPENGO_LOGIN_RESOURCE_PATH);

    // For logout, clear authentication
    if (authentication instanceof AuthenticationState.Succeeded) {
      if (uri.equals(WebServerConstants.LOGOUT_RESOURCE_PATH)) {
        return null;
      }
      // Already logged in
      return authentication;
    }

    // Try to authenticate
    return authenticateSession(request, response, callback, servletContextRequest, httpReq, session, mandatory);
  }

  /**
   * Method to authenticate a user session using the SPNEGO token passed in AUTHORIZATION header of request.
   */
  private AuthenticationState authenticateSession(Request request, Response response, Callback callback,
                                                    ServletContextRequest servletContextRequest,
                                                    HttpServletRequest httpReq, HttpSession session, boolean mandatory)
      throws ServerAuthException {

    // Defer the authentication if not mandatory
    if (!mandatory) {
      return AuthenticationState.CHALLENGE;
    }

    // Authentication is mandatory, get the Authorization header
    final HttpFields fields = request.getHeaders();
    final HttpField authField = fields.getField(HttpHeader.AUTHORIZATION);
    final String header = authField != null ? authField.getValue() : null;

    // Authorization header is null, send 401 challenge
    if (header == null) {
      response.getHeaders().put(HttpHeader.WWW_AUTHENTICATE, HttpHeader.NEGOTIATE.asString());
      Response.writeError(request, response, callback, HttpStatus.UNAUTHORIZED_401);
      logger.debug("DrillSpnegoAuthenticator: Sending challenge to client {}", httpReq.getRemoteAddr());
      return new UserAuthenticationSent(AUTH_METHOD, null);
    }

    // Valid Authorization header received. Get the SPNEGO token sent by client and try to authenticate
    logger.debug("DrillSpnegoAuthenticator: Received NEGOTIATE Response back from client {}", httpReq.getRemoteAddr());
    final String negotiateString = HttpHeader.NEGOTIATE.asString();

    if (header.startsWith(negotiateString)) {
      final String spnegoToken = header.substring(negotiateString.length() + 1);
      final UserIdentity user = this.login(null, spnegoToken, request, response);

      // Redirect the request to the desired page after successful login
      if (user != null) {
        String newUri = (String) session.getAttribute("org.eclipse.jetty.security.form_URI");
        if (Strings.isNullOrEmpty(newUri)) {
          newUri = httpReq.getContextPath();
          if (Strings.isNullOrEmpty(newUri)) {
            newUri = WebServerConstants.WEBSERVER_ROOT_PATH;
          }
        }

        // Send redirect
        Response.sendRedirect(request, response, callback, newUri);

        logger.debug("DrillSpnegoAuthenticator: Successfully authenticated this client session: {}",
            user.getUserPrincipal().getName());

        // Store authentication in session
        final SessionAuthentication cached = new SessionAuthentication(AUTH_METHOD, user, spnegoToken);
        session.setAttribute(SessionAuthentication.AUTHENTICATED_ATTRIBUTE, cached);

        return new UserAuthenticationSucceeded(AUTH_METHOD, user);
      }
    }

    logger.debug("DrillSpnegoAuthenticator: Authentication failed for client session: {}", httpReq.getRemoteAddr());
    return AuthenticationState.CHALLENGE;
  }

  @Override
  public String getAuthenticationType() {
    return AUTH_METHOD;
  }
}
