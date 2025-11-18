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


import jakarta.servlet.http.HttpServletRequest;
import org.eclipse.jetty.ee10.servlet.ServletContextRequest;
import org.eclipse.jetty.http.HttpField;
import org.eclipse.jetty.http.HttpFields;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.security.AuthenticationState;
import org.eclipse.jetty.security.Authenticator;
import org.eclipse.jetty.security.ServerAuthException;
import org.eclipse.jetty.security.UserIdentity;
import org.eclipse.jetty.security.authentication.LoginAuthenticator;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.util.Callback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Custom SpnegoAuthenticator for Drill - Jetty 12 version
 *
 * This class extends LoginAuthenticator and provides SPNEGO authentication support.
 */
public class DrillSpnegoAuthenticator extends LoginAuthenticator {

  private static final Logger logger = LoggerFactory.getLogger(DrillSpnegoAuthenticator.class);

  public DrillSpnegoAuthenticator() {
    super();
  }


  /**
   * Jetty 12 validateRequest implementation using core Request/Response/Callback API.
   * Handles:
   * 1) Check for existing valid authentication in session
   * 2) Try to authenticate using SPNEGO token if present
   * 3) Send challenge if no authentication exists
   * 4) Clear session information on logout
   */
  @Override
  public AuthenticationState validateRequest(Request request, Response response, Callback callback)
      throws ServerAuthException {

    try {
      // Get the servlet request from the core request
      ServletContextRequest servletContextRequest = Request.as(request, ServletContextRequest.class);

      if (servletContextRequest == null) {
        logger.debug("ServletContextRequest is null - returning SEND_SUCCESS");
        return AuthenticationState.SEND_SUCCESS;
      }

      HttpServletRequest httpReq = servletContextRequest.getServletApiRequest();
      final String uri = httpReq.getRequestURI();
      logger.debug("Validating request for URI: {}", uri);

      // Try to authenticate using SPNEGO token if present
      // Session caching is handled automatically by ConstraintSecurityHandler
      return authenticateRequest(request, response, callback, httpReq);
    } catch (Exception e) {
      logger.error("Exception in validateRequest: {}", e.getMessage(), e);
      throw e;
    }
  }

  /**
   * Method to authenticate a request using the SPNEGO token passed in AUTHORIZATION header of request.
   * Session management is handled automatically by Jetty's ConstraintSecurityHandler.
   */
  private AuthenticationState authenticateRequest(Request request, Response response, Callback callback,
                                                   HttpServletRequest httpReq)
      throws ServerAuthException {

    // Get the Authorization header
    final HttpFields fields = request.getHeaders();
    final HttpField authField = fields.getField(HttpHeader.AUTHORIZATION);
    final String header = authField != null ? authField.getValue() : null;

    // Authorization header is null, send 401 challenge
    if (header == null) {
      logger.debug("No Authorization header - sending challenge to client {}", httpReq.getRemoteAddr());
      sendChallenge(request, response, callback);
      return AuthenticationState.CHALLENGE;
    }

    // Valid Authorization header received. Get the SPNEGO token sent by client and try to authenticate
    logger.debug("Received NEGOTIATE response from client {}", httpReq.getRemoteAddr());
    final String negotiateString = HttpHeader.NEGOTIATE.asString();

    if (header.startsWith(negotiateString)) {
      final String spnegoToken = header.substring(negotiateString.length() + 1);
      final UserIdentity user = this.login(null, spnegoToken, request, response);

      // Authentication successful
      if (user != null) {
        logger.debug("Successfully authenticated client: {}", user.getUserPrincipal().getName());

        // Return success - session caching is handled by DrillHttpSecurityHandlerProvider
        return new LoginAuthenticator.UserAuthenticationSucceeded(Authenticator.SPNEGO_AUTH, user);
      }
    }

    logger.debug("Authentication failed for client: {}", httpReq.getRemoteAddr());

    // Send 401 challenge when authentication fails
    sendChallenge(request, response, callback);
    return AuthenticationState.CHALLENGE;
  }

  /**
   * Sends a 401 Unauthorized challenge with WWW-Authenticate: Negotiate header.
   * This method properly handles both setting the response headers and completing the callback.
   */
  private void sendChallenge(Request request, Response response, Callback callback) {
    // Set WWW-Authenticate header
    response.getHeaders().put(HttpHeader.WWW_AUTHENTICATE, HttpHeader.NEGOTIATE.asString());

    // Use Response.writeError to properly send the 401 response and complete the callback
    Response.writeError(request, response, callback, HttpStatus.UNAUTHORIZED_401);
  }

  @Override
  public String getAuthenticationType() {
    return Authenticator.SPNEGO_AUTH;
  }
}
