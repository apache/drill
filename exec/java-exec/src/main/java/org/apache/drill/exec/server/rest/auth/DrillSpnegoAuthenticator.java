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
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpVersion;
import org.eclipse.jetty.security.ServerAuthException;
import org.eclipse.jetty.security.UserAuthentication;
import org.eclipse.jetty.security.authentication.DeferredAuthentication;
import org.eclipse.jetty.security.authentication.SessionAuthentication;
import org.eclipse.jetty.security.authentication.SpnegoAuthenticator;
import org.eclipse.jetty.server.Authentication;
import org.eclipse.jetty.server.HttpChannel;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.server.UserIdentity;

import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import java.io.IOException;

/**
 * Custom SpnegoAuthenticator for Drill which provides following:
 * 1) Perform SPNEGO authentication only when spnegoLogin resource is requested. This helps to avoid authentication
 *    for each and every resource which the JETTY provided authenticator does.
 * 2) Helps to redirect to the target URL after authentication is done successfully.
 * 3) Clear-Up in memory session information once LogOut is triggered. Such that any future request also trigger the
 *    SPNEGO authentication.
 */
public class DrillSpnegoAuthenticator extends SpnegoAuthenticator {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillSpnegoAuthenticator.class);

  public DrillSpnegoAuthenticator(String authMethod) {
    super(authMethod);
  }

  @Override
  public Authentication validateRequest(ServletRequest request, ServletResponse response, boolean mandatory)
      throws ServerAuthException {

    HttpServletRequest req = (HttpServletRequest) request;
    HttpServletResponse res = (HttpServletResponse) response;
    HttpSession session = req.getSession(true);
    final Authentication authentication =
        (Authentication) session.getAttribute("org.eclipse.jetty.security.UserIdentity");
    String uri = req.getRequestURI();

    //If the Request URI is for /spnegoLogin then perform login
    mandatory |= uri.equals(WebServerConstants.SPENGO_LOGIN_RESOURCE_PATH);

    //For logout remove the attribute from the session that holds UserIdentity
    if (authentication != null && uri.equals(WebServerConstants.LOGOUT_RESOURCE_PATH)) {
      logger.debug("Logging out user {}", req.getRemoteAddr());
      session.removeAttribute("org.eclipse.jetty.security.UserIdentity");
      return null;
    } else if (authentication != null) { // Since already logged in just return the session attribute
      return authentication;
    } else { // The session is not yet authenticated
      final String header = req.getHeader(HttpHeader.AUTHORIZATION.asString());
      if (!mandatory) {
        return new DeferredAuthentication(this);
      } else if (header == null) {
        try {
          if (DeferredAuthentication.isDeferred(res)) {
            return Authentication.UNAUTHENTICATED;
          } else {
            res.setHeader(HttpHeader.WWW_AUTHENTICATE.asString(), HttpHeader.NEGOTIATE.asString());
            res.sendError(401);
            logger.debug("SPNEGOAuthenticator: Sending challenge to client {}", req.getRemoteAddr());
            return Authentication.SEND_CONTINUE;
          }
        } catch (IOException var9) {
          throw new ServerAuthException(var9);
        }
      } else {
        logger.debug("SPNEGOAuthenticator: Received NEGOTIATE Response back from client {}", req.getRemoteAddr());
        final String negotiateString = HttpHeader.NEGOTIATE.asString();

        if (header.startsWith(negotiateString)) {
          final String spnegoToken = header.substring(negotiateString.length() + 1);
          final UserIdentity user = this.login(null, spnegoToken, request);
          //redirect the request to the desired page after successful login
          if (user != null) {
            String newUri = (String) session.getAttribute("org.eclipse.jetty.security.form_URI");
            if (Strings.isNullOrEmpty(newUri)) {
              newUri = req.getContextPath();
              if (Strings.isNullOrEmpty(newUri)) {
                newUri = WebServerConstants.WEBSERVER_ROOT_PATH;
              }
            }

            response.setContentLength(0);
            final HttpChannel channel = HttpChannel.getCurrentHttpChannel();
            final Response base_response = channel.getResponse();
            final Request base_request = channel.getRequest();
            final int redirectCode =
                base_request.getHttpVersion().getVersion() < HttpVersion.HTTP_1_1.getVersion() ? 302 : 303;
            try {
              base_response.sendRedirect(redirectCode, res.encodeRedirectURL(newUri));
            } catch (IOException e) {
              e.printStackTrace();
            }

            logger.debug("SPNEGOAuthenticator: Successfully authenticated this client session: {}",
                user.getUserPrincipal().getName());
            return new UserAuthentication(this.getAuthMethod(), user);
          }
        }
        logger.debug("SPNEGOAuthenticator: Authentication failed for client session: {}", req.getRemoteAddr());
        return Authentication.UNAUTHENTICATED;
      }
    }
  }

  public UserIdentity login(String username, Object password, ServletRequest request) {
    final UserIdentity user = super.login(username, password, request);

    if (user != null) {
      HttpSession session = ((HttpServletRequest) request).getSession(true);
      Authentication cached = new SessionAuthentication(this.getAuthMethod(), user, password);
      session.setAttribute("org.eclipse.jetty.security.UserIdentity", cached);
    }

    return user;
  }
}