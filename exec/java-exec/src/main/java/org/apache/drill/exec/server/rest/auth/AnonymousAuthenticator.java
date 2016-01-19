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
package org.apache.drill.exec.server.rest.auth;

import org.eclipse.jetty.security.Authenticator;
import org.eclipse.jetty.security.LoginService;
import org.eclipse.jetty.security.ServerAuthException;
import org.eclipse.jetty.security.authentication.FormAuthenticator;
import org.eclipse.jetty.security.authentication.SessionAuthentication;
import org.eclipse.jetty.server.Authentication;
import org.eclipse.jetty.server.Authentication.User;

import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

/**
 * Equivalent to {@link FormAuthenticator} used when user authentication is disabled. Purpose is to make sure
 * a session is established even if the user is "anonymous".
 */
public class AnonymousAuthenticator implements Authenticator {
  public static final String METHOD = "ANONYMOUS";

  private LoginService loginService;

  @Override
  public void setConfiguration(AuthConfiguration configuration) {
    loginService = configuration.getLoginService();
  }

  @Override
  public String getAuthMethod() {
    return METHOD;
  }

  @Override
  public void prepareRequest(ServletRequest request) {
    // No-op
  }

  @Override
  public Authentication validateRequest(ServletRequest req, ServletResponse resp, boolean mandatory)
      throws ServerAuthException {
    final HttpServletRequest request = (HttpServletRequest)req;

    // Create a session if one not exists already
    final HttpSession session = request.getSession(true);

    if (session.getAttribute(SessionAuthentication.__J_AUTHENTICATED) != null) {
      // If the session already been authenticated, return the stored auth details
      return (SessionAuthentication) session.getAttribute(SessionAuthentication.__J_AUTHENTICATED);
    }

    // Create a new session authentication and set it in session.
    final SessionAuthentication sessionAuth = new SessionAuthentication(getAuthMethod(),
        loginService.login(null, null), null);

    session.setAttribute(SessionAuthentication.__J_AUTHENTICATED, sessionAuth);

    return sessionAuth;
  }

  @Override
  public boolean secureResponse(ServletRequest req, ServletResponse resp, boolean mandatory,
      User validatedUser) throws ServerAuthException {
    return true;
  }
}
