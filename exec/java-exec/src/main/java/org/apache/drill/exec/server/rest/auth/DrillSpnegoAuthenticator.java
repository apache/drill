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


import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpVersion;
import org.eclipse.jetty.security.ServerAuthException;
import org.eclipse.jetty.security.SpnegoLoginService;
import org.eclipse.jetty.security.UserAuthentication;
import org.eclipse.jetty.security.authentication.DeferredAuthentication;
import org.eclipse.jetty.security.authentication.FormAuthenticator;
import org.eclipse.jetty.security.authentication.SessionAuthentication;
import org.eclipse.jetty.security.authentication.SpnegoAuthenticator;
import org.eclipse.jetty.server.Authentication;
import org.eclipse.jetty.server.HttpChannel;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.server.UserIdentity;
import org.eclipse.jetty.server.Request;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletResponseWrapper;
import javax.servlet.http.HttpSession;
import java.io.IOException;

public class DrillSpnegoAuthenticator extends SpnegoAuthenticator {

    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillSpnegoAuthenticator.class);

    @Override
    public Authentication validateRequest(ServletRequest request, ServletResponse response, boolean mandatory) throws ServerAuthException {

        HttpServletRequest req = (HttpServletRequest) request;
        HttpServletResponse res = (HttpServletResponse) response;
        HttpSession session = req.getSession(true);
        Authentication authentication = (Authentication) session.getAttribute("org.eclipse.jetty.security.UserIdentity");
        String uri = req.getRequestURI();
        //If the Request URI is for Spnego login then the request is sent to the login module my modifying this flag
        if (uri.equals("/spnegoLogin")) {
            mandatory = true;
        }
        //For logout remove the attribute from the session that holds useridentity
        if (authentication != null && uri.equals("/logout")) {
            session.removeAttribute("org.eclipse.jetty.security.UserIdentity");
            Authentication auth = (Authentication) session.getAttribute("org.eclipse.jetty.security.UserIdentity");
            return auth;
        } else if (authentication != null) {
            return authentication;
        } else {
            String header = req.getHeader(HttpHeader.AUTHORIZATION.asString());
            if (!mandatory) {
                return new DeferredAuthentication(this);
            } else if (header == null) {
                try {
                    if (DeferredAuthentication.isDeferred(res)) {
                        return Authentication.UNAUTHENTICATED;
                    } else {
                        res.setHeader(HttpHeader.WWW_AUTHENTICATE.asString(), HttpHeader.NEGOTIATE.asString());
                        res.sendError(401);
                        return Authentication.SEND_CONTINUE;
                    }
                } catch (IOException var9) {
                    throw new ServerAuthException(var9);
                }
            } else {
                if (header != null && header.startsWith(HttpHeader.NEGOTIATE.asString())) {
                    String spnegoToken = header.substring(10);
                    UserIdentity user = this.login((String) null, spnegoToken, request);
                    //redirect the request to the desired page after successful login
                    if (user != null) {
                        String newUri;
                        synchronized (session) {
                            newUri = (String) session.getAttribute("org.eclipse.jetty.security.form_URI");
                            if (newUri == null || newUri.length() == 0) {
                                newUri = req.getContextPath();
                                if (newUri.length() == 0) {
                                    newUri = "/";
                                }
                            }
                        }
                        response.setContentLength(0);
                        Response base_response = HttpChannel.getCurrentHttpChannel().getResponse();
                        Request base_request = HttpChannel.getCurrentHttpChannel().getRequest();
                        int redirectCode = base_request.getHttpVersion().getVersion() < HttpVersion.HTTP_1_1.getVersion() ? 302 : 303;
                        try {
                            base_response.sendRedirect(redirectCode, res.encodeRedirectURL(newUri));
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        return new UserAuthentication(this.getAuthMethod(), user);
                    }
                }
                return Authentication.UNAUTHENTICATED;
            }
        }
    }


    public UserIdentity login(String username, Object password, ServletRequest request) {
        UserIdentity user = super.login(username, password, request);
        if (user != null) {
            HttpSession session = ((HttpServletRequest) request).getSession(true);
            Authentication cached = new SessionAuthentication(this.getAuthMethod(), user, password);
            session.setAttribute("org.eclipse.jetty.security.UserIdentity", cached);
        }

        return user;
    }
}