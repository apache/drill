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

import jakarta.annotation.security.PermitAll;
import jakarta.inject.Inject;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpSession;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import java.util.Collections;
import java.util.Map;

/**
 * Exposes the CSRF protection token to the React SPA. The token is generated
 * by {@link CsrfTokenInjectFilter} and stored on the session. The client
 * fetches it once at app boot and attaches it to mutating requests via the
 * {@code X-XSRF-TOKEN} header.
 */
@Path("/api/v1/csrf-token")
@PermitAll
public class CsrfTokenResource {

  @Inject
  private HttpServletRequest request;

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Map<String, String> getCsrfToken() {
    // Create a session if one isn't present yet so anonymous-mode clients
    // still get a stable token for the lifetime of their browser session.
    HttpSession session = request.getSession(true);
    String token = (String) session.getAttribute(WebServerConstants.CSRF_TOKEN);
    if (token == null) {
      token = WebUtils.generateCsrfToken();
      session.setAttribute(WebServerConstants.CSRF_TOKEN, token);
    }
    return Collections.singletonMap("token", token);
  }
}
