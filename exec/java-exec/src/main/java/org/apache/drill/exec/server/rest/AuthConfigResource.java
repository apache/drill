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
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.server.rest.auth.DrillHttpSecurityHandlerProvider;
import org.apache.drill.exec.work.WorkManager;
import org.eclipse.jetty.security.Authenticator;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Exposes which authentication mechanisms are configured on this Drillbit so
 * the React login UI knows which buttons to render. Anonymous-readable — the
 * login screen needs this before the user has any session.
 */
@Path("/api/v1/auth-config")
@PermitAll
public class AuthConfigResource {

  @Inject
  private WorkManager workManager;

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Map<String, Object> getAuthConfig() {
    DrillConfig config = workManager.getContext().getConfig();
    boolean authEnabled = config.getBoolean(ExecConstants.USER_AUTHENTICATION_ENABLED);
    Set<String> mechs = authEnabled
        ? DrillHttpSecurityHandlerProvider.getHttpAuthMechanisms(config)
        : Set.of();

    Map<String, Object> result = new HashMap<>();
    result.put("authEnabled", authEnabled);
    result.put("formEnabled", mechs.contains(Authenticator.FORM_AUTH));
    result.put("spnegoEnabled", mechs.contains(Authenticator.SPNEGO_AUTH));
    return result;
  }
}
