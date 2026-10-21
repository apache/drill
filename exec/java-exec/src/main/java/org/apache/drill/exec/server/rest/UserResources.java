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

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.apache.drill.exec.server.rest.auth.DrillUserPrincipal;

import jakarta.annotation.security.RolesAllowed;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.SecurityContext;

/**
 * REST endpoint that returns the current user's identity and role.
 * When authentication is disabled, returns anonymous with admin=true.
 */
@Path("/api/v1/user")
@Tag(name = "User", description = "Current user identity")
@RolesAllowed(DrillUserPrincipal.AUTHENTICATED_ROLE)
public class UserResources {

  @Inject
  SecurityContext securityContext;

  @Inject
  DrillUserPrincipal principal;

  public static class UserInfo {
    @JsonProperty
    public String username;

    @JsonProperty
    public boolean isAdmin;

    @JsonProperty
    public boolean authEnabled;

    public UserInfo() {
    }

    public UserInfo(String username, boolean isAdmin, boolean authEnabled) {
      this.username = username;
      this.isAdmin = isAdmin;
      this.authEnabled = authEnabled;
    }
  }

  @GET
  @Path("/me")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Get current user",
      description = "Returns the current user's username and admin status")
  public UserInfo getCurrentUser() {
    String username = principal.getName();
    boolean isAdmin = principal.isAdminUser();
    boolean authEnabled = !DrillUserPrincipal.ANONYMOUS_USER.equals(username);
    return new UserInfo(username, isAdmin, authEnabled);
  }
}
