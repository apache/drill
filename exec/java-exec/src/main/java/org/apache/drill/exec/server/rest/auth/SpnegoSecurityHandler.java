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

import com.google.common.collect.ImmutableSet;
import org.apache.drill.common.exceptions.DrillException;
import org.apache.drill.exec.server.DrillbitContext;
import org.eclipse.jetty.ee10.servlet.security.ConstraintMapping;
import org.eclipse.jetty.security.Authenticator;
import org.eclipse.jetty.security.Constraint;

import java.util.Collections;

import static org.apache.drill.exec.server.rest.auth.DrillUserPrincipal.ADMIN_ROLE;
import static org.apache.drill.exec.server.rest.auth.DrillUserPrincipal.AUTHENTICATED_ROLE;

@SuppressWarnings({"rawtypes", "unchecked"})
public class SpnegoSecurityHandler extends DrillHttpConstraintSecurityHandler {

  @Override
  public String getImplName() {
    return Authenticator.SPNEGO_AUTH;
  }

  @Override
  public void doSetup(DrillbitContext dbContext) throws DrillException {
    // Use custom DrillSpnegoAuthenticator with Drill-specific configuration
    DrillSpnegoAuthenticator authenticator = new DrillSpnegoAuthenticator();
    DrillSpnegoLoginService loginService = new DrillSpnegoLoginService(dbContext);

    // Create constraint that requires authentication
    Constraint constraint = new Constraint.Builder()
        .name("SPNEGO")
        .roles(AUTHENTICATED_ROLE)
        .build();

    // Apply constraint to all paths (/*)
    ConstraintMapping mapping = new ConstraintMapping();
    mapping.setPathSpec("/*");
    mapping.setConstraint(constraint);

    // Set up the security handler with constraint mappings
    setConstraintMappings(Collections.singletonList(mapping), ImmutableSet.of(AUTHENTICATED_ROLE, ADMIN_ROLE));
    setAuthenticator(authenticator);
    setLoginService(loginService);

    // Enable session management for authentication caching
    setSessionRenewedOnAuthentication(true); // Renew session ID on auth for security
  }
}