/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
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
import org.eclipse.jetty.security.ConstraintMapping;
import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.eclipse.jetty.security.LoginService;
import org.eclipse.jetty.security.authentication.LoginAuthenticator;

import java.util.Collections;
import java.util.Set;

import static org.apache.drill.exec.server.rest.auth.DrillUserPrincipal.ADMIN_ROLE;
import static org.apache.drill.exec.server.rest.auth.DrillUserPrincipal.AUTHENTICATED_ROLE;

/**
 * Accessor class that extends the ConstraintSecurityHandler to expose protected method's for start and stop of Handler.
 * This is needed since now {@link DrillHttpSecurityHandlerProvider} composes of 2 security handlers -
 * For FORM and SPNEGO and has responsibility to start/stop of those handlers.
 **/
public abstract class DrillHttpConstraintSecurityHandler extends ConstraintSecurityHandler {

    @Override
    public void doStart() throws Exception {
        super.doStart();
    }

    @Override
    public void doStop() throws Exception {
        super.doStop();
    }

    public abstract void doSetup(DrillbitContext dbContext) throws DrillException;

    public void setup(LoginAuthenticator authenticator, LoginService loginService) {
      final Set<String> knownRoles = ImmutableSet.of(AUTHENTICATED_ROLE, ADMIN_ROLE);
      setConstraintMappings(Collections.<ConstraintMapping>emptyList(), knownRoles);
      setAuthenticator(authenticator);
      setLoginService(loginService);
    }

    public abstract String getImplName();
}