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

import org.apache.drill.common.AutoCloseables;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.server.DrillbitContext;
import org.eclipse.jetty.server.UserIdentity;

import javax.security.auth.Subject;

/**
 * LoginService used when user authentication is disabled. This allows all users and establishes a session for each
 * "anonymous" user.
 */
public class AnonymousLoginService extends AbstractDrillLoginService {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AnonymousLoginService.class);

  public AnonymousLoginService(DrillbitContext drillbitContext) {
    super(drillbitContext);
  }

  @Override
  public String getName() {
    return "AnonymousLoginService";
  }

  @Override
  public UserIdentity login(String userName, Object credentials) {
    DrillClient client = null;
    try {
      client = createDrillClient(DrillUserPrincipal.ANONYMOUS_USER, null);
      final DrillUserPrincipal principal = new DrillUserPrincipal(DrillUserPrincipal.ANONYMOUS_USER,
          true /* all users are admins when auth is disabled */, client);
      final Subject subject = new Subject();
      subject.getPrincipals().add(principal);
      subject.getPrivateCredentials().add(credentials);

      subject.getPrincipals().addAll(DrillUserPrincipal.ADMIN_PRINCIPALS);
      return identityService.newUserIdentity(subject, principal, DrillUserPrincipal.ADMIN_USER_ROLES);
    } catch (final Exception e) {
      AutoCloseables.close(e, client);
      logger.error("Login failed.", e);
      return null;
    }
  }
}
