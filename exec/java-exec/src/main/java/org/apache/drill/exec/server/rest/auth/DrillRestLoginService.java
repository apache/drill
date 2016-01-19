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
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.proto.UserProtos.HandshakeStatus;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.options.SystemOptionManager;
import org.apache.drill.exec.util.ImpersonationUtil;
import org.eclipse.jetty.server.UserIdentity;

import javax.security.auth.Subject;
import java.security.Principal;

/**
 * LoginService used when user authentication is enabled in Drillbit. It validates the user against the user
 * authenticator set in BOOT config.
 */
public class DrillRestLoginService extends AbstractDrillLoginService {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillRestLoginService.class);

  public DrillRestLoginService(final DrillbitContext drillbitContext) {
    super(drillbitContext);
  }

  @Override
  public String getName() {
    return "DrillRestLoginService";
  }

  @Override
  public UserIdentity login(String username, Object credentials) {
    if (!(credentials instanceof String)) {
      return null;
    }

    DrillClient drillClient = null;

    try {
      // Create a DrillClient
      drillClient = createDrillClient(username, (String)credentials);

      final SystemOptionManager sysOptions = drillbitContext.getOptionManager();
      final boolean isAdmin = ImpersonationUtil.hasAdminPrivileges(username,
          sysOptions.getOption(ExecConstants.ADMIN_USERS_KEY).string_val,
          sysOptions.getOption(ExecConstants.ADMIN_USER_GROUPS_KEY).string_val);

      final Principal userPrincipal = new DrillUserPrincipal(username, isAdmin, drillClient);

      final Subject subject = new Subject();
      subject.getPrincipals().add(userPrincipal);
      subject.getPrivateCredentials().add(credentials);

      if (isAdmin) {
        subject.getPrincipals().addAll(DrillUserPrincipal.ADMIN_PRINCIPALS);
        return identityService.newUserIdentity(subject, userPrincipal, DrillUserPrincipal.ADMIN_USER_ROLES);
      } else {
        subject.getPrincipals().addAll(DrillUserPrincipal.NON_ADMIN_PRINCIPALS);
        return identityService.newUserIdentity(subject, userPrincipal, DrillUserPrincipal.NON_ADMIN_USER_ROLES);
      }
    } catch (final Exception e) {
      AutoCloseables.close(e, drillClient);
      if (e.getMessage().contains(HandshakeStatus.AUTH_FAILED.toString())) {
        logger.trace("Authentication failed for user '{}'", username, e);
      } else {
        logger.error("Error while creating the DrillClient: user '{}'", username, e);
      }
      return null;
    }
  }
}
