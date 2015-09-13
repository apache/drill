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
import org.eclipse.jetty.security.DefaultIdentityService;
import org.eclipse.jetty.security.IdentityService;
import org.eclipse.jetty.security.LoginService;
import org.eclipse.jetty.server.UserIdentity;

import java.util.Properties;

/**
 * LoginService implementation which abstracts common functionality needed when user authentication is enabled or
 * disabled.
 */
public abstract class AbstractDrillLoginService implements LoginService {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractDrillLoginService.class);

  protected final DrillbitContext drillbitContext;
  protected IdentityService identityService = new DefaultIdentityService();

  public AbstractDrillLoginService(final DrillbitContext drillbitContext) {
    this.drillbitContext = drillbitContext;
  }

  protected DrillClient createDrillClient(final String userName, final String password) throws Exception {
    DrillClient drillClient = null;

    try {
      // Create a DrillClient
      drillClient = new DrillClient(drillbitContext.getConfig(),
          drillbitContext.getClusterCoordinator(), drillbitContext.getAllocator());
      final Properties props = new Properties();
      props.setProperty("user", userName);
      if (password != null) {
        props.setProperty("password", password);
      }
      drillClient.connect(props);
      return  drillClient;
    } catch (final Exception e) {
      AutoCloseables.close(e, drillClient);
      throw e;
    }
  }

  @Override
  public boolean validate(UserIdentity user) {
    // This is called for every request after authentication is complete to make sure the user is still valid.
    // Once a user is authenticated we assume that the user is still valid. This behavior is similar to ODBC/JDBC where
    // once a user is logged-in we don't recheck the credentials again in the same session.
    return true;
  }

  @Override
  public IdentityService getIdentityService() {
    return identityService;
  }

  @Override
  public void setIdentityService(IdentityService identityService) {
    this.identityService = identityService;
  }

  /**
   * This gets called whenever a session is invalidated (because of user logout) or timed out.
   * @param user
   */
  @Override
  public void logout(UserIdentity user) {
    final DrillUserPrincipal principal = (DrillUserPrincipal) user.getUserPrincipal();
    try {
      principal.close();
    } catch (final Exception e) {
      logger.error("Failure in logging out.", e);
    }
  }
}
