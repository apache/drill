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

import com.google.common.collect.ImmutableList;
import org.apache.drill.common.AutoCloseables;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.server.DrillbitContext;
import org.eclipse.jetty.security.MappedLoginService.RolePrincipal;

import java.io.IOException;
import java.security.Principal;
import java.util.List;

/**
 * Captures Drill user credentials and resources in a session.
 */
public class DrillUserPrincipal implements Principal, AutoCloseable {
  public static final String ANONYMOUS_USER = "anonymous";

  public static final String AUTHENTICATED_ROLE = "authenticated";
  public static final String ADMIN_ROLE = "admin";

  public static final String[] ADMIN_USER_ROLES = new String[] { AUTHENTICATED_ROLE, ADMIN_ROLE };
  public static final String[] NON_ADMIN_USER_ROLES = new String[] { AUTHENTICATED_ROLE };

  public static final List<RolePrincipal> ADMIN_PRINCIPALS = ImmutableList.of(
      new RolePrincipal(AUTHENTICATED_ROLE),
      new RolePrincipal(ADMIN_ROLE));

  public static final List<RolePrincipal> NON_ADMIN_PRINCIPALS =
      ImmutableList.of(new RolePrincipal(AUTHENTICATED_ROLE));

  protected DrillClient drillClient;

  private final String userName;
  private final boolean isAdmin;

  public DrillUserPrincipal(final String userName, final boolean isAdmin, final DrillClient drillClient) {
    this.userName = userName;
    this.isAdmin = isAdmin;
    this.drillClient = drillClient;
  }

  @Override
  public String getName() {
    return userName;
  }

  /**
   * @return Return {@link DrillClient} instanced with credentials of this user principal. Returned {@link DrillClient}
   * must be returned using {@link #recycleDrillClient(DrillClient)} for proper resource cleanup.
   */
  public DrillClient getDrillClient() throws IOException {
    return drillClient;
  }

  /**
   * Return {@link DrillClient} returned from {@link #getDrillClient()} for proper resource cleanup or reuse.
   */
  public void recycleDrillClient(final DrillClient client) throws IOException {
    // default is no-op. we reuse DrillClient
  }

  /**
   * Is the user identified by this user principal can manage (read) the profile owned by the given user?
   * @param profileOwner Owner of the profile.
   * @return
   */
  public boolean canManageProfileOf(final String profileOwner) {
    return isAdmin || userName.equals(profileOwner);
  }

  /**
   * Is the user identified by this user principal can manage (cancel) the query issued by the given user?
   * @param queryUser User who launched the query.
   * @return
   */
  public boolean canManageQueryOf(final String queryUser) {
    return isAdmin || userName.equals(queryUser);
  }

  @Override
  public void close() throws Exception {
    if (drillClient != null) {
      drillClient.close();
      drillClient = null; // Reset it to null to avoid closing multiple times.
    }
  }

  /**
   * {@link DrillUserPrincipal} for anonymous (auth disabled) mode.
   */
  public static class AnonDrillUserPrincipal extends DrillUserPrincipal {
    private final DrillbitContext drillbitContext;

    public AnonDrillUserPrincipal(final DrillbitContext drillbitContext) {
      super(ANONYMOUS_USER, true /* in anonymous (auth disabled) mode all users are admins */, null);
      this.drillbitContext = drillbitContext;
    }

    @Override
    public DrillClient getDrillClient() throws IOException {
      try {
        // Create a DrillClient
        drillClient = new DrillClient(drillbitContext.getConfig(),
            drillbitContext.getClusterCoordinator(), drillbitContext.getAllocator());
        drillClient.connect();
        return  drillClient;
      } catch (final Exception e) {
        AutoCloseables.close(e, drillClient);
        throw new IOException("Failed to create DrillClient: " + e.getMessage(), e);
      }
    }

    @Override
    public void recycleDrillClient(DrillClient client) throws IOException {
      drillClient.close();
    }
  }
}