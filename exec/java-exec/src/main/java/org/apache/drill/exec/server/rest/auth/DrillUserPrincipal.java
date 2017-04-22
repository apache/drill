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

import com.google.common.collect.ImmutableList;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultChannelPromise;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.rpc.ChannelClosedException;
import org.apache.drill.exec.rpc.user.UserSession;
import org.apache.drill.exec.server.DrillbitContext;
import org.eclipse.jetty.security.MappedLoginService.RolePrincipal;
import org.eclipse.jetty.server.UserIdentity;

import java.security.Principal;
import java.util.List;


/**
 * Captures Drill user credentials and resources in a session.
 */
public class DrillUserPrincipal implements Principal, AutoCloseable {
  public static final String ANONYMOUS_USER = "anonymous";

  public static final String AUTHENTICATED_ROLE = "authenticated";

  public static final String ADMIN_ROLE = "admin";

  public static final String[] ADMIN_USER_ROLES = new String[]{AUTHENTICATED_ROLE, ADMIN_ROLE};

  public static final String[] NON_ADMIN_USER_ROLES = new String[]{AUTHENTICATED_ROLE};

  public static final List<RolePrincipal> ADMIN_PRINCIPALS = ImmutableList.of(new RolePrincipal(AUTHENTICATED_ROLE), new RolePrincipal(ADMIN_ROLE));

  public static final List<RolePrincipal> NON_ADMIN_PRINCIPALS = ImmutableList.of(new RolePrincipal(AUTHENTICATED_ROLE));

  protected UserSession webUserSession;

  // Create a DefaultChannelPromise for each WebUserSession with null channel since there is no physical
  // channel opened for any WebUser with the local Drillbit.
  private final ChannelPromise sessionCloseFuture = new DefaultChannelPromise(null);


  private final String userName;

  private final boolean isAdmin;

  public DrillUserPrincipal(final String userName, final boolean isAdmin, final UserSession userSession) {
    this.userName = userName;
    this.isAdmin = isAdmin;
    this.webUserSession = userSession;
  }

  @Override
  public String getName() {
    return userName;
  }

  /**
   * Get the native WebUser Session created for a user after successful authentication.
   *
   * @return UserSession
   */

  public UserSession getWebUserSession() {
    return webUserSession;
  }

  /**
   * Dummy session close future created for each WebUserSession. Which when set will help to perform cleanup
   * related to this WebUser session.
   *
   * @return ChannelPromise
   */
  public ChannelPromise getSessionCloseFuture() {
    return sessionCloseFuture;
  }

  public void recycleUserSession() {
    // default is no-op. we reuse the session for logged in user
  }

  /**
   * Is the user identified by this user principal can manage (read) the profile owned by the given user?
   *
   * @param profileOwner Owner of the profile.
   * @return true/false
   */
  public boolean canManageProfileOf(final String profileOwner) {
    return isAdmin || userName.equals(profileOwner);
  }

  /**
   * Is the user identified by this user principal can manage (cancel) the query issued by the given user?
   *
   * @param queryUser User who launched the query.
   * @return true/false
   */
  public boolean canManageQueryOf(final String queryUser) {
    return isAdmin || userName.equals(queryUser);
  }

  /**
   * Get's called when {@link AbstractDrillLoginService#logout(UserIdentity)} is triggered by the WebServer
   * SecurityHandler. It means the user session is timed out or user actually logged out of the session.
   */
  @Override
  public void close() throws Exception {
    // Clean up the logged user session
    if (webUserSession != null) {
      webUserSession.close();
      webUserSession = null;
    }

    // Set the channel close future so that the resources related to this WebUserSession is cleaned up properly.
    sessionCloseFuture.setFailure(new ChannelClosedException("WebUser Session is logged out"));
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
    public UserSession getWebUserSession() {
      webUserSession = UserSession.Builder.newBuilder()
          .withCredentials(UserBitShared.UserCredentials.newBuilder()
              .setUserName(ANONYMOUS_USER)
              .build())
          .withOptionManager(drillbitContext.getOptionManager())
          .setSupportComplexTypes(drillbitContext.getConfig().getBoolean(ExecConstants.CLIENT_SUPPORT_COMPLEX_TYPES))
          .build();

      return webUserSession;
    }

    /**
     * For anonymous user we recycle the session after each query is completed.
     */
    @Override
    public void recycleUserSession() {
      if (webUserSession != null) {
        webUserSession.close();
        webUserSession = null;
      }
    }
  }
}