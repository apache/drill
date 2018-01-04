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
package org.apache.drill.exec.store.sys;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.proto.UserBitShared.QueryProfile;
import org.apache.drill.exec.server.QueryProfileStoreContext;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.util.ImpersonationUtil;

/**
 * Base class for Profile Iterators
 */
public abstract class ProfileIterator implements Iterator<Object> {
  protected final QueryProfileStoreContext profileStoreContext;
  protected final String queryingUsername;
  protected final boolean isAdmin;

  public ProfileIterator(FragmentContext context) {
    //Grab profile Store Context
    profileStoreContext = context
        .getDrillbitContext()
        .getProfileStoreContext();

    queryingUsername = context.getQueryUserName();
    isAdmin = hasAdminPrivileges(context);
  }

  protected boolean hasAdminPrivileges(FragmentContext context) {
    OptionManager options = context.getOptions();
    if (context.isUserAuthenticationEnabled() &&
        !ImpersonationUtil.hasAdminPrivileges(
          context.getQueryUserName(),
          ExecConstants.ADMIN_USERS_VALIDATOR.getAdminUsers(options),
          ExecConstants.ADMIN_USER_GROUPS_VALIDATOR.getAdminUserGroups(options))) {
      return false;
    }

    //Passed checks
    return true;
  }

  //Returns an iterator for authorized profiles
  protected Iterator<Entry<String, QueryProfile>> getAuthorizedProfiles (
      Iterator<Entry<String, QueryProfile>> allProfiles, String username, boolean isAdministrator) {
    if (isAdministrator) {
      return allProfiles;
    }

    List<Entry<String, QueryProfile>> authorizedProfiles = new LinkedList<Entry<String, QueryProfile>>();
    while (allProfiles.hasNext()) {
      Entry<String, QueryProfile> profileKVPair = allProfiles.next();
      //Check if user matches
      if (profileKVPair.getValue().getUser().equals(username)) {
        authorizedProfiles.add(profileKVPair);
      }
    }
    return authorizedProfiles.iterator();
  }

  protected long computeDuration(long startTime, long endTime) {
    if (endTime > startTime && startTime > 0) {
      return (endTime - startTime);
    } else {
      return 0;
    }
  }
}
