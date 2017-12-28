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

import com.google.common.base.Function;
import com.google.common.collect.Iterators;

import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.proto.UserBitShared.QueryProfile;
import org.apache.drill.exec.serialization.InstanceSerializer;
import org.apache.drill.exec.server.QueryProfileStoreContext;
import org.apache.drill.exec.server.options.QueryOptionManager;
import org.apache.drill.exec.util.ImpersonationUtil;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * DRILL-5068: Add a new system table for completed profiles (JSON)
 */
public class ProfileJsonIterator implements Iterator<Object> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ProfileJsonIterator.class);

  private final QueryProfileStoreContext profileStoreContext;
  private final InstanceSerializer<QueryProfile> profileSerializer;
  private final Iterator<ProfileInfoJson> itr;
  private final String queryingUsername;
  private final boolean isAdmin;

  public ProfileJsonIterator(FragmentContext context) {
    //Grab profile Store Context
    profileStoreContext = context
        .getDrillbitContext()
        .getProfileStoreContext();

    //Holding a serializer (for JSON extract)
    profileSerializer = profileStoreContext.
        getProfileStoreConfig().getSerializer();

    queryingUsername = context.getQueryUserName();
    isAdmin = hasAdminPrivileges(context.getQueryContext());

    itr = iterateProfileInfoJson(context);
  }

  private boolean hasAdminPrivileges(QueryContext queryContext) {
    if (queryContext == null) {
      return false;
    }
    QueryOptionManager options = queryContext.getOptions();
    if (queryContext.isUserAuthenticationEnabled() &&
        !ImpersonationUtil.hasAdminPrivileges(
          queryContext.getQueryUserName(),
          ExecConstants.ADMIN_USERS_VALIDATOR.getAdminUsers(options),
          ExecConstants.ADMIN_USER_GROUPS_VALIDATOR.getAdminUserGroups(options))) {
      return false;
    }

    //Passed checks
    return true;
  }

  private Iterator<ProfileInfoJson> iterateProfileInfoJson(FragmentContext context) {
    try {
      PersistentStore<UserBitShared.QueryProfile> profiles = profileStoreContext.getCompletedProfileStore();

      return transformJson(getAuthorizedProfiles(profiles.getAll(), queryingUsername, isAdmin));

    } catch (Exception e) {
      logger.error(String.format("Unable to get persistence store: %s, ", profileStoreContext.getProfileStoreConfig().getName(), e));
      return Iterators.singletonIterator(ProfileInfoJson.getDefault());
    }
  }

  //Returns an iterator for authorized profiles
  private Iterator<Entry<String, QueryProfile>> getAuthorizedProfiles (
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

  /**
   * Iterating persistentStore as a iterator of {@link org.apache.drill.exec.store.sys.ProfileJsonIterator.ProfileInfoJson}.
   */
  private Iterator<ProfileInfoJson> transformJson(Iterator<Map.Entry<String, UserBitShared.QueryProfile>> all) {
    return Iterators.transform(all, new Function<Map.Entry<String, UserBitShared.QueryProfile>, ProfileInfoJson>() {
      @Nullable
      @Override
      public ProfileInfoJson apply(@Nullable Map.Entry<String, UserBitShared.QueryProfile> input) {
        if (input == null || input.getValue() == null) {
          return ProfileInfoJson.getDefault();
        }

        //Constructing ProfileInfo
        final String queryID = input.getKey();
        String profileJson = null;
        try {
          profileJson = new String(profileSerializer.serialize(input.getValue()));
        } catch (IOException e) {
          logger.debug("Failed to serialize profile for: " + queryID);
          profileJson = "{ 'message' : 'error (unable to serialize profile: "+ queryID +")' }";
        }

        return new ProfileInfoJson(
            queryID,
            profileJson
         );
      }
    });
  }

  @Override
  public boolean hasNext() {
    return itr.hasNext();
  }

  @Override
  public Object next() {
    return itr.next();
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

  public static class ProfileInfoJson {
    private static final String UnknownValue = "UNKNOWN";

    private static final ProfileInfoJson DEFAULT = new ProfileInfoJson();

    public final String queryId;
    public final String json;

    public ProfileInfoJson(String query_id, String profileJson) {
      this.queryId = query_id;
      this.json = profileJson;
    }

    private ProfileInfoJson() {
      this(UnknownValue, UnknownValue);
    }

    /**
     * If unable to get ProfileInfo, use this default instance instead.
     * @return the default instance
     */
    public static final ProfileInfoJson getDefault() {
      return DEFAULT;
    }
  }
}


