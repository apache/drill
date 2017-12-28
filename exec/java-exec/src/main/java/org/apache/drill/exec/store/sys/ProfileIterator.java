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
import org.apache.drill.exec.server.QueryProfileStoreContext;
import org.apache.drill.exec.server.options.QueryOptionManager;
import org.apache.drill.exec.util.ImpersonationUtil;

import javax.annotation.Nullable;
import java.sql.Timestamp;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * DRILL-5068: Add a new system table for completed profiles
 */
public class ProfileIterator implements Iterator<Object> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ProfileIterator.class);

  private final QueryProfileStoreContext profileStoreContext;
  private final Iterator<?> itr;
  private final String queryingUsername;
  private final boolean isAdmin;

  public ProfileIterator(FragmentContext context) {
    //Grab profile Store Context
    profileStoreContext = context
        .getDrillbitContext()
        .getProfileStoreContext();

    queryingUsername = context.getQueryUserName();
    isAdmin = hasAdminPrivileges(context.getQueryContext());

    itr = iterateProfileInfo(context);
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

  private Iterator<ProfileInfo> iterateProfileInfo(FragmentContext context) {
    try {
      PersistentStore<UserBitShared.QueryProfile> profiles = profileStoreContext.getCompletedProfileStore();

      return transform(getAuthorizedProfiles(profiles.getAll(), queryingUsername, isAdmin));

    } catch (Exception e) {
      logger.error(String.format("Unable to get persistence store: %s, ", profileStoreContext.getProfileStoreConfig().getName(), e));
      return Iterators.singletonIterator(ProfileInfo.getDefault());
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
   * Iterating persistentStore as a iterator of {@link org.apache.drill.exec.store.sys.ProfileIterator.ProfileInfo}.
   */
  private Iterator<ProfileInfo> transform(Iterator<Map.Entry<String, UserBitShared.QueryProfile>> all) {
    return Iterators.transform(all, new Function<Map.Entry<String, UserBitShared.QueryProfile>, ProfileInfo>() {
      @Nullable
      @Override
      public ProfileInfo apply(@Nullable Map.Entry<String, UserBitShared.QueryProfile> input) {
        if (input == null || input.getValue() == null) {
          return ProfileInfo.getDefault();
        }

        //Constructing ProfileInfo
        final String queryID = input.getKey();
        final QueryProfile profile = input.getValue();
        //For cases where query was never queued
        final long assumedQueueEndTime = profile.getQueueWaitEnd()> 0 ? profile.getQueueWaitEnd() : profile.getPlanEnd();
        return new ProfileInfo(
            queryID,
            new Timestamp(profile.getStart()),
            profile.getForeman().getAddress(),
            profile.getTotalFragments(),
            profile.getUser(),
            profile.getQueueName(),
            computeDuration(profile.getStart(), profile.getPlanEnd()),
            computeDuration(profile.getPlanEnd(), assumedQueueEndTime),
            computeDuration(assumedQueueEndTime, profile.getEnd()),
            profile.getState().name(),
            profile.getQuery()
         );
      }
    });
  }

  protected long computeDuration(long startTime, long endTime) {
    if (endTime > startTime && startTime > 0) {
      return (endTime - startTime);
    } else {
      return 0;
    }
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

  public static class ProfileInfo {
    private static final String UnknownValue = "UNKNOWN";

    private static final ProfileInfo DEFAULT = new ProfileInfo();

    public final String queryId;
    public final Timestamp startTime;
    public final String foreman;
    public final long fragments;
    public final String user;
    public final String queue;
    public final long planTime;
    public final long queueTime;
    public final long executeTime;
    public final long totalTime;
    public final String state;
    public final String query;

    public ProfileInfo(String query_id, Timestamp time, String foreman, long fragmentCount, String username,
        String queueName, long planDuration, long queueWaitDuration, long executeDuration,
        String state, String query) {
      this.queryId = query_id;
      this.startTime = time;
      this.foreman = foreman;
      this.fragments = fragmentCount;
      this.user = username;
      this.queue = queueName;
      this.planTime = planDuration;
      this.queueTime = queueWaitDuration;
      this.executeTime = executeDuration;
      this.totalTime = this.planTime + this.queueTime + this.executeTime;
      this.query = query;
      this.state = state;
    }

    private ProfileInfo() {
      this(UnknownValue, new Timestamp(0), UnknownValue, 0L, UnknownValue, UnknownValue, 0L, 0L, 0L, UnknownValue, UnknownValue);
    }

    /**
     * If unable to get ProfileInfo, use this default instance instead.
     * @return the default instance
     */
    public static final ProfileInfo getDefault() {
      return DEFAULT;
    }
  }
}


