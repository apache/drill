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

import org.apache.drill.exec.ops.ExecutorFragmentContext;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.proto.UserBitShared.QueryProfile;

import javax.annotation.Nullable;
import java.sql.Timestamp;
import java.util.Iterator;
import java.util.Map.Entry;

/**
 * System table listing completed profiles
 */
public class ProfileInfoIterator extends ProfileIterator {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ProfileInfoIterator.class);

  private final Iterator<ProfileInfo> itr;

  public ProfileInfoIterator(ExecutorFragmentContext context) {
    super(context);
    itr = iterateProfileInfo();
  }

  //Returns an iterator for authorized profiles
  private Iterator<ProfileInfo> iterateProfileInfo() {
    try {
      //Transform authorized profiles to iterator for ProfileInfo
      return transform(
          getAuthorizedProfiles(
            profileStoreContext
              .getCompletedProfileStore()
              .getAll(),
            queryingUsername, isAdmin));

    } catch (Exception e) {
      logger.error(e.getMessage());
      return Iterators.singletonIterator(ProfileInfo.getDefault());
    }
  }

  /**
   * Iterating persistentStore as a iterator of {@link org.apache.drill.exec.store.sys.ProfileInfoIterator.ProfileInfo}.
   */
  private Iterator<ProfileInfo> transform(Iterator<Entry<String, UserBitShared.QueryProfile>> all) {
    return Iterators.transform(all, new Function<Entry<String, UserBitShared.QueryProfile>, ProfileInfo>() {
      @Nullable
      @Override
      public ProfileInfo apply(@Nullable Entry<String, UserBitShared.QueryProfile> input) {
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
    private static final String UnknownValue = "N/A";

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


