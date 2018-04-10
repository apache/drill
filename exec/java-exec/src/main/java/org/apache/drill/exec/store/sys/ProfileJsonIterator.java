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

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

import javax.annotation.Nullable;

import org.apache.drill.exec.ops.ExecutorFragmentContext;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.proto.UserBitShared.QueryProfile;
import org.apache.drill.exec.serialization.InstanceSerializer;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;

/**
 * System table listing completed profiles as JSON documents
 */
public class ProfileJsonIterator extends ProfileIterator {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ProfileJsonIterator.class);

  private final InstanceSerializer<QueryProfile> profileSerializer;
  private final Iterator<ProfileJson> itr;

  public ProfileJsonIterator(ExecutorFragmentContext context) {
    super(context);
    //Holding a serializer (for JSON extract)
    profileSerializer = profileStoreContext.
        getProfileStoreConfig().getSerializer();

    itr = iterateProfileInfoJson();
  }

  //Returns an iterator for authorized profiles
  private Iterator<ProfileJson> iterateProfileInfoJson() {
    try {
      //Transform authorized profiles to iterator for ProfileInfoJson
      return transformJson(
          getAuthorizedProfiles(
            profileStoreContext
              .getCompletedProfileStore()
              .getAll(),
            queryingUsername, isAdmin));

    } catch (Exception e) {
      logger.error(e.getMessage());
      return Iterators.singletonIterator(ProfileJson.getDefault());
    }
  }

  /**
   * Iterating persistentStore as a iterator of {@link org.apache.drill.exec.store.sys.ProfileJsonIterator.ProfileJson}.
   */
  private Iterator<ProfileJson> transformJson(Iterator<Entry<String, UserBitShared.QueryProfile>> all) {
    return Iterators.transform(all, new Function<Entry<String, UserBitShared.QueryProfile>, ProfileJson>() {
      @Nullable
      @Override
      public ProfileJson apply(@Nullable Entry<String, UserBitShared.QueryProfile> input) {
        if (input == null || input.getValue() == null) {
          return ProfileJson.getDefault();
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

        return new ProfileJson(
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

  public static class ProfileJson {
    private static final String UnknownValue = "N/A";

    private static final ProfileJson DEFAULT = new ProfileJson();

    public final String queryId;
    public final String json;

    public ProfileJson(String query_id, String profileJson) {
      this.queryId = query_id;
      this.json = profileJson;
    }

    private ProfileJson() {
      this(UnknownValue, UnknownValue);
    }

    /**
     * If unable to get ProfileInfo, use this default instance instead.
     * @return the default instance
     */
    public static final ProfileJson getDefault() {
      return DEFAULT;
    }
  }
}
