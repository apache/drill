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
package org.apache.drill.exec.work.foreman;

import org.apache.drill.exec.cache.DistributedCache;
import org.apache.drill.exec.cache.DistributedCache.CacheConfig;
import org.apache.drill.exec.cache.DistributedCache.SerializationMode;
import org.apache.drill.exec.cache.DistributedMap;
import org.apache.drill.exec.proto.BitControl.FragmentStatus;
import org.apache.drill.exec.proto.UserBitShared.MajorFragmentProfile;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.UserBitShared.QueryProfile;
import org.apache.drill.exec.proto.UserProtos.RunQuery;
import org.apache.drill.exec.proto.helper.QueryIdHelper;

import com.carrotsearch.hppc.IntObjectOpenHashMap;

public class QueryStatus {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(QueryStatus.class);

  public static final CacheConfig<String, QueryProfile> QUERY_PROFILE = CacheConfig //
      .newBuilder(QueryProfile.class) //
      .name("sys.queries") //
      .mode(SerializationMode.PROTOBUF) //
      .build();

  // doesn't need to be thread safe as map is generated in a single thread and then accessed by multiple threads for reads only.
  private IntObjectOpenHashMap<IntObjectOpenHashMap<FragmentData>> map = new IntObjectOpenHashMap<IntObjectOpenHashMap<FragmentData>>();

  private final String queryId;
  private final QueryId id;
  private RunQuery query;
  private String planText;

  private final DistributedMap<String, QueryProfile> profileCache;

  public QueryStatus(RunQuery query, QueryId id, DistributedCache cache){
    this.id = id;
    this.query = query;
    this.queryId = QueryIdHelper.getQueryId(id);
    this.profileCache = cache.getMap(QUERY_PROFILE);
  }

  public void setPlanText(String planText){
    this.planText = planText;
    updateCache();

  }
  void add(FragmentData data){
    int majorFragmentId = data.getHandle().getMajorFragmentId();
    int minorFragmentId = data.getHandle().getMinorFragmentId();
    IntObjectOpenHashMap<FragmentData> minorMap = map.get(majorFragmentId);
    if(minorMap == null){
      minorMap = new IntObjectOpenHashMap<FragmentData>();
      map.put(majorFragmentId, minorMap);
    }

    minorMap.put(minorFragmentId, data);
  }

  void update(FragmentStatus status){
    int majorFragmentId = status.getHandle().getMajorFragmentId();
    int minorFragmentId = status.getHandle().getMinorFragmentId();
    map.get(majorFragmentId).get(minorFragmentId).setStatus(status);
    updateCache();
  }

  private void updateCache(){
    profileCache.put(queryId, getAsProfile());
  }

  public String toString(){
    return map.toString();
  }

  public static class FragmentId{
    int major;
    int minor;

    public FragmentId(FragmentStatus status){
      this.major = status.getHandle().getMajorFragmentId();
      this.minor = status.getHandle().getMinorFragmentId();
    }

    public FragmentId(FragmentData data){
      this.major = data.getHandle().getMajorFragmentId();
      this.minor = data.getHandle().getMinorFragmentId();
    }

    public FragmentId(int major, int minor) {
      super();
      this.major = major;
      this.minor = minor;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + major;
      result = prime * result + minor;
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      FragmentId other = (FragmentId) obj;
      if (major != other.major)
        return false;
      if (minor != other.minor)
        return false;
      return true;
    }

    public String toString(){
      return major + ":" + minor;
    }
  }

  public QueryProfile getAsProfile(){
    QueryProfile.Builder b = QueryProfile.newBuilder();
    b.setQuery(query.getPlan());
    b.setType(query.getType());
    if(planText != null) b.setPlan(planText);
    b.setId(id);
    for(int i = 0; i < map.allocated.length; i++){
      if(map.allocated[i]){
        int majorFragmentId = map.keys[i];
        IntObjectOpenHashMap<FragmentData> minorMap = (IntObjectOpenHashMap<FragmentData>) ((Object[]) map.values)[i];

        MajorFragmentProfile.Builder fb = MajorFragmentProfile.newBuilder();
        fb.setMajorFragmentId(majorFragmentId);
        for(int v = 0; v < minorMap.allocated.length; v++){
          if(minorMap.allocated[v]){
            FragmentData data = (FragmentData) ((Object[]) minorMap.values)[v];
            fb.addMinorFragmentProfile(data.getStatus().getProfile());
          }
        }
        b.addFragmentProfile(fb);
      }
    }

    return b.build();
  }
}
