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

import java.io.IOException;
import java.util.List;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.proto.BitControl.FragmentStatus;
import org.apache.drill.exec.proto.SchemaUserBitShared;
import org.apache.drill.exec.proto.UserBitShared.MajorFragmentProfile;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.UserBitShared.QueryProfile;
import org.apache.drill.exec.proto.UserBitShared.QueryResult.QueryState;
import org.apache.drill.exec.proto.UserProtos.RunQuery;
import org.apache.drill.exec.proto.helper.QueryIdHelper;
import org.apache.drill.exec.store.sys.PStore;
import org.apache.drill.exec.store.sys.PStoreConfig;
import org.apache.drill.exec.store.sys.PStoreProvider;

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.google.common.collect.Lists;

public class QueryStatus {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(QueryStatus.class);

  public static final PStoreConfig<QueryProfile> QUERY_PROFILE = PStoreConfig.
          newProtoBuilder(SchemaUserBitShared.QueryProfile.WRITE, SchemaUserBitShared.QueryProfile.MERGE).name("query_profiles").build();


  // doesn't need to be thread safe as fragmentDataMap is generated in a single thread and then accessed by multiple threads for reads only.
  private IntObjectOpenHashMap<IntObjectOpenHashMap<FragmentData>> fragmentDataMap = new IntObjectOpenHashMap<IntObjectOpenHashMap<FragmentData>>();
  private List<FragmentData> fragmentDataSet = Lists.newArrayList();

  private final String queryId;
  private final QueryId id;
  private RunQuery query;
  private String planText;
  private Foreman foreman;
  private long startTime;
  private long endTime;
  private int totalFragments;
  private int finishedFragments = 0;

  private final PStore<QueryProfile> profileCache;

  public QueryStatus(RunQuery query, QueryId id, PStoreProvider provider, Foreman foreman){
    this.id = id;
    this.query = query;
    this.queryId = QueryIdHelper.getQueryId(id);
    try {
      this.profileCache = provider.getPStore(QUERY_PROFILE);
    } catch (IOException e) {
      throw new DrillRuntimeException(e);
    }
    this.foreman = foreman;
  }

  public List<FragmentData> getFragmentData() {
    return fragmentDataSet;
  }

  public void setPlanText(String planText){
    this.planText = planText;
    updateCache();

  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  public void setEndTime(long endTime) {
    this.endTime = endTime;
  }

  public void setTotalFragments(int totalFragments) {
    this.totalFragments = totalFragments;
  }

  public void incrementFinishedFragments() {
    finishedFragments++;
    assert finishedFragments <= totalFragments;
  }

  void add(FragmentData data){
    int majorFragmentId = data.getHandle().getMajorFragmentId();
    int minorFragmentId = data.getHandle().getMinorFragmentId();
    IntObjectOpenHashMap<FragmentData> minorMap = fragmentDataMap.get(majorFragmentId);
    if(minorMap == null){
      minorMap = new IntObjectOpenHashMap<FragmentData>();
      fragmentDataMap.put(majorFragmentId, minorMap);
    }

    minorMap.put(minorFragmentId, data);
    fragmentDataSet.add(data);
  }

  void update(FragmentStatus status, boolean updateCache){
    int majorFragmentId = status.getHandle().getMajorFragmentId();
    int minorFragmentId = status.getHandle().getMinorFragmentId();
    fragmentDataMap.get(majorFragmentId).get(minorFragmentId).setStatus(status);
    if (updateCache) {
      updateCache();
    }
  }

  public void updateCache(){
    QueryState queryState = foreman.getQueryState();
    boolean fullStatus = queryState == QueryState.COMPLETED || queryState == QueryState.FAILED;
    profileCache.put(queryId, getAsProfile(fullStatus));
  }

  @Override
  public String toString(){
    return fragmentDataMap.toString();
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

    @Override
    public String toString(){
      return major + ":" + minor;
    }
  }

  public QueryProfile getAsProfile(boolean fullStatus){
    QueryProfile.Builder b = QueryProfile.newBuilder();
    b.setQuery(query.getPlan());
    b.setType(query.getType());
    if(planText != null) b.setPlan(planText);
    b.setId(id);
    if (fullStatus) {
      for(int i = 0; i < fragmentDataMap.allocated.length; i++){
        if(fragmentDataMap.allocated[i]){
          int majorFragmentId = fragmentDataMap.keys[i];
          IntObjectOpenHashMap<FragmentData> minorMap = (IntObjectOpenHashMap<FragmentData>) ((Object[]) fragmentDataMap.values)[i];

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
    }

    b.setState(foreman.getQueryState());
    b.setForeman(foreman.getContext().getCurrentEndpoint());
    b.setStart(startTime);
    b.setEnd(endTime);
    b.setTotalFragments(totalFragments);
    b.setFinishedFragments(finishedFragments);
    return b.build();
  }
}
