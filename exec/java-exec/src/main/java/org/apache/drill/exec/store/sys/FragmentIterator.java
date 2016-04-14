/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.sys;

import com.google.common.collect.ImmutableList;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.proto.CoordinationProtos;
import org.apache.drill.exec.proto.UserBitShared.MinorFragmentProfile;
import org.apache.drill.exec.proto.UserBitShared.OperatorProfile;
import org.apache.drill.exec.proto.UserBitShared.StreamProfile;
import org.apache.drill.exec.proto.helper.QueryIdHelper;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.work.WorkManager;
import org.apache.drill.exec.work.fragment.FragmentExecutor;

import java.sql.Timestamp;
import java.util.Collection;
import java.util.Iterator;

/**
 * Iterator which returns {@link FragmentInfo} for every fragment running in this drillbit.
 */
public class FragmentIterator implements Iterator<Object> {
  private final WorkManager workManager;
  private final Iterator<FragmentExecutor> iter;

  public FragmentIterator(FragmentContext c) {
    this.workManager = c.getDrillbitContext().getWorkManager();
    iter = ImmutableList.copyOf(workManager.getRunningFragments()).iterator();
  }

  @Override
  public boolean hasNext() {
    return iter.hasNext();
  }

  @Override
  public Object next() {
    FragmentExecutor fragmentExecutor = iter.next();
    MinorFragmentProfile profile = fragmentExecutor.getStatus().getProfile();
    FragmentInfo fragmentInfo = new FragmentInfo();
    fragmentInfo.hostname = workManager.getContext().getEndpoint().getAddress();
    fragmentInfo.queryId = QueryIdHelper.getQueryId(fragmentExecutor.getContext().getHandle().getQueryId());
    fragmentInfo.majorFragmentId = fragmentExecutor.getContext().getHandle().getMajorFragmentId();
    fragmentInfo.minorFragmentId = fragmentExecutor.getContext().getHandle().getMinorFragmentId();
    fragmentInfo.rowsProcessed = getRowsProcessed(profile);
    fragmentInfo.memoryUsage = profile.getMemoryUsed();
    fragmentInfo.startTime = new Timestamp(profile.getStartTime());
    return fragmentInfo;
  }

  private long getRowsProcessed(MinorFragmentProfile profile) {
    long maxRecords = 0;
    for (OperatorProfile operatorProfile : profile.getOperatorProfileList()) {
      long records = 0;
      for (StreamProfile inputProfile :operatorProfile.getInputProfileList()) {
        if (inputProfile.hasRecords()) {
          records += inputProfile.getRecords();
        }
      }
      maxRecords = Math.max(maxRecords, records);
    }
    return maxRecords;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

  public static class FragmentInfo {
    public String hostname;
    public String queryId;
    public int majorFragmentId;
    public int minorFragmentId;
    public Long memoryUsage;
    /**
     * The maximum number of input records across all Operators in fragment
     */
    public Long rowsProcessed;
    public Timestamp startTime;
  }
}
