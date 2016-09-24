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
import org.apache.drill.exec.proto.UserBitShared.QueryProfile;
import org.apache.drill.exec.proto.helper.QueryIdHelper;
import org.apache.drill.exec.work.WorkManager;

import java.sql.Timestamp;
import java.util.Iterator;

/**
 * Iterator which returns {@link QueryInfo} for each query foreman running in this drillbit
 */
public class QueryIterator implements Iterator<Object> {
  private final WorkManager workManager;
  private final Iterator<QueryProfile> iter;

  public QueryIterator(FragmentContext c) {
    this.workManager = c.getDrillbitContext().getWorkManager();
    iter = ImmutableList.copyOf(workManager.getQueries()).iterator();
  }

  @Override
  public boolean hasNext() {
    return iter.hasNext();
  }

  @Override
  public Object next() {
    QueryProfile profile = iter.next();
    QueryInfo queryInfo = new QueryInfo();
    queryInfo.foreman = profile.getForeman().getAddress();
    queryInfo.user = profile.getUser();
    queryInfo.queryId = QueryIdHelper.getQueryId(profile.getId());
    queryInfo.query = profile.getQuery();
    queryInfo.startTime = new Timestamp(profile.getStart());
    return queryInfo;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

  public static class QueryInfo {
    /**
     * The host where foreman is running
     */
    public String foreman;
    /**
     * User who submitted query
     */
    public String user;
    public String queryId;
    /**
     * Query sql string
     */
    public String query;
    public Timestamp startTime;
  }
}
