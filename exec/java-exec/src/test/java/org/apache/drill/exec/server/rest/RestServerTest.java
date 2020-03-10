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
package org.apache.drill.exec.server.rest;

import io.netty.channel.DefaultChannelPromise;
import io.netty.channel.local.LocalAddress;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.proto.helper.QueryIdHelper;
import org.apache.drill.exec.rpc.user.UserSession;
import org.apache.drill.exec.server.options.SystemOptionManager;
import org.apache.drill.exec.server.rest.auth.DrillUserPrincipal;
import org.apache.drill.exec.work.WorkManager;
import org.apache.drill.exec.work.foreman.Foreman;
import org.apache.drill.test.ClusterTest;

public class RestServerTest extends ClusterTest {
  protected QueryWrapper.QueryResult runQuery(String sql) throws Exception {
    return runQuery(new QueryWrapper(sql, "SQL", null, null, null));
  }

  protected QueryWrapper.QueryResult runQueryWithUsername(String sql, String userName) throws Exception {
    return runQuery(new QueryWrapper(sql, "SQL", null, userName, null));
  }

  protected QueryWrapper.QueryResult runQuery(QueryWrapper q) throws Exception {
    SystemOptionManager systemOptions = cluster.drillbit().getContext().getOptionManager();
    DrillUserPrincipal principal = new DrillUserPrincipal.AnonDrillUserPrincipal();
    WebSessionResources webSessionResources = new WebSessionResources(
      cluster.drillbit().getContext().getAllocator(),
      new LocalAddress("test"),
      UserSession.Builder.newBuilder()
        .withOptionManager(systemOptions)
        .withCredentials(UserBitShared.UserCredentials.newBuilder().setUserName(principal.getName()).build())
        .build(),
      new DefaultChannelPromise(null));
    WebUserConnection connection = new WebUserConnection.AnonWebUserConnection(webSessionResources);
    return q.run(cluster.drillbit().getManager(), connection);
  }


  protected UserBitShared.QueryProfile getQueryProfile(QueryWrapper.QueryResult result) {
    String queryId = result.getQueryId();
    WorkManager workManager = cluster.drillbit().getManager();
    Foreman f = workManager.getBee().getForemanForQueryId(QueryIdHelper.getQueryIdFromString(queryId));
    if (f != null) {
      UserBitShared.QueryProfile queryProfile = f.getQueryManager().getQueryProfile();
      if (queryProfile != null) {
        return queryProfile;
      }
    }
    return workManager.getContext().getProfileStoreContext().getCompletedProfileStore().get(queryId);
  }

}
