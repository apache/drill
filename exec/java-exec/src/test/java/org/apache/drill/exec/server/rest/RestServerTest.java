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

import io.netty.util.concurrent.Promise;
import io.netty.channel.local.LocalAddress;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.proto.UserBitShared.QueryProfile;
import org.apache.drill.exec.proto.helper.QueryIdHelper;
import org.apache.drill.exec.rpc.user.UserSession;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.rest.QueryWrapper.RestQueryBuilder;
import org.apache.drill.exec.server.rest.RestQueryRunner.QueryResult;
import org.apache.drill.exec.work.WorkManager;
import org.apache.drill.exec.work.foreman.Foreman;
import org.apache.drill.test.ClusterTest;
import org.mockito.Mockito;

import static org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl.PROCESS_USER;

public class RestServerTest extends ClusterTest {

  protected QueryResult runQuery(String sql) throws Exception {
    return runQuery(new RestQueryBuilder().query(sql).build());
  }

  protected QueryResult runQueryWithUsername(String sql, String userName) throws Exception {
    return runQuery(
        new RestQueryBuilder()
          .query(sql)
          .userName(userName)
          .build());
  }

  protected QueryResult runQuery(QueryWrapper q) throws Exception {
    DrillbitContext context = cluster.drillbit().getContext();
    WebSessionResources webSessionResources = new WebSessionResources(
      context.getAllocator(),
      new LocalAddress("test"),
      UserSession.Builder.newBuilder()
        .withCredentials(UserBitShared.UserCredentials.newBuilder().setUserName(PROCESS_USER).build())
        .withOptionManagers(context)
        .withStorage(context)
        .build(),
      Mockito.mock(Promise.class));
    WebUserConnection connection = new WebUserConnection.AnonWebUserConnection(webSessionResources);
    return new RestQueryRunner(q, cluster.drillbit().getManager(), connection).run();
  }

  protected QueryProfile getQueryProfile(QueryResult result) {
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
