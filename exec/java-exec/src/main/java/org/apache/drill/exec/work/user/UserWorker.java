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
package org.apache.drill.exec.work.user;

import java.util.concurrent.ThreadLocalRandom;

import org.apache.drill.exec.proto.GeneralRPCProtos.Ack;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.UserProtos.GetQueryPlanFragments;
import org.apache.drill.exec.proto.UserProtos.QueryPlanFragments;
import org.apache.drill.exec.proto.UserProtos.RunQuery;
import org.apache.drill.exec.rpc.Acks;
import org.apache.drill.exec.rpc.user.UserServer.UserClientConnection;
import org.apache.drill.exec.rpc.user.UserSession;
import org.apache.drill.exec.rpc.user.UserSession.QueryCountIncrementer;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.work.WorkManager.WorkerBee;
import org.apache.drill.exec.work.foreman.Foreman;

public class UserWorker{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(UserWorker.class);

  private final WorkerBee bee;
  private final QueryCountIncrementer incrementer = new QueryCountIncrementer() {
    @Override
    public void increment(final UserSession session) {
      session.incrementQueryCount(this);
    }
  };

  public UserWorker(WorkerBee bee) {
    super();
    this.bee = bee;
  }

  /**
   * Helper method to generate QueryId
   * @return generated QueryId
   */
  private QueryId queryIdGenerator() {
    ThreadLocalRandom r = ThreadLocalRandom.current();

    // create a new queryid where the first four bytes are a growing time (each new value comes earlier in sequence).  Last 12 bytes are random.
    final long time = (int) (System.currentTimeMillis()/1000);
    final long p1 = ((Integer.MAX_VALUE - time) << 32) + r.nextInt();
    final long p2 = r.nextLong();
    final QueryId id = QueryId.newBuilder().setPart1(p1).setPart2(p2).build();
    return id;
  }

  public QueryId submitWork(UserClientConnection connection, RunQuery query) {
    final QueryId id = queryIdGenerator();
    incrementer.increment(connection.getSession());
    Foreman foreman = new Foreman(bee, bee.getContext(), connection, id, query);
    bee.addNewForeman(foreman);
    return id;
  }

  public Ack cancelQuery(QueryId query) {
    Foreman foreman = bee.getForemanForQueryId(query);
    if(foreman != null) {
      foreman.cancel();
    }
    return Acks.OK;
  }

  public Ack resumeQuery(final QueryId queryId) {
    final Foreman foreman = bee.getForemanForQueryId(queryId);
    if (foreman != null) {
      foreman.resume();
    }
    return Acks.OK;
  }

  public OptionManager getSystemOptions() {
    return bee.getContext().getOptionManager();
  }

  public QueryPlanFragments getQueryPlan(UserClientConnection connection,
      GetQueryPlanFragments req) {
    final QueryId queryId = queryIdGenerator();
    final QueryPlanFragments qPlanFragments = new PlanSplitter().planFragments(bee.getContext(), queryId, req, connection);
    return qPlanFragments;
  }
}
