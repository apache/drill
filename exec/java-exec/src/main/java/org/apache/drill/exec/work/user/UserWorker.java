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

import java.util.Random;
import java.util.UUID;

import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.GeneralRPCProtos.Ack;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.UserBitShared.QueryResult;
import org.apache.drill.exec.proto.UserBitShared.QueryResult.QueryState;
import org.apache.drill.exec.proto.UserProtos.RequestResults;
import org.apache.drill.exec.proto.UserProtos.RunQuery;
import org.apache.drill.exec.rpc.Acks;
import org.apache.drill.exec.rpc.user.UserServer.UserClientConnection;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.store.SchemaFactory;
import org.apache.drill.exec.work.WorkManager.WorkerBee;
import org.apache.drill.exec.work.foreman.Foreman;
import org.apache.drill.exec.work.fragment.FragmentExecutor;

public class UserWorker{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(UserWorker.class);

  private final WorkerBee bee;

  public UserWorker(WorkerBee bee) {
    super();
    this.bee = bee;
  }

  public QueryId submitWork(UserClientConnection connection, RunQuery query) {
    Random r = new Random();

    // create a new queryid where the first four bytes are a growing time (each new value comes earlier in sequence).  Last 12 bytes are random.
    long time = (int) (System.currentTimeMillis()/1000);
    long p1 = ((Integer.MAX_VALUE - time) << 32) + r.nextInt();
    long p2 = r.nextLong();
    QueryId id = QueryId.newBuilder().setPart1(p1).setPart2(p2).build();
    Foreman foreman = new Foreman(bee, bee.getContext(), connection, id, query);
    bee.addNewForeman(foreman);
    return id;
  }

  public QueryResult getResult(UserClientConnection connection, RequestResults req) {
    Foreman foreman = bee.getForemanForQueryId(req.getQueryId());
    if (foreman == null) {
      return QueryResult.newBuilder().setQueryState(QueryState.UNKNOWN_QUERY).build();
    }
    return foreman.getResult(connection, req);
  }

  public Ack cancelQuery(QueryId query) {
    Foreman foreman = bee.getForemanForQueryId(query);
    if(foreman != null) {
      foreman.cancel();
    }
    return Acks.OK;
  }

  public Ack cancelFragment(FragmentHandle handle) {
    FragmentExecutor runner = bee.getFragmentRunner(handle);
    if (runner != null) {
      runner.cancel();
    }
    return Acks.OK;
  }

  public SchemaFactory getSchemaFactory() {
    return bee.getContext().getSchemaFactory();
  }

  public OptionManager getSystemOptions() {
    return bee.getContext().getOptionManager();
  }

}
