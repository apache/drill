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
package org.apache.drill.exec.server.rest;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.coord.ClusterCoordinator;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.user.ConnectionThrottle;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.apache.drill.exec.rpc.user.UserResultsListener;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.work.WorkManager;
import org.glassfish.jersey.server.mvc.Viewable;

@Path("/query")
public class QueryResources {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(QueryResources.class);

  @Inject
  WorkManager work;

  @GET
  @Produces(MediaType.TEXT_HTML)
  public Viewable getQuery() {
    return new Viewable("/rest/query/query.ftl");
  }

  @POST
  @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
  @Produces(MediaType.TEXT_HTML)
  public Viewable submitQuery(@FormParam("query") String query, @FormParam("queryType") String queryType) throws Exception {
    final DrillConfig config = work.getContext().getConfig();
    final ClusterCoordinator coordinator = work.getContext().getClusterCoordinator();
    final BufferAllocator allocator = work.getContext().getAllocator();
    DrillClient client = new DrillClient(config, coordinator, allocator);

    UserBitShared.QueryType type = UserBitShared.QueryType.SQL;
    switch (queryType){
      case "SQL" : type = UserBitShared.QueryType.SQL; break;
      case "LOGICAL" : type = UserBitShared.QueryType.LOGICAL; break;
      case "PHYSICAL" : type = UserBitShared.QueryType.PHYSICAL; break;
    }

    client.connect();
    Listener listener = new Listener(new RecordBatchLoader(work.getContext().getAllocator()));
    client.runQuery(type, query, listener);
    List<LinkedList<String>> result = listener.waitForCompletion();
    client.close();

    return new Viewable("/rest/query/result.ftl", result);
  }

  private static class Listener implements UserResultsListener {
    private volatile Exception exception;
    private AtomicInteger count = new AtomicInteger();
    private CountDownLatch latch = new CountDownLatch(1);
    private LinkedList<LinkedList<String>> output = new LinkedList<>();
    private RecordBatchLoader loader;

    Listener(RecordBatchLoader loader) {
      this.loader = loader;
    }

    @Override
    public void submissionFailed(RpcException ex) {
      exception = ex;
      System.out.println("Query failed: " + ex.getMessage());
      latch.countDown();
    }

    @Override
    public void resultArrived(QueryResultBatch result, ConnectionThrottle throttle) {
      int rows = result.getHeader().getRowCount();
      if (result.getData() != null) {
        count.addAndGet(rows);
        try {
          loader.load(result.getHeader().getDef(), result.getData());
          output.add(new LinkedList<String>());
          for (int i = 0; i < loader.getSchema().getFieldCount(); ++i) {
            output.getLast().add(loader.getSchema().getColumn(i).getPath().getAsUnescapedPath());
          }
        } catch (SchemaChangeException e) {
          throw new RuntimeException(e);
        }
        for (int i = 0; i < rows; ++i) {
          output.add(new LinkedList<String>());
          for (VectorWrapper<?> vw : loader) {
            ValueVector.Accessor accessor = vw.getValueVector().getAccessor();
            output.getLast().add(accessor.getObject(i).toString());
          }
        }
      }
      result.release();
      if (result.getHeader().getIsLastChunk()) {
        latch.countDown();
      }
    }

    @Override
    public void queryIdArrived(UserBitShared.QueryId queryId) {
    }

    public List<LinkedList<String>> waitForCompletion() throws Exception {
      latch.await();
      if (exception != null) {
        throw exception;
      }
      return output;
    }
  }
}
