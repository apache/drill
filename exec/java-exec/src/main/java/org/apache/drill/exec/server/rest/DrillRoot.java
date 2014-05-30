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

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.drill.exec.cache.DistributedMap;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.proto.UserBitShared.QueryProfile;
import org.apache.drill.exec.proto.UserBitShared.QueryResult.QueryState;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.user.ConnectionThrottle;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.apache.drill.exec.rpc.user.UserResultsListener;
import org.apache.drill.exec.store.sys.PStore;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.work.WorkManager;
import org.apache.drill.exec.work.foreman.QueryStatus;
import org.glassfish.jersey.server.mvc.Viewable;

import com.google.common.collect.Lists;

@Path("/")
public class DrillRoot {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillRoot.class);

  @Inject WorkManager work;

  @GET
  @Produces(MediaType.TEXT_HTML)
  public Viewable getHello() {
    String status = "Running!";
    return new Viewable("/rest/status/index.ftl", status);
  }

  @GET
  @Path("/status")
  @Produces(MediaType.TEXT_HTML)
  public Viewable getStatus() {
    String status = "Running!";
    return new Viewable("/rest/status/status.ftl", status);
  }

  @GET
  @Path("/queries")
  @Produces(MediaType.TEXT_HTML)
  public Viewable getResults() throws IOException {
    PStore<QueryProfile> profiles = work.getContext().getPersistentStoreProvider().getPStore(QueryStatus.QUERY_PROFILE);

    List<Map.Entry<String, Long>> runningIds = Lists.newArrayList();
    List<Map.Entry<String, Long>> finishedIds = Lists.newArrayList();
    for(Map.Entry<String, QueryProfile> entry : profiles){
      QueryProfile q = entry.getValue();
      if (q.getState() == QueryState.RUNNING || q.getState() == QueryState.PENDING) {
        runningIds.add(new AbstractMap.SimpleEntry<>(entry.getKey(), q.getStart()));
      } else {
        finishedIds.add(new AbstractMap.SimpleEntry<>(entry.getKey(), q.getStart()));
      }
    }

    Comparator<Map.Entry<String,Long>> comparator = new Comparator<Map.Entry<String,Long>>() {
      @Override
      public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) {
        return o2.getValue().compareTo(o1.getValue());
      }
    };

    Collections.sort(runningIds, comparator);
    Collections.sort(finishedIds, comparator);

    List<Map.Entry<String, String>> runningQueries = Lists.newArrayList();
    List<Map.Entry<String, String>> oldQueries = Lists.newArrayList();
    for(Map.Entry<String, Long> entry : runningIds){
      runningQueries.add(new AbstractMap.SimpleEntry<>(entry.getKey(), new SimpleDateFormat("MM/dd/yyyy HH:mm:ss").format(new Date(entry.getValue()))));
    }

    for(Map.Entry<String, Long> entry : finishedIds){
      oldQueries.add(new AbstractMap.SimpleEntry<>(entry.getKey(), new SimpleDateFormat("MM/dd/yyyy HH:mm:ss").format(new Date(entry.getValue()))));
    }
    // add status (running, done)

    Queries queries = new Queries();
    queries.runningQueries = runningQueries;
    queries.oldQueries = oldQueries;

    return new Viewable("/rest/status/list.ftl", queries);
  }

  public static class Queries {
    List<Map.Entry<String, String>> runningQueries;
    List<Map.Entry<String, String>> oldQueries;

    public List<Map.Entry<String, String>> getRunningQueries() {
      return runningQueries;
    }

    public List<Map.Entry<String, String>> getOldQueries() {
      return oldQueries;
    }
  }

  @GET
  @Path("/query/{queryid}")
  @Produces(MediaType.TEXT_HTML)
  public Viewable getQuery(@PathParam("queryid") String queryId) throws IOException {
    PStore<QueryProfile> profiles = work.getContext().getPersistentStoreProvider().getPStore(QueryStatus.QUERY_PROFILE);
    QueryProfile profile = profiles.get(queryId);
    if(profile == null) profile = QueryProfile.getDefaultInstance();

    return new Viewable("/rest/status/profile.ftl", profile);

  }

  @GET
  @Path("/query")
  @Produces(MediaType.TEXT_HTML)
  public Viewable getQuery() {
    return new Viewable("/rest/status/query.ftl");
  }

  @POST
  @Path("/query")
  @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
  @Produces(MediaType.TEXT_HTML)
  public Viewable submitQuery(@FormParam("query") String query, @FormParam("queryType") String queryType) throws Exception {
    DrillClient client = new DrillClient(work.getContext().getConfig(), work.getContext().getClusterCoordinator());
    client.connect();

    UserBitShared.QueryType type = UserBitShared.QueryType.SQL;
    switch (queryType){
      case "SQL" : type = UserBitShared.QueryType.SQL; break;
      case "LOGICAL" : type = UserBitShared.QueryType.LOGICAL; break;
      case "PHYSICAL" : type = UserBitShared.QueryType.PHYSICAL; break;
    }

    Listener listener = new Listener(new RecordBatchLoader(work.getContext().getAllocator()));
    client.runQuery(type, query, listener);
    List<LinkedList<String>> result = listener.waitForCompletion();

    return new Viewable("/rest/status/result.ftl", result);
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
        for(int i = 0; i < rows; ++i) {
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
    public void queryIdArrived(UserBitShared.QueryId queryId) {}

    public List<LinkedList<String>> waitForCompletion() throws Exception {
      latch.await();
      if(exception != null) throw exception;
      System.out.println();
      return output;
    }
  }
}
