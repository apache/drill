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
import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.drill.exec.proto.UserBitShared.QueryProfile;
import org.apache.drill.exec.proto.UserBitShared.QueryResult.QueryState;
import org.apache.drill.exec.store.sys.PStore;
import org.apache.drill.exec.work.WorkManager;
import org.apache.drill.exec.work.foreman.QueryStatus;
import org.glassfish.jersey.server.mvc.Viewable;

import com.google.common.collect.Lists;

@Path("/profiles")
public class ProfileResources {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ProfileResources.class);

  @Inject WorkManager work;


  @GET
  @Path("/{queryid}")
  @Produces(MediaType.TEXT_HTML)
  public Viewable getQuery(@PathParam("queryid") String queryId) throws IOException {
    PStore<QueryProfile> profiles = work.getContext().getPersistentStoreProvider().getPStore(QueryStatus.QUERY_PROFILE);
    QueryProfile profile = profiles.get(queryId);
    if(profile == null) profile = QueryProfile.getDefaultInstance();

    ProfileWrapper wrapper = new ProfileWrapper(profile);

    return new Viewable("/rest/profile/profile.ftl", wrapper);

  }

  @GET
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

    return new Viewable("/rest/profile/list.ftl", queries);
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

}
