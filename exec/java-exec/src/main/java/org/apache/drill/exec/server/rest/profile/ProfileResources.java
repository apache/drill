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
package org.apache.drill.exec.server.rest.profile;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.drill.exec.proto.GeneralRPCProtos.Ack;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.UserBitShared.QueryInfo;
import org.apache.drill.exec.proto.UserBitShared.QueryProfile;
import org.apache.drill.exec.proto.UserBitShared.QueryResult.QueryState;
import org.apache.drill.exec.proto.helper.QueryIdHelper;
import org.apache.drill.exec.store.sys.EStore;
import org.apache.drill.exec.store.sys.PStore;
import org.apache.drill.exec.store.sys.PStoreProvider;
import org.apache.drill.exec.work.WorkManager;
import org.apache.drill.exec.work.foreman.Foreman;
import org.apache.drill.exec.work.foreman.QueryStatus;
import org.glassfish.jersey.server.mvc.Viewable;

import com.google.common.collect.Lists;

@Path("/")
public class ProfileResources {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ProfileResources.class);

  @Inject WorkManager work;

  public static class ProfileInfo implements Comparable<ProfileInfo> {
    public static final SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");

    private String queryId;
    private Date time;
    private String location;
    private String foreman;
    private String query;
    private String state;

    public ProfileInfo(String queryId, long time, String foreman, String query, String state) {
      this.queryId = queryId;
      this.time = new Date(time);
      this.foreman = foreman;
      this.location = "http://localhost:8047/profile/" + queryId + ".json";
      this.query = query = query.substring(0,  Math.min(query.length(), 150));
      this.state = state;
    }

    public String getQuery(){
      return query;
    }

    public String getQueryId() {
      return queryId;
    }

    public String getTime() {
      return format.format(time);
    }


    public String getState() {
      return state;
    }

    public String getLocation() {
      return location;
    }

    @Override
    public int compareTo(ProfileInfo other) {
      return time.compareTo(other.time);
    }

    public String getForeman() {
      return foreman;
    }

  }

  private PStoreProvider provider(){
    return work.getContext().getPersistentStoreProvider();
  }

  @XmlRootElement
  public class QProfiles {
    private List<ProfileInfo> runningQueries;
    private List<ProfileInfo> finishedQueries;

    public QProfiles(List<ProfileInfo> runningQueries, List<ProfileInfo> finishedQueries) {
      this.runningQueries = runningQueries;
      this.finishedQueries = finishedQueries;
    }

    public List<ProfileInfo> getRunningQueries() {
      return runningQueries;
    }

    public List<ProfileInfo> getFinishedQueries() {
      return finishedQueries;
    }
  }

  @GET
  @Path("/profiles.json")
  @Produces(MediaType.APPLICATION_JSON)
  public QProfiles getProfilesJSON() {
    PStore<QueryProfile> completed = null;
    PStore<QueryInfo> running = null;
    try {
      completed = provider().getStore(QueryStatus.QUERY_PROFILE);
      running = provider().getStore(QueryStatus.RUNNING_QUERY_INFO);
    } catch (IOException e) {
      logger.debug("Failed to get profiles from persistent or ephemeral store.");
      return new QProfiles(new ArrayList<ProfileInfo>(), new ArrayList<ProfileInfo>());
    }

    List<ProfileInfo> runningQueries = Lists.newArrayList();

    for (Map.Entry<String, QueryInfo> entry : running) {
      QueryInfo profile = entry.getValue();
      runningQueries.add(new ProfileInfo(entry.getKey(), profile.getStart(), profile.getForeman().getAddress(), profile.getQuery(), profile.getState().name()));
    }

    Collections.sort(runningQueries, Collections.reverseOrder());


    List<ProfileInfo> finishedQueries = Lists.newArrayList();
    for (Map.Entry<String, QueryProfile> entry : completed) {
      QueryProfile profile = entry.getValue();
      finishedQueries.add(new ProfileInfo(entry.getKey(), profile.getStart(), profile.getForeman().getAddress(), profile.getQuery(), profile.getState().name()));
    }

    return new QProfiles(runningQueries, finishedQueries);
  }

  @GET
  @Path("/profiles")
  @Produces(MediaType.TEXT_HTML)
  public Viewable getProfiles() {
    QProfiles profiles = getProfilesJSON();
    return new Viewable("/rest/profile/list.ftl", profiles);
  }

  private QueryProfile getQueryProfile(String queryId) {
    QueryId id = QueryIdHelper.getQueryIdFromString(queryId);

    // first check local running
    Foreman f = work.getBee().getForemanForQueryId(id);
    if(f != null){
      return f.getQueryStatus().getAsProfile();
    }

    // then check remote running
    try{
      PStore<QueryInfo> runningQueries = provider().getStore(QueryStatus.RUNNING_QUERY_INFO);
      QueryInfo info = runningQueries.get(queryId);
      return work.getContext().getController().getTunnel(info.getForeman()).requestQueryProfile(id).checkedGet(2, TimeUnit.SECONDS);
    }catch(Exception e){
      logger.debug("Failure to find query as running profile.", e);
    }

    // then check blob store
    try{
      PStore<QueryProfile> profiles = provider().getStore(QueryStatus.QUERY_PROFILE);
      return profiles.get(queryId);
    }catch(Exception e){
      logger.warn("Failure to load query profile for query {}", queryId, e);
    }

    // TODO: Improve error messaging.
    return QueryProfile.getDefaultInstance();

  }


  @GET
  @Path("/profiles/{queryid}.json")
  @Produces(MediaType.APPLICATION_JSON)
  public String getProfileJSON(@PathParam("queryid") String queryId) {
    try {
      return new String(QueryStatus.QUERY_PROFILE.getSerializer().serialize(getQueryProfile(queryId)));
    } catch (IOException e) {
      logger.debug("Failed to serialize profile for: " + queryId);
      return ("{ 'message' : 'error (unable to serialize profile)' }");
    }
  }

  @GET
  @Path("/profiles/{queryid}")
  @Produces(MediaType.TEXT_HTML)
  public Viewable getProfile(@PathParam("queryid") String queryId) {
    ProfileWrapper wrapper = new ProfileWrapper(getQueryProfile(queryId));

    return new Viewable("/rest/profile/profile.ftl", wrapper);

  }


  @GET
  @Path("/profiles/cancel/{queryid}")
  @Produces(MediaType.TEXT_PLAIN)
  public String cancelQuery(@PathParam("queryid") String queryId) throws IOException {

    QueryId id = QueryIdHelper.getQueryIdFromString(queryId);

    // first check local running
    Foreman f = work.getBee().getForemanForQueryId(id);
    if(f != null){
      f.cancel();
      return String.format("Cancelled query %s on locally running node.", queryId);
    }

    // then check remote running
    try{
      PStore<QueryInfo> runningQueries = provider().getStore(QueryStatus.RUNNING_QUERY_INFO);
      QueryInfo info = runningQueries.get(queryId);
      Ack a = work.getContext().getController().getTunnel(info.getForeman()).requestCancelQuery(id).checkedGet(2, TimeUnit.SECONDS);
      if(a.getOk()){
        return String.format("Query %s canceled on node %s.", queryId, info.getForeman().getAddress());
      }else{
        return String.format("Attempted to cancel query %s on %s but the query is no longer active on that node.", queryId, info.getForeman().getAddress());
      }
    }catch(Exception e){
      logger.debug("Failure to find query as running profile.", e);
      return String.format("Failure attempting to cancel query %s.  Unable to find information about where query is actively running.", queryId);
    }

  }

}
