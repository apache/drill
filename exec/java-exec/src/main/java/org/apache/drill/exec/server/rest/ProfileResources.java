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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.drill.exec.proto.UserBitShared.QueryProfile;
import org.apache.drill.exec.proto.UserBitShared.QueryResult.QueryState;
import org.apache.drill.exec.store.sys.PStore;
import org.apache.drill.exec.work.WorkManager;
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

    public ProfileInfo(String queryId, long time) {
      this.queryId = queryId;
      this.time = new Date(time);
      this.location = "http://localhost:8047/profile/" + queryId + ".json";
    }

    public String getQueryId() {
      return queryId;
    }

    public String getTime() {
      return format.format(time);
    }

    public String getLocation() {
      return location;
    }

    @Override
    public int compareTo(ProfileInfo other) {
      return time.compareTo(other.time);
    }
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
      return  finishedQueries;
    }
  }

  @GET
  @Path("/profiles.json")
  @Produces(MediaType.APPLICATION_JSON)
  public QProfiles getProfilesJSON() {
    PStore<QueryProfile> store = null;
    try {
      store = work.getContext().getPersistentStoreProvider().getPStore(QueryStatus.QUERY_PROFILE);
    } catch (IOException e) {
      logger.debug("Failed to get profiles from persistent store.");
      return new QProfiles(new ArrayList<ProfileInfo>(), new ArrayList<ProfileInfo>());
    }

    List<ProfileInfo> runningQueries = Lists.newArrayList();
    List<ProfileInfo> finishedQueries = Lists.newArrayList();

    for(Map.Entry<String, QueryProfile> entry : store){
      QueryProfile profile = entry.getValue();
      if (profile.getState() == QueryState.RUNNING || profile.getState() == QueryState.PENDING) {
        runningQueries.add(new ProfileInfo(entry.getKey(), profile.getStart()));
      } else {
        finishedQueries.add(new ProfileInfo(entry.getKey(), profile.getStart()));
      }
    }

    Collections.sort(runningQueries, Collections.reverseOrder());
    Collections.sort(finishedQueries, Collections.reverseOrder());

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
    PStore<QueryProfile> store = null;
    try {
      store = work.getContext().getPersistentStoreProvider().getPStore(QueryStatus.QUERY_PROFILE);
    } catch (IOException e) {
      logger.debug("Failed to get profile for: " + queryId);
      return QueryProfile.getDefaultInstance();
    }
    QueryProfile profile = store.get(queryId);
    return profile == null ?  QueryProfile.getDefaultInstance() : profile;
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
}