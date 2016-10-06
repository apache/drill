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

import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.SecurityContext;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.coord.ClusterCoordinator;
import org.apache.drill.exec.coord.store.TransientStore;
import org.apache.drill.exec.proto.GeneralRPCProtos.Ack;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.UserBitShared.QueryInfo;
import org.apache.drill.exec.proto.UserBitShared.QueryProfile;
import org.apache.drill.exec.proto.helper.QueryIdHelper;
import org.apache.drill.exec.server.rest.DrillRestServer.UserAuthEnabled;
import org.apache.drill.exec.server.rest.ViewableWithPermissions;
import org.apache.drill.exec.server.rest.auth.DrillUserPrincipal;
import org.apache.drill.exec.store.sys.PersistentStore;
import org.apache.drill.exec.store.sys.PersistentStoreProvider;
import org.apache.drill.exec.work.WorkManager;
import org.apache.drill.exec.work.foreman.Foreman;
import org.apache.drill.exec.work.foreman.QueryManager;
import org.glassfish.jersey.server.mvc.Viewable;

import com.google.common.collect.Lists;

@Path("/")
@RolesAllowed(DrillUserPrincipal.AUTHENTICATED_ROLE)
public class ProfileResources {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ProfileResources.class);

  public final static int MAX_PROFILES = 100;

  @Inject UserAuthEnabled authEnabled;
  @Inject WorkManager work;
  @Inject DrillUserPrincipal principal;
  @Inject SecurityContext sc;

  public static class ProfileInfo implements Comparable<ProfileInfo> {
    public static final SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");

    private String queryId;
    private Date time;
    private String location;
    private String foreman;
    private String query;
    private String state;
    private String user;

    public ProfileInfo(String queryId, long time, String foreman, String query, String state, String user) {
      this.queryId = queryId;
      this.time = new Date(time);
      this.foreman = foreman;
      this.location = "http://localhost:8047/profile/" + queryId + ".json";
      this.query = query.substring(0,  Math.min(query.length(), 150));
      this.state = state;
      this.user = user;
    }

    public String getUser() {
      return user;
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

  protected PersistentStoreProvider getProvider() {
    return work.getContext().getStoreProvider();
  }

  protected ClusterCoordinator getCoordinator() {
    return work.getContext().getClusterCoordinator();
  }

  @XmlRootElement
  public class QProfiles {
    private List<ProfileInfo> runningQueries;
    private List<ProfileInfo> finishedQueries;
    private List<String> errors;

    public QProfiles(List<ProfileInfo> runningQueries, List<ProfileInfo> finishedQueries, List<String> erorrs) {
      this.runningQueries = runningQueries;
      this.finishedQueries = finishedQueries;
      this.errors = erorrs;
    }

    public List<ProfileInfo> getRunningQueries() {
      return runningQueries;
    }

    public List<ProfileInfo> getFinishedQueries() {
      return finishedQueries;
    }

    public List<String> getErrors() { return errors; }
  }

  @GET
  @Path("/profiles.json")
  @Produces(MediaType.APPLICATION_JSON)
  public QProfiles getProfilesJSON() {
    try {
      final PersistentStore<QueryProfile> completed = getProvider().getOrCreateStore(QueryManager.QUERY_PROFILE);
      final TransientStore<QueryInfo> running = getCoordinator().getOrCreateTransientStore(QueryManager.RUNNING_QUERY_INFO);

      final List<String> errors = Lists.newArrayList();

      final List<ProfileInfo> runningQueries = Lists.newArrayList();

      final Iterator<Map.Entry<String, QueryInfo>> runningEntries = running.entries();
      while (runningEntries.hasNext()) {
        try {
          final Map.Entry<String, QueryInfo> runningEntry = runningEntries.next();
          final QueryInfo profile = runningEntry.getValue();
          if (principal.canManageProfileOf(profile.getUser())) {
            runningQueries.add(new ProfileInfo(runningEntry.getKey(), profile.getStart(), profile.getForeman().getAddress(), profile.getQuery(), profile.getState().name(), profile.getUser()));
          }
        } catch (Exception e) {
          errors.add(e.getMessage());
          logger.error("Error getting running query info.", e);
        }
      }

      Collections.sort(runningQueries, Collections.reverseOrder());

      final List<ProfileInfo> finishedQueries = Lists.newArrayList();

      final Iterator<Map.Entry<String, QueryProfile>> range = completed.getRange(0, MAX_PROFILES);
      while (range.hasNext()) {
        try {
          final Map.Entry<String, QueryProfile> profileEntry = range.next();
          final QueryProfile profile = profileEntry.getValue();
          if (principal.canManageProfileOf(profile.getUser())) {
            finishedQueries.add(new ProfileInfo(profileEntry.getKey(), profile.getStart(), profile.getForeman().getAddress(), profile.getQuery(), profile.getState().name(), profile.getUser()));
          }
        } catch (Exception e) {
          errors.add(e.getMessage());
          logger.error("Error getting finished query profile.", e);
        }
      }

      return new QProfiles(runningQueries, finishedQueries, errors);
    } catch (Exception e) {
      throw UserException.resourceError(e)
          .message("Failed to get profiles from persistent or ephemeral store.")
          .build(logger);
    }
  }

  @GET
  @Path("/profiles")
  @Produces(MediaType.TEXT_HTML)
  public Viewable getProfiles() {
    QProfiles profiles = getProfilesJSON();
    return ViewableWithPermissions.create(authEnabled.get(), "/rest/profile/list.ftl", sc, profiles);
  }

  private QueryProfile getQueryProfile(String queryId) {
    QueryId id = QueryIdHelper.getQueryIdFromString(queryId);

    // first check local running
    Foreman f = work.getBee().getForemanForQueryId(id);
    if(f != null){
      QueryProfile queryProfile = f.getQueryManager().getQueryProfile();
      checkOrThrowProfileViewAuthorization(queryProfile);
      return queryProfile;
    }

    // then check remote running
    try {
      final TransientStore<QueryInfo> running = getCoordinator().getOrCreateTransientStore(QueryManager.RUNNING_QUERY_INFO);
      final QueryInfo info = running.get(queryId);
      if (info != null) {
        QueryProfile queryProfile = work.getContext()
            .getController()
            .getTunnel(info.getForeman())
            .requestQueryProfile(id)
            .checkedGet(2, TimeUnit.SECONDS);
        checkOrThrowProfileViewAuthorization(queryProfile);
        return queryProfile;
      }
    }catch(Exception e){
      logger.trace("Failed to find query as running profile.", e);
    }

    // then check blob store
    try {
      final PersistentStore<QueryProfile> profiles = getProvider().getOrCreateStore(QueryManager.QUERY_PROFILE);
      final QueryProfile queryProfile = profiles.get(queryId);
      if (queryProfile != null) {
        checkOrThrowProfileViewAuthorization(queryProfile);
        return queryProfile;
      }
    } catch (final Exception e) {
      throw new DrillRuntimeException("error while retrieving profile", e);
    }

    throw UserException.validationError()
        .message("No profile with given query id '%s' exists. Please verify the query id.", queryId)
        .build(logger);
  }


  @GET
  @Path("/profiles/{queryid}.json")
  @Produces(MediaType.APPLICATION_JSON)
  public String getProfileJSON(@PathParam("queryid") String queryId) {
    try {
      return new String(QueryManager.QUERY_PROFILE.getSerializer().serialize(getQueryProfile(queryId)));
    } catch (Exception e) {
      logger.debug("Failed to serialize profile for: " + queryId);
      return ("{ 'message' : 'error (unable to serialize profile)' }");
    }
  }

  @GET
  @Path("/profiles/{queryid}")
  @Produces(MediaType.TEXT_HTML)
  public Viewable getProfile(@PathParam("queryid") String queryId){
    ProfileWrapper wrapper = new ProfileWrapper(getQueryProfile(queryId));
    return ViewableWithPermissions.create(authEnabled.get(), "/rest/profile/profile.ftl", sc, wrapper);
  }


  @GET
  @Path("/profiles/cancel/{queryid}")
  @Produces(MediaType.TEXT_PLAIN)
  public String cancelQuery(@PathParam("queryid") String queryId) {

    QueryId id = QueryIdHelper.getQueryIdFromString(queryId);

    // first check local running
    Foreman f = work.getBee().getForemanForQueryId(id);
    if(f != null){
      checkOrThrowQueryCancelAuthorization(f.getQueryContext().getQueryUserName(), queryId);
      f.cancel();
      return String.format("Cancelled query %s on locally running node.", queryId);
    }

    // then check remote running
    try {
      final TransientStore<QueryInfo> running = getCoordinator().getOrCreateTransientStore(QueryManager.RUNNING_QUERY_INFO);
      final QueryInfo info = running.get(queryId);
      checkOrThrowQueryCancelAuthorization(info.getUser(), queryId);
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

  private void checkOrThrowProfileViewAuthorization(final QueryProfile profile) {
    if (!principal.canManageProfileOf(profile.getUser())) {
      throw UserException.permissionError()
          .message("Not authorized to view the profile of query '%s'", profile.getId())
          .build(logger);
    }
  }

  private void checkOrThrowQueryCancelAuthorization(final String queryUser, final String queryId) {
    if (!principal.canManageQueryOf(queryUser)) {
      throw UserException.permissionError()
          .message("Not authorized to cancel the query '%s'", queryId)
          .build(logger);
    }
  }
}
