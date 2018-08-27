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
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.core.UriInfo;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.coord.ClusterCoordinator;
import org.apache.drill.exec.coord.store.TransientStore;
import org.apache.drill.exec.proto.GeneralRPCProtos.Ack;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.UserBitShared.QueryInfo;
import org.apache.drill.exec.proto.UserBitShared.QueryProfile;
import org.apache.drill.exec.proto.helper.QueryIdHelper;
import org.apache.drill.exec.server.rest.DrillRestServer.UserAuthEnabled;
import org.apache.drill.exec.server.QueryProfileStoreContext;
import org.apache.drill.exec.server.rest.ViewableWithPermissions;
import org.apache.drill.exec.server.rest.auth.DrillUserPrincipal;
import org.apache.drill.exec.store.sys.PersistentStore;
import org.apache.drill.exec.store.sys.PersistentStoreProvider;
import org.apache.drill.exec.work.WorkManager;
import org.apache.drill.exec.work.foreman.Foreman;
import org.glassfish.jersey.server.mvc.Viewable;

import org.apache.drill.shaded.guava.com.google.common.collect.Lists;

@Path("/")
@RolesAllowed(DrillUserPrincipal.AUTHENTICATED_ROLE)
public class ProfileResources {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ProfileResources.class);

  @Inject UserAuthEnabled authEnabled;
  @Inject WorkManager work;
  @Inject DrillUserPrincipal principal;
  @Inject SecurityContext sc;

  public static class ProfileInfo implements Comparable<ProfileInfo> {
    private static final int QUERY_SNIPPET_MAX_CHAR = 150;
    private static final int QUERY_SNIPPET_MAX_LINES = 8;

    public static final SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");

    private final String queryId;
    private final long startTime;
    private final long endTime;
    private final Date time;
    private final String link;
    private final String foreman;
    private final String query;
    private final String state;
    private final String user;
    private final double totalCost;
    private final String queueName;

    public ProfileInfo(DrillConfig drillConfig, String queryId, long startTime, long endTime, String foreman, String query,
                       String state, String user, double totalCost, String queueName) {
      this.queryId = queryId;
      this.startTime = startTime;
      this.endTime = endTime;
      this.time = new Date(startTime);
      this.foreman = foreman;
      this.link = generateLink(drillConfig, foreman, queryId);
      this.query = extractQuerySnippet(query);
      this.state = state;
      this.user = user;
      this.totalCost = totalCost;
      this.queueName = queueName;
    }

    public String getUser() { return user; }

    public String getQuery() { return query; }

    public String getQueryId() { return queryId; }

    public String getTime() { return format.format(time); }

    public long getStartTime() { return startTime; }

    public long getEndTime() { return endTime; }

    public String getDuration() {
      return (new SimpleDurationFormat(startTime, endTime)).verbose();
    }

    public String getState() { return state; }

    public String getLink() { return link; }

    public String getForeman() { return foreman; }

    public double getTotalCost() { return totalCost; }

    public String getQueueName() { return queueName; }

    @Override
    public int compareTo(ProfileInfo other) {
      return time.compareTo(other.time);
    }

    /**
     * Generates link which will return query profile in json representation.
     *
     * @param drillConfig drill configuration
     * @param foreman foreman hostname
     * @param queryId query id
     * @return link
     */
    private String generateLink(DrillConfig drillConfig, String foreman, String queryId) {
      StringBuilder sb = new StringBuilder();
      if (drillConfig.getBoolean(ExecConstants.HTTP_ENABLE_SSL)) {
        sb.append("https://");
      } else {
        sb.append("http://");
      }
      sb.append(foreman);
      sb.append(":");
      sb.append(drillConfig.getInt(ExecConstants.HTTP_PORT));
      sb.append("/profiles/");
      sb.append(queryId);
      sb.append(".json");
      return sb.toString();
    }

    /**
     * Extract only the first 150 characters of the query.
     * If this spans more than 8 lines, we truncate excess lines for sake of readability
     * @param queryText
     * @return truncated text
     */
    private String extractQuerySnippet(String queryText) {
      //Extract upto max char limit as snippet
      String sizeCappedQuerySnippet = queryText.substring(0,  Math.min(queryText.length(), QUERY_SNIPPET_MAX_CHAR));
      String[] queryParts = sizeCappedQuerySnippet.split(System.lineSeparator());
      //Trimming down based on line-count
      if (QUERY_SNIPPET_MAX_LINES < queryParts.length) {
        int linesConstructed = 0;
        StringBuilder lineCappedQuerySnippet = new StringBuilder();
        for (String qPart : queryParts) {
          lineCappedQuerySnippet.append(qPart);
          if (++linesConstructed < QUERY_SNIPPET_MAX_LINES) {
            lineCappedQuerySnippet.append(System.lineSeparator());
          } else {
            lineCappedQuerySnippet.append(" ... ");
            break;
          }
        }
        return lineCappedQuerySnippet.toString();
      }
      return sizeCappedQuerySnippet;
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

    public int getMaxFetchedQueries() {
      return work.getContext().getConfig().getInt(ExecConstants.HTTP_MAX_PROFILES);
    }

    public List<String> getErrors() { return errors; }
  }

  //max Param to cap listing of profiles
  private static final String MAX_QPROFILES_PARAM = "max";

  @SuppressWarnings("resource")
  @GET
  @Path("/profiles.json")
  @Produces(MediaType.APPLICATION_JSON)
  public QProfiles getProfilesJSON(@Context UriInfo uriInfo) {
    try {
      final QueryProfileStoreContext profileStoreContext = work.getContext().getProfileStoreContext();
      final PersistentStore<QueryProfile> completed = profileStoreContext.getCompletedProfileStore();
      final TransientStore<QueryInfo> running = profileStoreContext.getRunningProfileStore();

      final List<String> errors = Lists.newArrayList();

      final List<ProfileInfo> runningQueries = Lists.newArrayList();

      final Iterator<Map.Entry<String, QueryInfo>> runningEntries = running.entries();
      while (runningEntries.hasNext()) {
        try {
          final Map.Entry<String, QueryInfo> runningEntry = runningEntries.next();
          final QueryInfo profile = runningEntry.getValue();
          if (principal.canManageProfileOf(profile.getUser())) {
            runningQueries.add(
                new ProfileInfo(work.getContext().getConfig(),
                    runningEntry.getKey(), profile.getStart(), System.currentTimeMillis(),
                    profile.getForeman().getAddress(), profile.getQuery(),
                    ProfileUtil.getQueryStateDisplayName(profile.getState()),
                    profile.getUser(), profile.getTotalCost(), profile.getQueueName()));
          }
        } catch (Exception e) {
          errors.add(e.getMessage());
          logger.error("Error getting running query info.", e);
        }
      }

      Collections.sort(runningQueries, Collections.reverseOrder());

      final List<ProfileInfo> finishedQueries = Lists.newArrayList();

      //Defining #Profiles to load
      int maxProfilesToLoad = work.getContext().getConfig().getInt(ExecConstants.HTTP_MAX_PROFILES);
      String maxProfilesParams = uriInfo.getQueryParameters().getFirst(MAX_QPROFILES_PARAM);
      if (maxProfilesParams != null && !maxProfilesParams.isEmpty()) {
        maxProfilesToLoad = Integer.valueOf(maxProfilesParams);
      }

      final Iterator<Map.Entry<String, QueryProfile>> range = completed.getRange(0, maxProfilesToLoad);

      while (range.hasNext()) {
        try {
          final Map.Entry<String, QueryProfile> profileEntry = range.next();
          final QueryProfile profile = profileEntry.getValue();
          if (principal.canManageProfileOf(profile.getUser())) {
            finishedQueries.add(
                new ProfileInfo(work.getContext().getConfig(),
                    profileEntry.getKey(), profile.getStart(), profile.getEnd(),
                    profile.getForeman().getAddress(), profile.getQuery(),
                    ProfileUtil.getQueryStateDisplayName(profile.getState()),
                    profile.getUser(), profile.getTotalCost(), profile.getQueueName()));
          }
        } catch (Exception e) {
          errors.add(e.getMessage());
          logger.error("Error getting finished query profile.", e);
        }
      }

      Collections.sort(finishedQueries, Collections.reverseOrder());

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
  public Viewable getProfiles(@Context UriInfo uriInfo) {
    QProfiles profiles = getProfilesJSON(uriInfo);
    return ViewableWithPermissions.create(authEnabled.get(), "/rest/profile/list.ftl", sc, profiles);
  }

  @SuppressWarnings("resource")
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
      final TransientStore<QueryInfo> running = work.getContext().getProfileStoreContext().getRunningProfileStore();
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
      final PersistentStore<QueryProfile> profiles = work.getContext().getProfileStoreContext().getCompletedProfileStore();
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
      return new String(work.getContext().getProfileStoreContext().getProfileStoreConfig().getSerializer().serialize(getQueryProfile(queryId)));
    } catch (Exception e) {
      logger.debug("Failed to serialize profile for: " + queryId);
      return ("{ 'message' : 'error (unable to serialize profile)' }");
    }
  }

  @GET
  @Path("/profiles/{queryid}")
  @Produces(MediaType.TEXT_HTML)
  public Viewable getProfile(@PathParam("queryid") String queryId){
    ProfileWrapper wrapper = new ProfileWrapper(getQueryProfile(queryId), work.getContext().getConfig());
    return ViewableWithPermissions.create(authEnabled.get(), "/rest/profile/profile.ftl", sc, wrapper);
  }

  @SuppressWarnings("resource")
  @GET
  @Path("/profiles/cancel/{queryid}")
  @Produces(MediaType.TEXT_PLAIN)
  public String cancelQuery(@PathParam("queryid") String queryId) {

    QueryId id = QueryIdHelper.getQueryIdFromString(queryId);

    // first check local running
    if (work.getBee().cancelForeman(id, principal)) {
      return String.format("Cancelled query %s on locally running node.", queryId);
    }

    // then check remote running
    try {
      final TransientStore<QueryInfo> running = work.getContext().getProfileStoreContext().getRunningProfileStore();
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
      return String.format
          ("Failure attempting to cancel query %s.  Unable to find information about where query is actively running.", queryId);
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

