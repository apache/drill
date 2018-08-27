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

import org.apache.drill.shaded.guava.com.google.common.base.CharMatcher;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.apache.drill.exec.server.rest.DrillRestServer.UserAuthEnabled;
import org.apache.drill.exec.server.rest.auth.DrillUserPrincipal;
import org.apache.drill.exec.server.rest.QueryWrapper.QueryResult;
import org.apache.drill.exec.work.WorkManager;
import org.glassfish.jersey.server.mvc.Viewable;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.SecurityContext;
import java.util.List;
import java.util.Map;

@Path("/")
@RolesAllowed(DrillUserPrincipal.AUTHENTICATED_ROLE)
public class QueryResources {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(QueryResources.class);

  @Inject UserAuthEnabled authEnabled;
  @Inject WorkManager work;
  @Inject SecurityContext sc;
  @Inject WebUserConnection webUserConnection;


  @GET
  @Path("/query")
  @Produces(MediaType.TEXT_HTML)
  public Viewable getQuery() {
    return ViewableWithPermissions.create(
        authEnabled.get(),
        "/rest/query/query.ftl",
        sc,
        // if impersonation is enabled without authentication, will provide mechanism to add user name to request header from Web UI
        WebServer.isImpersonationOnlyEnabled(work.getContext().getConfig()));
  }

  @POST
  @Path("/query.json")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public QueryResult submitQueryJSON(QueryWrapper query) throws Exception {
    try {
      // Run the query
      return query.run(work, webUserConnection);
    } finally {
      // no-op for authenticated user
      webUserConnection.cleanupSession();
    }
  }

  @POST
  @Path("/query")
  @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
  @Produces(MediaType.TEXT_HTML)
  public Viewable submitQuery(@FormParam("query") String query,
                              @FormParam("queryType") String queryType) throws Exception {
    try {
      final String trimmedQueryString = CharMatcher.is(';').trimTrailingFrom(query.trim());
      final QueryResult result = submitQueryJSON(new QueryWrapper(trimmedQueryString, queryType));

      return ViewableWithPermissions.create(authEnabled.get(), "/rest/query/result.ftl", sc, new TabularResult(result));
    } catch (Exception | Error e) {
      logger.error("Query from Web UI Failed", e);
      return ViewableWithPermissions.create(authEnabled.get(), "/rest/query/errorMessage.ftl", sc, e);
    }
  }

  public static class TabularResult {
    private final List<String> columns;
    private final List<List<String>> rows;
    private final String queryId;

    public TabularResult(QueryResult result) {
      queryId = result.getQueryId();
      final List<List<String>> rows = Lists.newArrayList();
      for (Map<String, String> rowMap:result.rows) {
        final List<String> row = Lists.newArrayList();
        for (String col:result.columns) {
          row.add(rowMap.get(col));
        }
        rows.add(row);
      }

      this.columns = ImmutableList.copyOf(result.columns);
      this.rows = rows;
    }

    public boolean isEmpty() {
      return columns.isEmpty();
    }

    public String getQueryId() {
      return queryId;
    }

    public List<String> getColumns() {
      return columns;
    }

    public List<List<String>> getRows() {
      return rows;
    }
  }


}
