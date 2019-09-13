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
import org.apache.drill.shaded.guava.com.google.common.base.Joiner;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.server.rest.DrillRestServer.UserAuthEnabled;
import org.apache.drill.exec.server.rest.auth.DrillUserPrincipal;
import org.apache.drill.exec.server.rest.QueryWrapper.QueryResult;
import org.apache.drill.exec.work.WorkManager;
import org.glassfish.jersey.server.mvc.Viewable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.SecurityContext;

import java.util.Collections;
import java.util.List;
import java.util.Map;

@Path("/")
@RolesAllowed(DrillUserPrincipal.AUTHENTICATED_ROLE)
public class QueryResources {
   private static final Logger logger = LoggerFactory.getLogger(QueryResources.class);

  @Inject
  UserAuthEnabled authEnabled;

  @Inject
  WorkManager work;

  @Inject
  SecurityContext sc;

  @Inject
  WebUserConnection webUserConnection;

  @Inject
  HttpServletRequest request;

  @GET
  @Path("/query")
  @Produces(MediaType.TEXT_HTML)
  public Viewable getQuery() {
    return ViewableWithPermissions.create(
        authEnabled.get(), "/rest/query/query.ftl",
        sc, new QueryPage(work, request));
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
                              @FormParam("queryType") String queryType,
                              @FormParam("autoLimit") String autoLimit) throws Exception {
    try {
      final String trimmedQueryString = CharMatcher.is(';').trimTrailingFrom(query.trim());
      final QueryResult result = submitQueryJSON(new QueryWrapper(trimmedQueryString, queryType, autoLimit));
      List<Integer> rowsPerPageValues = work.getContext().getConfig().getIntList(ExecConstants.HTTP_WEB_CLIENT_RESULTSET_ROWS_PER_PAGE_VALUES);
      Collections.sort(rowsPerPageValues);
      final String rowsPerPageValuesAsStr = Joiner.on(",").join(rowsPerPageValues);
      return ViewableWithPermissions.create(authEnabled.get(), "/rest/query/result.ftl", sc, new TabularResult(result, rowsPerPageValuesAsStr));
    } catch (Exception | Error e) {
      logger.error("Query from Web UI Failed: {}", e);
      return ViewableWithPermissions.create(authEnabled.get(), "/rest/errorMessage.ftl", sc, e);
    }
  }

  /**
   * Model class for Query page
   */
  public static class QueryPage {
    private final boolean onlyImpersonationEnabled;
    private final boolean autoLimitEnabled;
    private final int defaultRowsAutoLimited;
    private final String csrfToken;

    public QueryPage(WorkManager work, HttpServletRequest request) {
      DrillConfig config = work.getContext().getConfig();
      //if impersonation is enabled without authentication, will provide mechanism to add user name to request header from Web UI
      onlyImpersonationEnabled = WebServer.isOnlyImpersonationEnabled(config);
      autoLimitEnabled = config.getBoolean(ExecConstants.HTTP_WEB_CLIENT_RESULTSET_AUTOLIMIT_CHECKED);
      defaultRowsAutoLimited = config.getInt(ExecConstants.HTTP_WEB_CLIENT_RESULTSET_AUTOLIMIT_ROWS);
      csrfToken = WebUtils.getCsrfTokenFromHttpRequest(request);
    }

    public boolean isOnlyImpersonationEnabled() {
      return onlyImpersonationEnabled;
    }

    public boolean isAutoLimitEnabled() {
      return autoLimitEnabled;
    }

    public int getDefaultRowsAutoLimited() {
      return defaultRowsAutoLimited;
    }

    public String getCsrfToken() {
      return csrfToken;
    }
  }

  /**
   * Model class for Results page
   */
  public static class TabularResult {
    private final List<String> columns;
    private final List<List<String>> rows;
    private final String queryId;
    private final String rowsPerPageValues;
    private final String queryState;
    private final int autoLimitedRowCount;

    public TabularResult(QueryResult result, String rowsPerPageValuesAsStr) {
      rowsPerPageValues = rowsPerPageValuesAsStr;
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
      this.queryState = result.queryState;
      this.autoLimitedRowCount = result.attemptedAutoLimit;
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

    // Used by results.ftl to render default number of pages per row
    public String getRowsPerPageValues() {
      return rowsPerPageValues;
    }

    public String getQueryState() {
      return queryState;
    }

    // Used by results.ftl to indicate autoLimited resultset
    public boolean isResultSetAutoLimited() {
      return autoLimitedRowCount > 0 && rows.size() == autoLimitedRowCount;
    }

    // Used by results.ftl to indicate autoLimited resultset size
    public int getAutoLimitedRowCount() {
      return autoLimitedRowCount;
    }
  }

}
