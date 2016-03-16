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

import java.util.List;
import java.util.Map;

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

import com.google.common.base.CharMatcher;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.server.rest.DrillRestServer.UserAuthEnabled;
import org.apache.drill.exec.server.rest.auth.DrillUserPrincipal;
import org.apache.drill.exec.work.WorkManager;
import org.glassfish.jersey.server.mvc.Viewable;

@Path("/")
@RolesAllowed(DrillUserPrincipal.AUTHENTICATED_ROLE)
public class QueryResources {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(QueryResources.class);

  @Inject UserAuthEnabled authEnabled;
  @Inject WorkManager work;
  @Inject SecurityContext sc;
  @Inject DrillUserPrincipal principal;

  @GET
  @Path("/query")
  @Produces(MediaType.TEXT_HTML)
  public Viewable getQuery() {
    return ViewableWithPermissions.create(authEnabled.get(), "/rest/query/query.ftl", sc);
  }

  @POST
  @Path("/query.json")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public QueryWrapper.QueryResult submitQueryJSON(QueryWrapper query) throws Exception {
    DrillClient drillClient = null;

    try {
      final BufferAllocator allocator = work.getContext().getAllocator();
      drillClient = principal.getDrillClient();
      return query.run(drillClient, allocator);
    } finally {
      principal.recycleDrillClient(drillClient);
    }
  }

  @POST
  @Path("/query")
  @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
  @Produces(MediaType.TEXT_HTML)
  public Viewable submitQuery(@FormParam("query") String query, @FormParam("queryType") String queryType) throws Exception {
    try {
      final String trimmedQueryString = CharMatcher.is(';').trimTrailingFrom(query.trim());
      final QueryWrapper.QueryResult result = submitQueryJSON(new QueryWrapper(trimmedQueryString, queryType));
      return ViewableWithPermissions.create(authEnabled.get(), "/rest/query/result.ftl", sc, new TabularResult(result));
    } catch(Exception | Error e) {
      logger.error("Query from Web UI Failed", e);
      return ViewableWithPermissions.create(authEnabled.get(), "/rest/query/errorMessage.ftl", sc, e);
    }
  }

  public static class TabularResult {
    private final List<String> columns;
    private final List<List<String>> rows;

    public TabularResult(QueryWrapper.QueryResult result) {
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

    public List<String> getColumns() {
      return columns;
    }

    public List<List<String>> getRows() {
      return rows;
    }
  }
}
