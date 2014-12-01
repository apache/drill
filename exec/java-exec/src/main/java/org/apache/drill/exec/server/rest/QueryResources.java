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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.google.common.collect.Lists;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.coord.ClusterCoordinator;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.work.WorkManager;
import org.glassfish.jersey.server.mvc.Viewable;

@Path("/")
public class QueryResources {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(QueryResources.class);

  @Inject
  WorkManager work;

  @GET
  @Path("/query")
  @Produces(MediaType.TEXT_HTML)
  public Viewable getQuery() {
    return new Viewable("/rest/query/query.ftl");
  }

  @POST
  @Path("/query.json")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public List<Map<String, Object>> submitQueryJSON(QueryWrapper query) throws Exception {
    final DrillConfig config = work.getContext().getConfig();
    final ClusterCoordinator coordinator = work.getContext().getClusterCoordinator();
    final BufferAllocator allocator = work.getContext().getAllocator();
    return query.run(config, coordinator, allocator);
  }

  @POST
  @Path("/query")
  @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
  @Produces(MediaType.TEXT_HTML)
  public Viewable submitQuery(@FormParam("query") String query, @FormParam("queryType") String queryType) throws Exception {
    List<Map<String, Object>> result = submitQueryJSON(new QueryWrapper(query, queryType));

    List<String> columnNames;
    if (result.isEmpty()) {
      columnNames = Lists.newArrayList();
    } else {
      columnNames = Lists.newArrayList(result.get(0).keySet());
    }
    List<List<Object>> records = new ArrayList<>();

    if(!isEmptyResult(result)) {
      for (Map m : result) {
        records.add(new ArrayList<Object>(m.values()));
      }
    }

    Table table = new Table(columnNames, records);

    return new Viewable("/rest/query/result.ftl", table);
  }

  private boolean isEmptyResult(List<Map<String, Object>> result) {
    if (result.size() > 1) {
      return false;
    } else {
      for(Object col : result.get(0).values()) {
        if(col != null) {
          return false;
        }
      }

      return true;
    }
  }

  public class Table {
    private List<String> columnNames;
    private List<List<Object>> records;

    public Table(List<String> columnNames, List<List<Object>> records) {
      this.columnNames = columnNames;
      this.records = records;
    }

    public boolean isEmpty() { return getColumnNames().isEmpty(); }

    public List<String> getColumnNames() {
      return columnNames;
    }

    public List<List<Object>> getRecords() {
      return records;
    }
  }
}
