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

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.cache.DistributedMap;
import org.apache.drill.exec.proto.UserBitShared.QueryProfile;
import org.apache.drill.exec.store.StoragePlugin;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.work.WorkManager;
import org.apache.drill.exec.work.foreman.QueryStatus;
import org.glassfish.jersey.server.mvc.Viewable;

import com.google.common.collect.Lists;

@Path("/")
public class DrillRoot {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillRoot.class);

  @Inject WorkManager work;

  @GET
  @Path("status")
  @Produces("text/plain")
  public String getHello() {
    return "running";
  }

  @GET
  @Path("queries")
  @Produces(MediaType.TEXT_HTML)
  public Viewable getQueries() {
    DistributedMap<String, QueryProfile> profiles = work.getContext().getCache().getMap(QueryStatus.QUERY_PROFILE);

    List<String> ids = Lists.newArrayList();
    for(Map.Entry<String, QueryProfile> entry : profiles.getLocalEntries()){
      ids.add(entry.getKey());
    }

    return new Viewable("/rest/status/list.ftl", ids);
  }


  @GET
  @Path("/query/{queryid}")
  @Produces(MediaType.TEXT_HTML)
  public Viewable getQuery(@PathParam("queryid") String queryId) {
    DistributedMap<String, QueryProfile> profiles = work.getContext().getCache().getMap(QueryStatus.QUERY_PROFILE);
    QueryProfile profile = profiles.get(queryId);
    if(profile == null) profile = QueryProfile.getDefaultInstance();

    return new Viewable("/rest/status/profile.ftl", profile);

  }


}
