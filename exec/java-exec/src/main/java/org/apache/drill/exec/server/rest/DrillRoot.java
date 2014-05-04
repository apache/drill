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

import org.apache.drill.exec.cache.DistributedMap;
import org.apache.drill.exec.cache.ProtobufDrillSerializable.CQueryProfile;
import org.apache.drill.exec.proto.UserBitShared.QueryProfile;
import org.apache.drill.exec.work.WorkManager;
import org.glassfish.jersey.server.mvc.Viewable;

import com.google.common.collect.Lists;

@Path("/")
public class DrillRoot {

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
    DistributedMap<CQueryProfile> profiles = work.getContext().getCache().getNamedMap("sys.queries", CQueryProfile.class);

    List<String> ids = Lists.newArrayList();
    for(Map.Entry<String, CQueryProfile> entry : profiles){
      ids.add(entry.getKey());
    }

    return new Viewable("/rest/status/list.ftl", ids);
  }

  @GET
  @Path("/query/{queryid}")
  @Produces(MediaType.TEXT_HTML)
  public Viewable getQuery(@PathParam("queryid") String queryId) {
    DistributedMap<CQueryProfile> profiles = work.getContext().getCache().getNamedMap("sys.queries", CQueryProfile.class);
    CQueryProfile c = profiles.get(queryId);
    QueryProfile q = c == null ? QueryProfile.getDefaultInstance() : c.getObj();

    return new Viewable("/rest/status/profile.ftl", q);

  }


}
