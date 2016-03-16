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

import javax.annotation.security.PermitAll;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.SecurityContext;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.proto.CoordinationProtos;
import org.apache.drill.exec.server.rest.DrillRestServer.UserAuthEnabled;
import org.apache.drill.exec.work.WorkManager;
import org.glassfish.jersey.server.mvc.Viewable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.collect.Lists;

@Path("/")
@PermitAll
public class DrillRoot {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillRoot.class);

  @Inject UserAuthEnabled authEnabled;
  @Inject WorkManager work;
  @Inject SecurityContext sc;

  @GET
  @Produces(MediaType.TEXT_HTML)
  public Viewable getStats() {
    return ViewableWithPermissions.create(authEnabled.get(), "/rest/index.ftl", sc, getStatsJSON());
  }

  @GET
  @Path("/stats.json")
  @Produces(MediaType.APPLICATION_JSON)
  public List<Stat> getStatsJSON() {
    List<Stat> stats = Lists.newLinkedList();
    stats.add(new Stat("Number of Drill Bits", work.getContext().getBits().size()));
    int number = 0;
    for (CoordinationProtos.DrillbitEndpoint bit : work.getContext().getBits()) {
      String initialized = bit.isInitialized() ? " initialized" : " not initialized";
      stats.add(new Stat("Bit #" + number, bit.getAddress() + initialized));
      ++number;
    }
    stats.add(new Stat("Data Port Address", work.getContext().getEndpoint().getAddress() +
      ":" + work.getContext().getEndpoint().getDataPort()));
    stats.add(new Stat("User Port Address", work.getContext().getEndpoint().getAddress() +
      ":" + work.getContext().getEndpoint().getUserPort()));
    stats.add(new Stat("Control Port Address", work.getContext().getEndpoint().getAddress() +
      ":" + work.getContext().getEndpoint().getControlPort()));
    stats.add(new Stat("Maximum Direct Memory", DrillConfig.getMaxDirectMemory()));

    return stats;
  }

  @XmlRootElement
  public class Stat {
    private String name;
    private Object value;

    @JsonCreator
    public Stat(String name, Object value) {
      this.name = name;
      this.value = value;
    }

    public String getName() {
      return name;
    }

    public Object getValue() {
      return value;
    }

  }
}
