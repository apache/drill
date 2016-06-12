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

import java.util.LinkedList;
import java.util.List;

import javax.annotation.security.PermitAll;
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
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.drill.exec.server.options.OptionValue;
import org.apache.drill.exec.server.options.OptionValue.Kind;
import org.apache.drill.exec.server.rest.DrillRestServer.UserAuthEnabled;
import org.apache.drill.exec.server.rest.auth.DrillUserPrincipal;
import org.apache.drill.exec.work.WorkManager;
import org.glassfish.jersey.server.mvc.Viewable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;

@Path("/")
@PermitAll
public class StatusResources {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(StatusResources.class);

  @Inject UserAuthEnabled authEnabled;
  @Inject WorkManager work;
  @Inject SecurityContext sc;

  @GET
  @Path("/status.json")
  @Produces(MediaType.APPLICATION_JSON)
  public Pair<String, String> getStatusJSON() {
    return new ImmutablePair<>("status", "Running!");
  }

  @GET
  @Path("/status")
  @Produces(MediaType.TEXT_HTML)
  public Viewable getStatus() {
    return ViewableWithPermissions.create(authEnabled.get(), "/rest/status.ftl", sc, getStatusJSON());
  }

  @GET
  @Path("/options.json")
  @RolesAllowed(DrillUserPrincipal.AUTHENTICATED_ROLE)
  @Produces(MediaType.APPLICATION_JSON)
  public List<OptionWrapper> getSystemOptionsJSON() {
    List<OptionWrapper> options = new LinkedList<>();
    for (OptionValue option : work.getContext().getOptionManager()) {
      options.add(new OptionWrapper(option.name, option.getValue(), option.type, option.kind));
    }
    return options;
  }

  @GET
  @Path("/options")
  @RolesAllowed(DrillUserPrincipal.AUTHENTICATED_ROLE)
  @Produces(MediaType.TEXT_HTML)
  public Viewable getSystemOptions() {
    return ViewableWithPermissions.create(authEnabled.get(), "/rest/options.ftl", sc, getSystemOptionsJSON());
  }

  @POST
  @Path("/option/{optionName}")
  @RolesAllowed(DrillUserPrincipal.ADMIN_ROLE)
  @Consumes("application/x-www-form-urlencoded")
  @Produces(MediaType.TEXT_HTML)
  public Viewable updateSystemOption(@FormParam("name") String name, @FormParam("value") String value,
                                   @FormParam("kind") String kind) {
    try {
      work.getContext()
        .getOptionManager()
        .setOption(OptionValue.createOption(
          OptionValue.Kind.valueOf(kind),
          OptionValue.OptionType.SYSTEM,
          name,
          value));
    } catch (Exception e) {
      logger.debug("Could not update.", e);
    }
    return getSystemOptions();
  }

  @XmlRootElement
  public class OptionWrapper {

    private String name;
    private Object value;
    private OptionValue.OptionType type;
    private String kind;

    @JsonCreator
    public OptionWrapper(String name, Object value, OptionValue.OptionType type, Kind kind) {
      this.name = name;
      this.value = value;
      this.type = type;
      this.kind = kind.name();
    }

    public String getName() {
      return name;
    }

    @JsonIgnore
    public String getValueAsString() {
      return value.toString();
    }

    public Object getValue() {
      return value;
    }

    public OptionValue.OptionType getType() {
      return type;
    }

    public String getKind() {
      return kind;
    }
  }

}
