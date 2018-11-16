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

import java.util.Collections;
import java.util.Comparator;
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

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.drill.exec.server.options.OptionList;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.server.options.OptionValue;
import org.apache.drill.exec.server.options.OptionValue.Kind;
import org.apache.drill.exec.server.options.SystemOptionManager;
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

  public static final String REST_API_SUFFIX = ".json";
  public static final String PATH_STATUS_JSON = "/status" + REST_API_SUFFIX;
  public static final String PATH_STATUS = "/status";
  public static final String PATH_OPTIONS_JSON = "/options" + REST_API_SUFFIX;
  public static final String PATH_INTERNAL_OPTIONS_JSON = "/internal_options" + REST_API_SUFFIX;
  public static final String PATH_OPTIONS = "/options";
  public static final String PATH_INTERNAL_OPTIONS = "/internal_options";

  @Inject UserAuthEnabled authEnabled;
  @Inject WorkManager work;
  @Inject SecurityContext sc;

  @GET
  @Path(StatusResources.PATH_STATUS_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Pair<String, String> getStatusJSON() {
    return new ImmutablePair<>("status", "Running!");
  }

  @GET
  @Path(StatusResources.PATH_STATUS)
  @Produces(MediaType.TEXT_HTML)
  public Viewable getStatus() {
    return ViewableWithPermissions.create(authEnabled.get(), "/rest/status.ftl", sc, getStatusJSON());
  }

  @SuppressWarnings("resource")
  private List<OptionWrapper> getSystemOptionsJSONHelper(boolean internal)
  {
    List<OptionWrapper> options = new LinkedList<>();
    OptionManager optionManager = work.getContext().getOptionManager();
    OptionList optionList = internal ? optionManager.getInternalOptionList(): optionManager.getPublicOptionList();

    for (OptionValue option : optionList) {
      options.add(new OptionWrapper(option.name, option.getValue(), optionManager.getDefault(option.name).getValue().toString(), option.accessibleScopes, option.kind, option.scope));
    }

    Collections.sort(options, new Comparator<OptionWrapper>() {
      @Override
      public int compare(OptionWrapper o1, OptionWrapper o2) {
         return o1.name.compareTo(o2.name);
      }
    });
    return options;
  }

  @GET
  @Path(StatusResources.PATH_OPTIONS_JSON)
  @RolesAllowed(DrillUserPrincipal.AUTHENTICATED_ROLE)
  @Produces(MediaType.APPLICATION_JSON)
  public List<OptionWrapper> getSystemPublicOptionsJSON() {
    return getSystemOptionsJSONHelper(false);
  }

  @GET
  @Path(StatusResources.PATH_INTERNAL_OPTIONS_JSON)
  @RolesAllowed(DrillUserPrincipal.AUTHENTICATED_ROLE)
  @Produces(MediaType.APPLICATION_JSON)
  public List<OptionWrapper> getSystemInternalOptionsJSON() {
    return getSystemOptionsJSONHelper(true);
  }

  private Viewable getSystemOptionsHelper(boolean internal) {
    return ViewableWithPermissions.create(authEnabled.get(),
      "/rest/options.ftl",
      sc,
      getSystemOptionsJSONHelper(internal));
  }

  @GET
  @Path(StatusResources.PATH_OPTIONS)
  @RolesAllowed(DrillUserPrincipal.AUTHENTICATED_ROLE)
  @Produces(MediaType.TEXT_HTML)
  public Viewable getSystemPublicOptions() {
    return getSystemOptionsHelper(false);
  }

  @GET
  @Path(StatusResources.PATH_INTERNAL_OPTIONS)
  @RolesAllowed(DrillUserPrincipal.AUTHENTICATED_ROLE)
  @Produces(MediaType.TEXT_HTML)
  public Viewable getSystemInternalOptions() {
    return getSystemOptionsHelper(true);
  }

  @SuppressWarnings("resource")
  @POST
  @Path("option/{optionName}")
  @RolesAllowed(DrillUserPrincipal.ADMIN_ROLE)
  @Consumes("application/x-www-form-urlencoded")
  @Produces(MediaType.TEXT_HTML)
  public Viewable updateSystemOption(@FormParam("name") String name,
                                   @FormParam("value") String value,
                                   @FormParam("kind") String kind) {
    SystemOptionManager optionManager = work.getContext()
      .getOptionManager();

    try {
      optionManager.setLocalOption(OptionValue.Kind.valueOf(kind), name, value);
    } catch (Exception e) {
      logger.debug("Could not update.", e);
    }

    if (optionManager.getOptionDefinition(name).getMetaData().isInternal()) {
      return getSystemInternalOptions();
    } else {
      return getSystemPublicOptions();
    }
  }

  @XmlRootElement
  public static class OptionWrapper {

    private String name;
    private Object value;
    private String defaultValue;
    private OptionValue.AccessibleScopes accessibleScopes;
    private String kind;
    private String optionScope;

    @JsonCreator
    public OptionWrapper(@JsonProperty("name") String name,
                         @JsonProperty("value") Object value,
                         @JsonProperty("defaultValue") String defaultValue,
                         @JsonProperty("accessibleScopes") OptionValue.AccessibleScopes type,
                         @JsonProperty("kind") Kind kind,
                         @JsonProperty("optionScope") OptionValue.OptionScope scope) {
      this.name = name;
      this.value = value;
      this.defaultValue = defaultValue;
      this.accessibleScopes = type;
      this.kind = kind.name();
      this.optionScope = scope.name();
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

    public String getDefaultValue() {
      return defaultValue;
    }

    public OptionValue.AccessibleScopes getAccessibleScopes() {
      return accessibleScopes;
    }

    public String getKind() {
      return kind;
    }

    public String getOptionScope() {
      return optionScope;
    }

    @Override
    public String toString() {
      return "OptionWrapper{" + "name='" + name + '\'' + ", value=" + value + ", default=" + defaultValue + ", accessibleScopes=" + accessibleScopes + ", kind='" + kind + '\'' + ", scope='" + optionScope + '\'' +'}';
    }
  }
}
