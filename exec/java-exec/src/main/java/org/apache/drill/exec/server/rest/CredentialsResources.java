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

import org.apache.commons.lang3.StringUtils;
import org.apache.drill.exec.server.rest.DrillRestServer.UserAuthEnabled;
import org.apache.drill.exec.server.rest.StorageResources.StoragePluginModel;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.StoragePluginRegistry.PluginFilter;
import org.glassfish.jersey.server.mvc.Viewable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.SecurityContext;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.apache.drill.exec.server.rest.auth.DrillUserPrincipal.AUTHENTICATED_ROLE;

@Path("/")
@RolesAllowed(AUTHENTICATED_ROLE)
public class CredentialsResources {
  private static final Logger logger = LoggerFactory.getLogger(CredentialsResources.class);
  private static final Comparator<PluginConfigWrapper> PLUGIN_COMPARATOR =
    Comparator.comparing(PluginConfigWrapper::getName);
  private static final String ALL_PLUGINS = "all";
  private static final String ENABLED_PLUGINS = "enabled";
  private static final String DISABLED_PLUGINS = "disabled";
  private static final String HAS_USER_CREDS = "has_user_creds";

  @Inject
  UserAuthEnabled authEnabled;

  @Inject
  StoragePluginRegistry storage;

  @Inject
  SecurityContext sc;

  @Inject
  HttpServletRequest request;

  @GET
  @Path("/credentials")
  @Produces(MediaType.TEXT_HTML)
  public Viewable getPlugins() {
    List<StoragePluginModel> model = getPluginsJSON().stream()
      .map(plugin -> new StoragePluginModel(plugin, request, sc))
      .collect(Collectors.toList());
    // Creating an empty model with CSRF token, if there are no storage plugins
    if (model.isEmpty()) {
      model.add(new StoragePluginModel(null, request, sc));
    }
    return ViewableWithPermissions.create(authEnabled.get(), "/rest/credentials/list.ftl", sc, model);
  }

  @GET
  @Path("/credentials.json")
  @Produces(MediaType.APPLICATION_JSON)
  public List<PluginConfigWrapper> getPluginsJSON() {
    return getConfigsFor(HAS_USER_CREDS);
  }

  @GET
  @Path("/credentials{group: (/[^/]+?)*}-plugins.json")
  @Produces(MediaType.APPLICATION_JSON)
  public List<PluginConfigWrapper> getConfigsFor(@PathParam("group") String pluginGroup) {
    PluginFilter filter;
    switch (pluginGroup.trim()) {
      case ALL_PLUGINS:
        filter = PluginFilter.ALL;
        break;
      case ENABLED_PLUGINS:
        filter = PluginFilter.ENABLED;
        break;
      case DISABLED_PLUGINS:
        filter = PluginFilter.DISABLED;
        break;
      case HAS_USER_CREDS:
        filter = PluginFilter.HAS_USER_CREDS;
        break;
      default:
        return Collections.emptyList();
    }
    pluginGroup = StringUtils.isNotEmpty(pluginGroup) ? pluginGroup.replace("/", "") : ALL_PLUGINS;
    return StreamSupport.stream(
        Spliterators.spliteratorUnknownSize(storage.storedConfigs(filter).entrySet().iterator(), Spliterator.ORDERED), false)
      .map(entry -> new PluginConfigWrapper(entry.getKey(), entry.getValue(), sc))
      .sorted(PLUGIN_COMPARATOR)
      .collect(Collectors.toList());
  }
}
