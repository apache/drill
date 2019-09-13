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

import java.io.IOException;
import java.io.StringReader;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.xml.bind.annotation.XmlRootElement;

import com.fasterxml.jackson.core.JsonParser;
import org.apache.commons.lang3.StringUtils;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.server.rest.DrillRestServer.UserAuthEnabled;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.sys.PersistentStore;
import org.glassfish.jersey.server.mvc.Viewable;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.drill.exec.server.rest.auth.DrillUserPrincipal.ADMIN_ROLE;

@Path("/")
@RolesAllowed(ADMIN_ROLE)
public class StorageResources {
  private static final Logger logger = LoggerFactory.getLogger(StorageResources.class);

  @Inject
  UserAuthEnabled authEnabled;

  @Inject
  StoragePluginRegistry storage;

  @Inject
  ObjectMapper mapper;

  @Inject
  SecurityContext sc;

  @Inject
  HttpServletRequest request;

  private static final String JSON_FORMAT = "json";
  private static final String HOCON_FORMAT = "conf";
  private static final String ALL_PLUGINS = "all";
  private static final String ENABLED_PLUGINS = "enabled";
  private static final String DISABLED_PLUGINS = "disabled";

  private static final Comparator<PluginConfigWrapper> PLUGIN_COMPARATOR =
      Comparator.comparing(PluginConfigWrapper::getName);

  /**
   * Regex allows the following paths:
   * /storage/{group}/plugins/export
   * /storage/{group}/plugins/export/{format}
   * Note: for the second case the format involves the leading slash, therefore it should be removed then
   */
  @GET
  @Path("/storage/{group}/plugins/export{format: (/[^/]+?)*}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getConfigsFor(@PathParam("group") String pluginGroup, @PathParam("format") String format) {
    format = StringUtils.isNotEmpty(format) ? format.replace("/", "") : JSON_FORMAT;
    return isSupported(format)
        ? Response.ok()
            .entity(getConfigsFor(pluginGroup).toArray())
            .header(HttpHeaders.CONTENT_DISPOSITION, String.format("attachment;filename=\"%s_storage_plugins.%s\"",
                pluginGroup, format))
            .build()
        : Response.status(Response.Status.NOT_FOUND.getStatusCode())
            .entity(String.format("Unknown \"%s\" file format for \"%s\" Storage Plugin configs group",
                    format, pluginGroup))
            .build();
  }

  @GET
  @Path("/storage.json")
  @Produces(MediaType.APPLICATION_JSON)
  public List<PluginConfigWrapper> getPluginsJSON() {
    return getConfigsFor(ALL_PLUGINS);
  }

  @GET
  @Path("/storage")
  @Produces(MediaType.TEXT_HTML)
  public Viewable getPlugins() {
    List<StoragePluginModel> model = getPluginsJSON().stream()
        .map(plugin -> new StoragePluginModel(plugin, request))
        .collect(Collectors.toList());
    // Creating an empty model with CSRF token, if there are no storage plugins
    if (model.size() == 0) {
      model.add(new StoragePluginModel(null, request));
    }
    return ViewableWithPermissions.create(authEnabled.get(), "/rest/storage/list.ftl", sc, model);
  }

  @GET
  @Path("/storage/{name}.json")
  @Produces(MediaType.APPLICATION_JSON)
  public PluginConfigWrapper getPluginConfig(@PathParam("name") String name) {
    try {
      PersistentStore<StoragePluginConfig> configStorage = storage.getStore();
      if (configStorage.contains(name)) {
        return new PluginConfigWrapper(name, configStorage.get(name));
      }
    } catch (Exception e) {
      logger.error("Failure while trying to access storage config: {}", name, e);
    }
    return new PluginConfigWrapper(name, null);
  }

  @GET
  @Path("/storage/{name}")
  @Produces(MediaType.TEXT_HTML)
  public Viewable getPlugin(@PathParam("name") String name) {
    StoragePluginModel model = new StoragePluginModel(getPluginConfig(name), request);
    return ViewableWithPermissions.create(authEnabled.get(), "/rest/storage/update.ftl", sc,
        model);
  }

  @GET
  @Path("/storage/{name}/enable/{val}")
  @Produces(MediaType.APPLICATION_JSON)
  public JsonResult enablePlugin(@PathParam("name") String name, @PathParam("val") Boolean enable) {
    PluginConfigWrapper plugin = getPluginConfig(name);
    try {
      return plugin.setEnabledInStorage(storage, enable)
          ? message("Success")
          : message("Error (plugin does not exist)");
    } catch (ExecutionSetupException e) {
      logger.debug("Error in enabling storage name: {} flag: {}",  name, enable);
      return message("Error (unable to enable / disable storage)");
    }
  }

  /**
   * Regex allows the following paths:
   * /storage/{name}/export
   * "/storage/{name}/export/{format}
   * Note: for the second case the format involves the leading slash, therefore it should be removed then
   */
  @GET
  @Path("/storage/{name}/export{format: (/[^/]+?)*}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response exportPlugin(@PathParam("name") String name, @PathParam("format") String format) {
    format = StringUtils.isNotEmpty(format) ? format.replace("/", "") : JSON_FORMAT;
    return isSupported(format)
        ? Response.ok(getPluginConfig(name))
            .header(HttpHeaders.CONTENT_DISPOSITION, String.format("attachment;filename=\"%s.%s\"", name, format))
            .build()
        : Response.status(Response.Status.NOT_FOUND.getStatusCode())
            .entity(String.format("Unknown \"%s\" file format for \"%s\" Storage Plugin config", format, name))
            .build();
  }

  @DELETE
  @Path("/storage/{name}.json")
  @Produces(MediaType.APPLICATION_JSON)
  public JsonResult deletePlugin(@PathParam("name") String name) {
    return getPluginConfig(name).deleteFromStorage(storage)
        ? message("Success")
        : message("Error (unable to delete %s storage plugin)", name);
  }

  @POST
  @Path("/storage/{name}.json")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public JsonResult createOrUpdatePluginJSON(PluginConfigWrapper plugin) {
    try {
      plugin.createOrUpdateInStorage(storage);
      return message("Success");
    } catch (ExecutionSetupException e) {
      logger.error("Unable to create/ update plugin: " + plugin.getName(), e);
      return message("Error while creating / updating storage : %s", e.getCause() == null ? e.getMessage() :
          e.getCause().getMessage());
    }
  }

  @POST
  @Path("/storage/create_update")
  @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
  @Produces(MediaType.APPLICATION_JSON)
  public JsonResult createOrUpdatePlugin(@FormParam("name") String name, @FormParam("config") String storagePluginConfig) {
    name = name.trim();
    if (name.isEmpty()) {
      return message("Error (a storage name cannot be empty)");
    }
    try {
      mapper.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
      StoragePluginConfig config = mapper.readValue(new StringReader(storagePluginConfig), StoragePluginConfig.class);
      return createOrUpdatePluginJSON(new PluginConfigWrapper(name, config));
    } catch (JsonMappingException e) {
      logger.debug("Error in JSON mapping: {}", storagePluginConfig, e);
      return message("Error (invalid JSON mapping)");
    } catch (JsonParseException e) {
      logger.debug("Error parsing JSON: {}", storagePluginConfig, e);
      return message("Error (unable to parse JSON)");
    } catch (IOException e) {
      logger.debug("Failed to read: {}", storagePluginConfig, e);
      return message("Error (unable to read)");
    }
  }

  private JsonResult message(String message, Object... args) {
    return new JsonResult(String.format(message, args));
  }

  private boolean isSupported(String format) {
    return JSON_FORMAT.equalsIgnoreCase(format) || HOCON_FORMAT.equalsIgnoreCase(format);
  }

  /**
   * Regex allows the following paths:
   * /storage.json
   * /storage/{group}-plugins.json
   * Note: for the second case the group involves the leading slash, therefore it should be removed then
   */
  @GET
  @Path("/storage{group: (/[^/]+?)*}-plugins.json")
  @Produces(MediaType.APPLICATION_JSON)
  public List<PluginConfigWrapper> getConfigsFor(@PathParam("group") String pluginGroup) {
    pluginGroup = StringUtils.isNotEmpty(pluginGroup) ? pluginGroup.replace("/", "") : ALL_PLUGINS;
    return StreamSupport.stream(
        Spliterators.spliteratorUnknownSize(storage.getStore().getAll(), Spliterator.ORDERED), false)
            .filter(byPluginGroup(pluginGroup))
            .map(entry -> new PluginConfigWrapper(entry.getKey(), entry.getValue()))
            .sorted(PLUGIN_COMPARATOR)
            .collect(Collectors.toList());
  }

  /**
   * @deprecated use {@link #createOrUpdatePluginJSON} instead
   */
  @POST
  @Path("/storage/{name}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Deprecated
  public JsonResult createOrUpdatePlugin(PluginConfigWrapper plugin) {
    return createOrUpdatePluginJSON(plugin);
  }

  /**
   * @deprecated use the method with DELETE request {@link #deletePlugin(String)} instead
   */
  @GET
  @Path("/storage/{name}/delete")
  @Produces(MediaType.APPLICATION_JSON)
  @Deprecated
  public JsonResult deletePluginViaGet(@PathParam("name") String name) {
    return getPluginConfig(name).deleteFromStorage(storage)
        ? message("Success")
        : message("Error (unable to delete %s storage plugin)", name);
  }

  private Predicate<Map.Entry<String, StoragePluginConfig>> byPluginGroup(String pluginGroup) {
    if (ALL_PLUGINS.equalsIgnoreCase(pluginGroup)) {
      return entry -> true;
    } else if (ENABLED_PLUGINS.equalsIgnoreCase(pluginGroup)) {
      return entry -> entry.getValue().isEnabled();
    } else if (DISABLED_PLUGINS.equalsIgnoreCase(pluginGroup)) {
      return entry -> !entry.getValue().isEnabled();
    } else {
      return entry -> false;
    }
  }

  @XmlRootElement
  public class JsonResult {

    private String result;

    public JsonResult(String result) {
      this.result = result;
    }

    public String getResult() {
      return result;
    }

  }

  /**
   * Model class for Storage Plugin page.
   * It contains a storage plugin as well as the CSRK token for the page.
   */
  public static class StoragePluginModel {
    private final PluginConfigWrapper plugin;
    private final String csrfToken;

    public StoragePluginModel(PluginConfigWrapper plugin, HttpServletRequest request) {
      this.plugin = plugin;
      csrfToken = WebUtils.getCsrfTokenFromHttpRequest(request);
    }

    public PluginConfigWrapper getPlugin() {
      return plugin;
    }

    public String getCsrfToken() {
      return csrfToken;
    }
  }
}
