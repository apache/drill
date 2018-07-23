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
import java.util.function.Predicate;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.xml.bind.annotation.XmlRootElement;

import com.fasterxml.jackson.core.JsonParser;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.server.rest.DrillRestServer.UserAuthEnabled;
import org.apache.drill.exec.store.StoragePlugin;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.glassfish.jersey.server.mvc.Viewable;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;

import static org.apache.drill.exec.server.rest.auth.DrillUserPrincipal.ADMIN_ROLE;

@Path("/")
@RolesAllowed(ADMIN_ROLE)
public class StorageResources {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(StorageResources.class);

  @Inject UserAuthEnabled authEnabled;
  @Inject StoragePluginRegistry storage;
  @Inject ObjectMapper mapper;
  @Inject SecurityContext sc;

  public static final String JSON_FILE_NAME = "json";
  public static final String HOCON_FILE_NAME = "conf";
  public static final String ALL_PLUGINS = "all";
  public static final String ENABLED_PLUGINS = "enabled";
  public static final String DISABLED_PLUGINS = "disabled";

  private static final Comparator<PluginConfigWrapper> PLUGIN_COMPARATOR =
      Comparator.comparing(PluginConfigWrapper::getName);

  @GET
  @Path("/storage/{group}/plugins/export/{format}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getPluginsConfigs(@PathParam("group") String pluginGroup, @PathParam("format") String format) {
    if (JSON_FILE_NAME.equals(format) || HOCON_FILE_NAME.equals(format)) {
      Response.ResponseBuilder response = Response.ok();
      response.entity(getPluginsConfigs(pluginGroup).toArray());
      response.header("Content-Disposition",
          String.format("attachment;filename=\"%s_storage_plugins.%s\"", pluginGroup, format));
      return response.build();
    }
    logger.error("Unknown file type {} for {} Storage Plugin Configs", format, pluginGroup);
    return Response.status(Response.Status.NOT_FOUND).build();
  }

  @GET
  @Path("/storage")
  @Produces(MediaType.TEXT_HTML)
  public Viewable getPlugins() {
    List<PluginConfigWrapper> list = getPluginsConfigs(ALL_PLUGINS);
    return ViewableWithPermissions.create(authEnabled.get(), "/rest/storage/list.ftl", sc, list);
  }

  private List<PluginConfigWrapper> getPluginsConfigs(String pluginsNumber) {
    List<PluginConfigWrapper> list = Lists.newArrayList();
    Predicate<Map.Entry<String, StoragePluginConfig>> predicate = entry -> false;
    if (ALL_PLUGINS.equals(pluginsNumber)) {
      predicate = entry -> true;
    } else if (ENABLED_PLUGINS.equals(pluginsNumber)) {
      predicate = entry -> entry.getValue().isEnabled();
    } else if (DISABLED_PLUGINS.equals(pluginsNumber)) {
      predicate = entry -> !entry.getValue().isEnabled();
    }
    Lists.newArrayList(storage.getStore().getAll()).stream()
        .filter(predicate)
        .map(entry -> new PluginConfigWrapper(entry.getKey(), entry.getValue()))
        .forEach(list::add);
    list.sort(PLUGIN_COMPARATOR);
    return list;
  }

  @SuppressWarnings("resource")
  @GET
  @Path("/storage/{name}.json")
  @Produces(MediaType.APPLICATION_JSON)
  public PluginConfigWrapper getPluginConfigs(@PathParam("name") String name) {
    try {
      // TODO: DRILL-6412: No need to get StoragePlugin. It is enough to have plugin name and config here
      StoragePlugin plugin = storage.getPlugin(name);
      if (plugin != null) {
        return new PluginConfigWrapper(name, plugin.getConfig());
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
    PluginConfigWrapper plugin = getPluginConfigs(name);
    return ViewableWithPermissions.create(authEnabled.get(), "/rest/storage/update.ftl", sc, plugin);
  }

  @GET
  @Path("/storage/{name}/enable/{val}")
  @Produces(MediaType.APPLICATION_JSON)
  public JsonResult enablePlugin(@PathParam("name") String name, @PathParam("val") Boolean enable) {
    PluginConfigWrapper plugin = getPluginConfigs(name);
    try {
      if (plugin.setEnabledInStorage(storage, enable)) {
        return message("success");
      } else {
        return message("error (plugin does not exist)");
      }
    } catch (ExecutionSetupException e) {
      logger.debug("Error in enabling storage name: " + name + " flag: " + enable);
      return message("error (unable to enable/ disable storage)");
    }
  }

  @GET
  @Path("/storage/{name}/export/{format}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response exportPlugin(@PathParam("name") String name, @PathParam("format") String format) {
    if (JSON_FILE_NAME.equals(format) || HOCON_FILE_NAME.equals(format)) {
      PluginConfigWrapper storagePluginConfigs = getPluginConfigs(name);
      Response.ResponseBuilder response = Response.ok(storagePluginConfigs);
      response.header("Content-Disposition", String.format("attachment;filename=\"%s.%s\"", name, format));
      return response.build();
    }
    logger.error("Unknown file type {} for Storage Plugin Config: {}", format, name);
    return Response.status(Response.Status.NOT_FOUND).build();
  }

  @DELETE
  @Path("/storage/{name}.{format}")
  @Produces(MediaType.APPLICATION_JSON)
  public JsonResult deletePlugin(@PathParam("name") String name, @PathParam("format") String format) {
    if (JSON_FILE_NAME.equals(format) || HOCON_FILE_NAME.equals(format)) {
      PluginConfigWrapper plugin = getPluginConfigs(name);
      if (plugin.deleteFromStorage(storage)) {
        return message("Success");
      }
    }
    return message("Error (unable to delete %s.%s storage plugin)", name, format);
  }

  @GET
  @Path("/storage/{name}/delete")
  @Produces(MediaType.APPLICATION_JSON)
  public JsonResult deletePlugin(@PathParam("name") String name) {
    return deletePlugin(name, JSON_FILE_NAME);
  }

  @POST
  @Path("/storage/{name}.json")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public JsonResult createOrUpdatePluginJSON(PluginConfigWrapper plugin) {
    try {
      plugin.createOrUpdateInStorage(storage);
      return message("success");
    } catch (ExecutionSetupException e) {
      logger.error("Unable to create/ update plugin: " + plugin.getName(), e);
      return message("Error while creating / updating storage : " + (e.getCause() != null ?
          e.getCause().getMessage() : e.getMessage()));
    }
  }

  @POST
  @Path("/storage/create_new")
  @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
  @Produces(MediaType.APPLICATION_JSON)
  public JsonResult createOrUpdatePlugin(@FormParam("name") String name, @FormParam("config") String storagePluginConfig) {
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
}
