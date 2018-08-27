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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

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

  private static final Comparator<PluginConfigWrapper> PLUGIN_COMPARATOR = new Comparator<PluginConfigWrapper>() {
    @Override
    public int compare(PluginConfigWrapper o1, PluginConfigWrapper o2) {
      return o1.getName().compareTo(o2.getName());
    }
  };

  @GET
  @Path("/storage.json")
  @Produces(MediaType.APPLICATION_JSON)
  public List<PluginConfigWrapper> getStoragePluginsJSON() {

    List<PluginConfigWrapper> list = Lists.newArrayList();
    for (Map.Entry<String, StoragePluginConfig> entry : Lists.newArrayList(storage.getStore().getAll())) {
      PluginConfigWrapper plugin = new PluginConfigWrapper(entry.getKey(), entry.getValue());
      list.add(plugin);
    }

    Collections.sort(list, PLUGIN_COMPARATOR);

    return list;
  }

  @GET
  @Path("/storage")
  @Produces(MediaType.TEXT_HTML)
  public Viewable getStoragePlugins() {
    List<PluginConfigWrapper> list = getStoragePluginsJSON();
    return ViewableWithPermissions.create(authEnabled.get(), "/rest/storage/list.ftl", sc, list);
  }

  @SuppressWarnings("resource")
  @GET
  @Path("/storage/{name}.json")
  @Produces(MediaType.APPLICATION_JSON)
  public PluginConfigWrapper getStoragePluginJSON(@PathParam("name") String name) {
    try {
      // TODO: DRILL-6412: No need to get StoragePlugin. It is enough to have plugin name and config here
      StoragePlugin plugin = storage.getPlugin(name);
      if (plugin != null) {
        return new PluginConfigWrapper(name, plugin.getConfig());
      }
    } catch (Exception e) {
      logger.info("Failure while trying to access storage config: {}", name, e);
    }
    return new PluginConfigWrapper(name, null);
  }

  @GET
  @Path("/storage/{name}")
  @Produces(MediaType.TEXT_HTML)
  public Viewable getStoragePlugin(@PathParam("name") String name) {
    PluginConfigWrapper plugin = getStoragePluginJSON(name);
    return ViewableWithPermissions.create(authEnabled.get(), "/rest/storage/update.ftl", sc, plugin);
  }

  @GET
  @Path("/storage/{name}/enable/{val}")
  @Produces(MediaType.APPLICATION_JSON)
  public JsonResult enablePlugin(@PathParam("name") String name, @PathParam("val") Boolean enable) {
    PluginConfigWrapper plugin = getStoragePluginJSON(name);
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
  @Path("/storage/{name}/export")
  @Produces(MediaType.APPLICATION_JSON)
  public Response exportPlugin(@PathParam("name") String name) {
    Response.ResponseBuilder response = Response.ok(getStoragePluginJSON(name));
    response.header("Content-Disposition", String.format("attachment;filename=\"%s.json\"", name));
    return response.build();
  }

  @DELETE
  @Path("/storage/{name}.json")
  @Produces(MediaType.APPLICATION_JSON)
  public JsonResult deletePluginJSON(@PathParam("name") String name) {
    PluginConfigWrapper plugin = getStoragePluginJSON(name);
    if (plugin.deleteFromStorage(storage)) {
      return message("success");
    } else {
      return message("error (unable to delete storage)");
    }
  }

  @GET
  @Path("/storage/{name}/delete")
  @Produces(MediaType.APPLICATION_JSON)
  public JsonResult deletePlugin(@PathParam("name") String name) {
    return deletePluginJSON(name);
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
      return message("Error while creating/ updating storage : " + (e.getCause() != null ? e.getCause().getMessage() : e.getMessage()));
    }
  }

  @POST
  @Path("/storage/{name}")
  @Consumes("application/x-www-form-urlencoded")
  @Produces(MediaType.APPLICATION_JSON)
  public JsonResult createOrUpdatePlugin(@FormParam("name") String name, @FormParam("config") String storagePluginConfig) {
    try {
      StoragePluginConfig config = mapper.readValue(new StringReader(storagePluginConfig), StoragePluginConfig.class);
      return createOrUpdatePluginJSON(new PluginConfigWrapper(name, config));
    } catch (JsonMappingException e) {
      logger.debug("Error in JSON mapping: {}", storagePluginConfig, e);
      return message("error (invalid JSON mapping)");
    } catch (JsonParseException e) {
      logger.debug("Error parsing JSON: {}", storagePluginConfig, e);
      return message("error (unable to parse JSON)");
    } catch (IOException e) {
      logger.debug("Failed to read: {}", storagePluginConfig, e);
      return message("error (unable to read)");
    }
  }

  private JsonResult message(String message) {
    return new JsonResult(message);
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
