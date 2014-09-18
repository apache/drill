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

import java.io.IOException;
import java.io.StringReader;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

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
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.store.StoragePlugin;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.glassfish.jersey.server.mvc.Viewable;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;

@Path("/")
public class StorageResources {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(StorageResources.class);

  @Inject
  StoragePluginRegistry storage;
//  @Inject
//  PStoreProvider storeProvider;
  @Inject
  ObjectMapper mapper;

  static final Comparator<PluginConfigWrapper> PLUGIN_COMPARATOR = new Comparator<PluginConfigWrapper>() {
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
    for (Map.Entry<String, StoragePluginConfig> entry : storage.getStore()) {
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
    return new Viewable("/rest/storage/list.ftl", list);
  }

  @GET
  @Path("/storage/{name}.json")
  @Produces(MediaType.APPLICATION_JSON)
  public PluginConfigWrapper getStoragePluginJSON(@PathParam("name") String name) {
    try {
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
    return new Viewable("/rest/storage/update.ftl", plugin);
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
      logger.debug("Unable to create/ update plugin: " + plugin.getName());
      return message("error (unable to create/ update storage)");
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
      logger.debug("Error in JSON mapping: " + storagePluginConfig);
      return message("error (invalid JSON mapping)");
    } catch (JsonParseException e) {
      logger.debug("Error parsing JSON: " + storagePluginConfig);
      return message("error (unable to parse JSON)");
    } catch (IOException e) {
      logger.debug("Failed to read: " + storagePluginConfig);
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
