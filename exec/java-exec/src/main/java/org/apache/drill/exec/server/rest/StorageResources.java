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
import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.store.StoragePlugin;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.sys.PStoreProvider;
import org.glassfish.jersey.server.mvc.Viewable;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;

import freemarker.template.SimpleHash;

@Path("/storage")
public class StorageResources {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(StorageResources.class);

  @Inject StoragePluginRegistry storage;
  @Inject PStoreProvider storeProvider;
  @Inject ObjectMapper mapper;

  @GET
  @Produces(MediaType.TEXT_HTML)
  public Viewable getQueries() {

    List<String> names = Lists.newArrayList();
    for (Map.Entry<String, StoragePluginConfig> config : storage.getStore()) {
      names.add(config.getKey());
    }

    return new Viewable("/rest/storage/list.ftl", names);
  }

  @GET
  @Path("/{name}/config/update")
  @Produces(MediaType.TEXT_HTML)
  public Viewable update(@PathParam("name") String name) throws JsonProcessingException {
    StoragePluginConfig config = findConfig(name);
    String conf = config == null ? "" : mapper.writeValueAsString(config);

    SimpleHash map = new SimpleHash();
    map.put("config", conf);
    map.put("name", name);
    map.put("exists", config != null);
    return new Viewable("/rest/storage/update.ftl", map);
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/{name}/config")
  public StoragePluginConfig getConfig(@PathParam("name") String name) {
    return findConfig(name);
  }

  private StoragePluginConfig findConfig(String name) {
    try {
      StoragePlugin plugin = storage.getPlugin(name);
      if (plugin != null) {
        return plugin.getConfig();
      }
    } catch (Exception e) {
      logger.info("Failure while trying to access storage config: {}", name, e);
      ;
    }
    return null;
  }

  @POST
  @Path("/config/update")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes("application/x-www-form-urlencoded")
  public JsonResult createTrackInJSON(@FormParam("name") String name, @FormParam("config") String storagePluginConfig)
      throws ExecutionSetupException, JsonParseException, JsonMappingException, IOException {
    StoragePluginConfig config = mapper.readValue(new StringReader(storagePluginConfig), StoragePluginConfig.class);
    storage.createOrUpdate(name, config, true);
    return r("success");
  }

  @POST
  @Path("/config/delete")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes("application/x-www-form-urlencoded")
  public JsonResult deleteConfig(@FormParam("name") String name) {
    storage.deletePlugin(name);
    return r("success");
  }

  private JsonResult r(String message) {
    return new JsonResult(message);
  }

  public static class JsonResult {
    public String result = "data updated";

    public JsonResult(String result) {
      super();
      this.result = result;
    }

  }
}
