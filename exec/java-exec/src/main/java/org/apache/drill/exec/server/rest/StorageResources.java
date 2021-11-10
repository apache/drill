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
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
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

import org.apache.commons.lang3.StringUtils;
import org.apache.drill.exec.server.rest.DrillRestServer.UserAuthEnabled;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.StoragePluginRegistry.PluginEncodingException;
import org.apache.drill.exec.store.StoragePluginRegistry.PluginException;
import org.apache.drill.exec.store.StoragePluginRegistry.PluginFilter;
import org.apache.drill.exec.store.StoragePluginRegistry.PluginNotFoundException;
import org.glassfish.jersey.server.mvc.Viewable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.drill.exec.server.rest.auth.DrillUserPrincipal.ADMIN_ROLE;

// Serialization of plugins to JSON is handled by the Jetty framework
// as configured in DrillRestServer.
@Path("/")
@RolesAllowed(ADMIN_ROLE)
public class StorageResources {
  private static final Logger logger = LoggerFactory.getLogger(StorageResources.class);

  @Inject
  UserAuthEnabled authEnabled;

  @Inject
  StoragePluginRegistry storage;

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
   * Regex allows the following paths:<pre><code>
   * /storage/{group}/plugins/export
   * /storage/{group}/plugins/export/{format}</code></pre>
   * <p>
   * Note: for the second case the format involves the leading slash,
   * therefore it should be removed then
   */
  // This code has a flaw: the Jackson ObjectMapper cannot serialize to
  // HOCON format, though it can read HOCON. Thus, the only valid format
  // is json.
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
      : Response.status(Response.Status.NOT_ACCEPTABLE)
          .entity(message("Unknown \"%s\" file format for Storage Plugin config", format))
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
    if (model.isEmpty()) {
      model.add(new StoragePluginModel(null, request));
    }
    return ViewableWithPermissions.create(authEnabled.get(), "/rest/storage/list.ftl", sc, model);
  }

  @GET
  @Path("/storage/{name}.json")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getPluginConfig(@PathParam("name") String name) {
    try {
      return Response.ok(new PluginConfigWrapper(name, storage.getStoredConfig(name)))
        .build();
    } catch (Exception e) {
      logger.error("Failure while trying to access storage config: {}", name, e);

      return Response.status(Response.Status.NOT_FOUND)
        .entity(message("Failure while trying to access storage config: %s", e.getMessage()))
        .build();
    }
  }

  @GET
  @Path("/storage/{name}")
  @Produces(MediaType.TEXT_HTML)
  public Viewable getPlugin(@PathParam("name") String name) {
    StoragePluginModel model = new StoragePluginModel(
      (PluginConfigWrapper) getPluginConfig(name).getEntity(),
      request
    );
    return ViewableWithPermissions.create(authEnabled.get(), "/rest/storage/update.ftl", sc,
        model);
  }

  @POST
  @Path("/storage/{name}/enable/{val}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response enablePlugin(@PathParam("name") String name, @PathParam("val") Boolean enable) {
    try {
      storage.setEnabled(name, enable);
      return Response.ok().entity(message("Success")).build();
    } catch (PluginNotFoundException e) {
      return Response.status(Response.Status.NOT_FOUND)
        .entity(message("No plugin exists with the given name: " + name))
        .build();
    } catch (PluginException e) {
      logger.info("Error when enabling storage name: {} flag: {}",  name, enable);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
        .entity(message("Unable to enable/disable plugin: %s", e.getMessage()))
        .build();
    }
  }

  /**
   * @deprecated use the method with POST request {@link #enablePlugin} instead
   */
  @GET
  @Path("/storage/{name}/enable/{val}")
  @Produces(MediaType.APPLICATION_JSON)
  @Deprecated
  public Response enablePluginViaGet(@PathParam("name") String name, @PathParam("val") Boolean enable) {
    return enablePlugin(name, enable);
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
    if (!isSupported(format)) {
      return Response.status(Response.Status.NOT_ACCEPTABLE)
        .entity(message("Unknown \"%s\" file format for Storage Plugin config", format))
        .build();
    }

    return Response.ok(new PluginConfigWrapper(name, storage.getStoredConfig(name)))
      .header(HttpHeaders.CONTENT_DISPOSITION, String.format("attachment;filename=\"%s.%s\"", name, format))
      .build();
  }

  @DELETE
  @Path("/storage/{name}.json")
  @Produces(MediaType.APPLICATION_JSON)
  public Response deletePlugin(@PathParam("name") String name) {
    try {
      storage.remove(name);
      return Response.ok().entity(message("Success")).build();
    } catch (PluginException e) {
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
        .entity(message("Error while deleting plugin: %s",  e.getMessage()))
        .build();
    }
  }

  @POST
  @Path("/storage/{name}.json")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response createOrUpdatePluginJSON(PluginConfigWrapper plugin) {
    try {
      plugin.createOrUpdateInStorage(storage);
      return Response.ok().entity(message("Success")).build();
    } catch (PluginException e) {
      logger.error("Unable to create/ update plugin: " + plugin.getName(), e);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
        .entity(message("Error while saving plugin: %s ", e.getMessage()))
        .build();
    }
  }

  // Allows JSON that includes comments. However, since the JSON is immediately
  // serialized, the comments are lost. Would be better to validate the JSON,
  // then write the original JSON directly to the persistent store, and read
  // JSON from the store, rather than letting Jackson produce it. That way,
  // comments are preserved.
  @POST
  @Path("/storage/create_update")
  @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
  @Produces(MediaType.APPLICATION_JSON)
  public Response createOrUpdatePlugin(@FormParam("name") String name, @FormParam("config") String storagePluginConfig) {
    name = name.trim();
    if (name.isEmpty()) {
      return Response.status(Response.Status.BAD_REQUEST)
        .entity(message("A storage config name may not be empty"))
        .build();
    }

    try {
      storage.putJson(name, storagePluginConfig);
      return Response.ok().entity(message("Success")).build();
    } catch (PluginEncodingException e) {
      logger.warn("Error in JSON mapping: {}", storagePluginConfig, e);
      return Response.status(Response.Status.BAD_REQUEST)
        .entity(message("Invalid JSON: %s", e.getMessage()))
        .build();
    } catch (PluginException e) {
      logger.error("Error while saving plugin", e);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
        .entity(message("Error while saving plugin: %s", e.getMessage()))
        .build();
    }
  }

  private JsonResult message(String message, Object... args) {
    return new JsonResult(String.format(message, args));
  }

  private boolean isSupported(String format) {
    return JSON_FORMAT.equalsIgnoreCase(format) || HOCON_FORMAT.equalsIgnoreCase(format);
  }

  /**
   * Regex allows the following paths:<pre><code>
   * /storage.json
   * /storage/{group}-plugins.json</code></pre>
   * Allowable groups:
   * <ul>
   * <li>"all" {@link #ALL_PLUGINS}</li>
   * <li>"enabled" {@link #ENABLED_PLUGINS}</li>
   * <li>"disabled" {@link #DISABLED_PLUGINS}</li>
   * </ul>
   * Any other group value results in an empty list.
   * <p>
   * Note: for the second case the group involves the leading slash,
   * therefore it should be removed then
   */
  @GET
  @Path("/storage{group: (/[^/]+?)*}-plugins.json")
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
    default:
      return Collections.emptyList();
    }
    pluginGroup = StringUtils.isNotEmpty(pluginGroup) ? pluginGroup.replace("/", "") : ALL_PLUGINS;
    return StreamSupport.stream(
        Spliterators.spliteratorUnknownSize(storage.storedConfigs(filter).entrySet().iterator(), Spliterator.ORDERED), false)
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
  public Response createOrUpdatePlugin(PluginConfigWrapper plugin) {
    return createOrUpdatePluginJSON(plugin);
  }

  /**
   * @deprecated use the method with DELETE request {@link #deletePlugin(String)} instead
   */
  @GET
  @Path("/storage/{name}/delete")
  @Produces(MediaType.APPLICATION_JSON)
  @Deprecated
  public Response deletePluginViaGet(@PathParam("name") String name) {
    return deletePlugin(name);
  }

  @XmlRootElement
  public class JsonResult {

    private final String result;

    public JsonResult(String result) {
      this.result = result;
    }

    public String getResult() {
      return result;
    }
  }

  /**
   * Model class for Storage Plugin page.
   * It contains a storage plugin as well as the CSRF token for the page.
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
