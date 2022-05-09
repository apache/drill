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

import io.swagger.v3.oas.annotations.ExternalDocumentation;
import io.swagger.v3.oas.annotations.Operation;
import okhttp3.OkHttpClient;
import okhttp3.OkHttpClient.Builder;
import okhttp3.Request;
import org.apache.drill.common.logical.CredentialedStoragePluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig.AuthMode;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.common.logical.security.CredentialsProvider;
import org.apache.drill.exec.oauth.OAuthTokenProvider;
import org.apache.drill.exec.oauth.PersistentTokenTable;
import org.apache.drill.exec.oauth.TokenRegistry;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.rest.DrillRestServer.UserAuthEnabled;
import org.apache.drill.exec.server.rest.StorageResources.StoragePluginModel;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.StoragePluginRegistry.PluginException;
import org.apache.drill.exec.store.StoragePluginRegistry.PluginFilter;
import org.apache.drill.exec.store.http.oauth.OAuthUtils;
import org.apache.drill.exec.store.security.oauth.OAuthTokenCredentials;
import org.eclipse.jetty.util.resource.Resource;
import org.glassfish.jersey.server.mvc.Viewable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.SecurityContext;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.apache.drill.exec.server.rest.auth.DrillUserPrincipal.AUTHENTICATED_ROLE;

@Path("/")
@RolesAllowed(AUTHENTICATED_ROLE)
public class CredentialResources {
  private static final Logger logger = LoggerFactory.getLogger(CredentialResources.class);
  private static final Comparator<PluginConfigWrapper> PLUGIN_COMPARATOR =
    Comparator.comparing(PluginConfigWrapper::getName);
  private static final String ALL_PLUGINS = "all";
  private static final String ENABLED_PLUGINS = "enabled";
  private static final String DISABLED_PLUGINS = "disabled";
  private static final String TRANSLATES_USERS = "translates_users";
  private static final String OAUTH_SUCCESS_PAGE = "/rest/storage/success.html";

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
  @Operation(externalDocs = @ExternalDocumentation(description = "Apache Drill REST API documentation:", url = "https://drill.apache.org/docs/rest-api-introduction/"))
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
    return getConfigsFor(TRANSLATES_USERS);
  }

  @GET
  @Path("/credentials{group: (/[^/]+?)*}-plugins.json")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(externalDocs = @ExternalDocumentation(description = "Apache Drill REST API documentation:", url = "https://drill.apache.org/docs/rest-api-introduction/"))
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
      case TRANSLATES_USERS:
        filter = PluginFilter.TRANSLATES_USERS;
        break;
      default:
        return Collections.emptyList();
    }
    List<PluginConfigWrapper> results = StreamSupport.stream(
        Spliterators.spliteratorUnknownSize(storage.storedConfigs(filter).entrySet().iterator(), Spliterator.ORDERED), false)
      .map(entry -> new PluginConfigWrapper(entry.getKey(), entry.getValue()))
      .sorted(PLUGIN_COMPARATOR)
      .collect(Collectors.toList());

    if (results.isEmpty()) {
      return Collections.emptyList();
    } else {
      return results;
    }

  }

  @POST
  @Path("/credentials/update_credentials")
  @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(externalDocs = @ExternalDocumentation(description = "Apache Drill REST API documentation:", url = "https://drill.apache.org/docs/rest-api-introduction/"))
  public Response createOrUpdateCredentials(@FormParam("plugin") String pluginName,
                                       @FormParam("username") String username,
                                       @FormParam("password") String password) {
    String queryUser = sc.getUserPrincipal().getName();
    pluginName = pluginName.trim();
    if (pluginName.isEmpty()) {
      return Response.status(Response.Status.BAD_REQUEST)
        .entity(message("A storage config name may not be empty"))
        .build();
    }

    // Get the config
    StoragePluginConfig rawConfig = storage.getStoredConfig(pluginName);
    if (!(rawConfig instanceof CredentialedStoragePluginConfig)) {
      return Response.status(Status.INTERNAL_SERVER_ERROR)
        .entity(message(pluginName + " does not support per user credentials."))
        .build();
    }

    CredentialedStoragePluginConfig config = (CredentialedStoragePluginConfig)rawConfig;

    if (config.getAuthMode() != AuthMode.USER_TRANSLATION) {
      return Response.status(Status.INTERNAL_SERVER_ERROR)
        .entity(message(pluginName + " does not support per user translation."))
        .build();
    }

    // Get the credential provider
    CredentialsProvider credentialProvider = config.getCredentialsProvider();
    credentialProvider.setUserCredentials(username, password, queryUser);

    // Since the config classes are not accessible from java-exec, we have to serialize them,
    // replace the credential provider with the updated one, and update the storage plugin registry
    CredentialedStoragePluginConfig newConfig = config.updateCredentialProvider(credentialProvider);
    newConfig.setEnabled(config.isEnabled());

    try {
      storage.validatedPut(pluginName, newConfig);
      // Force re-caching
      storage.setEnabled(pluginName, newConfig.isEnabled());
    } catch (PluginException e) {
      logger.error("Error while saving plugin", e);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
        .entity(message("Error while saving plugin: %s", e.getMessage()))
        .build();
    }

    return Response.ok().entity(message("Success")).build();
  }

  @POST
  @Path("/credentials/{pluginName}/update_credentials.json")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(externalDocs = @ExternalDocumentation(description = "Apache Drill REST API documentation:", url = "https://drill.apache.org/docs/rest-api-introduction/"))
  public Response createOrUpdatePlugin(@PathParam("pluginName") String pluginName, UsernamePasswordContainer credentials) {
    String queryUser = sc.getUserPrincipal().getName();
    String cleanPluginName;
    if (pluginName.isEmpty()) {
      return Response.status(Response.Status.BAD_REQUEST)
        .entity(message("A storage config name may not be empty"))
        .build();
    }
    cleanPluginName = pluginName.trim();
    StoragePluginConfig config = storage.getStoredConfig(cleanPluginName);

    if (!(config instanceof CredentialedStoragePluginConfig)) {
      return Response.status(Status.INTERNAL_SERVER_ERROR)
        .entity(message(cleanPluginName + " does not support user translation."))
        .build();
    }

    if (config.getAuthMode() != AuthMode.USER_TRANSLATION) {
      return Response.status(Status.INTERNAL_SERVER_ERROR)
        .entity(message(cleanPluginName + " does not have user translation enabled."))
        .build();
    }

    CredentialedStoragePluginConfig credsConfig = (CredentialedStoragePluginConfig)config;
    CredentialsProvider credentialProvider = credsConfig.getCredentialsProvider();
    credentialProvider.setUserCredentials(credentials.getUsername(), credentials.getPassword(), queryUser);

    // Since the config classes are not accessible from java-exec, we have to serialize them,
    // replace the credential provider with the updated one, and update the storage plugin registry
    CredentialedStoragePluginConfig newConfig = credsConfig.updateCredentialProvider(credentialProvider);
    newConfig.setEnabled(credsConfig.isEnabled());

    try {
      storage.validatedPut(cleanPluginName, newConfig);
      // Force re-caching
      storage.setEnabled(cleanPluginName, newConfig.isEnabled());
    } catch (PluginException e) {
      logger.error("Error while saving plugin", e);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
        .entity(message("Error while updating plugin credentials: %s", e.getMessage()))
        .build();
    }

    return Response.status(Status.OK)
      .entity("Credentials have been updated.")
      .build();
  }

  @POST
  @Path("/credentials/{name}/update_refresh_token")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response updateRefreshToken(@PathParam("name") String name, OAuthTokenContainer tokens) {
    try {
      if (storage.getPlugin(name).getConfig() instanceof CredentialedStoragePluginConfig) {
        DrillbitContext context = ((AbstractStoragePlugin) storage.getPlugin(name)).getContext();
        OAuthTokenProvider tokenProvider = context.getoAuthTokenProvider();
        PersistentTokenTable tokenTable = tokenProvider.getOauthTokenRegistry(getActiveUser(storage.getPlugin(name).getConfig())).getTokenTable(name);

        // Set the access token
        tokenTable.setRefreshToken(tokens.getRefreshToken());

        return Response.status(Status.OK)
          .entity("Refresh token have been updated.")
          .build();
      } else {
        logger.error("{} is not a HTTP plugin. You can only add access tokens to HTTP plugins.", name);
        return Response.status(Status.INTERNAL_SERVER_ERROR)
          .entity(message("Unable to add tokens: %s", name))
          .build();
      }
    } catch (PluginException e) {
      logger.error("Error when adding tokens to {}", name);
      return Response.status(Status.INTERNAL_SERVER_ERROR)
        .entity(message("Unable to add tokens: %s", e.getMessage()))
        .build();
    }
  }

  @POST
  @Path("/credentials/{name}/update_access_token")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response updateAccessToken(@PathParam("name") String name, OAuthTokenContainer tokens) {
    try {
      if (storage.getPlugin(name).getConfig() instanceof CredentialedStoragePluginConfig) {
        DrillbitContext context = ((AbstractStoragePlugin) storage.getPlugin(name)).getContext();
        OAuthTokenProvider tokenProvider = context.getoAuthTokenProvider();
        PersistentTokenTable tokenTable = tokenProvider.getOauthTokenRegistry(getActiveUser(storage.getPlugin(name).getConfig())).getTokenTable(name);

        // Set the access token
        tokenTable.setAccessToken(tokens.getAccessToken());

        return Response.status(Status.OK)
          .entity("Access tokens have been updated.")
          .build();
      } else {
        logger.error("{} is not a HTTP plugin. You can only add access tokens to HTTP plugins.", name);
        return Response.status(Status.INTERNAL_SERVER_ERROR)
          .entity(message("Unable to add tokens: %s", name))
          .build();
      }
    } catch (PluginException e) {
      logger.error("Error when adding tokens to {}", name);
      return Response.status(Status.INTERNAL_SERVER_ERROR)
        .entity(message("Unable to add tokens: %s", e.getMessage()))
        .build();
    }
  }

  @POST
  @Path("/credentials/{name}/update_oauth_tokens")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response updateOAuthTokens(@PathParam("name") String name,
                                    OAuthTokenContainer tokenContainer) {
    try {
      if (storage.getPlugin(name).getConfig() instanceof CredentialedStoragePluginConfig) {
        DrillbitContext context = ((AbstractStoragePlugin) storage.getPlugin(name)).getContext();
        OAuthTokenProvider tokenProvider = context.getoAuthTokenProvider();
        PersistentTokenTable tokenTable = tokenProvider.getOauthTokenRegistry(getActiveUser(storage.getPlugin(name).getConfig())).getTokenTable(name);

        // Set the access and refresh token
        tokenTable.setAccessToken(tokenContainer.getAccessToken());
        tokenTable.setRefreshToken(tokenContainer.getRefreshToken());

        return Response.status(Status.OK)
          .entity("Access tokens have been updated.")
          .build();
      } else {
        logger.error("{} is not a HTTP plugin. You can only add access tokens to HTTP plugins.", name);
        return Response.status(Status.INTERNAL_SERVER_ERROR)
          .entity(message("Unable to add tokens: %s", name))
          .build();
      }
    } catch (PluginException e) {
      logger.error("Error when adding tokens to {}", name);
      return Response.status(Status.INTERNAL_SERVER_ERROR)
        .entity(message("Unable to add tokens: %s", e.getMessage()))
        .build();
    }
  }

  @GET
  @Path("/credentials/{name}/update_oauth2_authtoken")
  @Produces(MediaType.TEXT_HTML)
  public Response updateAuthToken(@PathParam("name") String name, @QueryParam("code") String code) {
    try {
      if (storage.getPlugin(name).getConfig() instanceof CredentialedStoragePluginConfig) {
        CredentialedStoragePluginConfig securedStoragePluginConfig = (CredentialedStoragePluginConfig) storage.getPlugin(name).getConfig();
        CredentialsProvider credentialsProvider = securedStoragePluginConfig.getCredentialsProvider();
        String callbackURL = this.request.getRequestURL().toString();

        // Now exchange the authorization token for an access token
        Builder builder = new OkHttpClient.Builder();
        OkHttpClient client = builder.build();
        Request accessTokenRequest = OAuthUtils.getAccessTokenRequest(credentialsProvider, code, callbackURL);
        Map<String, String> updatedTokens = OAuthUtils.getOAuthTokens(client, accessTokenRequest);

        // Add to token registry
        TokenRegistry tokenRegistry = ((AbstractStoragePlugin) storage.getPlugin(name))
          .getContext()
          .getoAuthTokenProvider()
          .getOauthTokenRegistry(getActiveUser(storage.getPlugin(name).getConfig()));

        // Add a token registry table if none exists
        tokenRegistry.createTokenTable(name);
        PersistentTokenTable tokenTable = tokenRegistry.getTokenTable(name);

        // Add tokens to persistent storage
        tokenTable.setAccessToken(updatedTokens.get(OAuthTokenCredentials.ACCESS_TOKEN));
        tokenTable.setRefreshToken(updatedTokens.get(OAuthTokenCredentials.REFRESH_TOKEN));

        // Get success page
        String successPage = null;
        try (InputStream inputStream = Resource.newClassPathResource(OAUTH_SUCCESS_PAGE).getInputStream()) {
          InputStreamReader reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
          BufferedReader bufferedReader = new BufferedReader(reader);
          successPage = bufferedReader.lines()
            .collect(Collectors.joining("\n"));
          bufferedReader.close();
          reader.close();
        } catch (IOException e) {
          Response.status(Status.OK).entity("You may close this window.").build();
        }

        return Response.status(Status.OK).entity(successPage).build();
      } else {
        logger.error("{} is not a HTTP plugin. You can only add auth code to HTTP plugins.", name);
        return Response.status(Status.INTERNAL_SERVER_ERROR)
          .entity(message("Unable to add authorization code: %s", name))
          .build();
      }
    } catch (PluginException e) {
      logger.error("Error when adding auth token to {}", name);
      return Response.status(Status.INTERNAL_SERVER_ERROR)
        .entity(message("Unable to add authorization code: %s", e.getMessage()))
        .build();
    }
  }
  private String getActiveUser(StoragePluginConfig config) {
    if (config.getAuthMode() == AuthMode.USER_TRANSLATION) {
      return sc.getUserPrincipal().getName();
    } else {
      return null;
    }
  }

  private JsonResult message(String message, Object... args) {
    return new JsonResult(String.format(message, args)); // lgtm [java/tainted-format-string]
  }

  @XmlRootElement
  public static class JsonResult {
    private final String result;
    public JsonResult(String result) {
      this.result = result;
    }
    public String getResult() {
      return result;
    }
  }
}
