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

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import javax.xml.bind.annotation.XmlRootElement;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.commons.lang3.StringUtils;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.logical.OAuthConfig;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.common.logical.security.CredentialsProvider;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.StoragePluginRegistry.PluginException;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.drill.exec.store.security.UsernamePasswordCredentials;
import org.apache.drill.exec.store.security.oauth.OAuthTokenCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@XmlRootElement
public class PluginConfigWrapper {
  private static final Logger logger = LoggerFactory.getLogger(PluginConfigWrapper.class);
  private final String name;
  private final StoragePluginConfig config;

  @JsonCreator
  public PluginConfigWrapper(@JsonProperty("name") String name,
                             @JsonProperty("config") StoragePluginConfig config) {
    this.name = name;
    this.config = config;
  }

  public String getName() { return name; }

  public StoragePluginConfig getConfig() { return config; }

  public boolean enabled() {
    return config.isEnabled();
  }

  @JsonIgnore
  public String getUserName(String queryUser) {
    CredentialsProvider credentialsProvider = config.getCredentialsProvider();
    Optional<UsernamePasswordCredentials> credentials = new UsernamePasswordCredentials.Builder()
      .setCredentialsProvider(credentialsProvider)
      .setQueryUser(queryUser)
      .build();

    return credentials.map(UsernamePasswordCredentials::getUsername).orElse(null);
  }

  @JsonIgnore
  public String getPassword(String queryUser) {
    CredentialsProvider credentialsProvider = config.getCredentialsProvider();
    Optional<UsernamePasswordCredentials> credentials = new UsernamePasswordCredentials.Builder()
      .setCredentialsProvider(credentialsProvider)
      .setQueryUser(queryUser)
      .build();

    return credentials.map(UsernamePasswordCredentials::getPassword).orElse(null);
  }

  public void createOrUpdateInStorage(StoragePluginRegistry storage) throws PluginException {
    storage.validatedPut(name, config);
  }

  /**
   * Determines whether the storage plugin in question needs the OAuth button in the UI.  In
   * order to be considered an OAuth plugin, the plugin must:
   * 1. Use AbstractSecuredStoragePluginConfig
   * 2. The credential provider must not be null
   * 3. The credentialsProvider must contain a client_id and client_secret
   * @return true if the plugin uses OAuth, false if not.
   */
  @JsonIgnore
  public boolean isOauth() {
    CredentialsProvider credentialsProvider = config.getCredentialsProvider();
    if (credentialsProvider == null) {
      return false;
    }

    Optional<OAuthTokenCredentials> tokenCredentials = new OAuthTokenCredentials.Builder()
      .setCredentialsProvider(credentialsProvider)
      .build();

    return tokenCredentials.map(OAuthTokenCredentials::getClientID).orElse(null) != null;
  }

  @JsonIgnore
  public String getClientID() {
    CredentialsProvider credentialsProvider = config.getCredentialsProvider();

    return credentialsProvider.getCredentials().getOrDefault("clientID", "");
  }

  /**
   * This function generates the authorization URI for use when a non-admin user is authorizing
   * OAuth2.0 access for a storage plugin.  This function is necessary as we do not wish to expose
   * any plugin configuration information to the user.
   *
   * If the plugin is not OAuth, or is missing components, the function will return an empty string.
   * @return The authorization URI for an OAuth enabled plugin.
   */
  @JsonIgnore
  public String getAuthorizationURIWithParams() {
    if (!isOauth()) {
      logger.warn("{} is not an OAuth enabled storage plugin", name);
      return "";
    }

    String clientID = getClientID();
    OAuthConfig oAuthConfig = config.oAuthConfig();
    String authorizationURI = oAuthConfig.getAuthorizationURL();

    StringBuilder finalUrlBuilder = new StringBuilder();

    // Add the client id and redirect URI
    finalUrlBuilder.append(authorizationURI)
      .append("?client_id=")
      .append(clientID)
      .append("&redirect_uri=")
      .append(oAuthConfig.getCallbackURL());

    // Add scope if populated
    if (StringUtils.isNotEmpty(oAuthConfig.getScope())) {
      finalUrlBuilder.append("&scope=")
        .append(URLEncodeValue(oAuthConfig.getScope()));
    }

    // Add additional params if present
    Map<String,String> params = oAuthConfig.getAuthorizationParams();
    if (params != null) {
      for (Entry<String, String> param: params.entrySet()) {
        finalUrlBuilder.append("&")
          .append(param.getKey())
          .append("=")
          .append(URLEncodeValue(param.getValue()));
      }
    }
    return finalUrlBuilder.toString();
  }

  /**
   * URL Encodes a String.  Throws a {@link UserException} if anything goes wrong.
   * @param value The unencoded String
   * @return The URL encoded version of the input String
   */
  private String URLEncodeValue(String value) {
    try {
      return URLEncoder.encode(value, StandardCharsets.UTF_8.toString());
    } catch (UnsupportedEncodingException e) {
      throw UserException
        .internalError(e)
        .message("Error encoding value: " + value)
        .build(logger);
    }
  }
}
