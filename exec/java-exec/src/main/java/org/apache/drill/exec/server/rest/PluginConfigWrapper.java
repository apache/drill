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

import javax.ws.rs.core.SecurityContext;
import javax.xml.bind.annotation.XmlRootElement;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.commons.lang3.StringUtils;
import org.apache.drill.common.logical.AbstractSecuredStoragePluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.common.logical.security.CredentialsProvider;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.StoragePluginRegistry.PluginException;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.drill.exec.store.security.PerUserUsernamePasswordCredentials;
import org.apache.drill.exec.store.security.oauth.OAuthTokenCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@XmlRootElement
public class PluginConfigWrapper {
  private static final Logger logger = LoggerFactory.getLogger(PluginConfigWrapper.class);
  private final String name;
  private final StoragePluginConfig config;
  private final SecurityContext sc;

  @JsonCreator
  public PluginConfigWrapper(@JsonProperty("name") String name,
                             @JsonProperty("config") StoragePluginConfig config,
                             @JacksonInject SecurityContext sc) {
    this.name = name;
    this.config = config;
    this.sc = sc;
  }

  public String getName() { return name; }

  public StoragePluginConfig getConfig() { return config; }

  public boolean enabled() {
    return config.isEnabled();
  }

  @JsonIgnore
  public String getUserName() {
    String username = "";
    String activeUser;
    if (config instanceof AbstractSecuredStoragePluginConfig) {
      logger.debug("Getting username");
      AbstractSecuredStoragePluginConfig securedStoragePluginConfig = (AbstractSecuredStoragePluginConfig) config;
      CredentialsProvider credentialsProvider = securedStoragePluginConfig.getCredentialsProvider();
      activeUser = sc.getUserPrincipal().getName();
      PerUserUsernamePasswordCredentials credentials = new PerUserUsernamePasswordCredentials(credentialsProvider, activeUser);
      username = credentials.getUsername();
      if (StringUtils.isEmpty(username)) {
        username = "";
      }
    }
    return username;
  }

  @JsonIgnore
  public String getPassword() {
    String password = "";
    String activeUser;
    if (config instanceof AbstractSecuredStoragePluginConfig) {
      AbstractSecuredStoragePluginConfig securedStoragePluginConfig = (AbstractSecuredStoragePluginConfig) config;
      CredentialsProvider credentialsProvider = securedStoragePluginConfig.getCredentialsProvider();
      activeUser = sc.getUserPrincipal().getName();
      PerUserUsernamePasswordCredentials credentials = new PerUserUsernamePasswordCredentials(credentialsProvider, activeUser);
      password = credentials.getPassword();
      if (StringUtils.isEmpty(password)) {
        password = "";
      }
    }
    return password;
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
    if (! (config instanceof AbstractSecuredStoragePluginConfig)) {
      return false;
    }
    AbstractSecuredStoragePluginConfig securedStoragePluginConfig = (AbstractSecuredStoragePluginConfig) config;
    CredentialsProvider credentialsProvider = securedStoragePluginConfig.getCredentialsProvider();
    if (credentialsProvider == null) {
      return false;
    }
    OAuthTokenCredentials tokenCredentials = new OAuthTokenCredentials(credentialsProvider);

    return !StringUtils.isEmpty(tokenCredentials.getClientID()) ||
      !StringUtils.isEmpty(tokenCredentials.getClientSecret());
  }
}
