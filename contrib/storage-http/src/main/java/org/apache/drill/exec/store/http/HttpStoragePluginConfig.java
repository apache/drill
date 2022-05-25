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
package org.apache.drill.exec.store.http;

import org.apache.drill.common.PlanStringBuilder;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.logical.OAuthConfig;
import org.apache.drill.common.map.CaseInsensitiveMap;
import org.apache.drill.common.logical.StoragePluginConfig;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.exec.store.security.CredentialProviderUtils;
import org.apache.drill.common.logical.security.CredentialsProvider;
import org.apache.drill.exec.store.security.UsernamePasswordWithProxyCredentials;
import org.apache.drill.exec.store.security.oauth.OAuthTokenCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;


@JsonTypeName(HttpStoragePluginConfig.NAME)
public class HttpStoragePluginConfig extends StoragePluginConfig {
  private static final Logger logger = LoggerFactory.getLogger(HttpStoragePluginConfig.class);
  public static final String NAME = "http";

  public final Map<String, HttpApiConfig> connections;
  public final boolean cacheResults;
  public final String proxyHost;
  public final int proxyPort;
  public final String proxyType;
  /**
   * Timeout in {@link TimeUnit#SECONDS}.
   */
  public final int timeout;

  @JsonCreator
  public HttpStoragePluginConfig(@JsonProperty("cacheResults") Boolean cacheResults,
                                 @JsonProperty("connections") Map<String, HttpApiConfig> connections,
                                 @JsonProperty("timeout") Integer timeout,
                                 @JsonProperty("username") String username,
                                 @JsonProperty("password") String password,
                                 @JsonProperty("proxyHost") String proxyHost,
                                 @JsonProperty("proxyPort") Integer proxyPort,
                                 @JsonProperty("proxyType") String proxyType,
                                 @JsonProperty("proxyUsername") String proxyUsername,
                                 @JsonProperty("proxyPassword") String proxyPassword,
                                 @JsonProperty("oAuthConfig") OAuthConfig oAuthConfig,
                                 @JsonProperty("credentialsProvider") CredentialsProvider credentialsProvider,
                                 @JsonProperty("authMode") String authMode
                                 ) {
    super(CredentialProviderUtils.getCredentialsProvider(
        null,
        null,
        null,
        normalize(username),
        normalize(password),
        normalize(proxyUsername),
        normalize(proxyPassword),
        credentialsProvider),
        credentialsProvider == null,
        AuthMode.parseOrDefault(authMode, AuthMode.SHARED_USER),
      oAuthConfig);
    this.cacheResults = cacheResults != null && cacheResults;

    this.connections = CaseInsensitiveMap.newHashMap();
    if (connections != null) {
      this.connections.putAll(connections);
    }

    this.timeout = timeout == null ? 0 : timeout;
    this.proxyHost = normalize(proxyHost);
    this.proxyPort = proxyPort == null ? 0 : proxyPort;

    proxyType = normalize(proxyType);
    this.proxyType = proxyType == null
        ? "direct" : proxyType.trim().toLowerCase();

    // Validate Proxy Type
    switch (this.proxyType) {
      case "direct":
      case "http":
      case "socks":
        break;
      default:
        throw UserException
          .validationError()
          .message("Invalid Proxy Type: %s.  Drill supports 'direct', 'http' and 'socks' proxies.", proxyType)
          .build(logger);
    }
  }

  private HttpStoragePluginConfig(HttpStoragePluginConfig that, CredentialsProvider credentialsProvider) {
    super(credentialsProvider, credentialsProvider == null, that.authMode);
    this.cacheResults = that.cacheResults;
    this.connections = that.connections;
    this.timeout = that.timeout;
    this.proxyHost = that.proxyHost;
    this.proxyPort = that.proxyPort;
    this.proxyType = that.proxyType;
    this.oAuthConfig = that.oAuthConfig;
  }

  /**
   * Clone constructor used for updating OAuth tokens
   * @param that The current HTTP Plugin Config
   * @param oAuthConfig The updated OAuth config
   */
  public HttpStoragePluginConfig(HttpStoragePluginConfig that, OAuthConfig oAuthConfig) {
    super(CredentialProviderUtils.getCredentialsProvider(that.proxyUsername(), that.proxyPassword(), that.credentialsProvider),
      that.credentialsProvider == null);

    this.cacheResults = that.cacheResults;
    this.connections = that.connections;
    this.timeout = that.timeout;
    this.proxyHost = that.proxyHost;
    this.proxyPort = that.proxyPort;
    this.proxyType = that.proxyType;
    this.oAuthConfig = that.oAuthConfig;
  }

  private static String normalize(String value) {
    if (value == null) {
      return value;
    }
    value = value.trim();
    return value.isEmpty() ? null : value;
  }

  /**
   * Create a copy of the plugin config with only the indicated connection.
   * The copy is used in the query plan to avoid including unnecessary information.
   */
  public HttpStoragePluginConfig copyForPlan(String connectionName) {
    Optional<UsernamePasswordWithProxyCredentials> creds = getUsernamePasswordCredentials();
    return new HttpStoragePluginConfig(
      cacheResults,
      configFor(connectionName),
      timeout,
      username(),
      password(),
      proxyHost,
      proxyPort,
      proxyType,
      proxyUsername(),
      proxyPassword(),
      oAuthConfig,
      credentialsProvider,
      authMode.name()
    );
  }

  private Map<String, HttpApiConfig> configFor(String connectionName) {
    Map<String, HttpApiConfig> single = new HashMap<>();
    single.put(connectionName, getConnection(connectionName));
    return single;
  }

  @Override
  public boolean equals(Object that) {
    if (this == that) {
      return true;
    } else if (that == null || getClass() != that.getClass()) {
      return false;
    }
    HttpStoragePluginConfig thatConfig = (HttpStoragePluginConfig) that;
    return Objects.equals(connections, thatConfig.connections) &&
      Objects.equals(cacheResults, thatConfig.cacheResults) &&
      Objects.equals(proxyHost, thatConfig.proxyHost) &&
      Objects.equals(proxyPort, thatConfig.proxyPort) &&
      Objects.equals(proxyType, thatConfig.proxyType) &&
      Objects.equals(oAuthConfig, thatConfig.oAuthConfig) &&
      Objects.equals(credentialsProvider, thatConfig.credentialsProvider) &&
      Objects.equals(authMode, thatConfig.authMode);
  }

  @Override
  public String toString() {
    return new PlanStringBuilder(this)
      .field("connections", connections)
      .field("cacheResults", cacheResults)
      .field("timeout", timeout)
      .field("proxyHost", proxyHost)
      .field("proxyPort", proxyPort)
      .field("credentialsProvider", credentialsProvider)
      .field("oauthConfig", oAuthConfig)
      .field("proxyType", proxyType)
      .field("authMode", authMode)
      .toString();
  }

  @Override
  public int hashCode() {
    return Objects.hash(connections, cacheResults, timeout,
        proxyHost, proxyPort, proxyType, oAuthConfig, credentialsProvider, authMode);
  }

  @JsonProperty("cacheResults")
  public boolean cacheResults() { return cacheResults; }

  @JsonProperty("connections")
  public Map<String, HttpApiConfig> connections() { return connections; }

  @JsonProperty("timeout")
  public int timeout() { return timeout;}

   @JsonProperty("proxyHost")
  public String proxyHost() { return proxyHost; }

  @JsonProperty("proxyPort")
  public int proxyPort() { return proxyPort; }

  @JsonProperty("username")
  public String username() {
    if (!directCredentials) {
      return null;
    }
    return getUsernamePasswordCredentials()
      .map(UsernamePasswordWithProxyCredentials::getUsername)
      .orElse(null);
  }

  @JsonProperty("password")
  public String password() {
    if (!directCredentials) {
      return null;
    }
    return getUsernamePasswordCredentials()
      .map(UsernamePasswordWithProxyCredentials::getPassword)
      .orElse(null);
  }

  @JsonProperty("proxyUsername")
  public String proxyUsername() {
    if (!directCredentials) {
      return null;
    }
    return getUsernamePasswordCredentials()
      .map(UsernamePasswordWithProxyCredentials::getProxyUsername)
      .orElse(null);
  }

  @JsonProperty("proxyPassword")
  public String proxyPassword() {
    if (!directCredentials) {
      return null;
    }
    return getUsernamePasswordCredentials()
      .map(UsernamePasswordWithProxyCredentials::getProxyPassword)
      .orElse(null);
  }

  @JsonIgnore
  private static String getClientID(OAuthTokenCredentials credentials) {
    return credentials.getClientID();
  }

  @JsonIgnore
  private static String getClientSecret(OAuthTokenCredentials credentials) {
    return credentials.getClientSecret();
  }

  @JsonIgnore
  private static String getTokenURL(OAuthTokenCredentials credentials) {
    return credentials.getTokenUri();
  }

  @JsonProperty("proxyType")
  public String proxyType() { return proxyType; }

  @JsonIgnore
  public HttpApiConfig getConnection(String connectionName) {
    return connections.get(connectionName);
  }

  @JsonIgnore
  public Optional<UsernamePasswordWithProxyCredentials> getUsernamePasswordCredentials() {
    return new UsernamePasswordWithProxyCredentials.Builder()
      .setCredentialsProvider(credentialsProvider)
      .build();
  }

  @JsonIgnore
  public Optional<UsernamePasswordWithProxyCredentials> getUsernamePasswordCredentials(String username) {
    return new UsernamePasswordWithProxyCredentials.Builder()
      .setCredentialsProvider(credentialsProvider)
      .setQueryUser(username)
      .build();
  }

  @JsonIgnore
  public static Optional<OAuthTokenCredentials> getOAuthCredentials(CredentialsProvider credentialsProvider) {
    return new OAuthTokenCredentials.Builder()
      .setCredentialsProvider(credentialsProvider)
      .build();
  }

  @Override
  public HttpStoragePluginConfig updateCredentialProvider(CredentialsProvider credentialsProvider) {
    return new HttpStoragePluginConfig(this, credentialsProvider);
  }
}
