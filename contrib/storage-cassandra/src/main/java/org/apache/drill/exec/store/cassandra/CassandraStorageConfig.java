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
package org.apache.drill.exec.store.cassandra;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import org.apache.drill.common.PlanStringBuilder;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.common.logical.security.PlainCredentialsProvider;
import org.apache.drill.exec.store.security.CredentialProviderUtils;
import org.apache.drill.common.logical.security.CredentialsProvider;
import org.apache.drill.exec.store.security.UsernamePasswordCredentials;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

@JsonTypeName(CassandraStorageConfig.NAME)
public class CassandraStorageConfig extends StoragePluginConfig {
  public static final String NAME = "cassandra";
  private final String host;
  private final int port;

  @JsonCreator
  public CassandraStorageConfig(
      @JsonProperty("host") String host,
      @JsonProperty("port") int port,
      @JsonProperty("username") String username,
      @JsonProperty("password") String password,
      @JsonProperty("authMode") String authMode,
      @JsonProperty("credentialsProvider") CredentialsProvider credentialsProvider) {
    super(CredentialProviderUtils.getCredentialsProvider(username, password, credentialsProvider),
        credentialsProvider == null, AuthMode.parseOrDefault(authMode, AuthMode.SHARED_USER));
    this.host = host;
    this.port = port;
  }

  private CassandraStorageConfig(CassandraStorageConfig that, CredentialsProvider credentialsProvider) {
    super(getCredentialsProvider(credentialsProvider), credentialsProvider == null, that.authMode);
    this.host = that.host;
    this.port = that.port;
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  @JsonIgnore
  public Optional<UsernamePasswordCredentials> getUsernamePasswordCredentials() {
    return new UsernamePasswordCredentials.Builder()
      .setCredentialsProvider(credentialsProvider)
      .build();
  }

  public String getUsername() {
    return getUsernamePasswordCredentials()
      .map(UsernamePasswordCredentials::getUsername)
      .orElse(null);
  }

  public String getPassword() {
    return getUsernamePasswordCredentials()
      .map(UsernamePasswordCredentials::getPassword)
      .orElse(null);
  }

  @JsonIgnore
  public Optional<UsernamePasswordCredentials> getUsernamePasswordCredentials(String username) {
    return new UsernamePasswordCredentials.Builder()
        .setCredentialsProvider(credentialsProvider)
        .setQueryUser(username)
        .build();
  }

  @JsonIgnore
  public Map<String, Object> toConfigMap(String username) {
    Optional<UsernamePasswordCredentials> credentials = getUsernamePasswordCredentials(username);

    Map<String, Object> result = new HashMap<>();
    result.put("host", host);
    result.put("port", port);
    if (credentials.isPresent()) {
      result.put("username", credentials.get().getUsername());
      result.put("password", credentials.get().getPassword());
    }
    return result;
  }

  @Override
  public CassandraStorageConfig updateCredentialProvider(CredentialsProvider credentialsProvider) {
    return new CassandraStorageConfig(this, credentialsProvider);
  }

  private static CredentialsProvider getCredentialsProvider(CredentialsProvider credentialsProvider) {
    return credentialsProvider != null ? credentialsProvider : PlainCredentialsProvider.EMPTY_CREDENTIALS_PROVIDER;
  }

  @JsonIgnore
  public Map<String, Object> toConfigMap() {
    Optional<UsernamePasswordCredentials> credentials = getUsernamePasswordCredentials();

    Map<String, Object> result = new HashMap<>();
    result.put("host", host);
    result.put("port", port);
    if (credentials.isPresent()) {
      result.put("username", credentials.get().getUsername());
      result.put("password", credentials.get().getPassword());
    }
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CassandraStorageConfig that = (CassandraStorageConfig) o;
    return Objects.equals(host, that.host) &&
        Objects.equals(port, that.port) &&
        Objects.equals(credentialsProvider, that.credentialsProvider);
  }

  @Override
  public int hashCode() {
    return Objects.hash(host, port, credentialsProvider);
  }

  @Override
  public String toString() {
    return new PlanStringBuilder(this)
        .field("host", host)
        .field("port", port)
        .field("credentialsProvider", credentialsProvider)
        .toString();
  }
}
