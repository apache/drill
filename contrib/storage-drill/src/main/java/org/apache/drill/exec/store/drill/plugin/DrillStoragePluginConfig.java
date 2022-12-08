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
package org.apache.drill.exec.store.drill.plugin;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.calcite.avatica.ConnectStringParser;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.config.DrillProperties;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.common.logical.security.CredentialsProvider;
import org.apache.drill.common.logical.security.PlainCredentialsProvider;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.UserBitShared.UserCredentials;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.store.security.UsernamePasswordCredentials;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

@JsonTypeName(DrillStoragePluginConfig.NAME)
public class DrillStoragePluginConfig extends StoragePluginConfig {
  private static final Logger logger = LoggerFactory.getLogger(DrillStoragePluginConfig.class);

  public static final String NAME = "drill";
  public static final String CONNECTION_STRING_PREFIX = "jdbc:drill:";

  private static final String DEFAULT_QUOTING_IDENTIFIER = "`";

  private final String connection;
  private final Properties properties;

  @JsonCreator
  public DrillStoragePluginConfig(
      @JsonProperty("connection") String connection,
      @JsonProperty("properties") Properties properties,
      @JsonProperty("credentialsProvider") CredentialsProvider credentialsProvider,
      @JsonProperty("authMode") String authMode) {
    super(getCredentialsProvider(credentialsProvider), credentialsProvider == null,
      AuthMode.parseOrDefault(authMode, AuthMode.SHARED_USER));
    this.connection = connection;
    this.properties = Optional.ofNullable(properties).orElse(new Properties());
  }

  private DrillStoragePluginConfig(DrillStoragePluginConfig that,
    CredentialsProvider credentialsProvider) {
    super(getCredentialsProvider(credentialsProvider),
      credentialsProvider == null, that.authMode);
    this.connection = that.connection;
    this.properties = that.properties;
  }

  @JsonProperty("connection")
  public String getConnection() {
    return connection;
  }

  @JsonProperty("properties")
  public Properties getProperties() {
    return properties;
  }

  private static CredentialsProvider getCredentialsProvider(CredentialsProvider credentialsProvider) {
    return credentialsProvider != null ? credentialsProvider : PlainCredentialsProvider.EMPTY_CREDENTIALS_PROVIDER;
  }

  @JsonIgnore
  public String getIdentifierQuoteString() {
    return properties.getProperty(DrillProperties.QUOTING_IDENTIFIERS, DEFAULT_QUOTING_IDENTIFIER);
  }

  @Override
  public DrillStoragePluginConfig updateCredentialProvider(CredentialsProvider credentialsProvider) {
    return new DrillStoragePluginConfig(this, credentialsProvider);
  }

  private Optional<UsernamePasswordCredentials> getUsernamePasswordCredentials(
    UserCredentials userCredentials) {
    switch (authMode) {
    case SHARED_USER:
      return new UsernamePasswordCredentials.Builder()
        .setCredentialsProvider(credentialsProvider)
        .build();
    case USER_TRANSLATION:
      Preconditions.checkNotNull(
        userCredentials,
        "A drill query user is required for user translation auth mode."
      );
      return new UsernamePasswordCredentials.Builder()
        .setCredentialsProvider(credentialsProvider)
        .setQueryUser(userCredentials.getUserName())
        .build();
    default:
      throw UserException.validationError()
        .message("This storage plugin does not support auth mode: %s", authMode)
        .build(logger);
    }
  }

  private Map<String, String> getCredentials(UserCredentials userCredentials) {
    return getUsernamePasswordCredentials(userCredentials)
      .<Map<String, String>>map(creds -> ImmutableMap.of(DrillProperties.USER, creds.getUsername(),
        DrillProperties.PASSWORD, creds.getPassword()))
      .orElse(Collections.emptyMap());
  }

  @JsonIgnore
  public DrillClient getDrillClient(String userName, BufferAllocator allocator) {
    try {
      String urlSuffix = connection.substring(CONNECTION_STRING_PREFIX.length());
      Properties props = ConnectStringParser.parse(urlSuffix, properties);
      props.putAll(getCredentials(UserCredentials.newBuilder().setUserName(userName).build()));

      DrillConfig dConfig = DrillConfig.forClient();
      boolean isDirect = props.getProperty(DrillProperties.DRILLBIT_CONNECTION) != null;
      DrillClient client = new DrillClient(dConfig, null, allocator, isDirect);

      String connect = props.getProperty(DrillProperties.ZOOKEEPER_CONNECTION);
      client.connect(connect, props);
      return client;
    } catch (RpcException | SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DrillStoragePluginConfig that = (DrillStoragePluginConfig) o;
    return Objects.equals(connection, that.connection);
  }

  @Override
  public int hashCode() {
    return Objects.hash(connection);
  }
}
