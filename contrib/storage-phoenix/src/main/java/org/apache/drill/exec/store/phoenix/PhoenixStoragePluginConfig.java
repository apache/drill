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
package org.apache.drill.exec.store.phoenix;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.apache.commons.lang3.StringUtils;
import org.apache.drill.common.PlanStringBuilder;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.common.logical.security.CredentialsProvider;
import org.apache.drill.exec.store.security.CredentialProviderUtils;
import org.apache.drill.exec.store.security.UsernamePasswordCredentials;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName(PhoenixStoragePluginConfig.NAME)
public class PhoenixStoragePluginConfig extends StoragePluginConfig {

  public static final String NAME = "phoenix";
  public static final String FAT_DRIVER_CLASS = "org.apache.phoenix.jdbc.PhoenixDriver";

  private final String zkQuorum;
  private final String zkPath;
  private final Integer port;
  private final String jdbcURL; // (options) Equal to host + port + zkPath
  private final Map<String, Object> props; // (options) See also http://phoenix.apache.org/tuning.html

  @JsonCreator
  public PhoenixStoragePluginConfig(
      @JsonProperty("zkQuorum") String zkQuorum,
      @JsonProperty("port") Integer port,
      @JsonProperty("zkPath") String zkPath,
      @JsonProperty("userName") String userName,
      @JsonProperty("password") String password,
      @JsonProperty("jdbcURL") String jdbcURL,
      @JsonProperty("credentialsProvider") CredentialsProvider credentialsProvider,
      @JsonProperty("props") Map<String, Object> props) {
    super(CredentialProviderUtils.getCredentialsProvider(userName, password, credentialsProvider), credentialsProvider == null);
    this.zkQuorum = zkQuorum;
    this.zkPath = zkPath;
    this.port = port;
    this.jdbcURL = jdbcURL;
    this.props = props == null ? Collections.emptyMap() : props;
  }

  @JsonIgnore
  public Optional<UsernamePasswordCredentials> getUsernamePasswordCredentials() {
    return new UsernamePasswordCredentials.Builder()
      .setCredentialsProvider(credentialsProvider)
      .build();
  }

  @JsonProperty("zkQuorum")
  public String getZkQuorum() {
    return zkQuorum;
  }

  @JsonProperty("zkPath")
  public String getZkPath() {
    return zkPath;
  }

  @JsonProperty("port")
  public Integer getPort() {
    return port;
  }

  @JsonProperty("userName")
  public String getUsername() {
    if (!directCredentials) {
      return null;
    }
    return getUsernamePasswordCredentials()
      .map(UsernamePasswordCredentials::getUsername)
      .orElse(null);
  }

  @JsonIgnore
  @JsonProperty("password")
  public String getPassword() {
    if (!directCredentials) {
      return null;
    }
    return getUsernamePasswordCredentials()
      .map(UsernamePasswordCredentials::getPassword)
      .orElse(null);
  }

  @JsonProperty("jdbcURL")
  public String getJdbcURL() {
    return jdbcURL;
  }

  @JsonProperty("props")
  public Map<String, Object> getProps() {
    return props;
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o == null || !(o instanceof PhoenixStoragePluginConfig)) {
      return false;
    }
    PhoenixStoragePluginConfig config = (PhoenixStoragePluginConfig) o;
    // URL first
    if (StringUtils.isNotBlank(config.getJdbcURL())) {
      return Objects.equals(this.jdbcURL, config.getJdbcURL());
    }
    // Then the host and port
    return Objects.equals(this.zkQuorum, config.getZkQuorum())
      && Objects.equals(this.port, config.getPort())
      && Objects.equals(this.zkPath, config.getZkPath());
  }

  @Override
  public int hashCode() {
    if (StringUtils.isNotBlank(jdbcURL)) {
     return Objects.hash(jdbcURL);
    }
    return Objects.hash(zkQuorum, port, zkPath);
  }

  @Override
  public String toString() {
    return new PlanStringBuilder(PhoenixStoragePluginConfig.NAME)
        .field("zkQuorum", zkQuorum)
        .field("port", port)
        .field("zkPath", zkPath)
        .field("userName", getUsername())
        .maskedField("password", getPassword()) // will set to "*******"
        .field("jdbcURL", jdbcURL)
        .field("props", props)
        .toString();
  }
}
