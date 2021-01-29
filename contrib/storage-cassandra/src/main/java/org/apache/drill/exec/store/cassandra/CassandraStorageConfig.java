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
import org.apache.drill.common.logical.StoragePluginConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

@JsonTypeName(CassandraStorageConfig.NAME)
public class CassandraStorageConfig extends StoragePluginConfig {
  public static final String NAME = "cassandra";

  private final String host;
  private final String username;
  private final String password;
  private final int port;

  @JsonCreator
  public CassandraStorageConfig(
      @JsonProperty("host") String host,
      @JsonProperty("port") int port,
      @JsonProperty("username") String username,
      @JsonProperty("password") String password) {
    this.host = host;
    this.username = username;
    this.password = password;
    this.port = port;
  }

  public String getHost() {
    return host;
  }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }

  public int getPort() {
    return port;
  }

  @JsonIgnore
  public Map<String, Object> toConfigMap() {
    Map<String, Object> result = new HashMap<>();

    result.put("host", host);
    result.put("port", port);
    result.put("username", username);
    result.put("password", password);
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
    return Objects.equals(host, that.host)
        && Objects.equals(username, that.username)
        && Objects.equals(password, that.password);
  }

  @Override
  public int hashCode() {
    return Objects.hash(host, username, password);
  }
}
