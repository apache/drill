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
import org.apache.drill.common.map.CaseInsensitiveMap;
import org.apache.drill.common.logical.StoragePluginConfigBase;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;


@JsonTypeName(HttpStoragePluginConfig.NAME)
public class HttpStoragePluginConfig extends StoragePluginConfigBase {
  private static final Logger logger = LoggerFactory.getLogger(HttpStoragePluginConfig.class);
  public static final String NAME = "http";
  public final Map<String, HttpApiConfig> connections;
  public final boolean cacheResults;
  public final String proxyHost;
  public final int proxyPort;
  public final String proxyType;
  public final String proxyUsername;
  public final String proxyPassword;
  /**
   * Timeout in seconds.
   */
  public int timeout;

  @JsonCreator
  public HttpStoragePluginConfig(@JsonProperty("cacheResults") Boolean cacheResults,
                                 @JsonProperty("connections") Map<String, HttpApiConfig> connections,
                                 @JsonProperty("timeout") Integer timeout,
                                 @JsonProperty("proxyHost") String proxyHost,
                                 @JsonProperty("proxyPort") Integer proxyPort,
                                 @JsonProperty("proxyType") String proxyType,
                                 @JsonProperty("proxyUsername") String proxyUsername,
                                 @JsonProperty("proxyPassword") String proxyPassword
                                 ) {
    this.cacheResults = cacheResults == null ? false : cacheResults;

    this.connections = CaseInsensitiveMap.newHashMap();
    if (connections != null) {
      this.connections.putAll(connections);
    }

    this.timeout = timeout == null ? 0 : timeout;
    this.proxyHost = normalize(proxyHost);
    this.proxyPort = proxyPort == null ? 0 : proxyPort;
    this.proxyUsername = normalize(proxyUsername);
    this.proxyPassword = normalize(proxyPassword);
    proxyType = normalize(proxyType);
    this.proxyType = proxyType == null
        ? "direct" : proxyType.trim().toLowerCase();

    // Validate Proxy Type
    if (this.proxyType != null) {
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
    return new HttpStoragePluginConfig(
        cacheResults, configFor(connectionName), timeout,
        proxyHost, proxyPort, proxyType, proxyUsername, proxyPassword);
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
           Objects.equals(proxyUsername, thatConfig.proxyUsername) &&
           Objects.equals(proxyPassword, thatConfig.proxyPassword);
  }

  @Override
  public String toString() {
    return new PlanStringBuilder(this)
      .field("connections", connections)
      .field("cacheResults", cacheResults)
      .field("timeout", timeout)
      .field("proxyHost", proxyHost)
      .field("proxyPort", proxyPort)
      .field("proxyUsername", proxyUsername)
      .maskedField("proxyPassword", proxyPassword)
      .field("proxyType", proxyType)
      .toString();
  }

  @Override
  public int hashCode() {
    return Objects.hash(connections, cacheResults, timeout,
        proxyHost, proxyPort, proxyType, proxyUsername, proxyPassword);
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

  @JsonProperty("proxyUsername")
  public String proxyUsername() { return proxyUsername; }

  @JsonProperty("proxyPassword")
  public String proxyPassword() { return proxyPassword; }

  @JsonProperty("proxyType")
  public String proxyType() { return proxyType; }

  @JsonIgnore
  public HttpApiConfig getConnection(String connectionName) {
    return connections.get(connectionName);
  }
}
