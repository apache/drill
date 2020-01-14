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
package org.apache.drill.exec.store.jdbc;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonFilter;
import org.apache.drill.common.logical.StoragePluginConfig;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName(JdbcStorageConfig.NAME)
@JsonFilter("passwordFilter")
public class JdbcStorageConfig extends StoragePluginConfig {

  public static final String NAME = "jdbc";

  private final String driver;
  private final String url;
  private final String username;
  private final String password;
  private final boolean caseInsensitiveTableNames;
  private final Map<String, Object> sourceParameters;

  @JsonCreator
  public JdbcStorageConfig(
      @JsonProperty("driver") String driver,
      @JsonProperty("url") String url,
      @JsonProperty("username") String username,
      @JsonProperty("password") String password,
      @JsonProperty("caseInsensitiveTableNames") boolean caseInsensitiveTableNames,
      @JsonProperty("sourceParameters") Map<String, Object> sourceParameters) {
    this.driver = driver;
    this.url = url;
    this.username = username;
    this.password = password;
    this.caseInsensitiveTableNames = caseInsensitiveTableNames;
    this.sourceParameters = sourceParameters == null ? Collections.emptyMap() : sourceParameters;
  }

  public String getDriver() {
    return driver;
  }

  public String getUrl() {
    return url;
  }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }

  @JsonProperty("caseInsensitiveTableNames")
  public boolean areTableNamesCaseInsensitive() {
    return caseInsensitiveTableNames;
  }

  public Map<String, Object> getSourceParameters() {
    return sourceParameters;
  }

  @Override
  public int hashCode() {
    return Objects.hash(driver, url, username, password, caseInsensitiveTableNames, sourceParameters);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    JdbcStorageConfig that = (JdbcStorageConfig) o;
    return caseInsensitiveTableNames == that.caseInsensitiveTableNames &&
        Objects.equals(driver, that.driver) &&
        Objects.equals(url, that.url) &&
        Objects.equals(username, that.username) &&
        Objects.equals(password, that.password) &&
        Objects.equals(sourceParameters, that.sourceParameters);
  }
}
