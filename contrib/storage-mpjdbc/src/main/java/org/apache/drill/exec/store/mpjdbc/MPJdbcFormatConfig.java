/**
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
package org.apache.drill.exec.store.mpjdbc;

import org.apache.drill.common.logical.StoragePluginConfig;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@JsonTypeName(MPJdbcFormatConfig.NAME)
public class MPJdbcFormatConfig extends StoragePluginConfig {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory
      .getLogger(MPJdbcFormatConfig.class);
  public static final String NAME = "jdbc";

  @JsonIgnore
  private String driver;
  @JsonIgnore
  private String uri;
  @JsonIgnore
  private String username;
  @JsonIgnore
  private String password;

  @JsonCreator
  public MPJdbcFormatConfig(@JsonProperty("driver") String driver,
      @JsonProperty("uri") String uri,
      @JsonProperty("username") String username,
      @JsonProperty("password") String password) {
    this.driver = driver == null ? "" : driver;
    this.uri = uri == null ? "jdbc://" : uri;
    this.username = username == null ? "" : username;
    this.password = password == null ? "" : password;

  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    MPJdbcFormatConfig that = (MPJdbcFormatConfig) o;

    if (uri != null ? !uri.equals(that.uri) : that.uri != null) {
      return false;
    }

    return true;
  }

  @JsonProperty("driver")
  public String getDriver() {
    return this.driver;
  }

  @JsonProperty("uri")
  public String getUri() {
    return this.uri;
  }

  @JsonProperty("username")
  public String getUser() {
    return this.username;
  }

  @JsonProperty("password")
  public String getPasswd() {
    return this.password;
  }

  @Override
  public int hashCode() {
    ObjectMapper mapper = new ObjectMapper();
    try {
      String outval = mapper.writeValueAsString(this);
      logger.info("FormatConfigHashCode:" + outval);

      return outval.hashCode();
    } catch (JsonProcessingException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      return uri.hashCode();
    }
  }

}
