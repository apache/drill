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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.common.PlanStringBuilder;

@JsonTypeName("http-scan-spec")
public class HttpScanSpec {

  protected final String schemaName;

  protected final String database;

  protected final String tableName;

  protected final HttpStoragePluginConfig config;

  @JsonCreator
  public HttpScanSpec(@JsonProperty("schemaName") String schemaName,
                      @JsonProperty("database") String database,
                      @JsonProperty("tableName") String tableName,
                      @JsonProperty("config") HttpStoragePluginConfig config) {
    this.schemaName = schemaName;
    this.database = database;
    this.tableName = tableName;
    this.config = config;
  }

  @JsonProperty("database")
  public String database() {
    return database;
  }

  @JsonProperty("tableName")
  public String tableName() {
    return tableName;
  }

  @JsonIgnore
  public String getURL() {
    return database;
  }

  @Override
  public String toString() {
    return new PlanStringBuilder(this)
      .field("schemaName", schemaName)
      .field("database", database)
      .field("tableName", tableName)
      .field("config", config)
      .toString();
  }
}
