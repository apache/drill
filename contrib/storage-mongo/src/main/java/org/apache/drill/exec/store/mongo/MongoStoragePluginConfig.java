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
package org.apache.drill.exec.store.mongo;

import org.apache.drill.common.logical.StoragePluginConfig;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;

@JsonTypeName(MongoStoragePluginConfig.NAME)
public class MongoStoragePluginConfig extends StoragePluginConfig {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory
      .getLogger(MongoStoragePluginConfig.class);

  public static final String NAME = "mongo";

  private String connection;

  @JsonIgnore
  private MongoClientURI clientURI;

  @JsonCreator
  public MongoStoragePluginConfig(@JsonProperty("connection") String connection) {
    this.connection = connection;
  }

  @Override
  public boolean equals(Object that) {
    if (this == that) {
      return true;
    } else if (that == null || getClass() != that.getClass()) {
      return false;
    }
    MongoStoragePluginConfig thatConfig = (MongoStoragePluginConfig) that;
    return this.connection.equals(thatConfig.connection);

  }

  @Override
  public int hashCode() {
    return this.connection != null ? this.connection.hashCode() : 0;
  }

  @JsonIgnore
  public MongoClientOptions getMongoOptions() {
    MongoClientURI clientURI = new MongoClientURI(connection);
    return clientURI.getOptions();
  }

  public String getConnection() {
    return connection;
  }
}
