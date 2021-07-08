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
package org.apache.drill.exec.store.mongo;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.mongodb.ConnectionString;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.drill.common.logical.AbstractSecuredStoragePluginConfig;
import org.apache.drill.common.logical.security.CredentialsProvider;
import org.apache.drill.common.logical.security.PlainCredentialsProvider;

@JsonTypeName(MongoStoragePluginConfig.NAME)
public class MongoStoragePluginConfig extends AbstractSecuredStoragePluginConfig {

  public static final String NAME = "mongo";

  private final String connection;

  @JsonIgnore
  private final ConnectionString clientURI;

  private final MongoPluginOptimizations pluginOptimizations;

  @JsonCreator
  public MongoStoragePluginConfig(@JsonProperty("connection") String connection,
    @JsonProperty("pluginOptimizations") MongoPluginOptimizations pluginOptimizations,
    @JsonProperty("credentialsProvider") CredentialsProvider credentialsProvider) {
    super(getCredentialsProvider(credentialsProvider), credentialsProvider == null);
    this.connection = connection;
    this.clientURI = new ConnectionString(connection);
    this.pluginOptimizations = ObjectUtils.defaultIfNull(pluginOptimizations, new MongoPluginOptimizations());
  }

  public MongoPluginOptimizations getPluginOptimizations() {
    return pluginOptimizations;
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
  public List<String> getHosts() {
    return clientURI.getHosts();
  }

  public String getConnection() {
    return connection;
  }

  private static CredentialsProvider getCredentialsProvider(CredentialsProvider credentialsProvider) {
    return credentialsProvider != null ? credentialsProvider : PlainCredentialsProvider.EMPTY_CREDENTIALS_PROVIDER;
  }

  public static class MongoPluginOptimizations {

    private boolean supportsProjectPushdown = true;

    private boolean supportsFilterPushdown = true;

    private boolean supportsAggregatePushdown = true;

    private boolean supportsSortPushdown = true;

    private boolean supportsUnionPushdown = true;

    private boolean supportsLimitPushdown = true;

    public boolean isSupportsProjectPushdown() {
      return supportsProjectPushdown;
    }

    public void setSupportsProjectPushdown(boolean supportsProjectPushdown) {
      this.supportsProjectPushdown = supportsProjectPushdown;
    }

    public boolean isSupportsFilterPushdown() {
      return supportsFilterPushdown;
    }

    public void setSupportsFilterPushdown(boolean supportsFilterPushdown) {
      this.supportsFilterPushdown = supportsFilterPushdown;
    }

    public boolean isSupportsAggregatePushdown() {
      return supportsAggregatePushdown;
    }

    public void setSupportsAggregatePushdown(boolean supportsAggregatePushdown) {
      this.supportsAggregatePushdown = supportsAggregatePushdown;
    }

    public boolean isSupportsSortPushdown() {
      return supportsSortPushdown;
    }

    public void setSupportsSortPushdown(boolean supportsSortPushdown) {
      this.supportsSortPushdown = supportsSortPushdown;
    }

    public boolean isSupportsUnionPushdown() {
      return supportsUnionPushdown;
    }

    public void setSupportsUnionPushdown(boolean supportsUnionPushdown) {
      this.supportsUnionPushdown = supportsUnionPushdown;
    }

    public boolean isSupportsLimitPushdown() {
      return supportsLimitPushdown;
    }

    public void setSupportsLimitPushdown(boolean supportsLimitPushdown) {
      this.supportsLimitPushdown = supportsLimitPushdown;
    }
  }
}
