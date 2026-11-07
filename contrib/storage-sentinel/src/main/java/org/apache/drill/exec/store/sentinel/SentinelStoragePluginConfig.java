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

package org.apache.drill.exec.store.sentinel;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.common.logical.security.CredentialsProvider;
import org.apache.drill.exec.store.security.CredentialProviderUtils;

import java.util.List;
import java.util.Objects;

@JsonTypeName("sentinel")
public class SentinelStoragePluginConfig extends StoragePluginConfig {
  private final String workspaceId;
  private final List<String> workspaceIds;
  private final String tenantId;
  private final String clientId;
  private final String clientSecret;
  private final String defaultTimespan;
  private final int maxRows;
  private final List<String> tables;
  private final String apiEndpoint;
  private final String tokenEndpoint;
  private final boolean cacheResults;

  @JsonCreator
  public SentinelStoragePluginConfig(
      @JsonProperty("workspaceId") String workspaceId,
      @JsonProperty("workspaceIds") List<String> workspaceIds,
      @JsonProperty("tenantId") String tenantId,
      @JsonProperty("clientId") String clientId,
      @JsonProperty("clientSecret") String clientSecret,
      @JsonProperty("defaultTimespan") String defaultTimespan,
      @JsonProperty("maxRows") int maxRows,
      @JsonProperty("tables") List<String> tables,
      @JsonProperty("authMode") AuthMode authMode,
      @JsonProperty("credentialsProvider") CredentialsProvider credentialsProvider,
      @JsonProperty("apiEndpoint") String apiEndpoint,
      @JsonProperty("tokenEndpoint") String tokenEndpoint,
      @JsonProperty("cacheResults") Boolean cacheResults) {
    super(CredentialProviderUtils.getCredentialsProvider(clientId, clientSecret, null, null,
        null, null, null, credentialsProvider), false, authMode);
    this.workspaceId = workspaceId;
    this.workspaceIds = (workspaceIds != null && !workspaceIds.isEmpty()) ? workspaceIds :
        (workspaceId != null ? List.of(workspaceId) : List.of());
    this.tenantId = tenantId;
    this.clientId = clientId;
    this.clientSecret = clientSecret;
    this.defaultTimespan = defaultTimespan != null ? defaultTimespan : "P1D";
    this.maxRows = maxRows > 0 ? maxRows : 10000;
    this.tables = tables != null ? tables : List.of();
    this.apiEndpoint = apiEndpoint != null ? apiEndpoint : "https://api.loganalytics.io/v1";
    this.tokenEndpoint = tokenEndpoint;
    this.cacheResults = cacheResults != null && cacheResults;
  }

  public SentinelStoragePluginConfig(SentinelStoragePluginConfig that, CredentialsProvider credentialsProvider) {
    super(credentialsProvider, false, that.authMode);
    this.workspaceId = that.workspaceId;
    this.workspaceIds = that.workspaceIds;
    this.tenantId = that.tenantId;
    this.clientId = that.clientId;
    this.clientSecret = that.clientSecret;
    this.defaultTimespan = that.defaultTimespan;
    this.maxRows = that.maxRows;
    this.tables = that.tables;
    this.apiEndpoint = that.apiEndpoint;
    this.tokenEndpoint = that.tokenEndpoint;
    this.cacheResults = that.cacheResults;
  }

  public String getWorkspaceId() {
    return workspaceId;
  }

  public List<String> getWorkspaceIds() {
    return workspaceIds;
  }

  public String getTenantId() {
    return tenantId;
  }

  public String getClientId() {
    return clientId;
  }

  public String getClientSecret() {
    return clientSecret;
  }

  public String getDefaultTimespan() {
    return defaultTimespan;
  }

  public int getMaxRows() {
    return maxRows;
  }

  public List<String> getTables() {
    return tables;
  }

  public String getApiEndpoint() {
    return apiEndpoint;
  }

  public String getTokenEndpoint() {
    return tokenEndpoint;
  }

  public boolean cacheResults() {
    return cacheResults;
  }

  public AuthMode getAuthMode() {
    return authMode;
  }

  public CredentialsProvider getCredentialsProvider() {
    return credentialsProvider;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SentinelStoragePluginConfig that = (SentinelStoragePluginConfig) o;
    return maxRows == that.maxRows
        && cacheResults == that.cacheResults
        && Objects.equals(workspaceId, that.workspaceId)
        && Objects.equals(workspaceIds, that.workspaceIds)
        && Objects.equals(tenantId, that.tenantId)
        && Objects.equals(clientId, that.clientId)
        && Objects.equals(clientSecret, that.clientSecret)
        && Objects.equals(defaultTimespan, that.defaultTimespan)
        && Objects.equals(tables, that.tables)
        && Objects.equals(apiEndpoint, that.apiEndpoint)
        && Objects.equals(tokenEndpoint, that.tokenEndpoint)
        && Objects.equals(credentialsProvider, that.credentialsProvider)
        && authMode == that.authMode;
  }

  @Override
  public int hashCode() {
    return Objects.hash(workspaceId, workspaceIds, tenantId, clientId, clientSecret, defaultTimespan, maxRows, tables, apiEndpoint, tokenEndpoint, cacheResults, credentialsProvider, authMode);
  }

  @Override
  public String toString() {
    return "SentinelStoragePluginConfig{" +
        "workspaceId='" + workspaceId + '\'' +
        ", workspaceIds=" + workspaceIds +
        ", tenantId='" + tenantId + '\'' +
        ", clientId='" + clientId + '\'' +
        ", defaultTimespan='" + defaultTimespan + '\'' +
        ", maxRows=" + maxRows +
        ", tables=" + tables +
        ", apiEndpoint='" + apiEndpoint + '\'' +
        ", cacheResults=" + cacheResults +
        ", authMode=" + authMode +
        '}';
  }

  @Override
  public StoragePluginConfig updateCredentialProvider(CredentialsProvider credentialsProvider) {
    return new SentinelStoragePluginConfig(this, credentialsProvider);
  }
}
