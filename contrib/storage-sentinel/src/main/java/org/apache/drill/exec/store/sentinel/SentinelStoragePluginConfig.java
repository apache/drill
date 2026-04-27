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
  private final String tenantId;
  private final String clientId;
  private final String clientSecret;
  private final String defaultTimespan;
  private final int maxRows;
  private final List<String> tables;

  @JsonCreator
  public SentinelStoragePluginConfig(
      @JsonProperty("workspaceId") String workspaceId,
      @JsonProperty("tenantId") String tenantId,
      @JsonProperty("clientId") String clientId,
      @JsonProperty("clientSecret") String clientSecret,
      @JsonProperty("defaultTimespan") String defaultTimespan,
      @JsonProperty("maxRows") int maxRows,
      @JsonProperty("tables") List<String> tables,
      @JsonProperty("authMode") AuthMode authMode,
      @JsonProperty("credentialsProvider") CredentialsProvider credentialsProvider) {
    super(CredentialProviderUtils.getCredentialsProvider(clientId, clientSecret, null, null,
        null, null, null, credentialsProvider), false, authMode);
    this.workspaceId = workspaceId;
    this.tenantId = tenantId;
    this.clientId = clientId;
    this.clientSecret = clientSecret;
    this.defaultTimespan = defaultTimespan != null ? defaultTimespan : "P1D";
    this.maxRows = maxRows > 0 ? maxRows : 10000;
    this.tables = tables != null ? tables : List.of();
  }

  public SentinelStoragePluginConfig(SentinelStoragePluginConfig that, CredentialsProvider credentialsProvider) {
    super(credentialsProvider, false, that.authMode);
    this.workspaceId = that.workspaceId;
    this.tenantId = that.tenantId;
    this.clientId = that.clientId;
    this.clientSecret = that.clientSecret;
    this.defaultTimespan = that.defaultTimespan;
    this.maxRows = that.maxRows;
    this.tables = that.tables;
  }

  public String getWorkspaceId() {
    return workspaceId;
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
        && Objects.equals(workspaceId, that.workspaceId)
        && Objects.equals(tenantId, that.tenantId)
        && Objects.equals(clientId, that.clientId)
        && Objects.equals(clientSecret, that.clientSecret)
        && Objects.equals(defaultTimespan, that.defaultTimespan)
        && Objects.equals(tables, that.tables)
        && Objects.equals(credentialsProvider, that.credentialsProvider)
        && authMode == that.authMode;
  }

  @Override
  public int hashCode() {
    return Objects.hash(workspaceId, tenantId, clientId, clientSecret, defaultTimespan, maxRows, tables, credentialsProvider, authMode);
  }

  @Override
  public String toString() {
    return "SentinelStoragePluginConfig{" +
        "workspaceId='" + workspaceId + '\'' +
        ", tenantId='" + tenantId + '\'' +
        ", clientId='" + clientId + '\'' +
        ", defaultTimespan='" + defaultTimespan + '\'' +
        ", maxRows=" + maxRows +
        ", tables=" + tables +
        ", authMode=" + authMode +
        '}';
  }

  @Override
  public StoragePluginConfig updateCredentialProvider(CredentialsProvider credentialsProvider) {
    return new SentinelStoragePluginConfig(this, credentialsProvider);
  }
}
