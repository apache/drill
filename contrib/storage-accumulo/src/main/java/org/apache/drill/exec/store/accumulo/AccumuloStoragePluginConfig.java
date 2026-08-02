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
package org.apache.drill.exec.store.accumulo;

import java.util.Objects;
import java.util.Optional;

import org.apache.drill.common.PlanStringBuilder;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.common.logical.security.CredentialsProvider;
import org.apache.drill.exec.store.security.CredentialProviderUtils;
import org.apache.drill.exec.proto.UserBitShared.UserCredentials;
import org.apache.drill.exec.store.security.UsernamePasswordCredentials;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * Configuration for the Accumulo storage plugin.
 *
 * <p>This configuration supports connecting to an Accumulo cluster via ZooKeeper
 * and includes settings for authentication, Kerberos, user impersonation,
 * and optional schema metadata table.</p>
 *
 * <h3>Password Authentication Example:</h3>
 * <pre>
 * {
 *   "type": "accumulo",
 *   "zookeeperQuorum": "localhost:2181",
 *   "instanceName": "accumulo",
 *   "username": "root",
 *   "password": "secret",
 *   "enabled": true
 * }
 * </pre>
 *
 * <h3>Kerberos Authentication Example:</h3>
 * <pre>
 * {
 *   "type": "accumulo",
 *   "zookeeperQuorum": "zk1:2181,zk2:2181",
 *   "instanceName": "accumulo",
 *   "authenticationType": "KERBEROS",
 *   "principal": "drill/hostname@REALM",
 *   "keytabPath": "/etc/security/keytabs/drill.keytab",
 *   "saslQop": "auth",
 *   "useDelegationTokens": true,
 *   "authMode": "USER_IMPERSONATION",
 *   "enabled": true
 * }
 * </pre>
 */
@JsonTypeName(AccumuloStoragePluginConfig.NAME)
public class AccumuloStoragePluginConfig extends StoragePluginConfig {

  public static final String NAME = "accumulo";

  /**
   * Default SASL QoP (Quality of Protection) value.
   */
  public static final String DEFAULT_SASL_QOP = "auth";

  /**
   * Default Accumulo service name for SASL authentication.
   */
  public static final String DEFAULT_ACCUMULO_SERVICE_PRIMARY = "accumulo";

  // ===== Connection settings =====

  /**
   * Comma-separated list of ZooKeeper servers (host:port format).
   * Example: "zk1:2181,zk2:2181,zk3:2181"
   */
  private final String zookeeperQuorum;

  /**
   * The Accumulo instance name.
   */
  private final String instanceName;

  // ===== Password authentication settings (for backward compatibility) =====

  /**
   * The username for password authentication.
   * Deprecated: prefer using credentialsProvider.
   */
  private final String username;

  /**
   * The password for password authentication.
   * Deprecated: prefer using credentialsProvider.
   */
  private final String password;

  // ===== Kerberos authentication settings =====

  /**
   * The authentication type: PASSWORD or KERBEROS.
   */
  private final AccumuloAuthType authenticationType;

  /**
   * Kerberos principal for service authentication.
   * Format: primary/instance@REALM (e.g., "drill/hostname@EXAMPLE.COM")
   */
  private final String principal;

  /**
   * Path to the Kerberos keytab file.
   */
  private final String keytabPath;

  /**
   * SASL Quality of Protection: "auth", "auth-int", or "auth-conf".
   * - auth: authentication only
   * - auth-int: authentication + integrity protection
   * - auth-conf: authentication + integrity + confidentiality (encryption)
   */
  private final String saslQop;

  /**
   * Accumulo service primary name for SASL authentication.
   * Default is "accumulo".
   */
  private final String accumuloServicePrimary;

  /**
   * Whether to use delegation tokens for distributed execution.
   * When true, the service will obtain delegation tokens for query users.
   */
  private final boolean useDelegationTokens;

  // ===== Optional settings =====

  /**
   * Optional name of the schema metadata table.
   * If set, the plugin will look for table schema definitions in this Accumulo table.
   * Default is "_drill_schema".
   */
  private final String schemaMetadataTable;

  /**
   * Timeout in milliseconds for Accumulo client operations.
   * Default is 30000 (30 seconds).
   */
  private final Integer clientTimeout;

  /**
   * Number of threads for BatchScanner operations.
   * Default is 10.
   */
  private final Integer batchScannerThreads;

  @JsonCreator
  public AccumuloStoragePluginConfig(
      @JsonProperty("zookeeperQuorum") String zookeeperQuorum,
      @JsonProperty("instanceName") String instanceName,
      @JsonProperty("username") String username,
      @JsonProperty("password") String password,
      @JsonProperty("authenticationType") String authenticationType,
      @JsonProperty("principal") String principal,
      @JsonProperty("keytabPath") String keytabPath,
      @JsonProperty("saslQop") String saslQop,
      @JsonProperty("accumuloServicePrimary") String accumuloServicePrimary,
      @JsonProperty("useDelegationTokens") Boolean useDelegationTokens,
      @JsonProperty("authMode") String authMode,
      @JsonProperty("credentialsProvider") CredentialsProvider credentialsProvider,
      @JsonProperty("schemaMetadataTable") String schemaMetadataTable,
      @JsonProperty("clientTimeout") Integer clientTimeout,
      @JsonProperty("batchScannerThreads") Integer batchScannerThreads) {

    super(
        CredentialProviderUtils.getCredentialsProvider(username, password, credentialsProvider),
        credentialsProvider == null,
        AuthMode.parseOrDefault(authMode, AuthMode.SHARED_USER)
    );

    this.zookeeperQuorum = zookeeperQuorum;
    this.instanceName = instanceName;
    this.username = username;
    this.password = password;

    this.authenticationType = AccumuloAuthType.parseOrDefault(authenticationType, AccumuloAuthType.PASSWORD);
    this.principal = principal;
    this.keytabPath = keytabPath;
    this.saslQop = saslQop != null ? saslQop : DEFAULT_SASL_QOP;
    this.accumuloServicePrimary = accumuloServicePrimary != null ? accumuloServicePrimary : DEFAULT_ACCUMULO_SERVICE_PRIMARY;
    this.useDelegationTokens = useDelegationTokens != null ? useDelegationTokens : false;

    this.schemaMetadataTable = schemaMetadataTable != null ? schemaMetadataTable : "_drill_schema";
    this.clientTimeout = clientTimeout != null ? clientTimeout : 30000;
    this.batchScannerThreads = batchScannerThreads != null ? batchScannerThreads : 10;
  }

  /**
   * Simplified constructor for password authentication (backward compatible).
   */
  public AccumuloStoragePluginConfig(
      String zookeeperQuorum,
      String instanceName,
      String username,
      String password) {
    this(zookeeperQuorum, instanceName, username, password,
        null, null, null, null, null, null, null, null, null, null, null);
  }

  // ===== Connection Getters =====

  @JsonProperty("zookeeperQuorum")
  public String getZookeeperQuorum() {
    return zookeeperQuorum;
  }

  @JsonProperty("instanceName")
  public String getInstanceName() {
    return instanceName;
  }

  // ===== Password Auth Getters =====

  @JsonProperty("username")
  public String getUsername() {
    return username;
  }

  @JsonProperty("password")
  public String getPassword() {
    return password;
  }

  // ===== Kerberos Auth Getters =====

  @JsonProperty("authenticationType")
  public AccumuloAuthType getAuthenticationType() {
    return authenticationType;
  }

  @JsonProperty("principal")
  public String getPrincipal() {
    return principal;
  }

  @JsonProperty("keytabPath")
  public String getKeytabPath() {
    return keytabPath;
  }

  @JsonProperty("saslQop")
  public String getSaslQop() {
    return saslQop;
  }

  @JsonProperty("accumuloServicePrimary")
  public String getAccumuloServicePrimary() {
    return accumuloServicePrimary;
  }

  @JsonProperty("useDelegationTokens")
  public boolean isUseDelegationTokens() {
    return useDelegationTokens;
  }

  // ===== Optional Settings Getters =====

  @JsonProperty("schemaMetadataTable")
  public String getSchemaMetadataTable() {
    return schemaMetadataTable;
  }

  @JsonProperty("clientTimeout")
  public Integer getClientTimeout() {
    return clientTimeout;
  }

  @JsonProperty("batchScannerThreads")
  public Integer getBatchScannerThreads() {
    return batchScannerThreads;
  }

  // ===== Credential Helper Methods =====

  /**
   * Returns username/password credentials for the specified user context.
   *
   * <p>For SHARED_USER mode, returns the configured credentials.
   * For USER_TRANSLATION mode, returns per-user credentials from the provider.</p>
   *
   * @param userCredentials the query user credentials (may be null for SHARED_USER)
   * @return Optional containing credentials if available
   */
  @JsonIgnore
  public Optional<UsernamePasswordCredentials> getUsernamePasswordCredentials(
      UserCredentials userCredentials) {

    switch (authMode) {
      case SHARED_USER:
        return new UsernamePasswordCredentials.Builder()
            .setCredentialsProvider(credentialsProvider)
            .build();

      case USER_TRANSLATION:
        if (userCredentials == null) {
          return Optional.empty();
        }
        return new UsernamePasswordCredentials.Builder()
            .setCredentialsProvider(credentialsProvider)
            .setQueryUser(userCredentials.getUserName())
            .build();

      case USER_IMPERSONATION:
        // For impersonation, service credentials are used for initial auth
        return new UsernamePasswordCredentials.Builder()
            .setCredentialsProvider(credentialsProvider)
            .build();

      default:
        return Optional.empty();
    }
  }

  /**
   * Returns whether user impersonation is enabled.
   */
  @JsonIgnore
  public boolean isUserImpersonationEnabled() {
    return authMode == AuthMode.USER_IMPERSONATION;
  }

  /**
   * Returns whether user translation is enabled.
   */
  @JsonIgnore
  public boolean isUserTranslationEnabled() {
    return authMode == AuthMode.USER_TRANSLATION;
  }

  /**
   * Returns whether Kerberos authentication is configured.
   */
  @JsonIgnore
  public boolean isKerberosEnabled() {
    return authenticationType == AccumuloAuthType.KERBEROS;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AccumuloStoragePluginConfig that = (AccumuloStoragePluginConfig) o;
    return useDelegationTokens == that.useDelegationTokens
        && Objects.equals(zookeeperQuorum, that.zookeeperQuorum)
        && Objects.equals(instanceName, that.instanceName)
        && Objects.equals(username, that.username)
        && Objects.equals(password, that.password)
        && authenticationType == that.authenticationType
        && Objects.equals(principal, that.principal)
        && Objects.equals(keytabPath, that.keytabPath)
        && Objects.equals(saslQop, that.saslQop)
        && Objects.equals(accumuloServicePrimary, that.accumuloServicePrimary)
        && Objects.equals(schemaMetadataTable, that.schemaMetadataTable)
        && Objects.equals(clientTimeout, that.clientTimeout)
        && Objects.equals(batchScannerThreads, that.batchScannerThreads)
        && Objects.equals(authMode, that.authMode);
  }

  @Override
  public int hashCode() {
    return Objects.hash(zookeeperQuorum, instanceName, username, password,
        authenticationType, principal, keytabPath, saslQop, accumuloServicePrimary,
        useDelegationTokens, schemaMetadataTable, clientTimeout, batchScannerThreads,
        authMode);
  }

  @Override
  public String toString() {
    return new PlanStringBuilder(this)
        .field("zookeeperQuorum", zookeeperQuorum)
        .field("instanceName", instanceName)
        .field("authenticationType", authenticationType)
        .field("authMode", authMode)
        .field("username", username)
        .maskedField("password", password)
        .field("principal", principal)
        .maskedField("keytabPath", keytabPath)
        .field("saslQop", saslQop)
        .field("accumuloServicePrimary", accumuloServicePrimary)
        .field("useDelegationTokens", useDelegationTokens)
        .field("schemaMetadataTable", schemaMetadataTable)
        .field("clientTimeout", clientTimeout)
        .field("batchScannerThreads", batchScannerThreads)
        .toString();
  }
}
