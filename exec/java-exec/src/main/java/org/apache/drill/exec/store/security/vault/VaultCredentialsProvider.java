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
package org.apache.drill.exec.store.security.vault;

import com.bettercloud.vault.Vault;
import com.bettercloud.vault.VaultConfig;
import com.bettercloud.vault.VaultException;
import com.bettercloud.vault.response.AuthResponse;
import com.bettercloud.vault.response.LogicalResponse;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.OptBoolean;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.logical.security.CredentialsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Implementation of {@link CredentialsProvider} that obtains credential values from
 * {@link Vault}.
 */
public class VaultCredentialsProvider implements CredentialsProvider {

  private static final Logger logger = LoggerFactory.getLogger(VaultCredentialsProvider.class);
  // Drill boot options used to configure a Vault credentials provider
  public static final String VAULT_ADDRESS = "drill.exec.storage.vault.address";
  public static final String VAULT_APP_ROLE_ID = "drill.exec.storage.vault.app_role_id";
  public static final String VAULT_SECRET_ID = "drill.exec.storage.vault.secret_id";
  public static final String QUERY_USER_VAR = "$user";

  private final String secretPath, appRoleId, secretId;
  private final Map<String, String> propertyNames;
  private final VaultConfig vaultConfig;
  private Vault vault;

  /**
   * @param secretPath The Vault key value from which to read
   * @param propertyNames map with credential names as keys and vault keys as values.
   * @param config drill config
   * @throws VaultException if exception happens when connecting to Vault.
   */
  @JsonCreator
  public VaultCredentialsProvider(
      @JsonProperty("secretPath") String secretPath,
      @JsonProperty("propertyNames") Map<String, String> propertyNames,
      @JacksonInject(useInput = OptBoolean.FALSE) DrillConfig config) throws VaultException {

    this.propertyNames = propertyNames;
    this.secretPath = secretPath;
    this.appRoleId = Objects.requireNonNull(
      config.getString(VAULT_APP_ROLE_ID),
      String.format(
        "Vault app role id is not specified. Please set [%s] config property.",
        VAULT_APP_ROLE_ID
      )
    );
    this.secretId = Objects.requireNonNull(
      config.getString(VAULT_SECRET_ID),
      String.format(
        "Vault secret id is not specified. Please set [%s] config property.",
        VAULT_SECRET_ID
      )
    );
    String vaultAddress = Objects.requireNonNull(
      config.getString(VAULT_ADDRESS),
      String.format(
        "Vault address is not specified. Please set [%s] config property.",
        VAULT_ADDRESS
      )
    );

    this.vaultConfig = new VaultConfig()
        .address(vaultAddress)
        .build();
    // Initial unauthenticated Vault client, needed for the first auth() call.
    this.vault = new Vault(vaultConfig);
  }

  private Map<String, String> extractCredentials(Map<String, String> vaultSecrets) {
    Map<String, String> credentials = new HashMap<>();
    for (Map.Entry<String, String> entry : propertyNames.entrySet()) {
      String cred = vaultSecrets.get(entry.getValue());
      if (cred != null) {
        credentials.put(entry.getKey(), vaultSecrets.get(entry.getValue()));
      }
    }
    return credentials;
  }

  private Map<String, String> getCredentialsAt(String path) {
    LogicalResponse resp;
    // Obtain this thread's own reference to the current Vault object to use
    // for deciding whether _we_ need to reauthenticate in the event of an
    // unauthorised read, or another thread has done that already.
    Vault threadVault = this.vault;

    try {
      logger.debug("Attempting to fetch secrets from Vault path {}.", path);
      resp = threadVault.logical().read(path);

      if (resp.getRestResponse().getStatus() == 403) {
        logger.info("Attempt to fetch secrets received HTTP 403 from Vault.");
        synchronized (this) {
          if (threadVault == vault) {
            // The Vault object has not already been replaced by another thread,
            // reauthenticate and replace it.
            logger.info("Attempting to reauthenticate.");
            AuthResponse authResp = vault.auth().loginByAppRole(appRoleId, secretId);
            vault = new Vault(vaultConfig.token(authResp.getAuthClientToken()));
          } else {
            logger.debug("Another caller has already attempted reauthentication.");
          }
        }
        logger.debug("Reattempting to fetch secrets from Vault path {}", path);
        resp = vault.logical().read(path);
      }
      return extractCredentials(resp.getData());

    } catch (VaultException ex) {
      throw UserException.systemError(ex)
        .message("Error while fetching credentials from vault")
        .build(logger);
    }
  }

  @Override
  public Map<String, String> getCredentials() {
    Map<String, String> creds = getCredentialsAt(secretPath);
    if (creds.isEmpty()) {
      logger.warn(
        "No credentials matching the configured property names were readable at {}",
        secretPath
      );
    }
    return creds;
  }

  @Override
  public Map<String, String> getUserCredentials(String queryUser) {
    // Resolve a Vault path that may contain the $user var, e.g. /org/dept/$user -> /org/dept/alice
    String resolvedPath = secretPath.replace(QUERY_USER_VAR, queryUser);
    Map<String, String> creds = getCredentialsAt(resolvedPath);
    if (creds.isEmpty()) {
      logger.warn(
        "No credentials for {} matching the configured property names were readable at {}",
        queryUser,
        resolvedPath
      );
    }
    return creds;
  }

  public String getSecretPath() {
    return secretPath;
  }

  public Map<String, String> getPropertyNames() {
    return propertyNames;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    VaultCredentialsProvider that = (VaultCredentialsProvider) o;
    return Objects.equals(secretPath, that.secretPath) && Objects.equals(propertyNames, that.propertyNames);
  }

  @Override
  public int hashCode() {
    return Objects.hash(secretPath, propertyNames);
  }
}
