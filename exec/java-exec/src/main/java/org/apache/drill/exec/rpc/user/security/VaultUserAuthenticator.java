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
package org.apache.drill.exec.rpc.user.security;

import com.bettercloud.vault.response.AuthResponse;
import com.bettercloud.vault.Vault;
import com.bettercloud.vault.VaultConfig;
import com.bettercloud.vault.VaultException;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.exception.DrillbitStartupException;

import java.io.IOException;
import java.util.Objects;

import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;

/**
 * Implement {@link org.apache.drill.exec.rpc.user.security.UserAuthenticator} based on Pluggable Authentication
 * Module (PAM) configuration. Configure the PAM profiles using "drill.exec.security.user.auth.pam_profiles" BOOT
 * option. Ex. value  <i>[ "login", "sudo" ]</i> (value is an array of strings).
 */
@Slf4j
@EqualsAndHashCode
@UserAuthenticatorTemplate(type = "vault")
public class VaultUserAuthenticator implements UserAuthenticator {

  // Drill boot options used to configure Vault auth.
  public static final String VAULT_ADDRESS = "drill.exec.security.user.auth.vault.address";
  public static final String VAULT_TOKEN = "drill.exec.security.user.auth.vault.token";
  public static final String VAULT_AUTH_METHOD = "drill.exec.security.user.auth.vault.method";

  // The subset of Vault auth methods that are supported by this authenticator
  public enum VaultAuthMethod {
    APP_ROLE,
    GCP,
    KUBERNETES,
    LDAP,
    USER_PASS
  }

  private Vault vault;

  private VaultAuthMethod authMethod;

  private String userPassMount;

  @Override
  public void setup(DrillConfig config) throws DrillbitStartupException {
    // Read config values
    String vaultAddress = Objects.requireNonNull(
      config.getString(VAULT_ADDRESS),
      String.format(
        "Vault address is not specified. Please set [%s] config property.",
        VAULT_ADDRESS
      )
    );

    String vaultToken = Objects.requireNonNull(
      config.getString(VAULT_TOKEN),
      String.format(
        "Vault token is not specified. Please set [%s] config property.",
        VAULT_TOKEN
      )
    );

    this.authMethod = VaultAuthMethod.valueOf(
      Objects.requireNonNull(
        config.getString(VAULT_AUTH_METHOD),
        String.format(
          "Vault auth method is not specified. Please set [%s] config property.",
          VAULT_AUTH_METHOD
        )
      )
    );

    // Initialise Vault client
    try {
      logger.debug(
        "tries to init a Vault client with Vault addr = {}, auth method = {}",
        vaultAddress,
        authMethod
      );

      VaultConfig vaultConfig = new VaultConfig()
          .address(vaultAddress)
          .token(vaultToken)
          .build();

      this.vault = new Vault(vaultConfig);
    } catch (VaultException e) {
      logger.error(String.join(System.lineSeparator(),
          "error initialising the Vault client library using configuration: ",
          "\tvaultAddress: {}",
          "\tvaultToken: {}",
          "\tauthMethod: {}"
        ),
        vaultAddress,
        vaultToken,
        authMethod,
        e
      );
      throw new DrillbitStartupException(
        "Error initialising the Vault client library: " + e.getMessage(),
        e
      );
    }
  }

  @Override
  public void authenticate(String user, String password) throws UserAuthenticationException {

    AuthResponse authResp;

    try {
      logger.debug("tries to authenticate user {} using {}", user, authMethod);

      switch (authMethod) {
        case APP_ROLE:
          authResp = vault.auth().loginByAppRole(user, password);
          break;
        case GCP:
          authResp = vault.auth().loginByGCP(user, password);
          break;
        case KUBERNETES:
          authResp = vault.auth().loginByKubernetes(user, password);
          break;
        case LDAP:
          authResp = vault.auth().loginByLDAP(user, password);
          break;
        case USER_PASS:
          authResp = vault.auth().loginByUserPass(user, password);
          break;
        default:
          throw new UserAuthenticationException(
            String.format(
              "The Vault authentication method '%s' is not supported",
              authMethod
            )
          );
        }
    } catch (VaultException e) {
      logger.warn("failed to authenticate user {} using {}: {}.", user, authMethod, e);
      throw new UserAuthenticationException(
        String.format(
          "Failed to authenticate user %s using %s: %s",
          user,
          authMethod,
          e.getMessage()
        )
      );
    }

    logger.info(
      "user {} authenticated against Vault successfully.",
      authResp.getUsername()
    );
  }

  @Override
  public void close() throws IOException {
    this.vault = null;
    logger.debug("has been closed.");
  }
}
