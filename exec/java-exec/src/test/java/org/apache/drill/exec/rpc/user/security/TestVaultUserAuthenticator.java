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

import org.apache.drill.common.config.DrillProperties;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.test.ClientFixture;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.vault.VaultContainer;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.fail;

public class TestVaultUserAuthenticator extends ClusterTest {

  private static final String VAULT_TOKEN_VALUE = "vault-token";

  @ClassRule
  public static final VaultContainer<?> vaultContainer =
      new VaultContainer<>(DockerImageName.parse("vault").withTag("1.1.3"))
          .withVaultToken(VAULT_TOKEN_VALUE)
          .withVaultPort(8200)
          .withSecretInVault(
            null,
            "alice=pass1",
            "bob=buzzkill"
          );

  @BeforeClass
  public static void init() throws Exception {
    String vaultAddr = String.format(
      "http://%s:%d",
      vaultContainer.getHost(),
      vaultContainer.getMappedPort(8200)
    );

    ClusterFixtureBuilder cfb = ClusterFixture.builder(dirTestWatcher)
      .configProperty(ExecConstants.ALLOW_LOOPBACK_ADDRESS_BINDING, true)
      .configProperty(ExecConstants.USER_AUTHENTICATION_ENABLED, true)
      .configProperty(ExecConstants.USER_AUTHENTICATOR_IMPL, "vault")
      .configProperty(VaultUserAuthenticator.VAULT_ADDRESS, vaultAddr)
      .configProperty(VaultUserAuthenticator.VAULT_TOKEN, VAULT_TOKEN_VALUE)
      .configProperty(
        VaultUserAuthenticator.VAULT_AUTH_METHOD,
        VaultUserAuthenticator.VaultAuthMethod.USER_PASS
      );

    startCluster(cfb);
  }

  @Test
  public void passwordChecksGiveCorrectResults() throws Exception {
    tryCredentials("alice", "pass1", cluster, true);
    tryCredentials("bob", "buzzkill", cluster, true);
    tryCredentials("notalice", "pass1", cluster, false);
    tryCredentials("notbob", "buzzkill", cluster, false);
    tryCredentials("alice", "wrong", cluster, false);
    tryCredentials("bob", "incorrect", cluster, false);
  }

  private static void tryCredentials(String user, String password, ClusterFixture cluster, boolean shouldSucceed) throws Exception {
    try {
      ClientFixture client = cluster.clientBuilder()
        .property(DrillProperties.USER, user)
        .property(DrillProperties.PASSWORD, password)
        .build();

      // Run few queries using the new client
      List<String> queries = Arrays.asList(
        "SHOW SCHEMAS",
        "USE INFORMATION_SCHEMA",
        "SHOW TABLES",
        "SELECT * FROM INFORMATION_SCHEMA.`TABLES` WHERE TABLE_NAME LIKE 'COLUMNS'",
        "SELECT * FROM cp.`region.json` LIMIT 5");

      for (String query : queries) {
        client.queryBuilder().sql(query).run();
      }

      if (!shouldSucceed) {
        fail("Expected connect to fail because of incorrect username / password combination, but it succeeded");
      }
    } catch (IllegalStateException e) {
      if (shouldSucceed) {
        throw e;
      }
    }
  }

}
