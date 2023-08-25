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
package org.apache.drill.storage;

import com.bettercloud.vault.Vault;
import com.bettercloud.vault.VaultConfig;
import com.bettercloud.vault.VaultException;
import com.bettercloud.vault.response.LogicalResponse;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.logical.security.CredentialsProvider;
import org.apache.drill.exec.store.security.EnvCredentialsProvider;
import org.apache.drill.exec.store.security.HadoopCredentialsProvider;
import org.apache.drill.exec.store.security.UsernamePasswordCredentials;
import org.apache.drill.exec.store.security.vault.VaultCredentialsProvider;
import com.google.common.collect.ImmutableMap;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.apache.hadoop.conf.Configuration;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.BindMode;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.vault.VaultContainer;

import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class CredentialsProviderImplementationsTest extends ClusterTest {

  private static final String VAULT_ROOT_TOKEN = "vault-token";
  private static final String VAULT_APP_ROLE_PATH = "auth/approle/role/drill-role";
  private static final String SHARED_SECRET_PATH = "secret/testing";
  private static final String USER_SECRET_PATH = "secret/testing/$user";
  private static final String CONTAINER_POLICY_PATH = "/tmp/read-vault-secrets.hcl";

  @ClassRule
  public static final VaultContainer<?> vaultContainer =
    new VaultContainer<>(DockerImageName.parse("vault").withTag("1.10.3"))
      .withVaultToken(VAULT_ROOT_TOKEN)
      .withSecretInVault(SHARED_SECRET_PATH,
          "top_secret=password1",
          "db_password=dbpassword1")
      .withSecretInVault(USER_SECRET_PATH.replace(VaultCredentialsProvider.QUERY_USER_VAR, "alice"),
          "top_secret=password1",
          "db_password=dbpassword1")
      .withClasspathResourceMapping("vault/read-vault-secrets.hcl", CONTAINER_POLICY_PATH, BindMode.READ_ONLY)
      .withInitCommand(
        "auth enable approle",
        String.format("policy write read-secrets %s", CONTAINER_POLICY_PATH),
        String.format("write %s policies=read-secrets", VAULT_APP_ROLE_PATH)
      );

  @BeforeClass
  public static void init() throws Exception {
    String vaultAddr = String.format(
      "http://%s:%d",
      vaultContainer.getHost(),
      vaultContainer.getFirstMappedPort()
    );

    VaultConfig vaultConfig = new VaultConfig()
      .address(vaultAddr)
      .token(VAULT_ROOT_TOKEN)
      .build();

    // While other Vault paths in the test container seem to work fine with KV engine v2,
    // the AppRole paths produced 404s and forced the specification of version 1 in the
    // BetterCloud client used to perform AppRole operations.
    Vault vault = new Vault(vaultConfig, 1);

    LogicalResponse resp = vault.logical()
      .read(String.format("%s/role-id", VAULT_APP_ROLE_PATH));
    String appRoleId = resp.getData().get("role_id");

    resp = vault.logical().write(
      String.format("%s/secret-id", VAULT_APP_ROLE_PATH),
      Collections.emptyMap()
    );
    String secretId = resp.getData().get("secret_id");

    startCluster(ClusterFixture.builder(dirTestWatcher)
      .configProperty(VaultCredentialsProvider.VAULT_ADDRESS, vaultAddr)
      .configProperty(VaultCredentialsProvider.VAULT_APP_ROLE_ID, appRoleId)
      .configProperty(VaultCredentialsProvider.VAULT_SECRET_ID, secretId)
    );
  }

  @Test
  public void testEnvCredentialsProvider() {
    String variableName = "USER";
    String expectedValue = System.getenv(variableName);

    CredentialsProvider envCredentialsProvider = new EnvCredentialsProvider(ImmutableMap.of(
        UsernamePasswordCredentials.USERNAME, variableName));

    Map<String, String> actualCredentials = envCredentialsProvider.getCredentials();

    assertEquals(Collections.singletonMap(UsernamePasswordCredentials.USERNAME, expectedValue),
        actualCredentials);
  }

  @Test
  public void testHadoopCredentialsProvider() {
    Configuration configuration = new Configuration();
    String expectedUsernameValue = "user1";
    String expectedPassValue = "pass123!@#";
    String usernamePropertyName = "username_key";
    String passwordPropertyName = "password_key";
    configuration.set(usernamePropertyName, expectedUsernameValue);
    configuration.set(passwordPropertyName, expectedPassValue);

    CredentialsProvider envCredentialsProvider = new HadoopCredentialsProvider(configuration,
        ImmutableMap.of(
            UsernamePasswordCredentials.USERNAME, usernamePropertyName,
            UsernamePasswordCredentials.PASSWORD, passwordPropertyName));

    Map<String, String> actualCredentials = envCredentialsProvider.getCredentials();

    assertEquals(ImmutableMap.of(
        UsernamePasswordCredentials.USERNAME, expectedUsernameValue,
        UsernamePasswordCredentials.PASSWORD, expectedPassValue),
        actualCredentials);
  }

  @Test
  public void testVaultCredentialsProvider() throws VaultException {
    DrillConfig config = cluster.drillbit().getContext().getConfig();

    CredentialsProvider vaultCredsProvider = new VaultCredentialsProvider(
        SHARED_SECRET_PATH,
        ImmutableMap.of(UsernamePasswordCredentials.USERNAME, "top_secret",
            UsernamePasswordCredentials.PASSWORD, "db_password"),
        config);

    Map<String, String> actualCredentials = vaultCredsProvider.getCredentials();

    assertEquals(ImmutableMap.of(UsernamePasswordCredentials.USERNAME, "password1",
        UsernamePasswordCredentials.PASSWORD, "dbpassword1"),
        actualCredentials);
  }

  @Test
  public void testVaultUserCredentialsPresent() throws VaultException {
    DrillConfig config = cluster.drillbit().getContext().getConfig();

    CredentialsProvider vaultCredsProvider = new VaultCredentialsProvider(
        USER_SECRET_PATH,
        ImmutableMap.of(UsernamePasswordCredentials.USERNAME, "top_secret",
            UsernamePasswordCredentials.PASSWORD, "db_password"),
        config);

    Map<String, String> actualCredentials = vaultCredsProvider.getUserCredentials("alice");

    assertEquals(
      ImmutableMap.of(
        UsernamePasswordCredentials.USERNAME, "password1",
        UsernamePasswordCredentials.PASSWORD, "dbpassword1"
      ),
      actualCredentials
    );
  }

  @Test
  public void testVaultUserCredentialsAbsent() throws VaultException {
    DrillConfig config = cluster.drillbit().getContext().getConfig();

    CredentialsProvider vaultCredsProvider = new VaultCredentialsProvider(
        USER_SECRET_PATH,
        ImmutableMap.of(UsernamePasswordCredentials.USERNAME, "top_secret",
            UsernamePasswordCredentials.PASSWORD, "db_password"),
        config);

    Map<String, String> actualCredentials = vaultCredsProvider.getUserCredentials("bob");

    assertEquals(Collections.<String, String>emptyMap(), actualCredentials);
  }
}
