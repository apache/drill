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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.apache.drill.common.logical.StoragePluginConfig.AuthMode;
import org.apache.drill.test.BaseTest;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Unit tests for Kerberos-specific configuration in AccumuloStoragePluginConfig.
 */
public class AccumuloKerberosConfigTest extends BaseTest {

  @Test
  public void testAuthTypeEnum() {
    assertEquals(AccumuloAuthType.PASSWORD, AccumuloAuthType.parseOrDefault(null, AccumuloAuthType.PASSWORD));
    assertEquals(AccumuloAuthType.PASSWORD, AccumuloAuthType.parseOrDefault("", AccumuloAuthType.PASSWORD));
    assertEquals(AccumuloAuthType.PASSWORD, AccumuloAuthType.parseOrDefault("PASSWORD", AccumuloAuthType.KERBEROS));
    assertEquals(AccumuloAuthType.KERBEROS, AccumuloAuthType.parseOrDefault("KERBEROS", AccumuloAuthType.PASSWORD));
    assertEquals(AccumuloAuthType.KERBEROS, AccumuloAuthType.parseOrDefault("kerberos", AccumuloAuthType.PASSWORD));
    assertEquals(AccumuloAuthType.KERBEROS, AccumuloAuthType.parseOrDefault("Kerberos", AccumuloAuthType.PASSWORD));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testAuthTypeEnumInvalidValue() {
    AccumuloAuthType.parseOrDefault("INVALID", AccumuloAuthType.PASSWORD);
  }

  @Test
  public void testKerberosSharedUserConfig() {
    AccumuloStoragePluginConfig config = new AccumuloStoragePluginConfig(
        "zk:2181",
        "accumulo",
        null,
        null,
        "KERBEROS",
        "drill/host@REALM",
        "/etc/security/keytabs/drill.keytab",
        "auth",
        "accumulo",
        false,
        "SHARED_USER",
        null,
        null,
        null,
        null
    );

    assertTrue(config.isKerberosEnabled());
    assertFalse(config.isUserImpersonationEnabled());
    assertFalse(config.isUseDelegationTokens());
    assertEquals(AuthMode.SHARED_USER, config.getAuthMode());
  }

  @Test
  public void testKerberosUserImpersonationConfig() {
    AccumuloStoragePluginConfig config = new AccumuloStoragePluginConfig(
        "zk:2181",
        "accumulo",
        null,
        null,
        "KERBEROS",
        "drill/host@REALM",
        "/etc/security/keytabs/drill.keytab",
        "auth-conf",
        "accumulo",
        true,
        "USER_IMPERSONATION",
        null,
        null,
        null,
        null
    );

    assertTrue(config.isKerberosEnabled());
    assertTrue(config.isUserImpersonationEnabled());
    assertTrue(config.isUseDelegationTokens());
    assertEquals(AuthMode.USER_IMPERSONATION, config.getAuthMode());
    assertEquals("auth-conf", config.getSaslQop());
  }

  @Test
  public void testSaslQopValues() {
    // Test auth
    AccumuloStoragePluginConfig authConfig = new AccumuloStoragePluginConfig(
        "zk:2181", "accumulo", null, null,
        "KERBEROS", "drill@REALM", "/keytab", "auth", null, null, null, null, null, null, null
    );
    assertEquals("auth", authConfig.getSaslQop());

    // Test auth-int
    AccumuloStoragePluginConfig authIntConfig = new AccumuloStoragePluginConfig(
        "zk:2181", "accumulo", null, null,
        "KERBEROS", "drill@REALM", "/keytab", "auth-int", null, null, null, null, null, null, null
    );
    assertEquals("auth-int", authIntConfig.getSaslQop());

    // Test auth-conf
    AccumuloStoragePluginConfig authConfConfig = new AccumuloStoragePluginConfig(
        "zk:2181", "accumulo", null, null,
        "KERBEROS", "drill@REALM", "/keytab", "auth-conf", null, null, null, null, null, null, null
    );
    assertEquals("auth-conf", authConfConfig.getSaslQop());
  }

  @Test
  public void testBackwardCompatibilityPasswordAuth() {
    // Old-style configuration without any Kerberos fields should still work
    AccumuloStoragePluginConfig config = new AccumuloStoragePluginConfig(
        "localhost:2181",
        "accumulo",
        "root",
        "secret"
    );

    assertEquals(AccumuloAuthType.PASSWORD, config.getAuthenticationType());
    assertFalse(config.isKerberosEnabled());
    assertFalse(config.isUserImpersonationEnabled());
    assertEquals(AuthMode.SHARED_USER, config.getAuthMode());
    assertEquals("root", config.getUsername());
    assertEquals("secret", config.getPassword());
  }

  @Test
  public void testJsonSerializationFullKerberosConfig() throws Exception {
    ObjectMapper mapper = new ObjectMapper();

    AccumuloStoragePluginConfig config = new AccumuloStoragePluginConfig(
        "zk1:2181,zk2:2181",
        "accumulo_prod",
        null,
        null,
        "KERBEROS",
        "drill/drillserver.example.com@EXAMPLE.COM",
        "/etc/security/keytabs/drill.service.keytab",
        "auth-conf",
        "accumulo",
        true,
        "USER_IMPERSONATION",
        null,
        "_drill_schema",
        30000,
        10
    );

    String json = mapper.writeValueAsString(config);
    assertNotNull(json);

    // Verify JSON contains expected fields
    assertTrue(json.contains("zk1:2181,zk2:2181"));
    assertTrue(json.contains("accumulo_prod"));
    assertTrue(json.contains("KERBEROS"));
    assertTrue(json.contains("drill/drillserver.example.com@EXAMPLE.COM"));
    assertTrue(json.contains("auth-conf"));
    assertTrue(json.contains("\"useDelegationTokens\":true"));

    // Deserialize and verify
    AccumuloStoragePluginConfig deserialized = mapper.readValue(json, AccumuloStoragePluginConfig.class);
    assertEquals(config.getZookeeperQuorum(), deserialized.getZookeeperQuorum());
    assertEquals(config.getInstanceName(), deserialized.getInstanceName());
    assertEquals(config.getAuthenticationType(), deserialized.getAuthenticationType());
    assertEquals(config.getPrincipal(), deserialized.getPrincipal());
    assertEquals(config.getKeytabPath(), deserialized.getKeytabPath());
    assertEquals(config.getSaslQop(), deserialized.getSaslQop());
    assertEquals(config.getAccumuloServicePrimary(), deserialized.getAccumuloServicePrimary());
    assertEquals(config.isUseDelegationTokens(), deserialized.isUseDelegationTokens());
  }

  @Test
  public void testMixedAuthConfigInvalid() {
    // Config with both password creds and Kerberos settings
    // This should be allowed for migration scenarios
    AccumuloStoragePluginConfig config = new AccumuloStoragePluginConfig(
        "zk:2181",
        "accumulo",
        "fallback_user",  // password username
        "fallback_pass",  // password
        "KERBEROS",       // but auth type is Kerberos
        "drill@REALM",
        "/keytab",
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null
    );

    // When KERBEROS auth type is set, it should use Kerberos
    assertEquals(AccumuloAuthType.KERBEROS, config.getAuthenticationType());
    assertTrue(config.isKerberosEnabled());
    // But password fields are still accessible if needed
    assertEquals("fallback_user", config.getUsername());
  }

  @Test
  public void testUserTranslationConfig() {
    AccumuloStoragePluginConfig config = new AccumuloStoragePluginConfig(
        "zk:2181",
        "accumulo",
        "service_user",  // service account for fallback
        "service_pass",
        "PASSWORD",
        null,
        null,
        null,
        null,
        false,
        "USER_TRANSLATION",
        null,
        null,
        null,
        null
    );

    assertFalse(config.isKerberosEnabled());
    assertFalse(config.isUserImpersonationEnabled());
    assertTrue(config.isUserTranslationEnabled());
    assertEquals(AuthMode.USER_TRANSLATION, config.getAuthMode());
  }

  @Test
  public void testPrincipalFormats() {
    // Test simple principal (just user@REALM)
    AccumuloStoragePluginConfig simpleConfig = new AccumuloStoragePluginConfig(
        "zk:2181", "accumulo", null, null,
        "KERBEROS", "drill@EXAMPLE.COM", "/keytab", null, null, null, null, null, null, null, null
    );
    assertEquals("drill@EXAMPLE.COM", simpleConfig.getPrincipal());

    // Test service principal (primary/instance@REALM)
    AccumuloStoragePluginConfig serviceConfig = new AccumuloStoragePluginConfig(
        "zk:2181", "accumulo", null, null,
        "KERBEROS", "drill/drillserver.example.com@EXAMPLE.COM", "/keytab",
        null, null, null, null, null, null, null, null
    );
    assertEquals("drill/drillserver.example.com@EXAMPLE.COM", serviceConfig.getPrincipal());
  }
}
