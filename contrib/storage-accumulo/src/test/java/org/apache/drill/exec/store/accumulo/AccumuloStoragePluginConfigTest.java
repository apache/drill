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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig.AuthMode;
import org.apache.drill.test.BaseTest;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Unit tests for AccumuloStoragePluginConfig.
 */
public class AccumuloStoragePluginConfigTest extends BaseTest {

  @Test
  public void testConfigCreation() {
    AccumuloStoragePluginConfig config = new AccumuloStoragePluginConfig(
        "localhost:2181",
        "accumulo",
        "root",
        "secret",
        null, null, null, null, null, null, null, null, null, null, null
    );

    assertEquals("localhost:2181", config.getZookeeperQuorum());
    assertEquals("accumulo", config.getInstanceName());
    assertEquals("root", config.getUsername());
    assertEquals("secret", config.getPassword());
    assertEquals("_drill_schema", config.getSchemaMetadataTable());
    assertEquals(Integer.valueOf(30000), config.getClientTimeout());
    assertEquals(Integer.valueOf(10), config.getBatchScannerThreads());
  }

  @Test
  public void testConfigWithCustomValues() {
    AccumuloStoragePluginConfig config = new AccumuloStoragePluginConfig(
        "zk1:2181,zk2:2181,zk3:2181",
        "myinstance",
        "admin",
        "password123",
        null, null, null, null, null, null, null, null,
        "my_schema_table",
        60000,
        20
    );

    assertEquals("zk1:2181,zk2:2181,zk3:2181", config.getZookeeperQuorum());
    assertEquals("myinstance", config.getInstanceName());
    assertEquals("admin", config.getUsername());
    assertEquals("password123", config.getPassword());
    assertEquals("my_schema_table", config.getSchemaMetadataTable());
    assertEquals(Integer.valueOf(60000), config.getClientTimeout());
    assertEquals(Integer.valueOf(20), config.getBatchScannerThreads());
  }

  @Test
  public void testSimplifiedConstructorBackwardCompatibility() {
    AccumuloStoragePluginConfig config = new AccumuloStoragePluginConfig(
        "localhost:2181",
        "accumulo",
        "root",
        "secret"
    );

    assertEquals("localhost:2181", config.getZookeeperQuorum());
    assertEquals("accumulo", config.getInstanceName());
    assertEquals("root", config.getUsername());
    assertEquals("secret", config.getPassword());
    assertEquals(AccumuloAuthType.PASSWORD, config.getAuthenticationType());
    assertEquals(AuthMode.SHARED_USER, config.getAuthMode());
    assertFalse(config.isUseDelegationTokens());
  }

  @Test
  public void testKerberosConfigCreation() {
    AccumuloStoragePluginConfig config = new AccumuloStoragePluginConfig(
        "zk1:2181,zk2:2181",
        "accumulo",
        null,  // no username for Kerberos
        null,  // no password for Kerberos
        "KERBEROS",
        "drill/hostname@REALM",
        "/etc/security/keytabs/drill.keytab",
        "auth-conf",
        "accumulo",
        true,  // useDelegationTokens
        "USER_IMPERSONATION",
        null,
        null,
        null,
        null
    );

    assertEquals("zk1:2181,zk2:2181", config.getZookeeperQuorum());
    assertEquals("accumulo", config.getInstanceName());
    assertEquals(AccumuloAuthType.KERBEROS, config.getAuthenticationType());
    assertEquals("drill/hostname@REALM", config.getPrincipal());
    assertEquals("/etc/security/keytabs/drill.keytab", config.getKeytabPath());
    assertEquals("auth-conf", config.getSaslQop());
    assertEquals("accumulo", config.getAccumuloServicePrimary());
    assertTrue(config.isUseDelegationTokens());
    assertEquals(AuthMode.USER_IMPERSONATION, config.getAuthMode());
    assertTrue(config.isKerberosEnabled());
    assertTrue(config.isUserImpersonationEnabled());
  }

  @Test
  public void testKerberosDefaults() {
    AccumuloStoragePluginConfig config = new AccumuloStoragePluginConfig(
        "localhost:2181",
        "accumulo",
        null,
        null,
        "KERBEROS",
        "drill/host@REALM",
        "/path/to/keytab",
        null,  // saslQop - should default to "auth"
        null,  // accumuloServicePrimary - should default to "accumulo"
        null,  // useDelegationTokens - should default to false
        null,
        null,
        null,
        null,
        null
    );

    assertEquals(AccumuloAuthType.KERBEROS, config.getAuthenticationType());
    assertEquals("auth", config.getSaslQop());  // default
    assertEquals("accumulo", config.getAccumuloServicePrimary());  // default
    assertFalse(config.isUseDelegationTokens());  // default
    assertEquals(AuthMode.SHARED_USER, config.getAuthMode());  // default
  }

  @Test
  public void testAuthTypeParsingCaseInsensitive() {
    // Test lowercase
    AccumuloStoragePluginConfig config1 = new AccumuloStoragePluginConfig(
        "localhost:2181", "accumulo", null, null,
        "kerberos", "p", "k", null, null, null, null, null, null, null, null
    );
    assertEquals(AccumuloAuthType.KERBEROS, config1.getAuthenticationType());

    // Test mixed case
    AccumuloStoragePluginConfig config2 = new AccumuloStoragePluginConfig(
        "localhost:2181", "accumulo", null, null,
        "Kerberos", "p", "k", null, null, null, null, null, null, null, null
    );
    assertEquals(AccumuloAuthType.KERBEROS, config2.getAuthenticationType());

    // Test password (default)
    AccumuloStoragePluginConfig config3 = new AccumuloStoragePluginConfig(
        "localhost:2181", "accumulo", "user", "pass",
        "password", null, null, null, null, null, null, null, null, null, null
    );
    assertEquals(AccumuloAuthType.PASSWORD, config3.getAuthenticationType());
  }

  @Test
  public void testConfigEquality() {
    AccumuloStoragePluginConfig config1 = new AccumuloStoragePluginConfig(
        "localhost:2181", "accumulo", "root", "secret",
        null, null, null, null, null, null, null, null, null, null, null
    );

    AccumuloStoragePluginConfig config2 = new AccumuloStoragePluginConfig(
        "localhost:2181", "accumulo", "root", "secret",
        null, null, null, null, null, null, null, null, null, null, null
    );

    AccumuloStoragePluginConfig config3 = new AccumuloStoragePluginConfig(
        "localhost:2181", "accumulo", "different_user", "secret",
        null, null, null, null, null, null, null, null, null, null, null
    );

    assertEquals(config1, config2);
    assertEquals(config1.hashCode(), config2.hashCode());
    assertNotEquals(config1, config3);
  }

  @Test
  public void testKerberosConfigEquality() {
    AccumuloStoragePluginConfig config1 = new AccumuloStoragePluginConfig(
        "localhost:2181", "accumulo", null, null,
        "KERBEROS", "drill@REALM", "/keytab", "auth", "accumulo", true,
        "USER_IMPERSONATION", null, null, null, null
    );

    AccumuloStoragePluginConfig config2 = new AccumuloStoragePluginConfig(
        "localhost:2181", "accumulo", null, null,
        "KERBEROS", "drill@REALM", "/keytab", "auth", "accumulo", true,
        "USER_IMPERSONATION", null, null, null, null
    );

    AccumuloStoragePluginConfig config3 = new AccumuloStoragePluginConfig(
        "localhost:2181", "accumulo", null, null,
        "KERBEROS", "different@REALM", "/keytab", "auth", "accumulo", true,
        "USER_IMPERSONATION", null, null, null, null
    );

    assertEquals(config1, config2);
    assertEquals(config1.hashCode(), config2.hashCode());
    assertNotEquals(config1, config3);
  }

  @Test
  public void testJsonSerializationPasswordAuth() throws Exception {
    ObjectMapper mapper = new ObjectMapper();

    AccumuloStoragePluginConfig config = new AccumuloStoragePluginConfig(
        "localhost:2181", "accumulo", "root", "secret",
        null, null, null, null, null, null, null, null,
        "my_schema", 45000, 15
    );

    String json = mapper.writeValueAsString(config);
    assertNotNull(json);
    assertTrue(json.contains("localhost:2181"));
    assertTrue(json.contains("accumulo"));
    assertTrue(json.contains("root"));
    assertTrue(json.contains("my_schema"));
    assertTrue(json.contains("PASSWORD") || json.contains("\"authenticationType\":null"));

    // Deserialize back
    AccumuloStoragePluginConfig deserialized = mapper.readValue(json, AccumuloStoragePluginConfig.class);
    assertEquals(config.getZookeeperQuorum(), deserialized.getZookeeperQuorum());
    assertEquals(config.getInstanceName(), deserialized.getInstanceName());
    assertEquals(config.getUsername(), deserialized.getUsername());
    assertEquals(config.getPassword(), deserialized.getPassword());
  }

  @Test
  public void testJsonSerializationKerberosAuth() throws Exception {
    ObjectMapper mapper = new ObjectMapper();

    AccumuloStoragePluginConfig config = new AccumuloStoragePluginConfig(
        "zk:2181", "accumulo", null, null,
        "KERBEROS", "drill/host@REALM", "/etc/keytab", "auth-conf", "accumulo", true,
        "USER_IMPERSONATION", null, null, null, null
    );

    String json = mapper.writeValueAsString(config);
    assertNotNull(json);
    assertTrue(json.contains("KERBEROS"));
    assertTrue(json.contains("drill/host@REALM"));
    assertTrue(json.contains("/etc/keytab"));
    assertTrue(json.contains("auth-conf"));
    assertTrue(json.contains("useDelegationTokens"));

    // Deserialize back
    AccumuloStoragePluginConfig deserialized = mapper.readValue(json, AccumuloStoragePluginConfig.class);
    assertEquals(AccumuloAuthType.KERBEROS, deserialized.getAuthenticationType());
    assertEquals("drill/host@REALM", deserialized.getPrincipal());
    assertEquals("/etc/keytab", deserialized.getKeytabPath());
    assertEquals("auth-conf", deserialized.getSaslQop());
    assertTrue(deserialized.isUseDelegationTokens());
  }

  @Test
  public void testToStringMasksPassword() {
    AccumuloStoragePluginConfig config = new AccumuloStoragePluginConfig(
        "localhost:2181", "accumulo", "root", "supersecret",
        null, null, null, null, null, null, null, null, null, null, null
    );

    String toString = config.toString();
    assertTrue(toString.contains("localhost:2181"));
    assertTrue(toString.contains("accumulo"));
    assertTrue(toString.contains("root"));
    // Password should be masked
    assertTrue(!toString.contains("supersecret") || toString.contains("*"));
  }

  @Test
  public void testToStringMasksKeytabPath() {
    AccumuloStoragePluginConfig config = new AccumuloStoragePluginConfig(
        "localhost:2181", "accumulo", null, null,
        "KERBEROS", "drill@REALM", "/secure/path/to/keytab", null, null, null,
        null, null, null, null, null
    );

    String toString = config.toString();
    assertTrue(toString.contains("drill@REALM"));
    // Keytab path should be masked for security
    assertTrue(!toString.contains("/secure/path/to/keytab") || toString.contains("*"));
  }

  @Test
  public void testExtendsStoragePluginConfig() {
    AccumuloStoragePluginConfig config = new AccumuloStoragePluginConfig(
        "localhost:2181", "accumulo", "root", "secret",
        null, null, null, null, null, null, null, null, null, null, null
    );

    assertTrue(config instanceof StoragePluginConfig);
  }

  @Test
  public void testIsKerberosEnabled() {
    AccumuloStoragePluginConfig passwordConfig = new AccumuloStoragePluginConfig(
        "localhost:2181", "accumulo", "root", "secret",
        "PASSWORD", null, null, null, null, null, null, null, null, null, null
    );
    assertFalse(passwordConfig.isKerberosEnabled());

    AccumuloStoragePluginConfig kerberosConfig = new AccumuloStoragePluginConfig(
        "localhost:2181", "accumulo", null, null,
        "KERBEROS", "drill@REALM", "/keytab", null, null, null, null, null, null, null, null
    );
    assertTrue(kerberosConfig.isKerberosEnabled());
  }

  @Test
  public void testIsUserImpersonationEnabled() {
    AccumuloStoragePluginConfig sharedUserConfig = new AccumuloStoragePluginConfig(
        "localhost:2181", "accumulo", "root", "secret",
        null, null, null, null, null, null, "SHARED_USER", null, null, null, null
    );
    assertFalse(sharedUserConfig.isUserImpersonationEnabled());

    AccumuloStoragePluginConfig impersonationConfig = new AccumuloStoragePluginConfig(
        "localhost:2181", "accumulo", null, null,
        "KERBEROS", "drill@REALM", "/keytab", null, null, true,
        "USER_IMPERSONATION", null, null, null, null
    );
    assertTrue(impersonationConfig.isUserImpersonationEnabled());
  }
}
