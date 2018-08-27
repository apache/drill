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

import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import com.typesafe.config.ConfigValueFactory;
import org.apache.drill.test.BaseTestQuery;
import org.apache.drill.categories.SecurityTest;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.config.DrillProperties;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.rpc.NonTransientRpcException;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.control.ControlRpcMetrics;
import org.apache.drill.exec.rpc.data.DataRpcMetrics;
import org.apache.drill.exec.rpc.security.KerberosHelper;
import org.apache.drill.exec.rpc.user.UserRpcMetrics;
import org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.hadoop.security.authentication.util.KerberosUtil;
import org.apache.kerby.kerberos.kerb.client.JaasKrbUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import javax.security.auth.Subject;
import java.lang.reflect.Field;
import java.security.PrivilegedExceptionAction;
import java.util.Properties;

import static junit.framework.TestCase.assertTrue;
import static junit.framework.TestCase.fail;

@Ignore("See DRILL-5387")
@Category(SecurityTest.class)
public class TestUserBitKerberosEncryption extends BaseTestQuery {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(TestUserBitKerberosEncryption.class);

  private static KerberosHelper krbHelper;
  private static DrillConfig newConfig;

  @BeforeClass
  public static void setupTest() throws Exception {
    krbHelper = new KerberosHelper(TestUserBitKerberosEncryption.class.getSimpleName(), null);
    krbHelper.setupKdc(dirTestWatcher.getTmpDir());

    // Create a new DrillConfig which has user authentication enabled and authenticator set to
    // UserAuthenticatorTestImpl.
    newConfig = new DrillConfig(DrillConfig.create(cloneDefaultTestConfigProperties())
        .withValue(ExecConstants.USER_AUTHENTICATION_ENABLED,
            ConfigValueFactory.fromAnyRef(true))
        .withValue(ExecConstants.USER_AUTHENTICATOR_IMPL,
            ConfigValueFactory.fromAnyRef(UserAuthenticatorTestImpl.TYPE))
        .withValue(ExecConstants.SERVICE_PRINCIPAL,
            ConfigValueFactory.fromAnyRef(krbHelper.SERVER_PRINCIPAL))
        .withValue(ExecConstants.SERVICE_KEYTAB_LOCATION,
            ConfigValueFactory.fromAnyRef(krbHelper.serverKeytab.toString()))
        .withValue(ExecConstants.AUTHENTICATION_MECHANISMS,
            ConfigValueFactory.fromIterable(Lists.newArrayList("plain", "kerberos")))
        .withValue(ExecConstants.USER_ENCRYPTION_SASL_ENABLED,
            ConfigValueFactory.fromAnyRef(true)));

    final Properties connectionProps = new Properties();
    connectionProps.setProperty(DrillProperties.SERVICE_PRINCIPAL, krbHelper.SERVER_PRINCIPAL);
    connectionProps.setProperty(DrillProperties.USER, krbHelper.CLIENT_PRINCIPAL);
    connectionProps.setProperty(DrillProperties.KEYTAB, krbHelper.clientKeytab.getAbsolutePath());

    // Ignore the compile time warning caused by the code below.

    // Config is statically initialized at this point. But the above configuration results in a different
    // initialization which causes the tests to fail. So the following two changes are required.

    // (1) Refresh Kerberos config.
    sun.security.krb5.Config.refresh();
    // (2) Reset the default realm.
    final Field defaultRealm = KerberosName.class.getDeclaredField("defaultRealm");
    defaultRealm.setAccessible(true);
    defaultRealm.set(null, KerberosUtil.getDefaultRealm());

    // Start a secure cluster with client using Kerberos related parameters.
    updateTestCluster(1, newConfig, connectionProps);
  }

  @AfterClass
  public static void cleanTest() throws Exception {
    krbHelper.stopKdc();
  }

  @Test
  public void successKeytabWithoutChunking() throws Exception {
    final Properties connectionProps = new Properties();
    connectionProps.setProperty(DrillProperties.SERVICE_PRINCIPAL, krbHelper.SERVER_PRINCIPAL);
    connectionProps.setProperty(DrillProperties.USER, krbHelper.CLIENT_PRINCIPAL);
    connectionProps.setProperty(DrillProperties.KEYTAB, krbHelper.clientKeytab.getAbsolutePath());

    newConfig = new DrillConfig(DrillConfig.create(cloneDefaultTestConfigProperties())
      .withValue(ExecConstants.USER_AUTHENTICATION_ENABLED,
        ConfigValueFactory.fromAnyRef(true))
      .withValue(ExecConstants.USER_AUTHENTICATOR_IMPL,
        ConfigValueFactory.fromAnyRef(UserAuthenticatorTestImpl.TYPE))
      .withValue(ExecConstants.SERVICE_PRINCIPAL,
        ConfigValueFactory.fromAnyRef(krbHelper.SERVER_PRINCIPAL))
      .withValue(ExecConstants.SERVICE_KEYTAB_LOCATION,
        ConfigValueFactory.fromAnyRef(krbHelper.serverKeytab.toString()))
      .withValue(ExecConstants.AUTHENTICATION_MECHANISMS,
        ConfigValueFactory.fromIterable(Lists.newArrayList("plain", "kerberos")))
      .withValue(ExecConstants.USER_ENCRYPTION_SASL_ENABLED,
        ConfigValueFactory.fromAnyRef(true)));

    updateTestCluster(1, newConfig, connectionProps);

    // Run few queries using the new client
    testBuilder()
        .sqlQuery("SELECT session_user FROM (SELECT * FROM sys.drillbits LIMIT 1)")
        .unOrdered()
        .baselineColumns("session_user")
        .baselineValues(krbHelper.CLIENT_SHORT_NAME)
        .go();
    test("SHOW SCHEMAS");
    test("USE INFORMATION_SCHEMA");
    test("SHOW TABLES");
    test("SELECT * FROM INFORMATION_SCHEMA.`TABLES` WHERE TABLE_NAME LIKE 'COLUMNS'");
    test("SELECT * FROM cp.`region.json`");
  }

  /**
   * Test connection counter values for both encrypted and unencrypted connections over all Drillbit channels.
   * Encryption is enabled only for UserRpc NOT for ControlRpc and DataRpc. Test validates corresponding connection
   * count for each channel.
   * For example: There is only 1 DrillClient so encrypted connection count of UserRpcMetrics will be 1. Before
   * running any query there should not be any connection (control or data) between Drillbits, hence those counters
   * are 0. After running a simple query since there is only 1 fragment which is root fragment the Control Connection
   * count is 0 (for unencrypted counter) since with DRILL-5721 status update of fragment to Foreman happens locally.
   * There is no Data Connection because there is no data exchange between multiple fragments.
   *
   * @throws Exception
   */
  @Test
  public void testConnectionCounters() throws Exception {
    final Properties connectionProps = new Properties();
    connectionProps.setProperty(DrillProperties.SERVICE_PRINCIPAL, krbHelper.SERVER_PRINCIPAL);
    connectionProps.setProperty(DrillProperties.USER, krbHelper.CLIENT_PRINCIPAL);
    connectionProps.setProperty(DrillProperties.KEYTAB, krbHelper.clientKeytab.getAbsolutePath());

    newConfig = new DrillConfig(DrillConfig.create(cloneDefaultTestConfigProperties())
      .withValue(ExecConstants.USER_AUTHENTICATION_ENABLED,
        ConfigValueFactory.fromAnyRef(true))
      .withValue(ExecConstants.USER_AUTHENTICATOR_IMPL,
        ConfigValueFactory.fromAnyRef(UserAuthenticatorTestImpl.TYPE))
      .withValue(ExecConstants.SERVICE_PRINCIPAL,
        ConfigValueFactory.fromAnyRef(krbHelper.SERVER_PRINCIPAL))
      .withValue(ExecConstants.SERVICE_KEYTAB_LOCATION,
        ConfigValueFactory.fromAnyRef(krbHelper.serverKeytab.toString()))
      .withValue(ExecConstants.AUTHENTICATION_MECHANISMS,
        ConfigValueFactory.fromIterable(Lists.newArrayList("plain", "kerberos")))
      .withValue(ExecConstants.USER_ENCRYPTION_SASL_ENABLED,
        ConfigValueFactory.fromAnyRef(true)));

    updateTestCluster(1, newConfig, connectionProps);

    assertTrue(UserRpcMetrics.getInstance().getEncryptedConnectionCount() == 1);
    assertTrue(UserRpcMetrics.getInstance().getUnEncryptedConnectionCount() == 0);

    // Run few queries using the new client
    testBuilder()
        .sqlQuery("SELECT session_user FROM (SELECT * FROM sys.drillbits LIMIT 1)")
        .unOrdered()
        .baselineColumns("session_user")
        .baselineValues(krbHelper.CLIENT_SHORT_NAME)
        .go();

    // Check encrypted counters value
    assertTrue(1 == UserRpcMetrics.getInstance().getEncryptedConnectionCount());
    assertTrue(0 == ControlRpcMetrics.getInstance().getEncryptedConnectionCount());
    assertTrue(0 == DataRpcMetrics.getInstance().getEncryptedConnectionCount());

    // Check unencrypted counters value
    assertTrue(0 == UserRpcMetrics.getInstance().getUnEncryptedConnectionCount());
    assertTrue(0 == ControlRpcMetrics.getInstance().getUnEncryptedConnectionCount());
    assertTrue(0 == DataRpcMetrics.getInstance().getUnEncryptedConnectionCount());
  }

  @Test
  public void successTicketWithoutChunking() throws Exception {
    final Properties connectionProps = new Properties();
    connectionProps.setProperty(DrillProperties.SERVICE_PRINCIPAL, krbHelper.SERVER_PRINCIPAL);
    connectionProps.setProperty(DrillProperties.KERBEROS_FROM_SUBJECT, "true");
    final Subject clientSubject = JaasKrbUtil.loginUsingKeytab(krbHelper.CLIENT_PRINCIPAL,
                                                               krbHelper.clientKeytab.getAbsoluteFile());

    newConfig = new DrillConfig(DrillConfig.create(cloneDefaultTestConfigProperties())
      .withValue(ExecConstants.USER_AUTHENTICATION_ENABLED,
        ConfigValueFactory.fromAnyRef(true))
      .withValue(ExecConstants.USER_AUTHENTICATOR_IMPL,
        ConfigValueFactory.fromAnyRef(UserAuthenticatorTestImpl.TYPE))
      .withValue(ExecConstants.SERVICE_PRINCIPAL,
        ConfigValueFactory.fromAnyRef(krbHelper.SERVER_PRINCIPAL))
      .withValue(ExecConstants.SERVICE_KEYTAB_LOCATION,
        ConfigValueFactory.fromAnyRef(krbHelper.serverKeytab.toString()))
      .withValue(ExecConstants.AUTHENTICATION_MECHANISMS,
        ConfigValueFactory.fromIterable(Lists.newArrayList("plain", "kerberos")))
      .withValue(ExecConstants.USER_ENCRYPTION_SASL_ENABLED,
        ConfigValueFactory.fromAnyRef(true)));

    Subject.doAs(clientSubject, new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        updateTestCluster(1, newConfig, connectionProps);
        return null;
      }
    });

    // Run few queries using the new client
    testBuilder()
        .sqlQuery("SELECT session_user FROM (SELECT * FROM sys.drillbits LIMIT 1)")
        .unOrdered()
        .baselineColumns("session_user")
        .baselineValues(krbHelper.CLIENT_SHORT_NAME)
        .go();
    test("SHOW SCHEMAS");
    test("USE INFORMATION_SCHEMA");
    test("SHOW TABLES");
    test("SELECT * FROM INFORMATION_SCHEMA.`TABLES` WHERE TABLE_NAME LIKE 'COLUMNS'");
    test("SELECT * FROM cp.`region.json` LIMIT 5");
  }

  @Test
  public void successKeytabWithChunking() throws Exception {
    final Properties connectionProps = new Properties();
    connectionProps.setProperty(DrillProperties.SERVICE_PRINCIPAL, krbHelper.SERVER_PRINCIPAL);
    connectionProps.setProperty(DrillProperties.USER, krbHelper.CLIENT_PRINCIPAL);
    connectionProps.setProperty(DrillProperties.KEYTAB, krbHelper.clientKeytab.getAbsolutePath());

    newConfig = new DrillConfig(DrillConfig.create(cloneDefaultTestConfigProperties())
      .withValue(ExecConstants.USER_AUTHENTICATION_ENABLED,
        ConfigValueFactory.fromAnyRef(true))
      .withValue(ExecConstants.USER_AUTHENTICATOR_IMPL,
        ConfigValueFactory.fromAnyRef(UserAuthenticatorTestImpl.TYPE))
      .withValue(ExecConstants.SERVICE_PRINCIPAL,
        ConfigValueFactory.fromAnyRef(krbHelper.SERVER_PRINCIPAL))
      .withValue(ExecConstants.SERVICE_KEYTAB_LOCATION,
        ConfigValueFactory.fromAnyRef(krbHelper.serverKeytab.toString()))
      .withValue(ExecConstants.AUTHENTICATION_MECHANISMS,
        ConfigValueFactory.fromIterable(Lists.newArrayList("plain", "kerberos")))
      .withValue(ExecConstants.USER_ENCRYPTION_SASL_ENABLED,
        ConfigValueFactory.fromAnyRef(true))
      .withValue(ExecConstants.USER_ENCRYPTION_SASL_MAX_WRAPPED_SIZE,
        ConfigValueFactory.fromAnyRef(100)));

    updateTestCluster(1, newConfig, connectionProps);

    // Run few queries using the new client
    testBuilder()
      .sqlQuery("SELECT session_user FROM (SELECT * FROM sys.drillbits LIMIT 1)")
      .unOrdered()
      .baselineColumns("session_user")
      .baselineValues(krbHelper.CLIENT_SHORT_NAME)
      .go();
    test("SHOW SCHEMAS");
    test("USE INFORMATION_SCHEMA");
    test("SHOW TABLES");
    test("SELECT * FROM INFORMATION_SCHEMA.`TABLES` WHERE TABLE_NAME LIKE 'COLUMNS'");
    test("SELECT * FROM cp.`region.json`");
  }

  @Test
  public void successKeytabWithChunkingDefaultChunkSize() throws Exception {
    final Properties connectionProps = new Properties();
    connectionProps.setProperty(DrillProperties.SERVICE_PRINCIPAL, krbHelper.SERVER_PRINCIPAL);
    connectionProps.setProperty(DrillProperties.USER, krbHelper.CLIENT_PRINCIPAL);
    connectionProps.setProperty(DrillProperties.KEYTAB, krbHelper.clientKeytab.getAbsolutePath());

    newConfig = new DrillConfig(DrillConfig.create(cloneDefaultTestConfigProperties())
      .withValue(ExecConstants.USER_AUTHENTICATION_ENABLED,
        ConfigValueFactory.fromAnyRef(true))
      .withValue(ExecConstants.USER_AUTHENTICATOR_IMPL,
        ConfigValueFactory.fromAnyRef(UserAuthenticatorTestImpl.TYPE))
      .withValue(ExecConstants.SERVICE_PRINCIPAL,
        ConfigValueFactory.fromAnyRef(krbHelper.SERVER_PRINCIPAL))
      .withValue(ExecConstants.SERVICE_KEYTAB_LOCATION,
        ConfigValueFactory.fromAnyRef(krbHelper.serverKeytab.toString()))
      .withValue(ExecConstants.AUTHENTICATION_MECHANISMS,
        ConfigValueFactory.fromIterable(Lists.newArrayList("plain", "kerberos")))
      .withValue(ExecConstants.USER_ENCRYPTION_SASL_ENABLED,
        ConfigValueFactory.fromAnyRef(true)));

    updateTestCluster(1, newConfig, connectionProps);

    // Run few queries using the new client
    testBuilder()
      .sqlQuery("SELECT session_user FROM (SELECT * FROM sys.drillbits LIMIT 1)")
      .unOrdered()
      .baselineColumns("session_user")
      .baselineValues(krbHelper.CLIENT_SHORT_NAME)
      .go();
    test("SHOW SCHEMAS");
    test("USE INFORMATION_SCHEMA");
    test("SHOW TABLES");
    test("SELECT * FROM INFORMATION_SCHEMA.`TABLES` WHERE TABLE_NAME LIKE 'COLUMNS'");
    test("SELECT * FROM cp.`region.json` LIMIT 5");
  }


  /**
   *  This test will not cover the data channel since we are using only 1 Drillbit and the query doesn't involve
   *  any exchange operator. But Data Channel encryption testing is covered separately in
   *  {@link org.apache.drill.exec.rpc.data.TestBitBitKerberos}
   */
  @Test
  public void successEncryptionAllChannelChunkMode() throws Exception {
    final Properties connectionProps = new Properties();
    connectionProps.setProperty(DrillProperties.SERVICE_PRINCIPAL, krbHelper.SERVER_PRINCIPAL);
    connectionProps.setProperty(DrillProperties.USER, krbHelper.CLIENT_PRINCIPAL);
    connectionProps.setProperty(DrillProperties.KEYTAB, krbHelper.clientKeytab.getAbsolutePath());

    newConfig = new DrillConfig(DrillConfig.create(cloneDefaultTestConfigProperties())
      .withValue(ExecConstants.USER_AUTHENTICATION_ENABLED,
        ConfigValueFactory.fromAnyRef(true))
      .withValue(ExecConstants.USER_AUTHENTICATOR_IMPL,
        ConfigValueFactory.fromAnyRef(UserAuthenticatorTestImpl.TYPE))
      .withValue(ExecConstants.SERVICE_PRINCIPAL,
        ConfigValueFactory.fromAnyRef(krbHelper.SERVER_PRINCIPAL))
      .withValue(ExecConstants.SERVICE_KEYTAB_LOCATION,
        ConfigValueFactory.fromAnyRef(krbHelper.serverKeytab.toString()))
      .withValue(ExecConstants.AUTHENTICATION_MECHANISMS,
        ConfigValueFactory.fromIterable(Lists.newArrayList("plain", "kerberos")))
      .withValue(ExecConstants.USER_ENCRYPTION_SASL_ENABLED,
        ConfigValueFactory.fromAnyRef(true))
      .withValue(ExecConstants.USER_ENCRYPTION_SASL_MAX_WRAPPED_SIZE,
        ConfigValueFactory.fromAnyRef(10000))
      .withValue(ExecConstants.BIT_AUTHENTICATION_ENABLED,
        ConfigValueFactory.fromAnyRef(true))
      .withValue(ExecConstants.BIT_AUTHENTICATION_MECHANISM,
        ConfigValueFactory.fromAnyRef("kerberos"))
      .withValue(ExecConstants.USE_LOGIN_PRINCIPAL,
        ConfigValueFactory.fromAnyRef(true))
      .withValue(ExecConstants.BIT_ENCRYPTION_SASL_ENABLED,
        ConfigValueFactory.fromAnyRef(true))
      .withValue(ExecConstants.BIT_ENCRYPTION_SASL_MAX_WRAPPED_SIZE,
        ConfigValueFactory.fromAnyRef(10000)));

    updateTestCluster(1, newConfig, connectionProps);

    // Run few queries using the new client
    testBuilder()
      .sqlQuery("SELECT session_user FROM (SELECT * FROM sys.drillbits LIMIT 1)")
      .unOrdered()
      .baselineColumns("session_user")
      .baselineValues(krbHelper.CLIENT_SHORT_NAME)
      .go();
    test("SHOW SCHEMAS");
    test("USE INFORMATION_SCHEMA");
    test("SHOW TABLES");
    test("SELECT * FROM INFORMATION_SCHEMA.`TABLES` WHERE TABLE_NAME LIKE 'COLUMNS'");
    test("SELECT * FROM cp.`region.json` LIMIT 5");
  }



  /**
   *  This test will not cover the data channel since we are using only 1 Drillbit and the query doesn't involve
   *  any exchange operator. But Data Channel encryption testing is covered separately in
   *  {@link org.apache.drill.exec.rpc.data.TestBitBitKerberos}
   */
  @Test
  public void successEncryptionAllChannel() throws Exception {

    final Properties connectionProps = new Properties();
    connectionProps.setProperty(DrillProperties.SERVICE_PRINCIPAL, krbHelper.SERVER_PRINCIPAL);
    connectionProps.setProperty(DrillProperties.USER, krbHelper.CLIENT_PRINCIPAL);
    connectionProps.setProperty(DrillProperties.KEYTAB, krbHelper.clientKeytab.getAbsolutePath());

    newConfig = new DrillConfig(DrillConfig.create(cloneDefaultTestConfigProperties())
      .withValue(ExecConstants.USER_AUTHENTICATION_ENABLED,
        ConfigValueFactory.fromAnyRef(true))
      .withValue(ExecConstants.USER_AUTHENTICATOR_IMPL,
        ConfigValueFactory.fromAnyRef(UserAuthenticatorTestImpl.TYPE))
      .withValue(ExecConstants.SERVICE_PRINCIPAL,
        ConfigValueFactory.fromAnyRef(krbHelper.SERVER_PRINCIPAL))
      .withValue(ExecConstants.SERVICE_KEYTAB_LOCATION,
        ConfigValueFactory.fromAnyRef(krbHelper.serverKeytab.toString()))
      .withValue(ExecConstants.AUTHENTICATION_MECHANISMS,
        ConfigValueFactory.fromIterable(Lists.newArrayList("plain", "kerberos")))
      .withValue(ExecConstants.USER_ENCRYPTION_SASL_ENABLED,
        ConfigValueFactory.fromAnyRef(true))
      .withValue(ExecConstants.BIT_AUTHENTICATION_ENABLED,
        ConfigValueFactory.fromAnyRef(true))
      .withValue(ExecConstants.BIT_AUTHENTICATION_MECHANISM,
        ConfigValueFactory.fromAnyRef("kerberos"))
      .withValue(ExecConstants.USE_LOGIN_PRINCIPAL,
        ConfigValueFactory.fromAnyRef(true))
      .withValue(ExecConstants.BIT_ENCRYPTION_SASL_ENABLED,
        ConfigValueFactory.fromAnyRef(true)));

    updateTestCluster(1, newConfig, connectionProps);

    // Run few queries using the new client
    testBuilder()
      .sqlQuery("SELECT session_user FROM (SELECT * FROM sys.drillbits LIMIT 1)")
      .unOrdered()
      .baselineColumns("session_user")
      .baselineValues(krbHelper.CLIENT_SHORT_NAME)
      .go();
    test("SHOW SCHEMAS");
    test("USE INFORMATION_SCHEMA");
    test("SHOW TABLES");
    test("SELECT * FROM INFORMATION_SCHEMA.`TABLES` WHERE TABLE_NAME LIKE 'COLUMNS'");
    test("SELECT * FROM cp.`region.json` LIMIT 5");
  }

  /**
   * Test connection counter values for both encrypted and unencrypted connections over all Drillbit channels.
   * Encryption is enabled for UserRpc, ControlRpc and DataRpc. Test validates corresponding connection
   * count for each channel.
   * For example: There is only 1 DrillClient so encrypted connection count of UserRpcMetrics
   * will be 1. Before running any query there should not be any connection (control or data) between Drillbits,
   * hence those counters are 0. After running a simple query since there is only 1 fragment which is root fragment
   * the Control Connection count is 0 (for encrypted counter), since with DRILL-5721 status update of fragment to
   * Foreman happens locally. There is no Data Connection because there is no data exchange between multiple fragments.
   */

  @Test
  public void testEncryptedConnectionCountersAllChannel() throws Exception {
    final Properties connectionProps = new Properties();
    connectionProps.setProperty(DrillProperties.SERVICE_PRINCIPAL, krbHelper.SERVER_PRINCIPAL);
    connectionProps.setProperty(DrillProperties.USER, krbHelper.CLIENT_PRINCIPAL);
    connectionProps.setProperty(DrillProperties.KEYTAB, krbHelper.clientKeytab.getAbsolutePath());

    newConfig = new DrillConfig(DrillConfig.create(cloneDefaultTestConfigProperties())
        .withValue(ExecConstants.USER_AUTHENTICATION_ENABLED,
            ConfigValueFactory.fromAnyRef(true))
        .withValue(ExecConstants.USER_AUTHENTICATOR_IMPL,
            ConfigValueFactory.fromAnyRef(UserAuthenticatorTestImpl.TYPE))
        .withValue(ExecConstants.SERVICE_PRINCIPAL,
            ConfigValueFactory.fromAnyRef(krbHelper.SERVER_PRINCIPAL))
        .withValue(ExecConstants.SERVICE_KEYTAB_LOCATION,
            ConfigValueFactory.fromAnyRef(krbHelper.serverKeytab.toString()))
        .withValue(ExecConstants.AUTHENTICATION_MECHANISMS,
            ConfigValueFactory.fromIterable(Lists.newArrayList("plain", "kerberos")))
        .withValue(ExecConstants.USER_ENCRYPTION_SASL_ENABLED,
            ConfigValueFactory.fromAnyRef(true))
        .withValue(ExecConstants.BIT_AUTHENTICATION_ENABLED,
            ConfigValueFactory.fromAnyRef(true))
        .withValue(ExecConstants.BIT_AUTHENTICATION_MECHANISM,
            ConfigValueFactory.fromAnyRef("kerberos"))
        .withValue(ExecConstants.USE_LOGIN_PRINCIPAL,
            ConfigValueFactory.fromAnyRef(true))
        .withValue(ExecConstants.BIT_ENCRYPTION_SASL_ENABLED,
            ConfigValueFactory.fromAnyRef(true)));

    updateTestCluster(1, newConfig, connectionProps);

    // Run few queries using the new client
    testBuilder()
        .sqlQuery("SELECT session_user FROM (SELECT * FROM sys.drillbits LIMIT 1)")
        .unOrdered()
        .baselineColumns("session_user")
        .baselineValues(krbHelper.CLIENT_SHORT_NAME)
        .go();

    // Check encrypted counters value
    assertTrue(1 == UserRpcMetrics.getInstance().getEncryptedConnectionCount());
    assertTrue(0 == ControlRpcMetrics.getInstance().getEncryptedConnectionCount());
    assertTrue(0 == DataRpcMetrics.getInstance().getEncryptedConnectionCount());

    // Check unencrypted counters value
    assertTrue(0 == UserRpcMetrics.getInstance().getUnEncryptedConnectionCount());
    assertTrue(0 == ControlRpcMetrics.getInstance().getUnEncryptedConnectionCount());
    assertTrue(0 == DataRpcMetrics.getInstance().getUnEncryptedConnectionCount());
  }

  @Test
  public void failurePlainMech() {
    try {
      final Properties connectionProps = new Properties();
      connectionProps.setProperty(DrillProperties.USER, "anonymous");
      connectionProps.setProperty(DrillProperties.PASSWORD, "anything works!");

      newConfig = new DrillConfig(DrillConfig.create(cloneDefaultTestConfigProperties())
        .withValue(ExecConstants.USER_AUTHENTICATION_ENABLED,
          ConfigValueFactory.fromAnyRef(true))
        .withValue(ExecConstants.USER_AUTHENTICATOR_IMPL,
          ConfigValueFactory.fromAnyRef(UserAuthenticatorTestImpl.TYPE))
        .withValue(ExecConstants.SERVICE_PRINCIPAL,
          ConfigValueFactory.fromAnyRef(krbHelper.SERVER_PRINCIPAL))
        .withValue(ExecConstants.SERVICE_KEYTAB_LOCATION,
          ConfigValueFactory.fromAnyRef(krbHelper.serverKeytab.toString()))
        .withValue(ExecConstants.AUTHENTICATION_MECHANISMS,
          ConfigValueFactory.fromIterable(Lists.newArrayList("plain", "kerberos")))
        .withValue(ExecConstants.USER_ENCRYPTION_SASL_ENABLED,
          ConfigValueFactory.fromAnyRef(true)));

      updateTestCluster(1, newConfig, connectionProps);
      fail();
    } catch (Exception ex) {
      assert (ex.getCause() instanceof NonTransientRpcException);
      logger.error("Caught exception: ", ex);
    }
  }

  @Test
  public void encryptionEnabledWithOnlyPlainMech() {
    try {
      final Properties connectionProps = new Properties();
      connectionProps.setProperty(DrillProperties.SERVICE_PRINCIPAL, krbHelper.SERVER_PRINCIPAL);
      connectionProps.setProperty(DrillProperties.USER, krbHelper.CLIENT_PRINCIPAL);
      connectionProps.setProperty(DrillProperties.KEYTAB, krbHelper.clientKeytab.getAbsolutePath());

      newConfig = new DrillConfig(DrillConfig.create(cloneDefaultTestConfigProperties())
        .withValue(ExecConstants.USER_AUTHENTICATION_ENABLED,
          ConfigValueFactory.fromAnyRef(true))
        .withValue(ExecConstants.USER_AUTHENTICATOR_IMPL,
          ConfigValueFactory.fromAnyRef(UserAuthenticatorTestImpl.TYPE))
        .withValue(ExecConstants.SERVICE_PRINCIPAL,
          ConfigValueFactory.fromAnyRef(krbHelper.SERVER_PRINCIPAL))
        .withValue(ExecConstants.SERVICE_KEYTAB_LOCATION,
          ConfigValueFactory.fromAnyRef(krbHelper.serverKeytab.toString()))
        .withValue(ExecConstants.AUTHENTICATION_MECHANISMS,
          ConfigValueFactory.fromIterable(Lists.newArrayList("plain")))
        .withValue(ExecConstants.USER_ENCRYPTION_SASL_ENABLED,
          ConfigValueFactory.fromAnyRef(true)));
      updateTestCluster(1, newConfig, connectionProps);

      fail();
    } catch (Exception ex) {
      assert (ex.getCause() instanceof NonTransientRpcException);
      logger.error("Caught exception: ", ex);
    }
  }

  /**
   * Test to validate that older clients are not allowed to connect to secure cluster
   * with encryption enabled.
   */
  @Test
  public void failureOldClientEncryptionEnabled() {
    try {
      final Properties connectionProps = new Properties();
      connectionProps.setProperty(DrillProperties.SERVICE_PRINCIPAL, krbHelper.SERVER_PRINCIPAL);
      connectionProps.setProperty(DrillProperties.USER, krbHelper.CLIENT_PRINCIPAL);
      connectionProps.setProperty(DrillProperties.KEYTAB, krbHelper.clientKeytab.getAbsolutePath());
      connectionProps.setProperty(DrillProperties.TEST_SASL_LEVEL, "1");

      newConfig = new DrillConfig(DrillConfig.create(cloneDefaultTestConfigProperties())
        .withValue(ExecConstants.USER_AUTHENTICATION_ENABLED,
          ConfigValueFactory.fromAnyRef(true))
        .withValue(ExecConstants.USER_AUTHENTICATOR_IMPL,
          ConfigValueFactory.fromAnyRef(UserAuthenticatorTestImpl.TYPE))
        .withValue(ExecConstants.SERVICE_PRINCIPAL,
          ConfigValueFactory.fromAnyRef(krbHelper.SERVER_PRINCIPAL))
        .withValue(ExecConstants.SERVICE_KEYTAB_LOCATION,
          ConfigValueFactory.fromAnyRef(krbHelper.serverKeytab.toString()))
        .withValue(ExecConstants.AUTHENTICATION_MECHANISMS,
          ConfigValueFactory.fromIterable(Lists.newArrayList("plain", "kerberos")))
        .withValue(ExecConstants.USER_ENCRYPTION_SASL_ENABLED,
          ConfigValueFactory.fromAnyRef(true)));
      updateTestCluster(1, newConfig, connectionProps);

      fail();
    } catch (Exception ex) {
      assert (ex.getCause() instanceof RpcException);
      logger.error("Caught exception: ", ex);
    }
  }

  /**
   * Test to validate that older clients are successfully connecting to secure cluster
   * with encryption disabled.
   */
  @Test
  public void successOldClientEncryptionDisabled() {

    final Properties connectionProps = new Properties();
    connectionProps.setProperty(DrillProperties.SERVICE_PRINCIPAL, krbHelper.SERVER_PRINCIPAL);
    connectionProps.setProperty(DrillProperties.USER, krbHelper.CLIENT_PRINCIPAL);
    connectionProps.setProperty(DrillProperties.KEYTAB, krbHelper.clientKeytab.getAbsolutePath());
    connectionProps.setProperty(DrillProperties.TEST_SASL_LEVEL, "1");

    newConfig = new DrillConfig(DrillConfig.create(cloneDefaultTestConfigProperties())
      .withValue(ExecConstants.USER_AUTHENTICATION_ENABLED,
        ConfigValueFactory.fromAnyRef(true))
      .withValue(ExecConstants.USER_AUTHENTICATOR_IMPL,
        ConfigValueFactory.fromAnyRef(UserAuthenticatorTestImpl.TYPE))
      .withValue(ExecConstants.SERVICE_PRINCIPAL,
        ConfigValueFactory.fromAnyRef(krbHelper.SERVER_PRINCIPAL))
      .withValue(ExecConstants.SERVICE_KEYTAB_LOCATION,
        ConfigValueFactory.fromAnyRef(krbHelper.serverKeytab.toString()))
      .withValue(ExecConstants.AUTHENTICATION_MECHANISMS,
        ConfigValueFactory.fromIterable(Lists.newArrayList("plain", "kerberos"))));

    updateTestCluster(1, newConfig, connectionProps);
  }

  /**
   * Test to validate that clients which needs encrypted connection fails to connect
   * to server with encryption disabled.
   */
  @Test
  public void clientNeedsEncryptionWithNoServerSupport() throws Exception {
    try {
      final Properties connectionProps = new Properties();
      connectionProps.setProperty(DrillProperties.SERVICE_PRINCIPAL, krbHelper.SERVER_PRINCIPAL);
      connectionProps.setProperty(DrillProperties.USER, krbHelper.CLIENT_PRINCIPAL);
      connectionProps.setProperty(DrillProperties.KEYTAB, krbHelper.clientKeytab.getAbsolutePath());
      connectionProps.setProperty(DrillProperties.SASL_ENCRYPT, "true");

      newConfig = new DrillConfig(DrillConfig.create(cloneDefaultTestConfigProperties())
        .withValue(ExecConstants.USER_AUTHENTICATION_ENABLED,
          ConfigValueFactory.fromAnyRef(true))
        .withValue(ExecConstants.USER_AUTHENTICATOR_IMPL,
          ConfigValueFactory.fromAnyRef(UserAuthenticatorTestImpl.TYPE))
        .withValue(ExecConstants.SERVICE_PRINCIPAL,
          ConfigValueFactory.fromAnyRef(krbHelper.SERVER_PRINCIPAL))
        .withValue(ExecConstants.SERVICE_KEYTAB_LOCATION,
          ConfigValueFactory.fromAnyRef(krbHelper.serverKeytab.toString()))
        .withValue(ExecConstants.AUTHENTICATION_MECHANISMS,
          ConfigValueFactory.fromIterable(Lists.newArrayList("plain", "kerberos"))));

      updateTestCluster(1, newConfig, connectionProps);

      fail();
    } catch (Exception ex) {
      assert (ex.getCause() instanceof NonTransientRpcException);
    }
  }

  /**
   * Test to validate that clients which needs encrypted connection connects
   * to server with encryption enabled.
   */
  @Test
  public void clientNeedsEncryptionWithServerSupport() throws Exception {
    try {
      final Properties connectionProps = new Properties();
      connectionProps.setProperty(DrillProperties.SERVICE_PRINCIPAL, krbHelper.SERVER_PRINCIPAL);
      connectionProps.setProperty(DrillProperties.USER, krbHelper.CLIENT_PRINCIPAL);
      connectionProps.setProperty(DrillProperties.KEYTAB, krbHelper.clientKeytab.getAbsolutePath());
      connectionProps.setProperty(DrillProperties.SASL_ENCRYPT, "true");

      newConfig = new DrillConfig(DrillConfig.create(cloneDefaultTestConfigProperties())
        .withValue(ExecConstants.USER_AUTHENTICATION_ENABLED,
          ConfigValueFactory.fromAnyRef(true))
        .withValue(ExecConstants.USER_AUTHENTICATOR_IMPL,
          ConfigValueFactory.fromAnyRef(UserAuthenticatorTestImpl.TYPE))
        .withValue(ExecConstants.SERVICE_PRINCIPAL,
          ConfigValueFactory.fromAnyRef(krbHelper.SERVER_PRINCIPAL))
        .withValue(ExecConstants.SERVICE_KEYTAB_LOCATION,
          ConfigValueFactory.fromAnyRef(krbHelper.serverKeytab.toString()))
        .withValue(ExecConstants.AUTHENTICATION_MECHANISMS,
          ConfigValueFactory.fromIterable(Lists.newArrayList("plain", "kerberos")))
        .withValue(ExecConstants.USER_ENCRYPTION_SASL_ENABLED,
              ConfigValueFactory.fromAnyRef(true)));

      updateTestCluster(1, newConfig, connectionProps);
    } catch (Exception ex) {
      fail();
      assert (ex.getCause() instanceof NonTransientRpcException);
    }
  }
}
