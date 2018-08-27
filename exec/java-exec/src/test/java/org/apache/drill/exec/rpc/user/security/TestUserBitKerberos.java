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
import org.apache.drill.categories.SecurityTest;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.config.DrillProperties;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.rpc.control.ControlRpcMetrics;
import org.apache.drill.exec.rpc.data.DataRpcMetrics;
import org.apache.drill.exec.rpc.security.KerberosHelper;
import org.apache.drill.exec.rpc.user.UserRpcMetrics;
import org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl;
import org.apache.drill.test.BaseTestQuery;
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

@Ignore("See DRILL-5387")
@Category(SecurityTest.class)
public class TestUserBitKerberos extends BaseTestQuery {
  //private static final org.slf4j.Logger logger =org.slf4j.LoggerFactory.getLogger(TestUserBitKerberos.class);

  private static KerberosHelper krbHelper;

  @BeforeClass
  public static void setupTest() throws Exception {

    krbHelper = new KerberosHelper(TestUserBitKerberos.class.getSimpleName(), null);
    krbHelper.setupKdc(dirTestWatcher.getTmpDir());

    // Create a new DrillConfig which has user authentication enabled and authenticator set to
    // UserAuthenticatorTestImpl.
    final DrillConfig newConfig = new DrillConfig(DrillConfig.create(cloneDefaultTestConfigProperties())
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

    final Properties connectionProps = new Properties();
    connectionProps.setProperty(DrillProperties.USER, "anonymous");
    connectionProps.setProperty(DrillProperties.PASSWORD, "anything works!");

    // Ignore the compile time warning caused by the code below.

    // Config is statically initialized at this point. But the above configuration results in a different
    // initialization which causes the tests to fail. So the following two changes are required.

    // (1) Refresh Kerberos config.
    sun.security.krb5.Config.refresh();
    // (2) Reset the default realm.
    final Field defaultRealm = KerberosName.class.getDeclaredField("defaultRealm");
    defaultRealm.setAccessible(true);
    defaultRealm.set(null, KerberosUtil.getDefaultRealm());

    updateTestCluster(1, newConfig, connectionProps);
  }

  @Test
  public void successKeytab() throws Exception {
    final Properties connectionProps = new Properties();
    connectionProps.setProperty(DrillProperties.SERVICE_PRINCIPAL, krbHelper.SERVER_PRINCIPAL);
    connectionProps.setProperty(DrillProperties.USER, krbHelper.CLIENT_PRINCIPAL);
    connectionProps.setProperty(DrillProperties.KEYTAB, krbHelper.clientKeytab.getAbsolutePath());
    updateClient(connectionProps);

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
  public void successTicket() throws Exception {
    final Properties connectionProps = new Properties();
    connectionProps.setProperty(DrillProperties.SERVICE_PRINCIPAL, krbHelper.SERVER_PRINCIPAL);
    connectionProps.setProperty(DrillProperties.KERBEROS_FROM_SUBJECT, "true");
    final Subject clientSubject = JaasKrbUtil.loginUsingKeytab(krbHelper.CLIENT_PRINCIPAL,
      krbHelper.clientKeytab.getAbsoluteFile());

    Subject.doAs(clientSubject, new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        updateClient(connectionProps);
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
  public void testUnecryptedConnectionCounter() throws Exception {
    final Properties connectionProps = new Properties();
    connectionProps.setProperty(DrillProperties.SERVICE_PRINCIPAL, krbHelper.SERVER_PRINCIPAL);
    connectionProps.setProperty(DrillProperties.KERBEROS_FROM_SUBJECT, "true");
    final Subject clientSubject = JaasKrbUtil.loginUsingKeytab(krbHelper.CLIENT_PRINCIPAL,
        krbHelper.clientKeytab.getAbsoluteFile());

    Subject.doAs(clientSubject, new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        updateClient(connectionProps);
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

    // Check encrypted counters value
    assertTrue(0 == UserRpcMetrics.getInstance().getEncryptedConnectionCount());
    assertTrue(0 == ControlRpcMetrics.getInstance().getEncryptedConnectionCount());
    assertTrue(0 == DataRpcMetrics.getInstance().getEncryptedConnectionCount());

    // Check unencrypted counters value
    assertTrue(1 == UserRpcMetrics.getInstance().getUnEncryptedConnectionCount());
    assertTrue(0 == ControlRpcMetrics.getInstance().getUnEncryptedConnectionCount());
    assertTrue(0 == DataRpcMetrics.getInstance().getUnEncryptedConnectionCount());
  }

  @Test
  public void testUnecryptedConnectionCounter_LocalControlMessage() throws Exception {
    final Properties connectionProps = new Properties();
    connectionProps.setProperty(DrillProperties.SERVICE_PRINCIPAL, krbHelper.SERVER_PRINCIPAL);
    connectionProps.setProperty(DrillProperties.KERBEROS_FROM_SUBJECT, "true");
    final Subject clientSubject = JaasKrbUtil.loginUsingKeytab(krbHelper.CLIENT_PRINCIPAL,
      krbHelper.clientKeytab.getAbsoluteFile());

    Subject.doAs(clientSubject, new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        updateClient(connectionProps);
        return null;
      }
    });

    // Run query on memory system table this sends remote fragments to all Drillbit and Drillbits then send data
    // using data channel. In this test we have only 1 Drillbit so there should not be any control connection but a
    // local data connections
    testSql("SELECT * FROM sys.memory");

    // Check encrypted counters value
    assertTrue(0 == UserRpcMetrics.getInstance().getEncryptedConnectionCount());
    assertTrue(0 == ControlRpcMetrics.getInstance().getEncryptedConnectionCount());
    assertTrue(0 == DataRpcMetrics.getInstance().getEncryptedConnectionCount());

    // Check unencrypted counters value
    assertTrue(1 == UserRpcMetrics.getInstance().getUnEncryptedConnectionCount());
    assertTrue(0 == ControlRpcMetrics.getInstance().getUnEncryptedConnectionCount());
    assertTrue(2 == DataRpcMetrics.getInstance().getUnEncryptedConnectionCount());
  }

  @AfterClass
  public static void cleanTest() throws Exception {
    krbHelper.stopKdc();
  }
}
