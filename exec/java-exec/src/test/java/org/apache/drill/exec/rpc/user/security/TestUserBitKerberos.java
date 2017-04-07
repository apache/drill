/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.rpc.user.security;

import com.google.common.collect.Lists;
import com.typesafe.config.ConfigValueFactory;
import org.apache.drill.BaseTestQuery;
import org.apache.drill.common.config.DrillProperties;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl;
import org.apache.drill.exec.server.BootStrapContext;
import org.apache.hadoop.security.UgiTestUtil;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.hadoop.security.authentication.util.KerberosUtil;
import org.apache.kerby.kerberos.kerb.KrbException;
import org.apache.kerby.kerberos.kerb.client.JaasKrbUtil;
import org.apache.kerby.kerberos.kerb.server.SimpleKdcServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import javax.security.auth.Subject;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.security.PrivilegedExceptionAction;
import java.util.Properties;

@Ignore("See DRILL-5387")
public class TestUserBitKerberos extends BaseTestQuery {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(TestUserBitKerberos.class);

  private static File workspace;

  private static File kdcDir;
  private static SimpleKdcServer kdc;
  private static int kdcPort;

  private static final String HOSTNAME = "localhost";
  private static final String REALM = "EXAMPLE.COM";

  private static final String CLIENT_SHORT_NAME = "testUser";
  private static final String CLIENT_PRINCIPAL = CLIENT_SHORT_NAME + "@" + REALM;
  private static final String SERVER_SHORT_NAME = System.getProperty("user.name");
  private static final String SERVER_PRINCIPAL = SERVER_SHORT_NAME + "/" + HOSTNAME + "@" + REALM;

  private static File keytabDir;
  private static File clientKeytab;
  private static File serverKeytab;

  private static boolean kdcStarted;

  @BeforeClass
  public static void setupKdc() throws Exception {
    kdc = new SimpleKdcServer();
    workspace = new File(getTempDir("kerberos_target"));

    kdcDir = new File(workspace, TestUserBitKerberos.class.getSimpleName());
    kdcDir.mkdirs();
    kdc.setWorkDir(kdcDir);

    kdc.setKdcHost(HOSTNAME);
    kdcPort = getFreePort();
    kdc.setAllowTcp(true);
    kdc.setAllowUdp(false);
    kdc.setKdcTcpPort(kdcPort);

    logger.debug("Starting KDC server at {}:{}", HOSTNAME, kdcPort);

    kdc.init();
    kdc.start();
    kdcStarted = true;


    keytabDir = new File(workspace, TestUserBitKerberos.class.getSimpleName()
        + "_keytabs");
    keytabDir.mkdirs();
    setupUsers(keytabDir);

    // Kerby sets "java.security.krb5.conf" for us!
    System.clearProperty("java.security.auth.login.config");
    System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
    // Uncomment the following lines for debugging.
    // System.setProperty("sun.security.spnego.debug", "true");
    // System.setProperty("sun.security.krb5.debug", "true");

    final DrillConfig newConfig = new DrillConfig(DrillConfig.create(cloneDefaultTestConfigProperties())
        .withValue(ExecConstants.USER_AUTHENTICATION_ENABLED,
            ConfigValueFactory.fromAnyRef(true))
        .withValue(ExecConstants.USER_AUTHENTICATOR_IMPL,
            ConfigValueFactory.fromAnyRef(UserAuthenticatorTestImpl.TYPE))
        .withValue(BootStrapContext.SERVICE_PRINCIPAL,
            ConfigValueFactory.fromAnyRef(SERVER_PRINCIPAL))
        .withValue(BootStrapContext.SERVICE_KEYTAB_LOCATION,
            ConfigValueFactory.fromAnyRef(serverKeytab.toString()))
        .withValue(ExecConstants.AUTHENTICATION_MECHANISMS,
            ConfigValueFactory.fromIterable(Lists.newArrayList("plain", "kerberos"))),
        false);

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

  private static int getFreePort() throws IOException {
    ServerSocket s = null;
    try {
      s = new ServerSocket(0);
      s.setReuseAddress(true);
      return s.getLocalPort();
    } finally {
      if (s != null) {
        s.close();
      }
    }
  }

  private static void setupUsers(File keytabDir) throws KrbException {
    // Create the client user
    String clientPrincipal = CLIENT_PRINCIPAL.substring(0, CLIENT_PRINCIPAL.indexOf('@'));
    clientKeytab = new File(keytabDir, clientPrincipal.replace('/', '_') + ".keytab");
    logger.debug("Creating {} with keytab {}", clientPrincipal, clientKeytab);
    setupUser(kdc, clientKeytab, clientPrincipal);

    // Create the server user
    String serverPrincipal = SERVER_PRINCIPAL.substring(0, SERVER_PRINCIPAL.indexOf('@'));
    serverKeytab = new File(keytabDir, serverPrincipal.replace('/', '_') + ".keytab");
    logger.debug("Creating {} with keytab {}", SERVER_PRINCIPAL, serverKeytab);
    setupUser(kdc, serverKeytab, SERVER_PRINCIPAL);
  }

  private static void setupUser(SimpleKdcServer kdc, File keytab, String principal)
      throws KrbException {
    kdc.createPrincipal(principal);
    kdc.exportPrincipal(principal, keytab);
  }

  @AfterClass
  public static void stopKdc() throws Exception {
    if (kdcStarted) {
      logger.info("Stopping KDC on {}", kdcPort);
      kdc.stop();
    }

    deleteIfExists(clientKeytab);
    deleteIfExists(serverKeytab);
    deleteIfExists(keytabDir);
    deleteIfExists(kdcDir);
    deleteIfExists(workspace);
    UgiTestUtil.resetUgi();
  }

  private static void deleteIfExists(File file) throws IOException {
    if (file != null) {
      Files.deleteIfExists(file.toPath());
    }
  }

  @Test
  public void successKeytab() throws Exception {
    final Properties connectionProps = new Properties();
    connectionProps.setProperty(DrillProperties.SERVICE_PRINCIPAL, SERVER_PRINCIPAL);
    connectionProps.setProperty(DrillProperties.USER, CLIENT_PRINCIPAL);
    connectionProps.setProperty(DrillProperties.KEYTAB, clientKeytab.getAbsolutePath());
    updateClient(connectionProps);

    // Run few queries using the new client
    testBuilder()
        .sqlQuery("SELECT session_user FROM (SELECT * FROM sys.drillbits LIMIT 1)")
        .unOrdered()
        .baselineColumns("session_user")
        .baselineValues(CLIENT_SHORT_NAME)
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
    connectionProps.setProperty(DrillProperties.SERVICE_PRINCIPAL, SERVER_PRINCIPAL);
    connectionProps.setProperty(DrillProperties.KERBEROS_FROM_SUBJECT, "true");
    final Subject clientSubject = JaasKrbUtil.loginUsingKeytab(CLIENT_PRINCIPAL, clientKeytab.getAbsoluteFile());

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
        .baselineValues(CLIENT_SHORT_NAME)
        .go();
    test("SHOW SCHEMAS");
    test("USE INFORMATION_SCHEMA");
    test("SHOW TABLES");
    test("SELECT * FROM INFORMATION_SCHEMA.`TABLES` WHERE TABLE_NAME LIKE 'COLUMNS'");
    test("SELECT * FROM cp.`region.json` LIMIT 5");
  }
}
