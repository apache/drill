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
package org.apache.drill.exec.store.phoenix.secured;

import static org.apache.hadoop.hbase.HConstants.HBASE_DIR;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.LocalHBaseCluster;
import org.apache.hadoop.hbase.security.HBaseKerberosUtils;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.phoenix.query.ConfigurationFactory;
import org.apache.phoenix.util.InstanceResolver;
import org.apache.phoenix.util.PhoenixRuntime;

/**
 * This is a copy of class from `org.apache.phoenix:phoenix-queryserver-it`,
 * see original javadoc in {@code org.apache.phoenix.end2end.QueryServerEnvironment}.
 * <p>
 * It is possible to use original QueryServerEnvironment, but need to solve several issues:
 * <ul>
 *   <li>TlsUtil.getClasspathDir(QueryServerEnvironment.class); in QueryServerEnvironment fails due to the path from jar.
 *   Can be fixed with copying TlsUtil in Drill project and changing getClasspathDir method to use
 *   SecuredPhoenixTestSuite.class instead of QueryServerEnvironment.class</li>
 *   <li>SERVICE_PRINCIPAL from QueryServerEnvironment is for `securecluster` not system user. So Test fails later
 *   in process of starting Drill cluster while creating udf area RemoteFunctionRegistry#createArea, it fails
 *   on checking Precondition ImpersonationUtil.getProcessUserName().equals(fileStatus.getOwner()),
 *   where ImpersonationUtil.getProcessUserName() is 'securecluster' and fileStatus.getOwner() is
 *   your local machine login user</li>
 * </ul>
 */
public class PhoenixEnvironment {

  private final File tempDir = new File(getTempDir());
  private final File keytabDir = new File(tempDir, "keytabs");
  private final List<File> userKeytabFiles = new ArrayList<>();

  private static final String LOCAL_HOST_REVERSE_DNS_LOOKUP_NAME;
  static final String LOGIN_USER;

  static {
    try {
      // uncomment it for debugging purposes
      // System.setProperty("sun.security.krb5.debug", "true");
      LOCAL_HOST_REVERSE_DNS_LOOKUP_NAME = InetAddress.getByName("127.0.0.1").getCanonicalHostName();
      String userName = System.getProperty("user.name");
      LOGIN_USER = userName != null ? userName : "securecluster";
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static final String SPNEGO_PRINCIPAL = "HTTP/" + LOCAL_HOST_REVERSE_DNS_LOOKUP_NAME;
  private static final String PQS_PRINCIPAL = "phoenixqs/" + LOCAL_HOST_REVERSE_DNS_LOOKUP_NAME;
  private static final String SERVICE_PRINCIPAL = LOGIN_USER + "/" + LOCAL_HOST_REVERSE_DNS_LOOKUP_NAME;
  private final File keytab;

  private final MiniKdc kdc;
  private final HBaseTestingUtility util = new HBaseTestingUtility(conf());
  private final LocalHBaseCluster hbaseCluster;
  private int numCreatedUsers;

  private final String phoenixUrl;

  private static Configuration conf() {
    Configuration configuration = HBaseConfiguration.create();
    configuration.set(User.HBASE_SECURITY_CONF_KEY, "kerberos");
    return configuration;
  }

  private static String getTempDir() {
    StringBuilder sb = new StringBuilder(32);
    sb.append(System.getProperty("user.dir")).append(File.separator);
    sb.append("target").append(File.separator);
    sb.append(PhoenixEnvironment.class.getSimpleName());
    sb.append("-").append(UUID.randomUUID());
    return sb.toString();
  }

  public String getPhoenixUrl() {
    return phoenixUrl;
  }

  public HBaseTestingUtility getUtil() {
    return util;
  }

  public File getServiceKeytab() {
    return keytab;
  }

  private static void updateDefaultRealm() throws Exception {
    // (at least) one other phoenix test triggers the caching of this field before the KDC is up
    // which causes principal parsing to fail.
    Field f = KerberosName.class.getDeclaredField("defaultRealm");
    f.setAccessible(true);
    // Default realm for MiniKDC
    f.set(null, "EXAMPLE.COM");
  }

  private void createUsers(int numUsers) throws Exception {
    assertNotNull("KDC is null, was setup method called?", kdc);
    numCreatedUsers = numUsers;
    for (int i = 1; i <= numUsers; i++) {
      String principal = "user" + i;
      File keytabFile = new File(keytabDir, principal + ".keytab");
      kdc.createPrincipal(keytabFile, principal);
      userKeytabFiles.add(keytabFile);
    }
  }

  public Map.Entry<String, File> getUser(int offset) {
    if (!(offset > 0 && offset <= numCreatedUsers)) {
      throw new IllegalArgumentException();
    }
    return new AbstractMap.SimpleImmutableEntry<>("user" + offset, userKeytabFiles.get(offset - 1));
  }

  /**
   * Setup the security configuration for hdfs.
   */
  private void setHdfsSecuredConfiguration(Configuration conf) throws Exception {
    // Set principal+keytab configuration for HDFS
    conf.set(DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY,
      SERVICE_PRINCIPAL + "@" + kdc.getRealm());
    conf.set(DFSConfigKeys.DFS_NAMENODE_KEYTAB_FILE_KEY, keytab.getAbsolutePath());
    conf.set(DFSConfigKeys.DFS_DATANODE_KERBEROS_PRINCIPAL_KEY,
      SERVICE_PRINCIPAL + "@" + kdc.getRealm());
    conf.set(DFSConfigKeys.DFS_DATANODE_KEYTAB_FILE_KEY, keytab.getAbsolutePath());
    conf.set(DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY,
      SPNEGO_PRINCIPAL + "@" + kdc.getRealm());
    // Enable token access for HDFS blocks
    conf.setBoolean(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
    // Only use HTTPS (required because we aren't using "secure" ports)
    conf.set(DFSConfigKeys.DFS_HTTP_POLICY_KEY, HttpConfig.Policy.HTTPS_ONLY.name());
    // Bind on localhost for spnego to have a chance at working
    conf.set(DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_KEY, "localhost:0");
    conf.set(DFSConfigKeys.DFS_DATANODE_HTTPS_ADDRESS_KEY, "localhost:0");

    // Generate SSL certs
    File keystoresDir = new File(util.getDataTestDir("keystore").toUri().getPath());
    keystoresDir.mkdirs();

    // Magic flag to tell hdfs to not fail on using ports above 1024
    conf.setBoolean("ignore.secure.ports.for.testing", true);
  }

  private static void ensureIsEmptyDirectory(File f) throws IOException {
    if (f.exists()) {
      if (f.isDirectory()) {
        FileUtils.deleteDirectory(f);
      } else {
        assertTrue("Failed to delete keytab directory", f.delete());
      }
    }
    assertTrue("Failed to create keytab directory", f.mkdirs());
  }

  /**
   * Setup and start kerberosed, hbase
   */
  public PhoenixEnvironment(final Configuration confIn, int numberOfUsers, boolean tls)
    throws Exception {

    Configuration conf = util.getConfiguration();
    conf.addResource(confIn);
    // Ensure the dirs we need are created/empty
    ensureIsEmptyDirectory(tempDir);
    ensureIsEmptyDirectory(keytabDir);
    keytab = new File(keytabDir, "test.keytab");
    // Start a MiniKDC
    kdc = util.setupMiniKdc(keytab);
    // Create a service principal and spnego principal in one keytab
    // NB. Due to some apparent limitations between HDFS and HBase in the same JVM, trying to
    // use separate identies for HBase and HDFS results in a GSS initiate error. The quick
    // solution is to just use a single "service" principal instead of "hbase" and "hdfs"
    // (or "dn" and "nn") per usual.
    kdc.createPrincipal(keytab, SPNEGO_PRINCIPAL, PQS_PRINCIPAL, SERVICE_PRINCIPAL);
    // Start ZK by hand
    util.startMiniZKCluster();

    // Create a number of unprivileged users
    createUsers(numberOfUsers);

    // Set configuration for HBase
    HBaseKerberosUtils.setPrincipalForTesting(SERVICE_PRINCIPAL + "@" + kdc.getRealm());
    HBaseKerberosUtils.setSecuredConfiguration(conf);
    setHdfsSecuredConfiguration(conf);
    UserGroupInformation.setConfiguration(conf);
    conf.setInt(HConstants.MASTER_PORT, 0);
    conf.setInt(HConstants.MASTER_INFO_PORT, 0);
    conf.setInt(HConstants.REGIONSERVER_PORT, 0);
    conf.setInt(HConstants.REGIONSERVER_INFO_PORT, 0);

    // Clear the cached singletons so we can inject our own.
    InstanceResolver.clearSingletons();
    // Make sure the ConnectionInfo doesn't try to pull a default Configuration
    InstanceResolver.getSingleton(ConfigurationFactory.class, new ConfigurationFactory() {
      @Override
      public Configuration getConfiguration() {
        return conf;
      }

      @Override
      public Configuration getConfiguration(Configuration confToClone) {
        Configuration copy = new Configuration(conf);
        copy.addResource(confToClone);
        return copy;
      }
    });
    updateDefaultRealm();

    // Use LocalHBaseCluster to avoid HBaseTestingUtility from doing something wrong
    // NB. I'm not actually sure what HTU does incorrect, but this was pulled from some test
    // classes in HBase itself. I couldn't get HTU to work myself (2017/07/06)
    Path rootdir = util.getDataTestDirOnTestFS(PhoenixEnvironment.class.getSimpleName());
    // There is no setRootdir method that is available in all supported HBase versions.
    conf.set(HBASE_DIR, rootdir.toString());
    hbaseCluster = new LocalHBaseCluster(conf, 1);
    hbaseCluster.startup();

    phoenixUrl = PhoenixRuntime.JDBC_PROTOCOL + ":localhost:" + getZookeeperPort();
  }

  public int getZookeeperPort() {
    return util.getConfiguration().getInt(HConstants.ZOOKEEPER_CLIENT_PORT, 2181);
  }

  public void stop() throws Exception {
    // Remove our custom ConfigurationFactory for future tests
    InstanceResolver.clearSingletons();
    if (hbaseCluster != null) {
      hbaseCluster.shutdown();
      hbaseCluster.join();
    }
    util.shutdownMiniCluster();
    if (kdc != null) {
      kdc.stop();
    }
    util.closeConnection();
  }
}
