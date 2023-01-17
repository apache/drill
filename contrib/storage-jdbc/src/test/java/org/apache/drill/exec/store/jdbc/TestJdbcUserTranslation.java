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

package org.apache.drill.exec.store.jdbc;

import org.apache.drill.categories.JdbcStorageTest;
import org.apache.drill.common.config.DrillProperties;
import org.apache.drill.common.logical.security.PlainCredentialsProvider;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.physical.rowSet.RowSetBuilder;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl;
import org.apache.drill.test.ClientFixture;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.test.QueryBuilder.QuerySummary;
import org.apache.drill.test.rowSet.RowSetUtilities;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.ext.ScriptUtils;
import org.testcontainers.jdbc.JdbcDatabaseDelegate;
import org.testcontainers.utility.DockerImageName;

import java.util.HashMap;

import static org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl.TEST_USER_1;
import static org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl.TEST_USER_1_PASSWORD;
import static org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl.TEST_USER_2;
import static org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl.TEST_USER_2_PASSWORD;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category(JdbcStorageTest.class)
public class TestJdbcUserTranslation extends ClusterTest {

  private static final String DOCKER_IMAGE_MYSQL = "mysql:5.7.27";
  private static final String DOCKER_IMAGE_MARIADB = "mariadb:10.6.0";
  private static JdbcDatabaseContainer<?> jdbcContainer;
  private static final String PLUGIN_NAME = "mysql";

  @BeforeClass
  public static void initMysql() throws Exception {
    ClusterFixtureBuilder builder = new ClusterFixtureBuilder(dirTestWatcher)
      .configProperty(ExecConstants.HTTP_ENABLE, true)
      .configProperty(ExecConstants.HTTP_PORT_HUNT, true)
      .configProperty(ExecConstants.USER_AUTHENTICATOR_IMPL, UserAuthenticatorTestImpl.TYPE)
      .configProperty(ExecConstants.IMPERSONATION_ENABLED, true);

    startCluster(builder);

    String osName = System.getProperty("os.name").toLowerCase();
    String mysqlDBName = "drill_mysql_test";

    DockerImageName imageName;
    if (osName.startsWith("linux") && "aarch64".equals(System.getProperty("os.arch"))) {
      imageName = DockerImageName.parse(DOCKER_IMAGE_MARIADB).asCompatibleSubstituteFor("mysql");
    } else {
      imageName = DockerImageName.parse(DOCKER_IMAGE_MYSQL);
    }

    jdbcContainer = new MySQLContainer<>(imageName)
      .withExposedPorts(3306)
      .withConfigurationOverride("mysql_config_override")
      .withUsername("mysqlUser")
      .withPassword("mysqlPass")
      .withDatabaseName(mysqlDBName)
      .withUrlParam("serverTimezone", "UTC")
      .withUrlParam("useJDBCCompliantTimezoneShift", "true")
      .withInitScript("mysql-test-data.sql");
    jdbcContainer.start();

    if (osName.startsWith("linux")) {
      JdbcDatabaseDelegate databaseDelegate = new JdbcDatabaseDelegate(jdbcContainer, "");
      ScriptUtils.runInitScript(databaseDelegate, "mysql-test-data-linux.sql");
    }

    PlainCredentialsProvider credentialsProvider = new PlainCredentialsProvider(new HashMap<>());

    String jdbcUrl = jdbcContainer.getJdbcUrl();
    JdbcStorageConfig jdbcStorageConfig = new JdbcStorageConfig("com.mysql.cj.jdbc.Driver", jdbcUrl,
      jdbcContainer.getUsername(), jdbcContainer.getPassword(), false, false,
      null, credentialsProvider, "user_translation", 10000);
    jdbcStorageConfig.setEnabled(true);

    cluster.defineStoragePlugin(PLUGIN_NAME, jdbcStorageConfig);
  }

  @AfterClass
  public static void stopMysql() {
    if (jdbcContainer != null) {
      jdbcContainer.stop();
    }
  }

  @Test
  public void testShowDatabasesWithUserWithNoCreds() throws Exception {
    // This test verifies that a user without credentials to a JDBC data source is able to query
    // Drill without causing various errors and NPEs.
    ClientFixture client = cluster.clientBuilder()
      .property(DrillProperties.USER, TEST_USER_2)
      .property(DrillProperties.PASSWORD, TEST_USER_2_PASSWORD)
      .build();

    String sql = "SHOW DATABASES";
    QuerySummary results = client.queryBuilder().sql(sql).run();
    assertTrue(results.succeeded());
    assertEquals(results.recordCount(), 7);
  }

  @Test
  public void testShowDatabasesWithUserWithValidCreds() throws Exception {
    ClientFixture client = cluster.clientBuilder()
      .property(DrillProperties.USER, TEST_USER_1)
      .property(DrillProperties.PASSWORD, TEST_USER_1_PASSWORD)
      .build();

    // Add the credentials to the user
    JdbcStorageConfig pluginConfig = (JdbcStorageConfig) cluster.storageRegistry().getPlugin(PLUGIN_NAME).getConfig();
    PlainCredentialsProvider credentialProvider = (PlainCredentialsProvider) pluginConfig.getCredentialsProvider();
    credentialProvider.setUserCredentials("mysqlUser", "mysqlPass", TEST_USER_1);
    pluginConfig.updateCredentialProvider(credentialProvider);

    String sql = "SHOW DATABASES";
    QuerySummary results = client.queryBuilder().sql(sql).run();
    assertTrue(results.succeeded());
    assertEquals(10, results.recordCount());
  }

  @Test
  public void testQueryWithInvalidCredentials() {
    // This test attempts to actually execute a query against a MySQL database with invalid credentials.
    // The query should fail, but Drill should not crash.
    ClientFixture client = cluster.clientBuilder()
      .property(DrillProperties.USER, TEST_USER_2)
      .property(DrillProperties.PASSWORD, TEST_USER_2_PASSWORD)
      .build();

    String sql = "SELECT * FROM mysql.`drill_mysql_test`.person";
    try {
      client.queryBuilder().sql(sql).rowSet();
      fail();
    } catch (Exception e) {
      assertThat(e.getMessage(), containsString("Object 'mysql' not found"));
    }
  }

  @Test
  public void testQueryWithValidCredentials() throws Exception {
    // This test validates that a user can query a JDBC data source with valid credentials
    // and user translation enabled.
    ClientFixture client = cluster.clientBuilder()
      .property(DrillProperties.USER, TEST_USER_1)
      .property(DrillProperties.PASSWORD, TEST_USER_1_PASSWORD)
      .build();

    // Add the credentials to the user
    JdbcStorageConfig pluginConfig = (JdbcStorageConfig) cluster.storageRegistry().getPlugin(PLUGIN_NAME).getConfig();
    PlainCredentialsProvider credentialProvider = (PlainCredentialsProvider) pluginConfig.getCredentialsProvider();
    credentialProvider.setUserCredentials("mysqlUser", "mysqlPass", TEST_USER_1);
    pluginConfig.updateCredentialProvider(credentialProvider);

    String sql = "SELECT first_name, last_name FROM mysql.`drill_mysql_test`.person";
    RowSet results = client.queryBuilder().sql(sql).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
      .addNullable("first_name", MinorType.VARCHAR, 38)
      .addNullable("last_name", MinorType.VARCHAR,38)
      .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
      .addRow("first_name_1", "last_name_1")
      .addRow("first_name_2", "last_name_2")
      .addRow("first_name_3", "last_name_3")
      .addRow(null, null)
      .build();

    RowSetUtilities.verify(expected, results);
  }
}
