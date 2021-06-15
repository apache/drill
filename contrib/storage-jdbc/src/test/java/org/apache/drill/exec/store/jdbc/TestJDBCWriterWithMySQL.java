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
import org.apache.drill.exec.physical.rowSet.DirectRowSet;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.ext.ScriptUtils;
import org.testcontainers.jdbc.JdbcDatabaseDelegate;
import org.testcontainers.utility.DockerImageName;

import static org.apache.drill.test.BaseTestQuery.test;

/**
 * JDBC storage plugin tests against MySQL.
 * Note: it requires libaio1.so library on Linux
 */

@Category(JdbcStorageTest.class)
public class TestJDBCWriterWithMySQL extends ClusterTest {
  private static final String DOCKER_IMAGE_MYSQL = "mysql:5.7.27";
  private static final String DOCKER_IMAGE_MARIADB = "mariadb:10.6.0";
  private static JdbcDatabaseContainer<?> jdbcContainer;

  @BeforeClass
  public static void initMysql() throws Exception {
    startCluster(ClusterFixture.builder(dirTestWatcher));
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

    String jdbcUrl = jdbcContainer.getJdbcUrl();
    JdbcStorageConfig jdbcStorageConfig = new JdbcStorageConfig("com.mysql.cj.jdbc.Driver", jdbcUrl,
      jdbcContainer.getUsername(), jdbcContainer.getPassword(), false, null, null);
    jdbcStorageConfig.setEnabled(true);

    cluster.defineStoragePlugin("mysql", jdbcStorageConfig);

    if (osName.startsWith("linux")) {
      // adds storage plugin with case insensitive table names
      JdbcStorageConfig jdbcCaseSensitiveStorageConfig = new JdbcStorageConfig("com.mysql.cj.jdbc.Driver", jdbcUrl,
        jdbcContainer.getUsername(), jdbcContainer.getPassword(), true, null, null);
      jdbcCaseSensitiveStorageConfig.setEnabled(true);
      cluster.defineStoragePlugin("mysqlCaseInsensitive", jdbcCaseSensitiveStorageConfig);
    }
  }

  @Test
  public void testBasicCTAS() throws Exception {
    String query = "CREATE TABLE  mysql.`drill_mysql_test`.`test_table` (ID, NAME) AS SELECT 1, 'bob' FROM (VALUES(1))";

    DirectRowSet results = queryBuilder().sql(query).rowSet();
    results.print();
  }

  @AfterClass
  public static void stopMysql() {
    if (jdbcContainer != null) {
      jdbcContainer.stop();
    }
  }

}
