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
import org.apache.drill.common.logical.StoragePluginConfig.AuthMode;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.test.QueryBuilder.QuerySummary;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.ext.ScriptUtils;
import org.testcontainers.jdbc.JdbcDatabaseDelegate;
import org.testcontainers.utility.DockerImageName;

import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

@Category(JdbcStorageTest.class)
public class TestJdbcInsertWithMySQL extends ClusterTest {
  private static final String DOCKER_IMAGE_MYSQL = "mysql:5.7.27";
  private static final String DOCKER_IMAGE_MARIADB = "mariadb:10.6.0";
  private static final Logger logger = LoggerFactory.getLogger(TestJdbcInsertWithMySQL.class);
  private static JdbcDatabaseContainer<?> jdbcContainer;

  @BeforeClass
  public static void initMysql() throws Exception {
    startCluster(ClusterFixture.builder(dirTestWatcher));
    dirTestWatcher.copyResourceToRoot(Paths.get(""));

    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
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

    Map<String, Object> sourceParameters =  new HashMap<>();
    sourceParameters.put("maximumPoolSize", "1");
    sourceParameters.put("idleTimeout", String.valueOf(TimeUnit.SECONDS.toMillis(5)));
    sourceParameters.put("keepaliveTime", String.valueOf(TimeUnit.SECONDS.toMillis(5)));
    sourceParameters.put("maxLifetime", String.valueOf(TimeUnit.SECONDS.toMillis(20)));
    sourceParameters.put("minimumIdle", "0");

    String jdbcUrl = jdbcContainer.getJdbcUrl();
    logger.debug("JDBC URL: {}", jdbcUrl);
    JdbcStorageConfig jdbcStorageConfig = new JdbcStorageConfig("com.mysql.cj.jdbc.Driver", jdbcUrl,
      jdbcContainer.getUsername(), jdbcContainer.getPassword(), false, true, sourceParameters, null, AuthMode.SHARED_USER.name(), 10000);
    jdbcStorageConfig.setEnabled(true);

    cluster.defineStoragePlugin("mysql", jdbcStorageConfig);

    JdbcStorageConfig jdbcStorageConfigNoWrite = new JdbcStorageConfig("com.mysql.cj.jdbc.Driver", jdbcUrl,
      jdbcContainer.getUsername(), jdbcContainer.getPassword(), false, false, sourceParameters, null, AuthMode.SHARED_USER.name(), 10000);
    jdbcStorageConfigNoWrite.setEnabled(true);

    cluster.defineStoragePlugin("mysql_no_write", jdbcStorageConfigNoWrite);

    if (osName.startsWith("linux")) {
      // adds storage plugin with case insensitive table names
      JdbcStorageConfig jdbcCaseSensitiveStorageConfig = new JdbcStorageConfig("com.mysql.cj.jdbc.Driver", jdbcUrl,
        jdbcContainer.getUsername(), jdbcContainer.getPassword(), true, true, sourceParameters, null, AuthMode.SHARED_USER.name(), 10000);
      jdbcCaseSensitiveStorageConfig.setEnabled(true);
      cluster.defineStoragePlugin("mysqlCaseInsensitive", jdbcCaseSensitiveStorageConfig);
    }
  }

  @Test
  public void testInsertValues() throws Exception {
    String tableName = "mysql.`drill_mysql_test`.`test_table`";
    try {
      String query = "CREATE TABLE %s (ID, NAME) AS (VALUES(1,2))";
      // Create the table and insert the values
      QuerySummary insertResults = queryBuilder()
        .sql(query, tableName)
        .run();
      assertTrue(insertResults.succeeded());

      String insertQuery = "insert into %s(ID, NAME) VALUES (3,4)";
      queryBuilder()
        .sql(insertQuery, tableName)
        .planMatcher()
        .include("Jdbc\\(sql=\\[INSERT INTO")
        .match();

      testBuilder()
        .sqlQuery(insertQuery, tableName)
        .unOrdered()
        .baselineColumns("ROWCOUNT")
        .baselineValues(1L)
        .go();

      testBuilder()
        .sqlQuery("select * from %s", tableName)
        .unOrdered()
        .baselineColumns("ID", "NAME")
        .baselineValues(1, 2)
        .baselineValues(3, 4)
        .go();
    } finally {
      queryBuilder()
        .sql("DROP TABLE IF EXISTS %s", tableName)
        .run();
    }
  }

  @Test
  public void testInsertSelectValues() throws Exception {
    String tableName = "mysql.`drill_mysql_test`.`test_table`";
    try {
      String query = "CREATE TABLE %s (ID, NAME) AS (VALUES(1,2))";
      // Create the table and insert the values
      QuerySummary insertResults = queryBuilder()
        .sql(query, tableName)
        .run();
      assertTrue(insertResults.succeeded());

      String insertQuery = "INSERT INTO %s SELECT * FROM (VALUES(1,2), (3,4))";
      queryBuilder()
        .sql(insertQuery, tableName)
        .planMatcher()
        .include("Jdbc\\(sql=\\[INSERT INTO")
        .match();

      testBuilder()
        .sqlQuery(insertQuery, tableName)
        .unOrdered()
        .baselineColumns("ROWCOUNT")
        .baselineValues(2L)
        .go();

      testBuilder()
        .sqlQuery("select * from %s", tableName)
        .unOrdered()
        .baselineColumns("ID", "NAME")
        .baselineValues(1, 2)
        .baselineValues(1, 2)
        .baselineValues(3, 4)
        .go();
    } finally {
      queryBuilder()
        .sql("DROP TABLE IF EXISTS %s", tableName)
        .run();
    }
  }

  @Test
  public void testInsertSelectFromJdbcTable() throws Exception {
    String tableName = "mysql.`drill_mysql_test`.`test_table`";
    try {
      String query = "CREATE TABLE %s (ID, NAME) AS (VALUES(1,2), (3,4))";
      // Create the table and insert the values
      QuerySummary insertResults = queryBuilder()
        .sql(query, tableName)
        .run();
      assertTrue(insertResults.succeeded());

      String insertQuery = "INSERT INTO %s SELECT * FROM %s";
      queryBuilder()
        .sql(insertQuery, tableName, tableName)
        .planMatcher()
        .include("Jdbc\\(sql=\\[INSERT INTO")
        .match();

      testBuilder()
        .sqlQuery(insertQuery, tableName, tableName)
        .unOrdered()
        .baselineColumns("ROWCOUNT")
        .baselineValues(2L)
        .go();

      testBuilder()
        .sqlQuery("select * from %s", tableName)
        .unOrdered()
        .baselineColumns("ID", "NAME")
        .baselineValues(1, 2)
        .baselineValues(3, 4)
        .baselineValues(1, 2)
        .baselineValues(3, 4)
        .go();
    } finally {
      queryBuilder()
        .sql("DROP TABLE IF EXISTS %s", tableName)
        .run();
    }
  }

  @Test
  public void testInsertSelectFromNonJdbcTable() throws Exception {
    String tableName = "mysql.`drill_mysql_test`.`test_table`";
    try {
      String query = "CREATE TABLE %s (ID, NAME) AS (VALUES(1,2))";
      // Create the table and insert the values
      QuerySummary insertResults = queryBuilder()
        .sql(query, tableName)
        .run();
      assertTrue(insertResults.succeeded());

      String insertQuery = "INSERT INTO %s SELECT n_nationkey, n_regionkey FROM cp.`tpch/nation.parquet` limit 3";
      queryBuilder()
        .sql(insertQuery, tableName, tableName)
        .planMatcher()
        .exclude("Jdbc\\(sql=\\[INSERT INTO") // insert cannot be pushed down
        .match();

      testBuilder()
        .sqlQuery(insertQuery, tableName, tableName)
        .unOrdered()
        .baselineColumns("ROWCOUNT")
        .baselineValues(3L)
        .go();

      testBuilder()
        .sqlQuery("select * from %s", tableName)
        .unOrdered()
        .baselineColumns("ID", "NAME")
        .baselineValues(1, 2)
        .baselineValues(0, 0)
        .baselineValues(1, 1)
        .baselineValues(2, 1)
        .go();
    } finally {
      queryBuilder()
        .sql("DROP TABLE IF EXISTS %s", tableName)
        .run();
    }
  }

  @AfterClass
  public static void stopMysql() {
    if (jdbcContainer != null) {
      jdbcContainer.stop();
    }
  }
}
