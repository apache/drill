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
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

@Category(JdbcStorageTest.class)
public class TestJdbcInsertWithPostgres extends ClusterTest {

  private static final String DOCKER_IMAGE_POSTGRES_X86 = "postgres:12.8-alpine3.14";
  private static JdbcDatabaseContainer<?> jdbcContainer;

  @BeforeClass
  public static void initPostgres() throws Exception {
    startCluster(ClusterFixture.builder(dirTestWatcher));
    dirTestWatcher.copyResourceToRoot(Paths.get(""));

    String postgresDBName = "drill_postgres_test";
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));

    DockerImageName imageName = DockerImageName.parse(DOCKER_IMAGE_POSTGRES_X86);
    jdbcContainer = new PostgreSQLContainer<>(imageName)
      .withUsername("postgres")
      .withPassword("password")
      .withDatabaseName(postgresDBName)
      .withInitScript("postgres-test-data.sql");
    jdbcContainer.start();

    Map<String, Object> sourceParameters =  new HashMap<>();
    sourceParameters.put("maximumPoolSize", "16");
    sourceParameters.put("idleTimeout", String.valueOf(TimeUnit.SECONDS.toMillis(5)));
    sourceParameters.put("keepaliveTime", String.valueOf(TimeUnit.SECONDS.toMillis(5)));
    sourceParameters.put("maxLifetime", String.valueOf(TimeUnit.SECONDS.toMillis(20)));
    sourceParameters.put("minimumIdle", "0");

    JdbcStorageConfig jdbcStorageConfig =
      new JdbcStorageConfig("org.postgresql.Driver",
        jdbcContainer.getJdbcUrl(), jdbcContainer.getUsername(), jdbcContainer.getPassword(),
        true, true, sourceParameters, null, AuthMode.SHARED_USER.name(), 10000);
    jdbcStorageConfig.setEnabled(true);
    cluster.defineStoragePlugin("pg", jdbcStorageConfig);

    JdbcStorageConfig unWritableJdbcStorageConfig =
      new JdbcStorageConfig("org.postgresql.Driver",
        jdbcContainer.getJdbcUrl(), jdbcContainer.getUsername(), jdbcContainer.getPassword(),
        true, false, sourceParameters, null, AuthMode.SHARED_USER.name(), 10000);
    unWritableJdbcStorageConfig.setEnabled(true);
    cluster.defineStoragePlugin("pg_unwritable", unWritableJdbcStorageConfig);

  }

  @AfterClass
  public static void stopPostgres() {
    if (jdbcContainer != null) {
      jdbcContainer.stop();
    }
  }

  @Test
  public void testInsertValues() throws Exception {
    String tableName = "`pg`.`public`.`test_table`";
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
    String tableName = "`pg`.`public`.`test_table`";
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
    String tableName = "`pg`.`public`.`test_table`";
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
    String tableName = "`pg`.`public`.`test_table`";
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
}
