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
import org.apache.drill.common.logical.security.PlainCredentialsProvider;
import org.apache.drill.exec.store.enumerable.plan.EnumMockPlugin;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.test.QueryBuilder.QuerySummary;
import org.h2.tools.RunScript;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.FileReader;
import java.net.URL;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@Category(JdbcStorageTest.class)
public class TestJdbcInsertWithH2 extends ClusterTest {

  @BeforeClass
  public static void init() throws Exception {
    startCluster(ClusterFixture.builder(dirTestWatcher));
    // Force timezone to UTC for these tests.
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    dirTestWatcher.copyResourceToRoot(Paths.get(""));

    Class.forName("org.h2.Driver");
    String connString = "jdbc:h2:" + dirTestWatcher.getTmpDir().getCanonicalPath();
    URL scriptFile = TestJdbcPluginWithH2IT.class.getClassLoader().getResource("h2-test-data.sql");
    assertNotNull("Script for test tables generation 'h2-test-data.sql' cannot be found in test resources", scriptFile);
    try (Connection connection = DriverManager.getConnection(connString, "root", "root");
         FileReader fileReader = new FileReader(scriptFile.getFile())) {
      RunScript.execute(connection, fileReader);
    }

    Map<String, String> credentials = new HashMap<>();
    credentials.put("username", "root");
    credentials.put("password", "root");
    PlainCredentialsProvider credentialsProvider = new PlainCredentialsProvider(credentials);

    Map<String, Object> sourceParameters =  new HashMap<>();
    sourceParameters.put("minimumIdle", 1);
    JdbcStorageConfig jdbcStorageConfig = new JdbcStorageConfig("org.h2.Driver", connString,
      "root", "root", true, true, sourceParameters, credentialsProvider, AuthMode.SHARED_USER.name(), 10000);
    jdbcStorageConfig.setEnabled(true);

    JdbcStorageConfig jdbcStorageConfigNoWrite = new JdbcStorageConfig("org.h2.Driver", connString,
      "root", "root", true, false, sourceParameters, credentialsProvider, AuthMode.SHARED_USER.name(), 10000);
    jdbcStorageConfig.setEnabled(true);
    jdbcStorageConfigNoWrite.setEnabled(true);

    cluster.defineStoragePlugin("h2", jdbcStorageConfig);
    cluster.defineStoragePlugin("h2_unwritable", jdbcStorageConfigNoWrite);

    EnumMockPlugin.EnumMockStoragePluginConfig config = new EnumMockPlugin.EnumMockStoragePluginConfig();
    config.setEnabled(true);
    cluster.defineStoragePlugin("mocked_enum", config);
  }

  @Test
  public void testInsertValues() throws Exception {
    String tableName = "h2.tmp.drill_h2_test.`test_table`";
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
    String tableName = "h2.tmp.drill_h2_test.`test_table`";
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
    String tableName = "h2.tmp.drill_h2_test.`test_table`";
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
    String tableName = "h2.tmp.drill_h2_test.`test_table`";
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
