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
package org.apache.drill.exec.impersonation.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import org.apache.drill.categories.HiveStorageTest;
import org.apache.drill.categories.SlowTest;
import org.apache.drill.exec.hive.HiveTestBase;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Simplified version of storage-based authorization test.
 * Tests basic database and table operations without full Hadoop security stack.
 * Original test required HDFS permissions, user/group setup, and authorization providers.
 */
@Category({SlowTest.class, HiveStorageTest.class})
public class TestStorageBasedHiveAuthorization extends HiveTestBase {

  @BeforeClass
  public static void generateTestData() throws Exception {
    String jdbcUrl = String.format("jdbc:hive2://%s:%d/default",
        HIVE_CONTAINER.getHost(),
        HIVE_CONTAINER.getMappedPort(10000));

    try (Connection conn = DriverManager.getConnection(jdbcUrl, "", "");
         Statement stmt = conn.createStatement()) {

      // Create test databases
      stmt.execute("CREATE DATABASE IF NOT EXISTS db_general");
      stmt.execute("CREATE DATABASE IF NOT EXISTS db_test");

      // Create test tables in db_general
      stmt.execute("USE db_general");
      stmt.execute("CREATE TABLE IF NOT EXISTS student(name STRING, age INT, gpa DOUBLE)");
      stmt.execute("INSERT INTO student VALUES ('Alice', 20, 3.5), ('Bob', 22, 3.8)");
      
      stmt.execute("CREATE TABLE IF NOT EXISTS voter(name STRING, age INT, registration_date DATE)");
      stmt.execute("INSERT INTO voter VALUES ('Carol', 25, CAST('2020-01-15' AS DATE))");

      // Create view
      stmt.execute("CREATE VIEW IF NOT EXISTS vw_student AS SELECT name, age FROM student WHERE age > 18");

      // Switch back to default
      stmt.execute("USE default");
    }
  }

  @Test
  public void testReadFromTable() throws Exception {
    testBuilder()
        .sqlQuery("SELECT * FROM hive.db_general.student")
        .unOrdered()
        .baselineColumns("name", "age", "gpa")
        .baselineValues("Alice", 20, 3.5)
        .baselineValues("Bob", 22, 3.8)
        .go();
  }

  @Test
  public void testReadFromView() throws Exception {
    testBuilder()
        .sqlQuery("SELECT * FROM hive.db_general.vw_student")
        .unOrdered()
        .baselineColumns("name", "age")
        .baselineValues("Alice", 20)
        .baselineValues("Bob", 22)
        .go();
  }

  @Test
  public void testShowDatabases() throws Exception {
    testBuilder()
        .sqlQuery("SHOW DATABASES IN hive")
        .unOrdered()
        .baselineColumns("SCHEMA_NAME")
        .baselineValues("hive.default")
        .baselineValues("hive.db_general")
        .baselineValues("hive.db_test")
        .go();
  }

  @Test
  public void testShowTablesInDatabase() throws Exception {
    testBuilder()
        .sqlQuery("SHOW TABLES IN hive.db_general")
        .unOrdered()
        .baselineColumns("TABLE_SCHEMA", "TABLE_NAME")
        .baselineValues("hive.db_general", "student")
        .baselineValues("hive.db_general", "voter")
        .baselineValues("hive.db_general", "vw_student")
        .go();
  }
}
