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
 * Simplified version of SQL standard authorization test.
 * Tests basic database and table operations without full authorization framework.
 * Original test required SQL standard authorization, GRANT/REVOKE, and role management.
 */
@Category({SlowTest.class, HiveStorageTest.class})
public class TestSqlStdBasedAuthorization extends HiveTestBase {

  @BeforeClass
  public static void generateTestData() throws Exception {
    String jdbcUrl = String.format("jdbc:hive2://%s:%d/default",
        HIVE_CONTAINER.getHost(),
        HIVE_CONTAINER.getMappedPort(10000));

    try (Connection conn = DriverManager.getConnection(jdbcUrl, "", "");
         Statement stmt = conn.createStatement()) {

      // Create test database
      stmt.execute("CREATE DATABASE IF NOT EXISTS db_general");
      stmt.execute("USE db_general");

      // Create test tables
      stmt.execute("CREATE TABLE IF NOT EXISTS student_user0(name STRING, age INT, gpa DOUBLE)");
      stmt.execute("INSERT INTO student_user0 VALUES ('David', 21, 3.7), ('Eve', 23, 3.9)");

      stmt.execute("CREATE TABLE IF NOT EXISTS voter_role0(name STRING, registered BOOLEAN)");
      stmt.execute("INSERT INTO voter_role0 VALUES ('Frank', true), ('Grace', false)");

      // Create views
      stmt.execute("CREATE VIEW IF NOT EXISTS vw_student_user0 AS SELECT name FROM student_user0");
      stmt.execute("CREATE VIEW IF NOT EXISTS vw_voter_role0 AS SELECT * FROM voter_role0 WHERE registered = true");

      stmt.execute("USE default");
    }
  }

  @Test
  public void testSelectOnTable() throws Exception {
    testBuilder()
        .sqlQuery("SELECT * FROM hive.db_general.student_user0")
        .unOrdered()
        .baselineColumns("name", "age", "gpa")
        .baselineValues("David", 21, 3.7)
        .baselineValues("Eve", 23, 3.9)
        .go();
  }

  @Test
  public void testSelectOnView() throws Exception {
    testBuilder()
        .sqlQuery("SELECT * FROM hive.db_general.vw_student_user0")
        .unOrdered()
        .baselineColumns("name")
        .baselineValues("David")
        .baselineValues("Eve")
        .go();
  }

  @Test
  public void testSelectOnTableWithRole() throws Exception {
    testBuilder()
        .sqlQuery("SELECT * FROM hive.db_general.voter_role0")
        .unOrdered()
        .baselineColumns("name", "registered")
        .baselineValues("Frank", true)
        .baselineValues("Grace", false)
        .go();
  }

  @Test
  public void testSelectOnViewWithRole() throws Exception {
    testBuilder()
        .sqlQuery("SELECT * FROM hive.db_general.vw_voter_role0")
        .unOrdered()
        .baselineColumns("name", "registered")
        .baselineValues("Frank", true)
        .go();
  }
}
