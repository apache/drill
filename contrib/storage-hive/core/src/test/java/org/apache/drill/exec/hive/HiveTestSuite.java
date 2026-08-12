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
package org.apache.drill.exec.hive;

import org.apache.drill.categories.HiveStorageTest;
import org.apache.drill.categories.SlowTest;
import org.apache.drill.test.BaseTest;
import org.apache.drill.test.BaseDirTestWatcher;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test suite for Hive storage plugin tests using Docker container.
 * This suite manages the lifecycle of a Hive container and provides
 * connection details to test classes.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
  // Test classes will be added here
})
@Category({SlowTest.class, HiveStorageTest.class})
public class HiveTestSuite extends BaseTest {

  private static final Logger logger = LoggerFactory.getLogger(HiveTestSuite.class);

  @ClassRule
  public static final BaseDirTestWatcher dirTestWatcher = new BaseDirTestWatcher();

  private static HiveContainer hiveContainer;
  private static String metastoreUri;
  private static String jdbcUrl;
  private static final AtomicInteger initCount = new AtomicInteger(0);

  /**
   * Gets the metastore URI for connecting to Hive metastore.
   *
   * @return Metastore URI
   */
  public static String getMetastoreUri() {
    return metastoreUri;
  }

  /**
   * Gets the JDBC URL for connecting to HiveServer2.
   *
   * @return JDBC URL
   */
  public static String getJdbcUrl() {
    return jdbcUrl;
  }

  /**
   * Gets the Hive container instance.
   *
   * @return HiveContainer instance
   */
  public static HiveContainer getHiveContainer() {
    return hiveContainer;
  }

  /**
   * Gets the base directory for test data.
   *
   * @return Base directory
   */
  public static File getBaseDir() {
    return dirTestWatcher.getRootDir();
  }

  @BeforeClass
  public static void initHive() throws Exception {
    synchronized (HiveTestSuite.class) {
      if (initCount.get() == 0) {
        logger.info("Getting shared Hive container for tests");

        // Get shared Hive container instance
        hiveContainer = HiveContainer.getInstance();

        metastoreUri = hiveContainer.getMetastoreUri();
        jdbcUrl = hiveContainer.getJdbcUrl();

        logger.info("Hive container started successfully");
        logger.info("Metastore URI: {}", metastoreUri);
        logger.info("JDBC URL: {}", jdbcUrl);

        // Generate test data
        generateTestData();
      }
      initCount.incrementAndGet();
    }
  }

  /**
   * Generates test data in the Hive instance.
   */
  private static void generateTestData() {
    logger.info("Generating test data in Hive");
    try (Connection connection = getConnection();
         Statement statement = connection.createStatement()) {

      // Create a simple test table to verify connectivity
      statement.execute("CREATE DATABASE IF NOT EXISTS default");
      statement.execute("USE default");

      logger.info("Test data generation completed");
    } catch (Exception e) {
      logger.error("Failed to generate test data", e);
      throw new RuntimeException("Failed to generate test data", e);
    }
  }

  /**
   * Gets a JDBC connection to HiveServer2.
   *
   * @return JDBC Connection
   * @throws SQLException if connection fails
   */
  public static Connection getConnection() throws SQLException {
    try {
      Class.forName("org.apache.hive.jdbc.HiveDriver");
    } catch (ClassNotFoundException e) {
      throw new SQLException("Hive JDBC driver not found", e);
    }
    return DriverManager.getConnection(jdbcUrl);
  }

  /**
   * Executes a Hive query using JDBC.
   *
   * @param query SQL query to execute
   * @throws SQLException if query execution fails
   */
  public static void executeQuery(String query) throws SQLException {
    try (Connection connection = getConnection();
         Statement statement = connection.createStatement()) {
      statement.execute(query);
    }
  }

  @AfterClass
  public static void tearDownHive() {
    synchronized (HiveTestSuite.class) {
      if (initCount.decrementAndGet() == 0) {
        // Container is shared singleton, will be cleaned up by Testcontainers at JVM shutdown
        logger.info("Test suite finished, container will be reused for other tests");
      }
    }
  }
}
