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
package org.apache.drill.exec.store.accumulo;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.drill.test.BaseTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration test suite for Accumulo storage plugin.
 *
 * <p>This suite manages the lifecycle of a MiniAccumuloCluster and runs
 * all integration tests that require a real Accumulo instance.</p>
 *
 * <p>Run with: {@code mvn test -Dtest=AccumuloIntegrationTestsSuite}</p>
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    AccumuloBasicQueryTest.class,
    AccumuloPushdownIntegrationTest.class
})
public class AccumuloIntegrationTestsSuite extends BaseTest {
  private static final Logger logger = LoggerFactory.getLogger(AccumuloIntegrationTestsSuite.class);

  public static final String ROOT_USER = "root";
  public static final String ROOT_PASSWORD = "drilltest";
  public static final String INSTANCE_NAME = "drill-accumulo-test";

  private static MiniAccumuloCluster miniCluster;
  private static AccumuloClient client;
  private static File tempDir;
  private static volatile AtomicInteger initCount = new AtomicInteger(0);
  private static boolean clusterStarted = false;
  private static boolean tablesCreated = false;

  /**
   * Whether to manage the MiniAccumuloCluster (start/stop).
   * Set to false to use an external Accumulo instance.
   */
  private static boolean manageMiniCluster = Boolean.parseBoolean(
      System.getProperty("drill.accumulo.tests.managed", "true"));

  /**
   * Whether to create test tables.
   */
  private static boolean createTables = Boolean.parseBoolean(
      System.getProperty("drill.accumulo.tests.createTables", "true"));

  @BeforeClass
  public static void initCluster() throws Exception {
    if (initCount.get() == 0) {
      synchronized (AccumuloIntegrationTestsSuite.class) {
        if (initCount.get() == 0) {
          if (manageMiniCluster) {
            startMiniCluster();
          } else {
            connectToExternalCluster();
          }

          if (createTables) {
            createTestTables();
          }

          initCount.incrementAndGet();
          return;
        }
      }
    }
    initCount.incrementAndGet();
  }

  @AfterClass
  public static void tearDownCluster() throws Exception {
    synchronized (AccumuloIntegrationTestsSuite.class) {
      if (initCount.decrementAndGet() == 0) {
        if (createTables && tablesCreated) {
          cleanupTestTables();
        }

        if (client != null) {
          client.close();
          client = null;
        }

        if (clusterStarted && miniCluster != null) {
          logger.info("Stopping MiniAccumuloCluster...");
          miniCluster.stop();
          miniCluster = null;
          logger.info("MiniAccumuloCluster stopped.");
        }

        // Clean up temp directory
        if (tempDir != null && tempDir.exists()) {
          deleteDirectory(tempDir);
        }
      }
    }
  }

  private static void startMiniCluster() throws Exception {
    logger.info("Starting MiniAccumuloCluster...");

    // Create temp directory for cluster data
    tempDir = new File(System.getProperty("accumulo.test.root",
        System.getProperty("java.io.tmpdir")), "mini-accumulo-" + System.currentTimeMillis());
    if (!tempDir.mkdirs()) {
      throw new IOException("Failed to create temp directory: " + tempDir);
    }

    MiniAccumuloConfig config = new MiniAccumuloConfig(tempDir, ROOT_PASSWORD);
    config.setInstanceName(INSTANCE_NAME);
    config.setNumTservers(1);

    miniCluster = new MiniAccumuloCluster(config);
    miniCluster.start();
    clusterStarted = true;

    // Create client
    client = miniCluster.createAccumuloClient(ROOT_USER, new PasswordToken(ROOT_PASSWORD));

    logger.info("MiniAccumuloCluster started. Instance: {}, ZooKeepers: {}",
        miniCluster.getInstanceName(), miniCluster.getZooKeepers());
  }

  private static void connectToExternalCluster() throws Exception {
    String zookeepers = System.getProperty("drill.accumulo.zookeepers", "localhost:2181");
    String instanceName = System.getProperty("drill.accumulo.instance", "accumulo");
    String user = System.getProperty("drill.accumulo.user", "root");
    String password = System.getProperty("drill.accumulo.password", "secret");

    logger.info("Connecting to external Accumulo instance: {} at {}", instanceName, zookeepers);

    client = org.apache.accumulo.core.client.Accumulo.newClient()
        .to(instanceName, zookeepers)
        .as(user, password)
        .build();
  }

  private static void createTestTables() throws Exception {
    logger.info("Creating test tables...");
    AccumuloTestUtils.createAllTestTables(client);
    tablesCreated = true;
    logger.info("Test tables created.");
  }

  private static void cleanupTestTables() {
    try {
      logger.info("Cleaning up test tables...");
      AccumuloTestUtils.deleteAllTestTables(client);
      logger.info("Test tables cleaned up.");
    } catch (Exception e) {
      logger.warn("Error cleaning up test tables", e);
    }
  }

  private static void deleteDirectory(File dir) {
    File[] files = dir.listFiles();
    if (files != null) {
      for (File file : files) {
        if (file.isDirectory()) {
          deleteDirectory(file);
        } else {
          file.delete();
        }
      }
    }
    dir.delete();
  }

  // Public accessors for test classes

  public static MiniAccumuloCluster getMiniCluster() {
    return miniCluster;
  }

  public static AccumuloClient getClient() {
    return client;
  }

  public static String getZooKeepers() {
    if (miniCluster != null) {
      return miniCluster.getZooKeepers();
    }
    return System.getProperty("drill.accumulo.zookeepers", "localhost:2181");
  }

  public static String getInstanceName() {
    if (miniCluster != null) {
      return miniCluster.getInstanceName();
    }
    return System.getProperty("drill.accumulo.instance", "accumulo");
  }

  public static String getRootUser() {
    return ROOT_USER;
  }

  public static String getRootPassword() {
    return ROOT_PASSWORD;
  }

  public static void configure(boolean manageMiniCluster, boolean createTables) {
    AccumuloIntegrationTestsSuite.manageMiniCluster = manageMiniCluster;
    AccumuloIntegrationTestsSuite.createTables = createTables;
  }
}
