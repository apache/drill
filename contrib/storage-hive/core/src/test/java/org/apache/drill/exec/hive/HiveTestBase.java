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

import org.apache.commons.io.FileUtils;
import org.apache.drill.PlanTestBase;
import org.apache.drill.test.BaseDirTestWatcher;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.UUID;

/**
 * Base class for Hive test. Takes care of generating and adding Hive test plugin before tests and deleting the
 * plugin after tests. Now uses Docker-based Hive for compatibility with Java 11+.
 *
 * <p>Tests are automatically skipped if Docker is not available or container fails to start.
 */
public class HiveTestBase extends PlanTestBase {

  private static final Logger logger = LoggerFactory.getLogger(HiveTestBase.class);

  // Lazy initialization - container is started only when needed
  private static HiveTestFixture hiveTestFixture;
  private static HiveContainer hiveContainer;
  private static String initializationError;
  private static boolean initialized = false;
  private static BaseDirTestWatcher generalDirWatcher;

  // Public accessors for backward compatibility
  public static HiveTestFixture HIVE_TEST_FIXTURE;
  public static HiveContainer HIVE_CONTAINER;

  /**
   * Initializes the Hive Docker container and test fixture.
   * This is called lazily to avoid blocking class loading if Docker is unavailable.
   */
  private static synchronized void initializeHiveInfrastructure() {
    if (initialized) {
      return;
    }
    initialized = true;

    generalDirWatcher = new BaseDirTestWatcher() {
      {
        /*
         * Below protected method invoked to create directory DirWatcher.dir with path like:
         * ./target/org.apache.drill.exec.hive.HiveTestBase123e4567-e89b-12d3-a456-556642440000.
         * Then subdirectory with name 'root' will be used to hold test data shared between
         * all derivatives of the class. Note that UUID suffix is necessary to avoid conflicts between forked JVMs.
         */
        starting(Description.createSuiteDescription(HiveTestBase.class.getName().concat(UUID.randomUUID().toString())));
      }
    };

    try {
      // Check if Docker is available first
      if (!HiveContainer.isDockerAvailable()) {
        initializationError = "Docker is not available. Hive tests will be skipped.";
        logger.warn(initializationError);
        return;
      }

      // Warn about ARM64 performance
      if (HiveContainer.isArm64()) {
        System.out.println("WARNING: Running on ARM64 architecture.");
        System.out.println("Hive Docker tests use x86 emulation and may take 15-30 minutes to start.");
        System.out.println("Consider skipping these tests with: mvn test -Dhive.test.excludedGroups=org.apache.drill.categories.HiveStorageTest");
      }

      // Get shared Docker container instance (starts on first access)
      logger.info("Getting shared Hive Docker container for tests");
      hiveContainer = HiveContainer.getInstance();
      HIVE_CONTAINER = hiveContainer;
      logger.info("Hive container ready");

      System.out.println("Configuring Hive storage plugin for Drill...");
      long setupStart = System.currentTimeMillis();

      File baseDir = generalDirWatcher.getRootDir();
      hiveTestFixture = HiveTestFixture.builderForDocker(baseDir, hiveContainer).build();
      HIVE_TEST_FIXTURE = hiveTestFixture;

      // Note: Test data generation for Docker-based Hive will be done via JDBC in individual tests
      // or test setup methods as needed, since we can't use embedded Hive Driver

      long setupSeconds = (System.currentTimeMillis() - setupStart) / 1000;
      System.out.println("Hive storage plugin configured in " + setupSeconds + " seconds");
      System.out.println("Hive test infrastructure ready!");

      // set hook for clearing resources on JVM shutdown
      Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        FileUtils.deleteQuietly(generalDirWatcher.getDir());
        // Note: Container is shared singleton, will be cleaned up by Testcontainers
      }));
    } catch (Exception e) {
      initializationError = "Failed to initialize Hive container: " + e.getMessage();
      logger.error(initializationError, e);
      // Don't throw - let tests be skipped gracefully
    }
  }

  @BeforeClass
  public static void setUp() {
    // Initialize lazily
    initializeHiveInfrastructure();

    // Skip tests if initialization failed
    Assume.assumeTrue("Hive infrastructure not available: " + initializationError,
        initializationError == null && hiveTestFixture != null);

    hiveTestFixture.getPluginManager().addHivePluginTo(bits);
  }

  @AfterClass
  public static void tearDown() {
    if (hiveTestFixture != null) {
      hiveTestFixture.getPluginManager().removeHivePluginFrom(bits);
    }
  }
}
