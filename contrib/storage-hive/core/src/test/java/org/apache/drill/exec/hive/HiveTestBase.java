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
import org.junit.BeforeClass;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.UUID;

/**
 * Base class for Hive test. Takes care of generating and adding Hive test plugin before tests and deleting the
 * plugin after tests. Now uses Docker-based Hive for compatibility with Java 11+.
 */
public class HiveTestBase extends PlanTestBase {

  private static final Logger logger = LoggerFactory.getLogger(HiveTestBase.class);

  public static final HiveTestFixture HIVE_TEST_FIXTURE;
  public static final HiveContainer HIVE_CONTAINER;

  static {
    // generate hive data common for all test classes using own dirWatcher
    BaseDirTestWatcher generalDirWatcher = new BaseDirTestWatcher() {
      {
      /*
         Below protected method invoked to create directory DirWatcher.dir with path like:
         ./target/org.apache.drill.exec.hive.HiveTestBase123e4567-e89b-12d3-a456-556642440000.
         Then subdirectory with name 'root' will be used to hold test data shared between
         all derivatives of the class. Note that UUID suffix is necessary to avoid conflicts between forked JVMs.
      */
        starting(Description.createSuiteDescription(HiveTestBase.class.getName().concat(UUID.randomUUID().toString())));
      }
    };

    try {
      // Get shared Docker container instance (starts on first access)
      logger.info("Getting shared Hive Docker container for tests");
      HIVE_CONTAINER = HiveContainer.getInstance();
      logger.info("Hive container ready");

      System.out.println("Configuring Hive storage plugin for Drill...");
      long setupStart = System.currentTimeMillis();

      File baseDir = generalDirWatcher.getRootDir();
      HIVE_TEST_FIXTURE = HiveTestFixture.builderForDocker(baseDir, HIVE_CONTAINER).build();

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
      logger.error("Failed to initialize Hive container", e);
      throw new RuntimeException("Failed to initialize Hive test infrastructure", e);
    }
  }

  @BeforeClass
  public static void setUp() {
    HIVE_TEST_FIXTURE.getPluginManager().addHivePluginTo(bits);
  }

  @AfterClass
  public static void tearDown() {
    if (HIVE_TEST_FIXTURE != null) {
      HIVE_TEST_FIXTURE.getPluginManager().removeHivePluginFrom(bits);
    }
  }
}
