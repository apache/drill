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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;

/**
 * Testcontainers implementation for Apache Hive.
 * Provides a containerized Hive metastore and HiveServer2 for testing.
 * Uses singleton pattern to share container across all tests.
 */
public class HiveContainer extends GenericContainer<HiveContainer> {
  private static final Logger logger = LoggerFactory.getLogger(HiveContainer.class);

  // Use custom Drill Hive test image built from Dockerfile in test resources
  // For ~1 minute startup: use "drill-hive-test:fast" (fast startup, test data via JDBC)
  // For ~1 minute startup: use "drill-hive-test:preinitialized" (build with build-preinitialized-image.sh)
  // For 10-20 minute startup: use "drill-hive-test:latest" (build with docker build)
  private static final String HIVE_IMAGE = System.getProperty("hive.image", "drill-hive-test:fast");
  private static final String FALLBACK_IMAGE = "apache/hive:3.1.3";
  private static final boolean USE_PREINITIALIZED = HIVE_IMAGE.contains("preinitialized");
  private static final int METASTORE_PORT = 9083;
  private static final int HIVESERVER2_PORT = 10000;
  private static final int HIVESERVER2_HTTP_PORT = 10002;

  private static HiveContainer instance;
  private boolean dataInitialized = false;

  private HiveContainer() {
    this(getHiveImage());
  }

  private static String getHiveImage() {
    // Try to use custom image if available, otherwise fall back to base image
    // Custom image will be built by Maven or manually
    return HIVE_IMAGE;
  }

  private HiveContainer(String dockerImageName) {
    super(DockerImageName.parse(dockerImageName).asCompatibleSubstituteFor("apache/hive"));

    withExposedPorts(METASTORE_PORT, HIVESERVER2_PORT, HIVESERVER2_HTTP_PORT);

    // Set environment variables for Hive configuration
    withEnv("SERVICE_NAME", "hiveserver2");
    // Don't set IS_RESUME - let the entrypoint initialize the schema

    // Wait strategy depends on image type:
    // - Standard image: Wait for data initialization to complete (20 minutes)
    // - Pre-initialized image: Wait for services to start only (2 minutes)
    if (USE_PREINITIALIZED) {
      // Pre-initialized image: schema and data already exist, just wait for services
      waitingFor(Wait.forLogMessage(".*Hive container ready \\(pre-initialized\\)!.*", 1)
          .withStartupTimeout(Duration.ofMinutes(2)));
    } else {
      // Standard image: wait for both HiveServer2 to start AND test data to be initialized
      // Allow up to 20 minutes: Metastore + HiveServer2 startup (~5-10 min) + data initialization (~5-10 min)
      // This is only on first run; container reuse makes subsequent tests fast (~1 second)
      waitingFor(Wait.forLogMessage(".*Test data loaded and ready for queries.*", 1)
          .withStartupTimeout(Duration.ofMinutes(20)));
    }

    // Enable reuse for faster test execution
    withReuse(true);

    logger.info("Hive container configured with image: {}", dockerImageName);
  }

  /**
   * Gets the singleton instance of HiveContainer.
   * Container is started on first access and reused for all subsequent tests.
   *
   * @return Shared HiveContainer instance
   */
  public static synchronized HiveContainer getInstance() {
    if (instance == null) {
      System.out.println("========================================");
      System.out.println("Starting Hive Docker container...");
      if (USE_PREINITIALIZED) {
        System.out.println("Using pre-initialized image (~1 minute startup)");
      } else {
        System.out.println("Using standard image (~15 minute startup on first run)");
      }
      System.out.println("Image: " + HIVE_IMAGE);
      System.out.println("========================================");
      logger.info("Creating new Hive container instance");
      instance = new HiveContainer();

      System.out.println("Pulling Docker image and starting container...");
      long startTime = System.currentTimeMillis();
      instance.start();
      long elapsedSeconds = (System.currentTimeMillis() - startTime) / 1000;

      System.out.println("========================================");
      System.out.println("Hive container started successfully!");
      System.out.println("Startup time: " + elapsedSeconds + " seconds");
      System.out.println("Metastore: " + instance.getMetastoreUri());
      System.out.println("JDBC: " + instance.getJdbcUrl());
      System.out.println("Container will be reused for all tests");
      if (USE_PREINITIALIZED) {
        System.out.println("Tip: Build pre-initialized image with build-preinitialized-image.sh");
      }
      System.out.println("========================================");
      logger.info("Hive container started and ready for tests");
    } else {
      logger.debug("Reusing existing Hive container instance");
    }
    return instance;
  }

  /**
   * Gets the JDBC URL for connecting to HiveServer2.
   *
   * @return JDBC connection string
   */
  public String getJdbcUrl() {
    return String.format("jdbc:hive2://%s:%d/default",
        getHost(),
        getMappedPort(HIVESERVER2_PORT));
  }

  /**
   * Gets the metastore URI for Hive metastore thrift service.
   *
   * @return Metastore URI
   */
  public String getMetastoreUri() {
    return String.format("thrift://%s:%d",
        getHost(),
        getMappedPort(METASTORE_PORT));
  }

  /**
   * Gets the host address of the container.
   *
   * @return Container host
   */
  @Override
  public String getHost() {
    return super.getHost();
  }

  /**
   * Gets the mapped port for the metastore service.
   *
   * @return Mapped metastore port
   */
  public Integer getMetastorePort() {
    return getMappedPort(METASTORE_PORT);
  }

  /**
   * Gets the mapped port for HiveServer2.
   *
   * @return Mapped HiveServer2 port
   */
  public Integer getHiveServer2Port() {
    return getMappedPort(HIVESERVER2_PORT);
  }

  @Override
  protected void doStart() {
    super.doStart();
    logger.info("Hive container started successfully");
    logger.info("Metastore URI: {}", getMetastoreUri());
    logger.info("JDBC URL: {}", getJdbcUrl());
  }
}
