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
  private static final int METASTORE_PORT = 9083;
  private static final int HIVESERVER2_PORT = 10000;
  private static final int HIVESERVER2_HTTP_PORT = 10002;

  private static HiveContainer instance;
  private static String initializationError = null;
  private final boolean usePreinitialized;
  private final boolean useFallbackImage;

  private HiveContainer(String dockerImageName, boolean useFallback) {
    super(DockerImageName.parse(dockerImageName).asCompatibleSubstituteFor("apache/hive"));
    this.useFallbackImage = useFallback;
    this.usePreinitialized = dockerImageName.contains("preinitialized");
    configureContainer();
  }

  private static String getHiveImage() {
    // Try to use custom image if available, otherwise fall back to base image
    // Custom image will be built by Maven or manually
    return HIVE_IMAGE;
  }

  /**
   * Checks if Docker is available on the system.
   */
  public static boolean isDockerAvailable() {
    try {
      org.testcontainers.DockerClientFactory.instance().client();
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  /**
   * Checks if running on ARM64 architecture (Apple Silicon, ARM Linux).
   * Hive Docker tests are very slow on ARM64 due to x86 emulation.
   */
  public static boolean isArm64() {
    String arch = System.getProperty("os.arch", "").toLowerCase();
    return arch.contains("aarch64") || arch.contains("arm64");
  }

  /**
   * Returns any initialization error that occurred.
   */
  public static String getInitializationError() {
    return initializationError;
  }

  private void configureContainer() {
    withExposedPorts(METASTORE_PORT, HIVESERVER2_PORT, HIVESERVER2_HTTP_PORT);

    // Set environment variables for Hive configuration
    withEnv("SERVICE_NAME", "hiveserver2");
    // Don't set IS_RESUME - let the entrypoint initialize the schema

    // Wait for ports to be listening - this is the most reliable strategy
    // The waitForMetastoreReady() method will verify the metastore is actually accepting connections
    waitingFor(Wait.forListeningPort()
        .withStartupTimeout(Duration.ofMinutes(15)));

    // Enable reuse for faster test execution
    withReuse(true);

    logger.info("Hive container configured with image: {}", getDockerImageName());
  }

  /**
   * Gets the singleton instance of HiveContainer.
   * Container is started on first access and reused for all subsequent tests.
   *
   * @return Shared HiveContainer instance
   * @throws RuntimeException if container fails to start
   */
  public static synchronized HiveContainer getInstance() {
    if (instance != null) {
      logger.debug("Reusing existing Hive container instance");
      return instance;
    }

    // Check if Docker is available
    if (!isDockerAvailable()) {
      initializationError = "Docker is not available. Please install and start Docker to run Hive tests.";
      logger.error(initializationError);
      throw new RuntimeException(initializationError);
    }

    System.out.println("========================================");
    System.out.println("Starting Hive Docker container...");
    System.out.println("Image: " + HIVE_IMAGE);
    System.out.println("========================================");

    try {
      instance = tryStartContainer(HIVE_IMAGE, HIVE_IMAGE.equals(FALLBACK_IMAGE));
      return instance;
    } catch (Exception e) {
      initializationError = "Failed to start Hive container with image '" + HIVE_IMAGE + "': " + e.getMessage() +
          "\n\nTo build the required Docker image, run:" +
          "\n  cd contrib/storage-hive/core/src/test/resources/docker" +
          "\n  docker build -f Dockerfile.fast -t drill-hive-test:fast ." +
          "\n\nOr use the public Apache Hive image (slower startup):" +
          "\n  mvn test -Dhive.image=apache/hive:3.1.3";
      logger.error(initializationError, e);
      throw new RuntimeException(initializationError, e);
    }
  }

  private static HiveContainer tryStartContainer(String imageName, boolean isFallback) {
    boolean usePreinit = imageName.contains("preinitialized");
    if (usePreinit) {
      System.out.println("Using pre-initialized image (~1 minute startup)");
    } else if (isFallback) {
      System.out.println("Using apache/hive official image (~10-15 minute startup)");
    } else {
      System.out.println("Using custom image (~3-5 minute startup on first run)");
    }

    logger.info("Creating Hive container with image: {}", imageName);
    HiveContainer container = new HiveContainer(imageName, isFallback);

    System.out.println("Pulling Docker image and starting container...");
    System.out.println("This may take several minutes on first run...");
    long startTime = System.currentTimeMillis();
    container.start();

    // Additional wait for metastore to stabilize
    System.out.println("Waiting for metastore to stabilize...");
    waitForMetastoreReady(container);

    long elapsedSeconds = (System.currentTimeMillis() - startTime) / 1000;

    System.out.println("========================================");
    System.out.println("Hive container started successfully!");
    System.out.println("Startup time: " + elapsedSeconds + " seconds");
    System.out.println("Metastore: " + container.getMetastoreUri());
    System.out.println("JDBC: " + container.getJdbcUrl());
    System.out.println("Container will be reused for all tests");
    System.out.println("========================================");
    logger.info("Hive container started and ready for tests");

    return container;
  }

  /**
   * Waits for the metastore to be fully ready by attempting socket connections.
   * This ensures the Thrift service is actually accepting connections.
   */
  private static void waitForMetastoreReady(HiveContainer container) {
    int maxAttempts = 30;
    int attemptDelayMs = 2000;
    String host = container.getHost();
    int port = container.getMetastorePort();

    for (int i = 1; i <= maxAttempts; i++) {
      try (java.net.Socket socket = new java.net.Socket()) {
        socket.connect(new java.net.InetSocketAddress(host, port), 1000);
        // Connection successful - wait a bit more for service to fully initialize
        Thread.sleep(3000);
        System.out.println("Metastore is ready and accepting connections");
        return;
      } catch (Exception e) {
        if (i < maxAttempts) {
          logger.debug("Metastore not ready yet (attempt {}/{}): {}", i, maxAttempts, e.getMessage());
          try {
            Thread.sleep(attemptDelayMs);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while waiting for metastore", ie);
          }
        } else {
          throw new RuntimeException("Metastore failed to become ready after " + maxAttempts + " attempts", e);
        }
      }
    }
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

  /**
   * Checks if this container is using the fallback image (apache/hive:3.1.3).
   * When using the fallback image, test data must be created via JDBC.
   *
   * @return true if using fallback image
   */
  public boolean isUsingFallbackImage() {
    return useFallbackImage;
  }

  /**
   * Checks if this container is using a pre-initialized image.
   *
   * @return true if using pre-initialized image
   */
  public boolean isUsingPreinitializedImage() {
    return usePreinitialized;
  }

  @Override
  protected void doStart() {
    super.doStart();
    logger.info("Hive container started successfully");
    logger.info("Metastore URI: {}", getMetastoreUri());
    logger.info("JDBC URL: {}", getJdbcUrl());
  }
}
