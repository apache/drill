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

package org.apache.drill.exec.store.splunk;

import org.apache.drill.categories.SlowTest;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig.AuthMode;
import org.apache.drill.common.logical.security.PlainCredentialsProvider;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.ClusterTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl.TEST_USER_1;
import static org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl.TEST_USER_2;


@RunWith(Suite.class)
@Suite.SuiteClasses({
  SplunkConnectionTest.class,
  SplunkQueryBuilderTest.class,
  SplunkLimitPushDownTest.class,
  SplunkIndexesTest.class,
  SplunkPluginTest.class,
  SplunkTestSplunkUtils.class,
  TestSplunkUserTranslation.class,
  SplunkWriterTest.class
})

@Category({SlowTest.class})
public class SplunkTestSuite extends ClusterTest {
  private static final Logger logger = LoggerFactory.getLogger(SplunkTestSuite.class);

  protected static SplunkPluginConfig SPLUNK_STORAGE_PLUGIN_CONFIG = null;

  protected static SplunkPluginConfig SPLUNK_STORAGE_PLUGIN_CONFIG_WITH_USER_TRANSLATION = null;
  public static final String SPLUNK_LOGIN = "admin";
  public static final String SPLUNK_PASS = "password";

  private static volatile boolean runningSuite = true;
  private static AtomicInteger initCount = new AtomicInteger(0);

  /**
   * Creates a Splunk default.yml configuration file with minimal disk space requirements.
   * This is the proper way to configure Splunk in Docker - the settings are applied at startup.
   */
  private static java.io.File createDefaultYmlFile() {
    try {
      java.io.File tempFile = java.io.File.createTempFile("splunk-default", ".yml");
      tempFile.deleteOnExit();

      String content = "---\n" +
             "splunk:\n" +
             "  conf:\n" +
             "    - key: server\n" +
             "      value:\n" +
             "        directory: /opt/splunk/etc/system/local\n" +
             "        content:\n" +
             "          diskUsage:\n" +
             "            minFreeSpace: 50\n" +
             "            pollingFrequency: 30\n" +
             "            pollingTimerFrequency: 5\n" +
             "    - key: limits\n" +
             "      value:\n" +
             "        directory: /opt/splunk/etc/system/local\n" +
             "        content:\n" +
             "          search:\n" +
             "            ttl: 60\n" +
             "            default_save_ttl: 60\n" +
             "            auto_cancel: 60\n" +
             "            auto_finalize_ec: 60\n" +
             "            auto_pause: 30\n";

      java.nio.file.Files.write(tempFile.toPath(), content.getBytes(java.nio.charset.StandardCharsets.UTF_8));
      return tempFile;
    } catch (java.io.IOException e) {
      throw new RuntimeException("Failed to create Splunk default.yml", e);
    }
  }

  @ClassRule
  public static GenericContainer<?> splunk = new GenericContainer<>(
    DockerImageName.parse("splunk/splunk:9.3")
  )
    .withExposedPorts(8089, 8089)
    .withEnv("SPLUNK_START_ARGS", "--accept-license")
    .withEnv("SPLUNK_PASSWORD", SPLUNK_PASS)
    .withEnv("SPLUNKD_SSL_ENABLE", "false")
    .withCopyFileToContainer(
      org.testcontainers.utility.MountableFile.forHostPath(
        createDefaultYmlFile().toPath()
      ),
      "/tmp/defaults/default.yml"
    );

  @BeforeClass
  public static void initSplunk() throws Exception {
    synchronized (SplunkTestSuite.class) {
      if (initCount.get() == 0) {
        ClusterFixtureBuilder builder = new ClusterFixtureBuilder(dirTestWatcher)
          .configProperty(ExecConstants.HTTP_ENABLE, true)
          .configProperty(ExecConstants.HTTP_PORT_HUNT, true)
          .configProperty(ExecConstants.IMPERSONATION_ENABLED, true);
        startCluster(builder);

        splunk.start();

        // Wait for Splunk to start and apply configuration from default.yml
        logger.info("Waiting for Splunk to start with custom configuration...");
        Thread.sleep(60000);

        // Clean up any existing dispatch files
        logger.info("Cleaning up existing dispatch directory...");
        cleanDispatchDirectory();

        // Verify configuration was applied
        logger.info("Verifying Splunk configuration...");
        try {
          var result = splunk.execInContainer("cat", "/opt/splunk/etc/system/local/server.conf");
          logger.info("Server.conf contents:\n" + result.getStdout());

          result = splunk.execInContainer("cat", "/opt/splunk/etc/system/local/limits.conf");
          logger.info("Limits.conf contents:\n" + result.getStdout());
        } catch (Exception e) {
          logger.warn("Could not verify config: " + e.getMessage());
        }

        String hostname = splunk.getHost();
        Integer port = splunk.getFirstMappedPort();
        StoragePluginRegistry pluginRegistry = cluster.drillbit().getContext().getStorage();
        SPLUNK_STORAGE_PLUGIN_CONFIG = new SplunkPluginConfig(
          SPLUNK_LOGIN, SPLUNK_PASS,
          "http", hostname, port,
          null, null, null, null, false, true, // app, owner, token, cookie, validateCertificates
          "1", "now",
          null,
          4,
          StoragePluginConfig.AuthMode.SHARED_USER.name(), true, null, null, null, null
        );
        SPLUNK_STORAGE_PLUGIN_CONFIG.setEnabled(true);
        pluginRegistry.put(SplunkPluginConfig.NAME, SPLUNK_STORAGE_PLUGIN_CONFIG);
        runningSuite = true;
        logger.info("Take a time to ready more Splunk events (10 sec)...");
        Thread.sleep(10000);


        PlainCredentialsProvider credentialsProvider = new PlainCredentialsProvider(new HashMap<>());
        // Add authorized user
        credentialsProvider.setUserCredentials(SPLUNK_LOGIN, SPLUNK_PASS, TEST_USER_1);
        // Add unauthorized user
        credentialsProvider.setUserCredentials("nope", "no way dude", TEST_USER_2);

        SPLUNK_STORAGE_PLUGIN_CONFIG_WITH_USER_TRANSLATION = new SplunkPluginConfig(
          null, null, // username, password
          "http", hostname, port,
          null, null, null, null, false, false, // app, owner, token, cookie, validateCertificates
          "1", "now",
          credentialsProvider,
          4,
          AuthMode.USER_TRANSLATION.name(), true, null, null, null, null
        );
        SPLUNK_STORAGE_PLUGIN_CONFIG_WITH_USER_TRANSLATION.setEnabled(true);
        pluginRegistry.put("ut_splunk", SPLUNK_STORAGE_PLUGIN_CONFIG_WITH_USER_TRANSLATION);

      }
      initCount.incrementAndGet();
      runningSuite = true;
    }
    logger.info("Initialized Splunk in Docker container");
  }

  /**
   * Cleans up the Splunk dispatch directory to free disk space.
   * This should be called between test classes to prevent disk space exhaustion.
   */
  public static void cleanDispatchDirectory() {
    try {
      logger.info("Cleaning up Splunk dispatch directory...");
      splunk.execInContainer("sh", "-c", "rm -rf /opt/splunk/var/run/splunk/dispatch/*");
      logger.debug("Splunk dispatch directory cleaned up successfully");
    } catch (Exception e) {
      logger.warn("Failed to clean up Splunk dispatch directory: " + e.getMessage());
    }
  }

  @AfterClass
  public static void tearDownCluster() {
    synchronized (SplunkTestSuite.class) {
      if (initCount.decrementAndGet() == 0) {
        // Clean up Splunk dispatch files to free disk space before shutdown
        cleanDispatchDirectory();
        splunk.close();
      }
    }
  }

  public static boolean isRunningSuite() {
    return runningSuite;
  }
}
