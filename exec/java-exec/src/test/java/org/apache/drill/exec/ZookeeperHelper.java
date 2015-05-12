/**
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
package org.apache.drill.exec;

import static com.google.common.base.Throwables.propagate;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.util.MiniZooKeeperCluster;

/**
 * Test utility for managing a Zookeeper instance.
 *
 * <p>Tests that need a Zookeeper instance can initialize a static instance of this class in
 * their {@link org.junit.BeforeClass} section to set up Zookeeper.
 */
public class ZookeeperHelper {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ZookeeperHelper.class);

  private final File testDir = new File("target/test-data");
  private final DrillConfig config;
  private final String zkUrl;
  private MiniZooKeeperCluster zkCluster;

  /**
   * Constructor.
   *
   * <p>Will create a "test-data" directory for Zookeeper's use if one doesn't already exist.
   */
  public ZookeeperHelper() {
    this(false);
  }

  /**
   * Constructor.
   *
   * <p>Will create a "test-data" directory for Zookeeper's use if one doesn't already exist.
   * @param failureInCancelled pass true if you want failures in cancelled fragments to be reported as failures
   */
  public ZookeeperHelper(boolean failureInCancelled) {
    final Properties overrideProps = new Properties();
    // Forced to disable this, because currently we leak memory which is a known issue for query cancellations.
    // Setting this causes unittests to fail.
    if (failureInCancelled) {
      overrideProps.setProperty(ExecConstants.RETURN_ERROR_FOR_FAILURE_IN_CANCELLED_FRAGMENTS, "true");
    }
    config = DrillConfig.create(overrideProps);
    zkUrl = config.getString(ExecConstants.ZK_CONNECTION);

    if (!testDir.exists()) {
      testDir.mkdirs();
    }
  }

  /**
   * Start the Zookeeper instance.
   *
   * <p>This must be used before any operations that depend on the Zookeeper instance being up.
   *
   * @param numServers how many servers the Zookeeper instance should have
   */
  public void startZookeeper(final int numServers) {
    if (zkCluster != null) {
      throw new IllegalStateException("Zookeeper cluster already running");
    }

    try {
      zkCluster = new MiniZooKeeperCluster();
      zkCluster.setDefaultClientPort(Integer.parseInt(zkUrl.split(":")[1]));
      zkCluster.startup(testDir, numServers);
    } catch (IOException | InterruptedException e) {
      propagate(e);
    }
  }

  /**
   * Shut down the Zookeeper instance.
   *
   * <p>This must be used before the program exits.
   */
  public void stopZookeeper() {
    try {
      zkCluster.shutdown();
      zkCluster = null;
    } catch (IOException e) {
      // since this is meant to be used in a test's cleanup, we don't propagate the exception
      final String message = "Unable to shutdown Zookeeper";
      System.err.println(message + '.');
      logger.warn(message, e);
    }
  }

  /**
   * Get the DrillConfig used for the Zookeeper instance.
   *
   * @return the DrillConfig used.
   */
  public DrillConfig getConfig() {
    return config;
  }
}
