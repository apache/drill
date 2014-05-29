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

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.util.MiniZooKeeperCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class TestWithZookeeper extends ExecTest {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestWithZookeeper.class);

  private static File testDir = new File("target/test-data");
  private static DrillConfig config;
  private static String zkUrl;
  private static MiniZooKeeperCluster zkCluster;

  @BeforeClass
  public static void setUp() throws Exception {
    config = DrillConfig.create();
    zkUrl = config.getString(ExecConstants.ZK_CONNECTION);
    setupTestDir();
    startZookeeper(1);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    stopZookeeper();
  }

  private static void setupTestDir() {
    if (!testDir.exists()) {
      testDir.mkdirs();
    }
  }

  private static void startZookeeper(int numServers) {
    try {
      zkCluster = new MiniZooKeeperCluster();
      zkCluster.setDefaultClientPort(Integer.parseInt(zkUrl.split(":")[1]));
      zkCluster.startup(testDir, numServers);
    } catch (IOException e) {
      propagate(e);
    } catch (InterruptedException e) {
      propagate(e);
    }
  }

  private static void stopZookeeper() {
    try {
      zkCluster.shutdown();
    } catch (IOException e) {
      propagate(e);
    }
  }

  public static DrillConfig getConfig(){
    return config;
  }

}
