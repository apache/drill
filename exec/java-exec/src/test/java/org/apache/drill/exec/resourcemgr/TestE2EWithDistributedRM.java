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
package org.apache.drill.exec.resourcemgr;

import org.apache.drill.exec.ExecConstants;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.ClusterTest;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestE2EWithDistributedRM extends ClusterTest {
  //private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestE2EWithDistributedRM.class);
  private String query;

  @BeforeClass
  public static void setupTestSuite() throws Exception {
    final ClusterFixtureBuilder fixtureBuilder = ClusterFixture.builder(dirTestWatcher)
      .configProperty(ExecConstants.RM_ENABLED, true)
      .setOptionDefault(ExecConstants.ENABLE_QUEUE.getOptionName(), false)
      .configProperty(ExecConstants.DRILL_PORT_HUNT, true)
      .configProperty(ExecConstants.ALLOW_LOOPBACK_ADDRESS_BINDING, true)
      .withLocalZk();
    startCluster(fixtureBuilder);
  }

  @Test
  public void testSystemTableQuery() throws Exception {
    query = "SELECT * FROM sys.drillbits;";
    runAndLog(query);
  }

  @Test
  public void testNonBufferedOperatorQuery() throws Exception {
    query = "SELECT * FROM cp.`employee.json` WHERE employee_id < 40 LIMIT 20";
    runAndLog(query);
  }

  @Test
  public void testBufferedOperatorQuery() throws Exception {
    query = "SELECT * FROM cp.`employee.json` WHERE employee_id < 40 ORDER BY first_name";
    runAndLog(query);
  }
}
