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
package org.apache.drill.test;

import org.apache.drill.categories.UnlikelyTest;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.server.Drillbit;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestRule;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;

@Category({UnlikelyTest.class})
public class TestWebServerTempDir extends BaseTestQuery {

  @Rule
  public final TestRule TIMEOUT = TestTools.getTimeoutRule(120_000);

  @Test // DRILL-7056
  public void testDrillbitTempDir() throws Exception {
    File originalDrillbitTempDir = null;
    ClusterFixtureBuilder fixtureBuilder = ClusterFixture.bareBuilder(dirTestWatcher).withLocalZk()
        .configProperty(ExecConstants.ALLOW_LOOPBACK_ADDRESS_BINDING, true)
        .configProperty(ExecConstants.INITIAL_USER_PORT, QueryTestUtil.getFreePortNumber(31170, 300))
        .configProperty(ExecConstants.INITIAL_BIT_PORT, QueryTestUtil.getFreePortNumber(31180, 300));

    try (ClusterFixture fixture = fixtureBuilder.build();
        Drillbit twinDrillbitOnSamePort = new Drillbit(fixture.config(),
            fixtureBuilder.configBuilder().getDefinitions(), fixture.serviceSet())) {
      // Assert preconditions :
      //      1. First drillbit instance should be started normally
      //      2. Second instance startup should fail, because ports are occupied by the first one
      Drillbit originalDrillbit = fixture.drillbit();
      assertNotNull("First drillbit instance should be initialized", originalDrillbit);
      originalDrillbitTempDir = originalDrillbit.getWebServerTempDirPath();
      assertTrue("First drillbit instance should have a temporary Javascript dir initialized", originalDrillbitTempDir.exists());
      try {
        twinDrillbitOnSamePort.run();
        fail("Invocation of 'twinDrillbitOnSamePort.run()' should throw UserException");
      } catch (UserException e) {
        assertNull("Second drillbit instance should NOT have a temporary Javascript dir", twinDrillbitOnSamePort.getWebServerTempDirPath());
      }
    }
    // Verify deletion
    assertFalse("First drillbit instance should have a temporary Javascript dir deleted", originalDrillbitTempDir.exists());
  }

}
