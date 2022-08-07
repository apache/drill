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
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({SlowTest.class})
public class TestSplunkUserTranslation extends SplunkBaseTest {

  @BeforeClass
  public static void setup() throws Exception {
    ClusterFixtureBuilder builder = new ClusterFixtureBuilder(dirTestWatcher)
      .configProperty(ExecConstants.HTTP_ENABLE, true)
      .configProperty(ExecConstants.HTTP_PORT_HUNT, true)
      .configProperty(ExecConstants.IMPERSONATION_ENABLED, true);
    startCluster(builder);
  }

  @Test
  public void testUserTranslation() throws Exception {

  }
}
