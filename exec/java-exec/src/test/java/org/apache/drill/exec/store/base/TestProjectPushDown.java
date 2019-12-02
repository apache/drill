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
package org.apache.drill.exec.store.base;

import static org.junit.Assert.assertEquals;

import org.apache.commons.lang3.StringUtils;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.ClusterTest;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestProjectPushDown extends ClusterTest {

  @BeforeClass
  public static void setup() throws Exception {
    ClusterFixtureBuilder builder = new ClusterFixtureBuilder(dirTestWatcher);
    startCluster(builder);

    StoragePluginRegistry pluginRegistry = cluster.drillbit().getContext().getStorage();
    DummyStoragePluginConfig config1 =
        new DummyStoragePluginConfig(true, false, true);
    pluginRegistry.createOrUpdate("pushOn", config1, true);

    DummyStoragePluginConfig config2 =
        new DummyStoragePluginConfig(false, false, true);
    pluginRegistry.createOrUpdate("pushOff", config2, true);
  }

  @Test
  public void testPushDownEnabled() throws Exception {
    String plan = client.queryBuilder().sql("SELECT a, b, c from pushOn.myTable").explainJson();
    // DRILL-7451: should be 0
    assertEquals(1, StringUtils.countMatches(plan, "\"pop\" : \"project\""));
  }

  @Test
  public void testPushDownDisabled() throws Exception {
    String plan = client.queryBuilder().sql("SELECT a, b, c from pushOff.myTable").explainJson();
    // DRILL-7451: should be 1
    assertEquals(2, StringUtils.countMatches(plan, "\"pop\" : \"project\""));
  }

  @Test
  public void testDummyReader() throws Exception {
    RowSet results = client.queryBuilder().sql("SELECT a, b, c from pushOn.myTable").rowSet();
    assertEquals(3, results.rowCount());
    results.clear();
  }
}
