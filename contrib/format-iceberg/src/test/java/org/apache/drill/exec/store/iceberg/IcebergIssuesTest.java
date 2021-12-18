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
package org.apache.drill.exec.store.iceberg;

import java.nio.file.Paths;

import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.ClusterTest;
import org.junit.BeforeClass;
import org.junit.Test;

public class IcebergIssuesTest extends ClusterTest {

  private static final String regularTestFile_1 = "cust_order_10_1.json";
  private static final String regularTestFile_2 = "cust_order_10_2.json";

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    dirTestWatcher.copyResourceToRoot(Paths.get("lateraljoin", "multipleFiles", regularTestFile_1));
    dirTestWatcher.copyResourceToRoot(Paths.get("lateraljoin", "multipleFiles", regularTestFile_2));

    ClusterFixtureBuilder builder = ClusterFixture.builder(dirTestWatcher)
        .sessionOption(PlannerSettings.ENABLE_UNNEST_LATERAL_KEY, true)
        .maxParallelization(1);

    startCluster(builder);
  }

  @Test
  public void testDrill8058() throws Exception {
    String sql = "SELECT customer.c_name, avg(orders.o_totalprice) AS avgPrice " +
        "FROM dfs.`lateraljoin/multipleFiles` customer, LATERAL " +
        "(SELECT t.ord.o_totalprice as o_totalprice FROM UNNEST(customer.c_orders) t(ord) WHERE t.ord.o_totalprice > 100000 LIMIT 2) " +
        "orders GROUP BY customer.c_name";
    runAndLog(sql);
  }
}
