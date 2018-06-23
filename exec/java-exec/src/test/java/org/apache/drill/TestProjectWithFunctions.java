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
package org.apache.drill;

import org.apache.drill.categories.PlannerTest;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.ClusterTest;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.nio.file.Paths;

/**
 * Test the optimizer plan in terms of projecting different functions e.g. cast
 */
@Category(PlannerTest.class)
public class TestProjectWithFunctions extends ClusterTest {

  @BeforeClass
  public static void setupFiles() {
    dirTestWatcher.copyResourceToRoot(Paths.get("view"));
  }

  @Before
  public void setup() throws Exception {
    ClusterFixtureBuilder builder = ClusterFixture.builder(dirTestWatcher);
    startCluster(builder);
  }

  @Test
  public void testCastFunctions() throws Exception {
    String sql = "select t1.f from dfs.`view/emp_6212.view.drill` as t inner join dfs.`view/emp_6212.view.drill` as t1 " +
        "on cast(t.f as int) = cast(t1.f as int) and cast(t.f as int) = 10 and cast(t1.f as int) = 10";
    client.queryBuilder().sql(sql).run();
  }
}
