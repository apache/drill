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

import org.apache.drill.categories.OperatorTest;
import org.apache.drill.categories.SqlTest;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.ClusterTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({SqlTest.class, OperatorTest.class})
public class TestGroupingSets extends ClusterTest {

  @BeforeClass
  public static void setUp() throws Exception {
    ClusterFixtureBuilder builder = ClusterFixture.builder(dirTestWatcher);
    startCluster(builder);
  }

  @Test
  public void testSimpleGroupingSets() throws Exception {
    try {
      String query = "select n_regionkey, n_nationkey, count(*) as cnt " +
          "from cp.`tpch/nation.parquet` " +
          "group by grouping sets ((n_regionkey), (n_nationkey))";

      queryBuilder().sql(query).run();
    } catch (UserException ex) {
      System.out.println("Exception message: " + ex.getMessage());
      System.out.println("Exception type: " + ex.getClass().getName());
      ex.printStackTrace();
      throw ex;
    }
  }

  @Test
  public void testRollup() throws Exception {
    try {
      String query = "select n_regionkey, n_nationkey, count(*) as cnt " +
          "from cp.`tpch/nation.parquet` " +
          "group by rollup(n_regionkey, n_nationkey)";

      queryBuilder().sql(query).run();
    } catch (UserException ex) {
      System.out.println("Exception message: " + ex.getMessage());
      System.out.println("Exception type: " + ex.getClass().getName());
      ex.printStackTrace();
      throw ex;
    }
  }

  @Test
  public void testCube() throws Exception {
    try {
      String query = "select n_regionkey, n_nationkey, count(*) as cnt " +
          "from cp.`tpch/nation.parquet` " +
          "group by cube(n_regionkey, n_nationkey)";

      queryBuilder().sql(query).run();
    } catch (UserException ex) {
      System.out.println("Exception message: " + ex.getMessage());
      System.out.println("Exception type: " + ex.getClass().getName());
      ex.printStackTrace();
      throw ex;
    }
  }
}
