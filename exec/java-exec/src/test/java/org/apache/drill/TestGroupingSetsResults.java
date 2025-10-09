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
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.ClusterTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({SqlTest.class, OperatorTest.class})
public class TestGroupingSetsResults extends ClusterTest {

  @BeforeClass
  public static void setUp() throws Exception {
    ClusterFixtureBuilder builder = ClusterFixture.builder(dirTestWatcher);
    startCluster(builder);
  }

  @Test
  public void testSimpleGroupingSetsResults() throws Exception {
    String query = "select n_regionkey, count(*) as cnt " +
        "from cp.`tpch/nation.parquet` " +
        "group by grouping sets ((n_regionkey), ())";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("n_regionkey", "cnt")
        .baselineValues(0, 5L)
        .baselineValues(1, 5L)
        .baselineValues(2, 5L)
        .baselineValues(3, 5L)
        .baselineValues(4, 5L)
        .baselineValues(null, 25L)  // Grand total
        .go();
  }

  @Test
  public void testRollupResults() throws Exception {
    // ROLLUP(a, b) creates grouping sets: (a, b), (a), ()
    String query = "select n_regionkey, count(*) as cnt " +
        "from cp.`tpch/nation.parquet` " +
        "where n_regionkey < 2 " +
        "group by rollup(n_regionkey)";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("n_regionkey", "cnt")
        .baselineValues(0, 5L)       // Region 0
        .baselineValues(1, 5L)       // Region 1
        .baselineValues(null, 10L)   // Grand total
        .go();
  }

  @Test
  public void testCubeResults() throws Exception {
    // CUBE(a) creates grouping sets: (a), ()
    String query = "select n_regionkey, count(*) as cnt " +
        "from cp.`tpch/nation.parquet` " +
        "where n_regionkey < 2 " +
        "group by cube(n_regionkey)";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("n_regionkey", "cnt")
        .baselineValues(0, 5L)       // Region 0
        .baselineValues(1, 5L)       // Region 1
        .baselineValues(null, 10L)   // Grand total
        .go();
  }

  @Test
  public void testMultiColumnGroupingSets() throws Exception {
    // Test GROUPING SETS with two columns
    String query = "select n_regionkey, n_nationkey, count(*) as cnt " +
        "from cp.`tpch/nation.parquet` " +
        "where n_regionkey = 0 and n_nationkey in (0, 5) " +
        "group by grouping sets ((n_regionkey, n_nationkey), (n_regionkey), ())";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("n_regionkey", "n_nationkey", "cnt")
        // Grouping set (n_regionkey, n_nationkey)
        .baselineValues(0, 0, 1L)    // Region 0, nation 0
        .baselineValues(0, 5, 1L)    // Region 0, nation 5
        // Grouping set (n_regionkey)
        .baselineValues(0, null, 2L) // Region 0 total
        // Grouping set ()
        .baselineValues(null, null, 2L) // Grand total
        .go();
  }

  @Test
  public void testRollupTwoColumns() throws Exception {
    // ROLLUP(a, b) creates grouping sets: (a, b), (a), ()
    String query = "select n_regionkey, n_nationkey, count(*) as cnt " +
        "from cp.`tpch/nation.parquet` " +
        "where n_regionkey = 0 and n_nationkey in (0, 5) " +
        "group by rollup(n_regionkey, n_nationkey)";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("n_regionkey", "n_nationkey", "cnt")
        // Grouping set (n_regionkey, n_nationkey)
        .baselineValues(0, 0, 1L)    // Region 0, nation 0
        .baselineValues(0, 5, 1L)    // Region 0, nation 5
        // Grouping set (n_regionkey)
        .baselineValues(0, null, 2L) // Region 0 subtotal
        // Grouping set ()
        .baselineValues(null, null, 2L) // Grand total
        .go();
  }

  @Test
  public void testCubeTwoColumns() throws Exception {
    // CUBE(a, b) creates grouping sets: (a, b), (a), (b), ()
    // Using specific nations to make the test deterministic
    String query = "select n_regionkey, n_nationkey, count(*) as cnt " +
        "from cp.`tpch/nation.parquet` " +
        "where (n_regionkey = 0 and n_nationkey in (0, 5)) " +
        "   or (n_regionkey = 1 and n_nationkey in (1, 2)) " +
        "group by cube(n_regionkey, n_nationkey)";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("n_regionkey", "n_nationkey", "cnt")
        // Grouping set (n_regionkey, n_nationkey)
        .baselineValues(0, 0, 1L)    // Region 0, nation 0
        .baselineValues(0, 5, 1L)    // Region 0, nation 5
        .baselineValues(1, 1, 1L)    // Region 1, nation 1
        .baselineValues(1, 2, 1L)    // Region 1, nation 2
        // Grouping set (n_regionkey)
        .baselineValues(0, null, 2L) // Region 0 total
        .baselineValues(1, null, 2L) // Region 1 total
        // Grouping set (n_nationkey)
        .baselineValues(null, 0, 1L) // Nation 0 across all regions
        .baselineValues(null, 1, 1L) // Nation 1 across all regions
        .baselineValues(null, 2, 1L) // Nation 2 across all regions
        .baselineValues(null, 5, 1L) // Nation 5 across all regions
        // Grouping set ()
        .baselineValues(null, null, 4L) // Grand total
        .go();
  }

  @Test
  public void testGroupingSetsWithAggregates() throws Exception {
    // Test multiple aggregate functions with GROUPING SETS
    String query = "select n_regionkey, " +
        "count(*) as cnt, " +
        "min(n_nationkey) as min_key, " +
        "max(n_nationkey) as max_key " +
        "from cp.`tpch/nation.parquet` " +
        "where n_regionkey < 2 " +
        "group by grouping sets ((n_regionkey), ())";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("n_regionkey", "cnt", "min_key", "max_key")
        .baselineValues(0, 5L, 0, 16)       // Region 0
        .baselineValues(1, 5L, 1, 24)       // Region 1
        .baselineValues(null, 10L, 0, 24)   // Grand total
        .go();
  }

  @Test
  public void testGroupingSetsEmptyGroupingSet() throws Exception {
    // Test just the empty grouping set (grand total only)
    String query = "select count(*) as cnt, sum(n_nationkey) as sum_key " +
        "from cp.`tpch/nation.parquet` " +
        "group by grouping sets (())";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("cnt", "sum_key")
        .baselineValues(25L, 300L)  // Grand total: 25 nations, sum 0+1+2+...+24 = 300
        .go();
  }

  @Test
  public void testGroupingSetsWithWhere() throws Exception {
    // Test GROUPING SETS with WHERE clause
    String query = "select n_regionkey, count(*) as cnt " +
        "from cp.`tpch/nation.parquet` " +
        "where n_regionkey in (0, 1, 2) " +
        "group by grouping sets ((n_regionkey), ())";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("n_regionkey", "cnt")
        .baselineValues(0, 5L)
        .baselineValues(1, 5L)
        .baselineValues(2, 5L)
        .baselineValues(null, 15L)  // Total of regions 0, 1, 2
        .go();
  }

  @Test
  public void testGroupingSetsWithExpression() throws Exception {
    // Test GROUPING SETS with computed columns
    String query = "select n_regionkey, " +
        "case when n_nationkey < 10 then 'low' else 'high' end as key_range, " +
        "count(*) as cnt " +
        "from cp.`tpch/nation.parquet` " +
        "where n_regionkey < 2 " +
        "group by grouping sets (" +
        "  (n_regionkey, case when n_nationkey < 10 then 'low' else 'high' end), " +
        "  (n_regionkey)" +
        ")";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("n_regionkey", "key_range", "cnt")
        // Grouping set (n_regionkey, key_range)
        .baselineValues(0, "low", 2L)    // Region 0, low keys (0,5)
        .baselineValues(0, "high", 3L)   // Region 0, high keys (14,15,16)
        .baselineValues(1, "low", 3L)    // Region 1, low keys (1,2,3)
        .baselineValues(1, "high", 2L)   // Region 1, high keys (17,24)
        // Grouping set (n_regionkey)
        .baselineValues(0, null, 5L)     // Region 0 total
        .baselineValues(1, null, 5L)     // Region 1 total
        .go();
  }

  @Test
  public void testRollupWithJSON() throws Exception {
    // Test ROLLUP with JSON data
    String query = "select education_level, count(*) as cnt " +
        "from cp.`employee.json` " +
        "where education_level in ('Graduate Degree', 'Bachelors Degree', 'Partial College') " +
        "group by rollup(education_level)";

    // This should now work with proper type handling
    queryBuilder()
        .sql(query)
        .run();
  }
}
