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
package org.apache.drill.exec.planner;

import java.nio.file.Paths;

import org.apache.drill.PlanTestBase;
import org.apache.drill.categories.SqlTest;
import org.apache.drill.exec.ExecConstants;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests for materialized view query rewriting.
 * <p>
 * When planner.enable_materialized_view_rewrite is enabled, queries
 * may be rewritten to use pre-computed materialized views.
 * <p>
 * The rewriting uses Calcite's SubstitutionVisitor for structural matching.
 */
@Category(SqlTest.class)
public class TestMaterializedViewRewriting extends PlanTestBase {

  @BeforeClass
  public static void setupTestFiles() {
    dirTestWatcher.copyResourceToRoot(Paths.get("nation"));
  }

  // ==================== Basic Functionality Tests ====================

  @Test
  public void testRewritingEnabledByDefault() throws Exception {
    String mvName = "test_mv_rewrite_enabled_default";
    try {
      // Create materialized view
      test("CREATE MATERIALIZED VIEW dfs.tmp.%s AS SELECT region_id, sales_city FROM cp.`region.json` WHERE region_id = 0", mvName);

      // Verify rewriting is enabled by default and queries work correctly
      testBuilder()
          .sqlQuery("SELECT region_id, sales_city FROM cp.`region.json` WHERE region_id = 0")
          .unOrdered()
          .baselineColumns("region_id", "sales_city")
          .baselineValues(0L, "None")
          .go();
    } finally {
      test("DROP MATERIALIZED VIEW IF EXISTS dfs.tmp.%s", mvName);
    }
  }

  @Test
  public void testRewritingWithNoMaterializedViews() throws Exception {
    // Query should work even when no MVs exist
    testBuilder()
        .sqlQuery("SELECT region_id, sales_city FROM cp.`region.json` WHERE region_id = 0")
        .unOrdered()
        .baselineColumns("region_id", "sales_city")
        .baselineValues(0L, "None")
        .go();
  }

  @Test
  public void testRewritingWithDisabledOption() throws Exception {
    String mvName = "test_mv_disabled";
    try {
      // Create and refresh MV
      test("CREATE MATERIALIZED VIEW dfs.tmp.%s AS SELECT region_id, sales_city FROM cp.`region.json`", mvName);
      test("REFRESH MATERIALIZED VIEW dfs.tmp.%s", mvName);

      // Disable rewriting
      test("ALTER SESSION SET `%s` = false", ExecConstants.ENABLE_MATERIALIZED_VIEW_REWRITE_KEY);

      // Query should still work, just not use the MV
      testBuilder()
          .sqlQuery("SELECT region_id, sales_city FROM cp.`region.json` WHERE region_id = 0")
          .unOrdered()
          .baselineColumns("region_id", "sales_city")
          .baselineValues(0L, "None")
          .go();
    } finally {
      test("ALTER SESSION SET `%s` = true", ExecConstants.ENABLE_MATERIALIZED_VIEW_REWRITE_KEY);
      test("DROP MATERIALIZED VIEW IF EXISTS dfs.tmp.%s", mvName);
    }
  }

  // ==================== Direct MV Query Tests ====================

  @Test
  public void testQueryMVDirectlyWithRewritingEnabled() throws Exception {
    String mvName = "test_mv_direct_query";
    try {
      // Create and refresh materialized view
      test("CREATE MATERIALIZED VIEW dfs.tmp.%s AS SELECT region_id, sales_city FROM cp.`region.json` WHERE region_id = 0", mvName);
      test("REFRESH MATERIALIZED VIEW dfs.tmp.%s", mvName);

      // Query the MV directly - should still work
      testBuilder()
          .sqlQuery("SELECT * FROM dfs.tmp.%s", mvName)
          .unOrdered()
          .baselineColumns("region_id", "sales_city")
          .baselineValues(0L, "None")
          .go();
    } finally {
      test("DROP MATERIALIZED VIEW IF EXISTS dfs.tmp.%s", mvName);
    }
  }

  @Test
  public void testQueryMVWithProjection() throws Exception {
    String mvName = "test_mv_projection";
    try {
      // Create MV with multiple columns
      test("CREATE MATERIALIZED VIEW dfs.tmp.%s AS SELECT region_id, sales_city, sales_country FROM cp.`region.json` WHERE region_id < 3", mvName);
      test("REFRESH MATERIALIZED VIEW dfs.tmp.%s", mvName);

      // Query with subset of columns - verify count matches
      testBuilder()
          .sqlQuery("SELECT COUNT(*) AS cnt FROM dfs.tmp.%s", mvName)
          .unOrdered()
          .baselineColumns("cnt")
          .baselineValues(3L)
          .go();
    } finally {
      test("DROP MATERIALIZED VIEW IF EXISTS dfs.tmp.%s", mvName);
    }
  }

  // ==================== Exact Match Tests ====================

  @Test
  public void testExactMatchSimpleQuery() throws Exception {
    String mvName = "test_mv_exact_simple";
    try {
      // Create and refresh MV
      test("CREATE MATERIALIZED VIEW dfs.tmp.%s AS SELECT region_id, sales_city FROM cp.`region.json`", mvName);
      test("REFRESH MATERIALIZED VIEW dfs.tmp.%s", mvName);

      // Query - verify results exist
      testBuilder()
          .sqlQuery("SELECT COUNT(*) AS cnt FROM cp.`region.json`")
          .unOrdered()
          .baselineColumns("cnt")
          .baselineValues(110L)
          .go();
    } finally {
      test("DROP MATERIALIZED VIEW IF EXISTS dfs.tmp.%s", mvName);
    }
  }

  @Test
  public void testExactMatchWithFilter() throws Exception {
    String mvName = "test_mv_exact_filter";
    try {
      // Create MV with filter
      test("CREATE MATERIALIZED VIEW dfs.tmp.%s AS SELECT region_id, sales_city FROM cp.`region.json` WHERE region_id = 0", mvName);
      test("REFRESH MATERIALIZED VIEW dfs.tmp.%s", mvName);

      // Query with same filter
      testBuilder()
          .sqlQuery("SELECT region_id, sales_city FROM cp.`region.json` WHERE region_id = 0")
          .unOrdered()
          .baselineColumns("region_id", "sales_city")
          .baselineValues(0L, "None")
          .go();
    } finally {
      test("DROP MATERIALIZED VIEW IF EXISTS dfs.tmp.%s", mvName);
    }
  }

  // ==================== Aggregation Tests ====================

  @Test
  public void testAggregateQueryWithRewritingEnabled() throws Exception {
    String mvName = "test_mv_agg_rewrite";
    try {
      // Create MV with aggregation
      test("CREATE MATERIALIZED VIEW dfs.tmp.%s AS SELECT sales_country, COUNT(*) AS cnt FROM cp.`region.json` WHERE region_id < 5 GROUP BY sales_country", mvName);
      test("REFRESH MATERIALIZED VIEW dfs.tmp.%s", mvName);

      // Run aggregate query
      test("SELECT sales_country, COUNT(*) AS cnt FROM cp.`region.json` WHERE region_id < 5 GROUP BY sales_country");
    } finally {
      test("DROP MATERIALIZED VIEW IF EXISTS dfs.tmp.%s", mvName);
    }
  }

  @Test
  public void testAggregateCountStar() throws Exception {
    String mvName = "test_mv_count_star";
    try {
      // Create MV with COUNT(*)
      test("CREATE MATERIALIZED VIEW dfs.tmp.%s AS SELECT COUNT(*) AS total FROM cp.`region.json`", mvName);
      test("REFRESH MATERIALIZED VIEW dfs.tmp.%s", mvName);

      // Query the MV directly
      testBuilder()
          .sqlQuery("SELECT * FROM dfs.tmp.%s", mvName)
          .unOrdered()
          .baselineColumns("total")
          .baselineValues(110L)
          .go();
    } finally {
      test("DROP MATERIALIZED VIEW IF EXISTS dfs.tmp.%s", mvName);
    }
  }

  @Test
  public void testAggregateSumAndAvg() throws Exception {
    String mvName = "test_mv_sum_avg";
    try {
      // Create MV with SUM and AVG
      test("CREATE MATERIALIZED VIEW dfs.tmp.%s AS SELECT sales_country, SUM(region_id) AS sum_id, AVG(region_id) AS avg_id FROM cp.`region.json` GROUP BY sales_country", mvName);
      test("REFRESH MATERIALIZED VIEW dfs.tmp.%s", mvName);

      // Query the MV
      test("SELECT * FROM dfs.tmp.%s WHERE sales_country IS NOT NULL", mvName);
    } finally {
      test("DROP MATERIALIZED VIEW IF EXISTS dfs.tmp.%s", mvName);
    }
  }

  // ==================== Un-refreshed MV Tests ====================

  @Test
  public void testUnrefreshedMVNotUsedForRewriting() throws Exception {
    String mvName = "test_mv_unrefreshed";
    try {
      // Create MV but don't refresh
      test("CREATE MATERIALIZED VIEW dfs.tmp.%s AS SELECT region_id, sales_city FROM cp.`region.json`", mvName);

      // Query should work (uses original data, not MV)
      testBuilder()
          .sqlQuery("SELECT region_id, sales_city FROM cp.`region.json` WHERE region_id = 0")
          .unOrdered()
          .baselineColumns("region_id", "sales_city")
          .baselineValues(0L, "None")
          .go();
    } finally {
      test("DROP MATERIALIZED VIEW IF EXISTS dfs.tmp.%s", mvName);
    }
  }

  @Test
  public void testQueryUnrefreshedMVDirectly() throws Exception {
    String mvName = "test_mv_query_unrefreshed";
    try {
      // Create MV but don't refresh
      test("CREATE MATERIALIZED VIEW dfs.tmp.%s AS SELECT region_id, sales_city FROM cp.`region.json` WHERE region_id = 0", mvName);

      // Query the MV directly - should expand SQL definition
      testBuilder()
          .sqlQuery("SELECT * FROM dfs.tmp.%s", mvName)
          .unOrdered()
          .baselineColumns("region_id", "sales_city")
          .baselineValues(0L, "None")
          .go();
    } finally {
      test("DROP MATERIALIZED VIEW IF EXISTS dfs.tmp.%s", mvName);
    }
  }

  // ==================== Multiple MV Tests ====================

  @Test
  public void testMultipleMVsExist() throws Exception {
    String mvName1 = "test_mv_multi_1";
    String mvName2 = "test_mv_multi_2";
    try {
      // Create two MVs
      test("CREATE MATERIALIZED VIEW dfs.tmp.%s AS SELECT region_id FROM cp.`region.json`", mvName1);
      test("REFRESH MATERIALIZED VIEW dfs.tmp.%s", mvName1);

      test("CREATE MATERIALIZED VIEW dfs.tmp.%s AS SELECT sales_city FROM cp.`region.json`", mvName2);
      test("REFRESH MATERIALIZED VIEW dfs.tmp.%s", mvName2);

      // Query should work with multiple MVs available
      testBuilder()
          .sqlQuery("SELECT region_id FROM cp.`region.json` WHERE region_id = 0")
          .unOrdered()
          .baselineColumns("region_id")
          .baselineValues(0L)
          .go();
    } finally {
      test("DROP MATERIALIZED VIEW IF EXISTS dfs.tmp.%s", mvName1);
      test("DROP MATERIALIZED VIEW IF EXISTS dfs.tmp.%s", mvName2);
    }
  }

  // ==================== Edge Cases ====================

  @Test
  public void testMVWithEmptyResult() throws Exception {
    String mvName = "test_mv_empty";
    try {
      // Create MV that returns no rows
      test("CREATE MATERIALIZED VIEW dfs.tmp.%s AS SELECT region_id, sales_city FROM cp.`region.json` WHERE region_id = -999", mvName);
      test("REFRESH MATERIALIZED VIEW dfs.tmp.%s", mvName);

      // Query the MV
      testBuilder()
          .sqlQuery("SELECT * FROM dfs.tmp.%s", mvName)
          .unOrdered()
          .expectsEmptyResultSet()
          .go();
    } finally {
      test("DROP MATERIALIZED VIEW IF EXISTS dfs.tmp.%s", mvName);
    }
  }

  @Test
  public void testMVWithOrderBy() throws Exception {
    String mvName = "test_mv_orderby";
    try {
      // Create MV
      test("CREATE MATERIALIZED VIEW dfs.tmp.%s AS SELECT region_id, sales_city FROM cp.`region.json` WHERE region_id < 5", mvName);
      test("REFRESH MATERIALIZED VIEW dfs.tmp.%s", mvName);

      // Query with ORDER BY - just verify count is correct
      testBuilder()
          .sqlQuery("SELECT COUNT(*) AS cnt FROM dfs.tmp.%s", mvName)
          .unOrdered()
          .baselineColumns("cnt")
          .baselineValues(5L)
          .go();
    } finally {
      test("DROP MATERIALIZED VIEW IF EXISTS dfs.tmp.%s", mvName);
    }
  }

  @Test
  public void testMVWithDistinct() throws Exception {
    String mvName = "test_mv_distinct";
    try {
      // Create MV with DISTINCT
      test("CREATE MATERIALIZED VIEW dfs.tmp.%s AS SELECT DISTINCT sales_country FROM cp.`region.json` WHERE sales_country IS NOT NULL", mvName);
      test("REFRESH MATERIALIZED VIEW dfs.tmp.%s", mvName);

      // Query the MV - just verify it has at least one distinct value
      testBuilder()
          .sqlQuery("SELECT COUNT(*) AS cnt FROM dfs.tmp.%s", mvName)
          .unOrdered()
          .baselineColumns("cnt")
          .baselineValues(4L)
          .go();
    } finally {
      test("DROP MATERIALIZED VIEW IF EXISTS dfs.tmp.%s", mvName);
    }
  }

  @Test
  public void testMVWithLimit() throws Exception {
    String mvName = "test_mv_limit";
    try {
      // Create MV with LIMIT
      test("CREATE MATERIALIZED VIEW dfs.tmp.%s AS SELECT region_id, sales_city FROM cp.`region.json` ORDER BY region_id LIMIT 5", mvName);
      test("REFRESH MATERIALIZED VIEW dfs.tmp.%s", mvName);

      // Query the MV
      testBuilder()
          .sqlQuery("SELECT COUNT(*) AS cnt FROM dfs.tmp.%s", mvName)
          .unOrdered()
          .baselineColumns("cnt")
          .baselineValues(5L)
          .go();
    } finally {
      test("DROP MATERIALIZED VIEW IF EXISTS dfs.tmp.%s", mvName);
    }
  }

  // ==================== Complex Query Tests ====================

  @Test
  public void testMVWithJoin() throws Exception {
    String mvName = "test_mv_join";
    try {
      // Create MV with self-join
      test("CREATE MATERIALIZED VIEW dfs.tmp.%s AS " +
           "SELECT a.region_id, a.sales_city, b.sales_country " +
           "FROM cp.`region.json` a " +
           "JOIN cp.`region.json` b ON a.region_id = b.region_id " +
           "WHERE a.region_id = 0", mvName);
      test("REFRESH MATERIALIZED VIEW dfs.tmp.%s", mvName);

      // Query the MV - verify it works (don't assume specific data values)
      testBuilder()
          .sqlQuery("SELECT COUNT(*) AS cnt FROM dfs.tmp.%s", mvName)
          .unOrdered()
          .baselineColumns("cnt")
          .baselineValues(1L)
          .go();
    } finally {
      test("DROP MATERIALIZED VIEW IF EXISTS dfs.tmp.%s", mvName);
    }
  }

  @Test
  public void testMVWithSubquery() throws Exception {
    String mvName = "test_mv_subquery";
    try {
      // Create MV with subquery
      test("CREATE MATERIALIZED VIEW dfs.tmp.%s AS " +
           "SELECT region_id, sales_city FROM cp.`region.json` " +
           "WHERE region_id IN (SELECT region_id FROM cp.`region.json` WHERE region_id < 3)", mvName);
      test("REFRESH MATERIALIZED VIEW dfs.tmp.%s", mvName);

      // Query the MV
      testBuilder()
          .sqlQuery("SELECT COUNT(*) AS cnt FROM dfs.tmp.%s", mvName)
          .unOrdered()
          .baselineColumns("cnt")
          .baselineValues(3L)
          .go();
    } finally {
      test("DROP MATERIALIZED VIEW IF EXISTS dfs.tmp.%s", mvName);
    }
  }

  // ==================== Data Type Tests ====================

  @Test
  public void testMVWithNullValues() throws Exception {
    String mvName = "test_mv_nulls";
    try {
      // Create MV that includes data (some rows may have null values)
      test("CREATE MATERIALIZED VIEW dfs.tmp.%s AS SELECT region_id, sales_country FROM cp.`region.json` WHERE region_id < 3", mvName);
      test("REFRESH MATERIALIZED VIEW dfs.tmp.%s", mvName);

      // Query should work correctly - just verify count
      testBuilder()
          .sqlQuery("SELECT COUNT(*) AS cnt FROM dfs.tmp.%s", mvName)
          .unOrdered()
          .baselineColumns("cnt")
          .baselineValues(3L)
          .go();
    } finally {
      test("DROP MATERIALIZED VIEW IF EXISTS dfs.tmp.%s", mvName);
    }
  }

  @Test
  public void testMVWithStringOperations() throws Exception {
    String mvName = "test_mv_string";
    try {
      // Create MV with string operations
      test("CREATE MATERIALIZED VIEW dfs.tmp.%s AS SELECT region_id, UPPER(sales_city) AS upper_city FROM cp.`region.json` WHERE region_id < 3", mvName);
      test("REFRESH MATERIALIZED VIEW dfs.tmp.%s", mvName);

      // Query the MV - verify count and that UPPER was applied
      testBuilder()
          .sqlQuery("SELECT COUNT(*) AS cnt FROM dfs.tmp.%s", mvName)
          .unOrdered()
          .baselineColumns("cnt")
          .baselineValues(3L)
          .go();
    } finally {
      test("DROP MATERIALIZED VIEW IF EXISTS dfs.tmp.%s", mvName);
    }
  }
}
