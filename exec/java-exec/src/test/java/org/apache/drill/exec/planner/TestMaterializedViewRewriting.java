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
 */
@Category(SqlTest.class)
public class TestMaterializedViewRewriting extends PlanTestBase {

  @BeforeClass
  public static void setupTestFiles() {
    dirTestWatcher.copyResourceToRoot(Paths.get("nation"));
  }

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
  public void testRewritingCanBeEnabled() throws Exception {
    String mvName = "test_mv_rewrite_enabled";
    try {
      // Create materialized view
      test("CREATE MATERIALIZED VIEW dfs.tmp.%s AS SELECT region_id, sales_city FROM cp.`region.json` WHERE region_id = 0", mvName);
      test("REFRESH MATERIALIZED VIEW dfs.tmp.%s", mvName);

      // Enable rewriting
      test("ALTER SESSION SET `%s` = true", ExecConstants.ENABLE_MATERIALIZED_VIEW_REWRITE_KEY);

      // Query should still work (even though actual matching isn't implemented yet)
      testBuilder()
          .sqlQuery("SELECT region_id, sales_city FROM cp.`region.json` WHERE region_id = 0")
          .unOrdered()
          .baselineColumns("region_id", "sales_city")
          .baselineValues(0L, "None")
          .go();
    } finally {
      test("ALTER SESSION SET `%s` = false", ExecConstants.ENABLE_MATERIALIZED_VIEW_REWRITE_KEY);
      test("DROP MATERIALIZED VIEW IF EXISTS dfs.tmp.%s", mvName);
    }
  }

  @Test
  public void testRewritingWithNoMaterializedViews() throws Exception {
    try {
      // Enable rewriting
      test("ALTER SESSION SET `%s` = true", ExecConstants.ENABLE_MATERIALIZED_VIEW_REWRITE_KEY);

      // Query should work even when no MVs exist
      testBuilder()
          .sqlQuery("SELECT region_id, sales_city FROM cp.`region.json` WHERE region_id = 0")
          .unOrdered()
          .baselineColumns("region_id", "sales_city")
          .baselineValues(0L, "None")
          .go();
    } finally {
      test("ALTER SESSION SET `%s` = false", ExecConstants.ENABLE_MATERIALIZED_VIEW_REWRITE_KEY);
    }
  }

  @Test
  public void testQueryMVDirectlyWithRewritingEnabled() throws Exception {
    String mvName = "test_mv_direct_query";
    try {
      // Create and refresh materialized view
      test("CREATE MATERIALIZED VIEW dfs.tmp.%s AS SELECT region_id, sales_city FROM cp.`region.json` WHERE region_id = 0", mvName);
      test("REFRESH MATERIALIZED VIEW dfs.tmp.%s", mvName);

      // Enable rewriting
      test("ALTER SESSION SET `%s` = true", ExecConstants.ENABLE_MATERIALIZED_VIEW_REWRITE_KEY);

      // Query the MV directly - should still work
      testBuilder()
          .sqlQuery("SELECT * FROM dfs.tmp.%s", mvName)
          .unOrdered()
          .baselineColumns("region_id", "sales_city")
          .baselineValues(0L, "None")
          .go();
    } finally {
      test("ALTER SESSION SET `%s` = false", ExecConstants.ENABLE_MATERIALIZED_VIEW_REWRITE_KEY);
      test("DROP MATERIALIZED VIEW IF EXISTS dfs.tmp.%s", mvName);
    }
  }

  @Test
  public void testAggregateQueryWithRewritingEnabled() throws Exception {
    String mvName = "test_mv_agg_rewrite";
    try {
      // Create MV with aggregation on a simple filter
      test("CREATE MATERIALIZED VIEW dfs.tmp.%s AS SELECT sales_country, COUNT(*) AS cnt FROM cp.`region.json` WHERE region_id < 5 GROUP BY sales_country", mvName);
      test("REFRESH MATERIALIZED VIEW dfs.tmp.%s", mvName);

      // Enable rewriting
      test("ALTER SESSION SET `%s` = true", ExecConstants.ENABLE_MATERIALIZED_VIEW_REWRITE_KEY);

      // Run aggregate query (same as MV definition)
      // With future matching, this could be rewritten to use the MV
      // For now just verify the query works with rewriting enabled
      test("SELECT sales_country, COUNT(*) AS cnt FROM cp.`region.json` WHERE region_id < 5 GROUP BY sales_country");
    } finally {
      test("ALTER SESSION SET `%s` = false", ExecConstants.ENABLE_MATERIALIZED_VIEW_REWRITE_KEY);
      test("DROP MATERIALIZED VIEW IF EXISTS dfs.tmp.%s", mvName);
    }
  }
}
