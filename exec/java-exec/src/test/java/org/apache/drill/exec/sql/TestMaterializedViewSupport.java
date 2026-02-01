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
package org.apache.drill.exec.sql;

import java.io.File;
import java.nio.file.Paths;

import org.apache.drill.PlanTestBase;
import org.apache.drill.categories.SqlTest;
import org.apache.drill.common.exceptions.UserRemoteException;
import org.apache.drill.exec.dotdrill.DotDrillType;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.apache.drill.exec.util.StoragePluginTestUtils.DFS_TMP_SCHEMA;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for materialized view support in Drill.
 * <p>
 * Tests CREATE, DROP, and REFRESH MATERIALIZED VIEW statements.
 */
@Category(SqlTest.class)
public class TestMaterializedViewSupport extends PlanTestBase {

  @BeforeClass
  public static void setupTestFiles() {
    dirTestWatcher.copyResourceToRoot(Paths.get("nation"));
  }

  @Test
  public void testCreateMaterializedView() throws Exception {
    String mvName = "test_mv_create";
    try {
      // Create a simple materialized view
      testBuilder()
          .sqlQuery("CREATE MATERIALIZED VIEW dfs.tmp.%s AS SELECT * FROM cp.`region.json` LIMIT 5", mvName)
          .unOrdered()
          .baselineColumns("ok", "summary")
          .baselineValues(true, String.format("Materialized view '%s' created successfully in 'dfs.tmp' schema", mvName))
          .go();

      // Verify the materialized view definition file exists
      File mvFile = new File(dirTestWatcher.getDfsTestTmpDir(), mvName + DotDrillType.MATERIALIZED_VIEW.getEnding());
      assertTrue("Materialized view definition file should exist", mvFile.exists());

    } finally {
      test("DROP MATERIALIZED VIEW IF EXISTS dfs.tmp.%s", mvName);
    }
  }

  @Test
  public void testCreateOrReplaceMaterializedView() throws Exception {
    String mvName = "test_mv_replace";
    try {
      // Create initial materialized view
      test("CREATE MATERIALIZED VIEW dfs.tmp.%s AS SELECT region_id FROM cp.`region.json` LIMIT 3", mvName);

      // Replace with different definition
      testBuilder()
          .sqlQuery("CREATE OR REPLACE MATERIALIZED VIEW dfs.tmp.%s AS SELECT region_id, sales_city FROM cp.`region.json` LIMIT 5", mvName)
          .unOrdered()
          .baselineColumns("ok", "summary")
          .baselineValues(true, String.format("Materialized view '%s' replaced successfully in 'dfs.tmp' schema", mvName))
          .go();

    } finally {
      test("DROP MATERIALIZED VIEW IF EXISTS dfs.tmp.%s", mvName);
    }
  }

  @Test
  public void testCreateMaterializedViewIfNotExists() throws Exception {
    String mvName = "test_mv_if_not_exists";
    try {
      // Create initial materialized view
      test("CREATE MATERIALIZED VIEW dfs.tmp.%s AS SELECT * FROM cp.`region.json` LIMIT 3", mvName);

      // Try to create again with IF NOT EXISTS - should not fail
      testBuilder()
          .sqlQuery("CREATE MATERIALIZED VIEW IF NOT EXISTS dfs.tmp.%s AS SELECT * FROM cp.`region.json` LIMIT 5", mvName)
          .unOrdered()
          .baselineColumns("ok", "summary")
          .baselineValues(false, String.format("A table or view with given name [%s] already exists in schema [dfs.tmp]", mvName))
          .go();

    } finally {
      test("DROP MATERIALIZED VIEW IF EXISTS dfs.tmp.%s", mvName);
    }
  }

  @Test
  public void testDropMaterializedView() throws Exception {
    String mvName = "test_mv_drop";

    // Create materialized view
    test("CREATE MATERIALIZED VIEW dfs.tmp.%s AS SELECT * FROM cp.`region.json` LIMIT 5", mvName);

    // Verify it exists
    File mvFile = new File(dirTestWatcher.getDfsTestTmpDir(), mvName + DotDrillType.MATERIALIZED_VIEW.getEnding());
    assertTrue("Materialized view should exist before drop", mvFile.exists());

    // Drop the materialized view
    testBuilder()
        .sqlQuery("DROP MATERIALIZED VIEW dfs.tmp.%s", mvName)
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(true, String.format("Materialized view [%s] deleted successfully from schema [dfs.tmp].", mvName))
        .go();

    // Verify it no longer exists
    assertFalse("Materialized view should not exist after drop", mvFile.exists());
  }

  @Test
  public void testDropMaterializedViewIfExists() throws Exception {
    String mvName = "test_mv_drop_if_exists";

    // Drop non-existent materialized view with IF EXISTS - should not fail
    testBuilder()
        .sqlQuery("DROP MATERIALIZED VIEW IF EXISTS dfs.tmp.%s", mvName)
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(false, String.format("Materialized view [%s] not found in schema [dfs.tmp].", mvName))
        .go();
  }

  @Test(expected = UserRemoteException.class)
  public void testDropNonExistentMaterializedView() throws Exception {
    // Should throw error when dropping non-existent MV without IF EXISTS
    test("DROP MATERIALIZED VIEW dfs.tmp.non_existent_mv");
  }

  @Test
  public void testRefreshMaterializedView() throws Exception {
    String mvName = "test_mv_refresh";
    try {
      // Create materialized view
      test("CREATE MATERIALIZED VIEW dfs.tmp.%s AS SELECT * FROM cp.`region.json` LIMIT 5", mvName);

      // Refresh the materialized view
      testBuilder()
          .sqlQuery("REFRESH MATERIALIZED VIEW dfs.tmp.%s", mvName)
          .unOrdered()
          .baselineColumns("ok", "summary")
          .baselineValues(true, String.format("Materialized view [%s] refreshed successfully in schema [dfs.tmp].", mvName))
          .go();

    } finally {
      test("DROP MATERIALIZED VIEW IF EXISTS dfs.tmp.%s", mvName);
    }
  }

  @Test(expected = UserRemoteException.class)
  public void testRefreshNonExistentMaterializedView() throws Exception {
    // Should throw error when refreshing non-existent MV
    test("REFRESH MATERIALIZED VIEW dfs.tmp.non_existent_mv");
  }

  @Test
  public void testQueryMaterializedView() throws Exception {
    String mvName = "test_mv_query";
    try {
      // Create materialized view
      test("CREATE MATERIALIZED VIEW dfs.tmp.%s AS SELECT region_id, sales_city FROM cp.`region.json` ORDER BY region_id LIMIT 3", mvName);

      // Query the materialized view
      testBuilder()
          .sqlQuery("SELECT * FROM dfs.tmp.%s", mvName)
          .unOrdered()
          .baselineColumns("region_id", "sales_city")
          .baselineValues(0L, "None")
          .baselineValues(1L, "San Francisco")
          .baselineValues(2L, "San Diego")
          .go();

    } finally {
      test("DROP MATERIALIZED VIEW IF EXISTS dfs.tmp.%s", mvName);
    }
  }

  @Test
  public void testMaterializedViewWithAggregation() throws Exception {
    String mvName = "test_mv_agg";
    try {
      // Create materialized view with aggregation
      test("CREATE MATERIALIZED VIEW dfs.tmp.%s AS SELECT sales_country, COUNT(*) AS cnt FROM cp.`region.json` GROUP BY sales_country", mvName);

      // Query the materialized view
      testBuilder()
          .sqlQuery("SELECT * FROM dfs.tmp.%s WHERE sales_country = 'No Country'", mvName)
          .unOrdered()
          .baselineColumns("sales_country", "cnt")
          .baselineValues("No Country", 1L)
          .go();

    } finally {
      test("DROP MATERIALIZED VIEW IF EXISTS dfs.tmp.%s", mvName);
    }
  }

  @Test
  public void testMaterializedViewInShowTables() throws Exception {
    String mvName = "test_mv_show";
    try {
      // Create materialized view
      test("CREATE MATERIALIZED VIEW dfs.tmp.%s AS SELECT * FROM cp.`region.json` LIMIT 1", mvName);

      // Verify it shows up in SHOW TABLES
      String showTablesQuery = "SHOW TABLES IN dfs.tmp LIKE '%s'";
      testBuilder()
          .sqlQuery(showTablesQuery, mvName)
          .unOrdered()
          .baselineColumns("TABLE_SCHEMA", "TABLE_NAME")
          .baselineValues("dfs.tmp", mvName)
          .go();

    } finally {
      test("DROP MATERIALIZED VIEW IF EXISTS dfs.tmp.%s", mvName);
    }
  }

  @Test(expected = UserRemoteException.class)
  public void testCreateMaterializedViewOnNonWritableSchema() throws Exception {
    // cp schema is not writable
    test("CREATE MATERIALIZED VIEW cp.test_mv AS SELECT * FROM cp.`region.json`");
  }

  @Test(expected = UserRemoteException.class)
  public void testCannotCreateMaterializedViewOverRegularView() throws Exception {
    String viewName = "test_regular_view";
    String mvName = viewName; // Same name as regular view
    try {
      // Create a regular view first
      test("CREATE VIEW dfs.tmp.%s AS SELECT * FROM cp.`region.json` LIMIT 5", viewName);

      // Try to create materialized view with same name using OR REPLACE - should fail
      test("CREATE OR REPLACE MATERIALIZED VIEW dfs.tmp.%s AS SELECT * FROM cp.`region.json`", mvName);
    } finally {
      test("DROP VIEW IF EXISTS dfs.tmp.%s", viewName);
    }
  }

  @Test
  public void testMaterializedViewWithFieldList() throws Exception {
    String mvName = "test_mv_fields";
    try {
      // Create materialized view with explicit field list
      test("CREATE MATERIALIZED VIEW dfs.tmp.%s (id, city) AS SELECT region_id, sales_city FROM cp.`region.json` LIMIT 3", mvName);

      // Query the materialized view with renamed fields
      testBuilder()
          .sqlQuery("SELECT id, city FROM dfs.tmp.%s ORDER BY id LIMIT 2", mvName)
          .ordered()
          .baselineColumns("id", "city")
          .baselineValues(0L, "None")
          .baselineValues(1L, "San Francisco")
          .go();

    } finally {
      test("DROP MATERIALIZED VIEW IF EXISTS dfs.tmp.%s", mvName);
    }
  }
}
