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
package org.apache.drill.exec.store.accumulo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.test.BaseTest;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for projection pushdown in AccumuloGroupScan.
 */
public class AccumuloProjectionPushdownTest extends BaseTest {

  /**
   * Creates a mock AccumuloGroupScan for testing.
   */
  private AccumuloGroupScan createTestGroupScan() {
    AccumuloStoragePlugin mockPlugin = Mockito.mock(AccumuloStoragePlugin.class);
    AccumuloStoragePluginConfig mockConfig = Mockito.mock(AccumuloStoragePluginConfig.class);
    Mockito.when(mockPlugin.getConfig()).thenReturn(mockConfig);

    AccumuloScanSpec scanSpec = new AccumuloScanSpec("test_table");
    return new AccumuloGroupScan("testUser", mockPlugin, scanSpec, null, -1);
  }

  @Test
  public void testCloneWithAllColumns() {
    AccumuloGroupScan original = createTestGroupScan();

    // Clone with ALL_COLUMNS should not mark projection as pushed down
    GroupScan cloned = original.clone(GroupScan.ALL_COLUMNS);

    assertTrue(cloned instanceof AccumuloGroupScan);
    AccumuloGroupScan accumuloCloned = (AccumuloGroupScan) cloned;

    assertNotSame(original, cloned);
    assertFalse("Projection should not be marked as pushed down for ALL_COLUMNS",
        accumuloCloned.isProjectionPushedDown());
  }

  @Test
  public void testCloneWithSpecificColumns() {
    AccumuloGroupScan original = createTestGroupScan();

    List<SchemaPath> projectedColumns = Arrays.asList(
        SchemaPath.getSimplePath("row_key"),
        SchemaPath.getSimplePath("name"),
        SchemaPath.getSimplePath("age")
    );

    GroupScan cloned = original.clone(projectedColumns);

    assertTrue(cloned instanceof AccumuloGroupScan);
    AccumuloGroupScan accumuloCloned = (AccumuloGroupScan) cloned;

    assertNotSame(original, cloned);
    assertTrue("Projection should be marked as pushed down for specific columns",
        accumuloCloned.isProjectionPushedDown());
    assertEquals(projectedColumns, accumuloCloned.getColumns());
  }

  @Test
  public void testCloneWithSingleColumn() {
    AccumuloGroupScan original = createTestGroupScan();

    List<SchemaPath> projectedColumns = Arrays.asList(
        SchemaPath.getSimplePath("row_key")
    );

    GroupScan cloned = original.clone(projectedColumns);

    assertTrue(cloned instanceof AccumuloGroupScan);
    AccumuloGroupScan accumuloCloned = (AccumuloGroupScan) cloned;

    assertTrue("Projection should be marked as pushed down for single column",
        accumuloCloned.isProjectionPushedDown());
    assertEquals(1, accumuloCloned.getColumns().size());
  }

  @Test
  public void testClonePreservesOtherPushdownFlags() {
    AccumuloGroupScan original = createTestGroupScan();
    original.setFilterPushedDown(true);
    original.setSortPushedDown(true);
    original.setLimitPushedDown(true);

    List<SchemaPath> projectedColumns = Arrays.asList(
        SchemaPath.getSimplePath("col1"),
        SchemaPath.getSimplePath("col2")
    );

    GroupScan cloned = original.clone(projectedColumns);
    AccumuloGroupScan accumuloCloned = (AccumuloGroupScan) cloned;

    // All flags should be preserved
    assertTrue("Filter pushdown flag should be preserved", accumuloCloned.isFilterPushedDown());
    assertTrue("Sort pushdown flag should be preserved", accumuloCloned.isSortPushedDown());
    assertTrue("Limit pushdown flag should be preserved", accumuloCloned.isLimitPushedDown());
    assertTrue("Projection should be marked as pushed down", accumuloCloned.isProjectionPushedDown());
  }

  @Test
  public void testClonePreservesScanSpec() {
    AccumuloGroupScan original = createTestGroupScan();

    List<SchemaPath> projectedColumns = Arrays.asList(
        SchemaPath.getSimplePath("col1")
    );

    GroupScan cloned = original.clone(projectedColumns);
    AccumuloGroupScan accumuloCloned = (AccumuloGroupScan) cloned;

    assertNotNull("ScanSpec should be preserved", accumuloCloned.getScanSpec());
    assertEquals("test_table", accumuloCloned.getTableName());
  }

  @Test
  public void testScanStatsWithProjectionPushdown() {
    AccumuloGroupScan scan = createTestGroupScan();
    ScanStats statsWithoutProjection = scan.getScanStats();

    // Clone with specific columns (triggers projection pushdown)
    List<SchemaPath> projectedColumns = Arrays.asList(
        SchemaPath.getSimplePath("col1"),
        SchemaPath.getSimplePath("col2")
    );
    AccumuloGroupScan projectedScan = (AccumuloGroupScan) scan.clone(projectedColumns);
    ScanStats statsWithProjection = projectedScan.getScanStats();

    // CPU cost should be reduced with projection pushdown
    assertTrue("CPU cost should be lower with projection pushdown",
        statsWithProjection.getCpuCost() < statsWithoutProjection.getCpuCost());
  }

  @Test
  public void testScanStatsWithFilterAndProjectionPushdown() {
    AccumuloGroupScan scan = createTestGroupScan();
    ScanStats baseStats = scan.getScanStats();

    // Apply both filter and projection pushdowns
    scan.setFilterPushedDown(true);
    List<SchemaPath> projectedColumns = Arrays.asList(
        SchemaPath.getSimplePath("col1")
    );
    AccumuloGroupScan projectedScan = (AccumuloGroupScan) scan.clone(projectedColumns);
    projectedScan.setFilterPushedDown(true);
    ScanStats combinedStats = projectedScan.getScanStats();

    // Combined pushdowns should result in even lower cost
    assertTrue("Combined pushdowns should reduce CPU cost significantly",
        combinedStats.getCpuCost() < baseStats.getCpuCost());
    assertTrue("Combined pushdowns should reduce row count estimate",
        combinedStats.getRecordCount() < baseStats.getRecordCount());
  }

  @Test
  public void testOriginalNotModifiedByClone() {
    AccumuloGroupScan original = createTestGroupScan();
    assertFalse("Original should not have projection pushed down initially",
        original.isProjectionPushedDown());

    List<SchemaPath> projectedColumns = Arrays.asList(
        SchemaPath.getSimplePath("col1")
    );

    // Clone with projection
    original.clone(projectedColumns);

    // Original should remain unchanged
    assertFalse("Original should still not have projection pushed down",
        original.isProjectionPushedDown());
  }

  @Test
  public void testCloneWithNullColumns() {
    AccumuloGroupScan original = createTestGroupScan();

    GroupScan cloned = original.clone(null);

    assertTrue(cloned instanceof AccumuloGroupScan);
    AccumuloGroupScan accumuloCloned = (AccumuloGroupScan) cloned;

    // Null columns should be treated as ALL_COLUMNS
    assertFalse("Projection should not be marked as pushed down for null columns",
        accumuloCloned.isProjectionPushedDown());
  }

  @Test
  public void testToStringIncludesProjectionFlag() {
    AccumuloGroupScan scan = createTestGroupScan();

    List<SchemaPath> projectedColumns = Arrays.asList(
        SchemaPath.getSimplePath("col1")
    );
    AccumuloGroupScan projectedScan = (AccumuloGroupScan) scan.clone(projectedColumns);

    String toString = projectedScan.toString();
    assertTrue("toString should include projectionPushedDown=true",
        toString.contains("projectionPushedDown=true"));
  }
}
