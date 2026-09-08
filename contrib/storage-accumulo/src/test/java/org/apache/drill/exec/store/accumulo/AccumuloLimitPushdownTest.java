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
import static org.junit.Assert.assertNull;
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
 * Unit tests for limit pushdown in AccumuloGroupScan.
 */
public class AccumuloLimitPushdownTest extends BaseTest {

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
  public void testSupportsLimitPushdown() {
    AccumuloGroupScan scan = createTestGroupScan();
    assertTrue("AccumuloGroupScan should support limit pushdown", scan.supportsLimitPushdown());
  }

  @Test
  public void testApplyLimitReturnsNewScan() {
    AccumuloGroupScan original = createTestGroupScan();

    GroupScan newScan = original.applyLimit(100);

    assertNotNull("applyLimit should return a new scan", newScan);
    assertNotSame("applyLimit should return a different instance", original, newScan);
    assertTrue(newScan instanceof AccumuloGroupScan);
  }

  @Test
  public void testApplyLimitSetsMaxRecords() {
    AccumuloGroupScan original = createTestGroupScan();

    AccumuloGroupScan newScan = (AccumuloGroupScan) original.applyLimit(100);

    assertEquals(100, newScan.getMaxRecords());
    assertTrue("Limit should be marked as pushed down", newScan.isLimitPushedDown());
  }

  @Test
  public void testApplyLimitDoesNotModifyOriginal() {
    AccumuloGroupScan original = createTestGroupScan();
    int originalMaxRecords = original.getMaxRecords();

    original.applyLimit(100);

    assertEquals("Original maxRecords should be unchanged", originalMaxRecords, original.getMaxRecords());
    assertFalse("Original should not have limit pushed down", original.isLimitPushedDown());
  }

  @Test
  public void testApplyLimitWithMoreRestrictiveExisting() {
    AccumuloStoragePlugin mockPlugin = Mockito.mock(AccumuloStoragePlugin.class);
    AccumuloStoragePluginConfig mockConfig = Mockito.mock(AccumuloStoragePluginConfig.class);
    Mockito.when(mockPlugin.getConfig()).thenReturn(mockConfig);

    AccumuloScanSpec scanSpec = new AccumuloScanSpec("test_table");
    // Create scan with limit already set to 50
    AccumuloGroupScan original = new AccumuloGroupScan("testUser", mockPlugin, scanSpec, null, 50);

    // Try to apply a higher limit
    GroupScan newScan = original.applyLimit(100);

    // Should return null because existing limit is more restrictive
    assertNull("Should return null when existing limit is more restrictive", newScan);
  }

  @Test
  public void testApplyLimitWithLessRestrictiveExisting() {
    AccumuloStoragePlugin mockPlugin = Mockito.mock(AccumuloStoragePlugin.class);
    AccumuloStoragePluginConfig mockConfig = Mockito.mock(AccumuloStoragePluginConfig.class);
    Mockito.when(mockPlugin.getConfig()).thenReturn(mockConfig);

    AccumuloScanSpec scanSpec = new AccumuloScanSpec("test_table");
    // Create scan with limit already set to 100
    AccumuloGroupScan original = new AccumuloGroupScan("testUser", mockPlugin, scanSpec, null, 100);

    // Try to apply a lower limit
    AccumuloGroupScan newScan = (AccumuloGroupScan) original.applyLimit(50);

    // Should return new scan with lower limit
    assertNotNull("Should return new scan with more restrictive limit", newScan);
    assertEquals(50, newScan.getMaxRecords());
  }

  @Test
  public void testApplyLimitIsNotReappliedToItsOwnResult() {
    // The planner rule keeps firing as long as applyLimit hands back a new scan, so
    // re-applying the same limit must return null or planning never terminates.
    AccumuloGroupScan original = createTestGroupScan();

    AccumuloGroupScan limited = (AccumuloGroupScan) original.applyLimit(100);

    assertNull("Re-applying the same limit should return null", limited.applyLimit(100));
  }

  @Test
  public void testApplyLimitZeroIsNotReapplied() {
    // LIMIT 0 is the case that regressed: a zero limit must still be recognised as
    // already pushed down.
    AccumuloGroupScan original = createTestGroupScan();

    AccumuloGroupScan limited = (AccumuloGroupScan) original.applyLimit(0);

    assertNotNull("A zero limit should still be pushed down", limited);
    assertEquals(0, limited.getMaxRecords());
    assertNull("Re-applying a zero limit should return null", limited.applyLimit(0));
  }

  @Test
  public void testApplyLimitPreservesOtherPushdowns() {
    AccumuloGroupScan original = createTestGroupScan();
    original.setFilterPushedDown(true);
    original.setProjectionPushedDown(true);
    original.setSortPushedDown(true);

    AccumuloGroupScan newScan = (AccumuloGroupScan) original.applyLimit(100);

    assertTrue("Filter pushdown should be preserved", newScan.isFilterPushedDown());
    assertTrue("Projection pushdown should be preserved", newScan.isProjectionPushedDown());
    assertTrue("Sort pushdown should be preserved", newScan.isSortPushedDown());
    assertTrue("Limit should be marked as pushed down", newScan.isLimitPushedDown());
  }

  @Test
  public void testApplyLimitPreservesScanSpec() {
    AccumuloGroupScan original = createTestGroupScan();

    AccumuloGroupScan newScan = (AccumuloGroupScan) original.applyLimit(100);

    assertNotNull("ScanSpec should be preserved", newScan.getScanSpec());
    assertEquals("test_table", newScan.getTableName());
  }

  @Test
  public void testApplyLimitPreservesColumns() {
    AccumuloGroupScan original = createTestGroupScan();
    List<SchemaPath> columns = Arrays.asList(
        SchemaPath.getSimplePath("row_key"),
        SchemaPath.getSimplePath("col1")
    );
    AccumuloGroupScan withColumns = (AccumuloGroupScan) original.clone(columns);

    AccumuloGroupScan newScan = (AccumuloGroupScan) withColumns.applyLimit(100);

    assertEquals(columns, newScan.getColumns());
  }

  @Test
  public void testScanStatsWithLimitPushdown() {
    AccumuloGroupScan scan = createTestGroupScan();
    ScanStats baseStats = scan.getScanStats();

    AccumuloGroupScan limitedScan = (AccumuloGroupScan) scan.applyLimit(50);
    ScanStats limitedStats = limitedScan.getScanStats();

    // With a limit of 50, row count should be capped at 50
    assertTrue("Row count should be reduced with limit pushdown",
        limitedStats.getRecordCount() <= 50);
    assertTrue("CPU cost should be lower with limit pushdown",
        limitedStats.getCpuCost() < baseStats.getCpuCost());
  }

  @Test
  public void testScanStatsWithFilterAndLimitPushdown() {
    AccumuloGroupScan scan = createTestGroupScan();
    scan.setFilterPushedDown(true);

    AccumuloGroupScan limitedScan = (AccumuloGroupScan) scan.applyLimit(100);
    limitedScan.setFilterPushedDown(true);
    ScanStats combinedStats = limitedScan.getScanStats();

    // Combined pushdowns should result in efficient stats
    assertTrue("Row count should be bounded by limit",
        combinedStats.getRecordCount() <= 100);
  }

  @Test
  public void testToStringIncludesLimitInfo() {
    AccumuloGroupScan scan = createTestGroupScan();
    AccumuloGroupScan limitedScan = (AccumuloGroupScan) scan.applyLimit(100);

    String toString = limitedScan.toString();
    assertTrue("toString should include maxRecords", toString.contains("maxRecords=100"));
    assertTrue("toString should include limitPushedDown=true", toString.contains("limitPushedDown=true"));
  }

  @Test
  public void testSubScanContainsMaxRecords() {
    AccumuloStoragePlugin mockPlugin = Mockito.mock(AccumuloStoragePlugin.class);
    AccumuloStoragePluginConfig mockConfig = Mockito.mock(AccumuloStoragePluginConfig.class);
    Mockito.when(mockPlugin.getConfig()).thenReturn(mockConfig);

    AccumuloScanSpec scanSpec = new AccumuloScanSpec("test_table");
    AccumuloGroupScan groupScan = new AccumuloGroupScan("testUser", mockPlugin, scanSpec, null, 100);
    groupScan.setLimitPushedDown(true);

    AccumuloSubScan subScan = groupScan.getSpecificScan(0);

    assertEquals("SubScan should have maxRecords from GroupScan", 100, subScan.getMaxRecords());
  }

  @Test
  public void testSubScanWithNoLimit() {
    AccumuloGroupScan scan = createTestGroupScan();

    AccumuloSubScan subScan = scan.getSpecificScan(0);

    assertEquals("SubScan should have -1 for no limit", -1, subScan.getMaxRecords());
  }
}
