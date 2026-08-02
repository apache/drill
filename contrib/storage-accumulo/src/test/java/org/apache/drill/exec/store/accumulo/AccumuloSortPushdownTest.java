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

import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.test.BaseTest;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for sort pushdown in AccumuloGroupScan.
 */
public class AccumuloSortPushdownTest extends BaseTest {

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
  public void testScanSpecWithSortOrderAscending() {
    AccumuloScanSpec original = new AccumuloScanSpec("test_table");
    assertFalse("Default should not use sorted scanner", original.isUseSortedScanner());
    assertFalse("Default should not be descending", original.isSortDescending());

    AccumuloScanSpec withSort = original.withSortOrder(false);

    assertTrue("Should use sorted scanner after withSortOrder", withSort.isUseSortedScanner());
    assertFalse("Should be ascending", withSort.isSortDescending());
    assertEquals("test_table", withSort.getTableName());
  }

  @Test
  public void testScanSpecWithSortOrderDescending() {
    AccumuloScanSpec original = new AccumuloScanSpec("test_table");

    AccumuloScanSpec withSort = original.withSortOrder(true);

    assertTrue("Should use sorted scanner after withSortOrder", withSort.isUseSortedScanner());
    assertTrue("Should be descending", withSort.isSortDescending());
    assertEquals("test_table", withSort.getTableName());
  }

  @Test
  public void testGroupScanSortPushedDown() {
    AccumuloGroupScan scan = createTestGroupScan();
    assertFalse("Sort should not be pushed down initially", scan.isSortPushedDown());

    scan.setSortPushedDown(true);
    assertTrue("Sort should be pushed down after setting", scan.isSortPushedDown());
  }

  @Test
  public void testCloneWithNewScanSpecPreservesSortFlag() {
    AccumuloGroupScan original = createTestGroupScan();
    original.setSortPushedDown(true);

    AccumuloScanSpec newSpec = original.getScanSpec().withSortOrder(false);
    AccumuloGroupScan cloned = original.cloneWithNewScanSpec(newSpec);

    assertTrue("Sort pushdown flag should be preserved", cloned.isSortPushedDown());
    assertTrue("New scan spec should use sorted scanner", cloned.getScanSpec().isUseSortedScanner());
  }

  @Test
  public void testScanStatsWithSortPushdown() {
    AccumuloGroupScan scan = createTestGroupScan();
    ScanStats baseStats = scan.getScanStats();

    scan.setSortPushedDown(true);
    ScanStats sortedStats = scan.getScanStats();

    // Sort pushdown has a slight cost penalty because we use Scanner instead of BatchScanner
    assertTrue("Sort pushdown should slightly increase CPU cost due to Scanner vs BatchScanner",
        sortedStats.getCpuCost() >= baseStats.getCpuCost());
  }

  @Test
  public void testToStringIncludesSortFlag() {
    AccumuloGroupScan scan = createTestGroupScan();
    scan.setSortPushedDown(true);

    String toString = scan.toString();
    assertTrue("toString should include sortPushedDown=true",
        toString.contains("sortPushedDown=true"));
  }

  @Test
  public void testScanSpecToStringIncludesSortInfo() {
    AccumuloScanSpec spec = new AccumuloScanSpec("test_table").withSortOrder(true);

    String toString = spec.toString();
    assertTrue("toString should include useSortedScanner=true",
        toString.contains("useSortedScanner=true"));
    assertTrue("toString should include sortDescending=true",
        toString.contains("sortDescending=true"));
  }

  @Test
  public void testScanSpecEquality() {
    AccumuloScanSpec spec1 = new AccumuloScanSpec("test_table").withSortOrder(false);
    AccumuloScanSpec spec2 = new AccumuloScanSpec("test_table").withSortOrder(false);
    AccumuloScanSpec spec3 = new AccumuloScanSpec("test_table").withSortOrder(true);

    assertEquals("Same sort order specs should be equal", spec1, spec2);
    assertFalse("Different sort order specs should not be equal", spec1.equals(spec3));
  }

  @Test
  public void testScanSpecHashCode() {
    AccumuloScanSpec spec1 = new AccumuloScanSpec("test_table").withSortOrder(false);
    AccumuloScanSpec spec2 = new AccumuloScanSpec("test_table").withSortOrder(false);
    AccumuloScanSpec spec3 = new AccumuloScanSpec("test_table").withSortOrder(true);

    assertEquals("Same sort order specs should have equal hash codes",
        spec1.hashCode(), spec2.hashCode());
    // Different specs may have different hash codes (not guaranteed, but likely)
    assertFalse("Different sort order specs likely have different hash codes",
        spec1.hashCode() == spec3.hashCode());
  }

  @Test
  public void testScanSpecWithMultiplePushdowns() {
    AccumuloScanSpec original = new AccumuloScanSpec("test_table");

    AccumuloScanSpec withAll = original
        .withFilter("row_key > 'a'")
        .withLimit(100)
        .withSortOrder(true);

    assertEquals("test_table", withAll.getTableName());
    assertEquals("row_key > 'a'", withAll.getFilterExpression());
    assertEquals(Integer.valueOf(100), withAll.getLimit());
    assertTrue(withAll.isUseSortedScanner());
    assertTrue(withAll.isSortDescending());
  }

  @Test
  public void testGroupScanCopyPreservesAllFlags() {
    AccumuloGroupScan original = createTestGroupScan();
    original.setFilterPushedDown(true);
    original.setProjectionPushedDown(true);
    original.setLimitPushedDown(true);
    original.setSortPushedDown(true);

    AccumuloScanSpec newSpec = original.getScanSpec().withSortOrder(false);
    AccumuloGroupScan cloned = original.cloneWithNewScanSpec(newSpec);

    assertTrue("Filter pushdown should be preserved", cloned.isFilterPushedDown());
    assertTrue("Projection pushdown should be preserved", cloned.isProjectionPushedDown());
    assertTrue("Limit pushdown should be preserved", cloned.isLimitPushedDown());
    assertTrue("Sort pushdown should be preserved", cloned.isSortPushedDown());
  }
}
