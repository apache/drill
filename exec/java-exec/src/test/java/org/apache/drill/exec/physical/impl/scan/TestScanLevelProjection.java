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
package org.apache.drill.exec.physical.impl.scan;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.impl.scan.project.ScanLevelProjection;
import org.apache.drill.exec.physical.impl.scan.project.UnresolvedColumn;
import org.apache.drill.exec.physical.rowSet.impl.RowSetTestUtils;
import org.apache.drill.exec.physical.rowSet.project.RequestedTuple.RequestedColumn;
import org.apache.drill.exec.record.metadata.ProjectionType;
import org.apache.drill.test.SubOperatorTest;
import org.junit.Test;

/**
 * Test the level of projection done at the level of the scan as a whole;
 * before knowledge of table "implicit" columns or the specific table schema.
 */

public class TestScanLevelProjection extends SubOperatorTest {

  /**
   * Basic test: select a set of columns (a, b, c) when the
   * data source has an early schema of (a, c, d). (a, c) are
   * projected, (d) is null.
   */

  @Test
  public void testBasics() {

    // Simulate SELECT a, b, c ...
    // Build the projection plan and verify

    ScanLevelProjection scanProj = new ScanLevelProjection(
        RowSetTestUtils.projectList("a", "b", "c"),
        ScanTestUtils.parsers());
    assertFalse(scanProj.projectAll());
    assertFalse(scanProj.projectNone());

    assertEquals(3, scanProj.requestedCols().size());
    assertEquals("a", scanProj.requestedCols().get(0).rootName());
    assertEquals("b", scanProj.requestedCols().get(1).rootName());
    assertEquals("c", scanProj.requestedCols().get(2).rootName());

    assertEquals(3, scanProj.columns().size());
    assertEquals("a", scanProj.columns().get(0).name());
    assertEquals("b", scanProj.columns().get(1).name());
    assertEquals("c", scanProj.columns().get(2).name());

    // Verify column type

    assertEquals(UnresolvedColumn.UNRESOLVED, scanProj.columns().get(0).nodeType());
  }

  @Test
  public void testMap() {
    ScanLevelProjection scanProj = new ScanLevelProjection(
        RowSetTestUtils.projectList("a.x", "b.x", "a.y", "b.y", "c"),
        ScanTestUtils.parsers());
    assertFalse(scanProj.projectAll());
    assertFalse(scanProj.projectNone());

    assertEquals(3, scanProj.columns().size());
    assertEquals("a", scanProj.columns().get(0).name());
    assertEquals("b", scanProj.columns().get(1).name());
    assertEquals("c", scanProj.columns().get(2).name());

    // Verify column type

    assertEquals(UnresolvedColumn.UNRESOLVED, scanProj.columns().get(0).nodeType());

    // Map structure

    RequestedColumn a = ((UnresolvedColumn) scanProj.columns().get(0)).element();
    assertTrue(a.isTuple());
    assertEquals(ProjectionType.UNSPECIFIED, a.mapProjection().projectionType("x"));
    assertEquals(ProjectionType.UNSPECIFIED, a.mapProjection().projectionType("y"));
    assertEquals(ProjectionType.UNPROJECTED,  a.mapProjection().projectionType("z"));

    RequestedColumn c = ((UnresolvedColumn) scanProj.columns().get(2)).element();
    assertTrue(c.isSimple());
  }

  @Test
  public void testArray() {
    ScanLevelProjection scanProj = new ScanLevelProjection(
        RowSetTestUtils.projectList("a[1]", "a[3]"),
        ScanTestUtils.parsers());
    assertFalse(scanProj.projectAll());
    assertFalse(scanProj.projectNone());

    assertEquals(1, scanProj.columns().size());
    assertEquals("a", scanProj.columns().get(0).name());

    // Verify column type

    assertEquals(UnresolvedColumn.UNRESOLVED, scanProj.columns().get(0).nodeType());

    // Map structure

    RequestedColumn a = ((UnresolvedColumn) scanProj.columns().get(0)).element();
    assertTrue(a.isArray());
    assertFalse(a.hasIndex(0));
    assertTrue(a.hasIndex(1));
    assertFalse(a.hasIndex(2));
    assertTrue(a.hasIndex(3));
  }

  /**
   * Simulate a SELECT * query by passing "**" (Drill's internal version
   * of the wildcard) as a column name.
   */

  @Test
  public void testWildcard() {
    ScanLevelProjection scanProj = new ScanLevelProjection(
        RowSetTestUtils.projectAll(),
        ScanTestUtils.parsers());

    assertTrue(scanProj.projectAll());
    assertFalse(scanProj.projectNone());
    assertEquals(1, scanProj.requestedCols().size());
    assertTrue(scanProj.requestedCols().get(0).isDynamicStar());

    assertEquals(1, scanProj.columns().size());
    assertEquals(SchemaPath.DYNAMIC_STAR, scanProj.columns().get(0).name());

    // Verify bindings

    assertEquals(scanProj.columns().get(0).name(), scanProj.requestedCols().get(0).rootName());

    // Verify column type

    assertEquals(UnresolvedColumn.WILDCARD, scanProj.columns().get(0).nodeType());
  }

  /**
   * Test an empty projection which occurs in a
   * SELECT COUNT(*) query.
   */

  @Test
  public void testEmptyProjection() {
    ScanLevelProjection scanProj = new ScanLevelProjection(
        RowSetTestUtils.projectList(),
        ScanTestUtils.parsers());

    assertFalse(scanProj.projectAll());
    assertTrue(scanProj.projectNone());
    assertEquals(0, scanProj.requestedCols().size());
  }

  /**
   * Can't include both a wildcard and a column name.
   */

  @Test
  public void testErrorWildcardAndColumns() {
    try {
      new ScanLevelProjection(
          RowSetTestUtils.projectList(SchemaPath.DYNAMIC_STAR, "a"),
          ScanTestUtils.parsers());
      fail();
    } catch (IllegalArgumentException e) {
      // Expected
    }
  }

  /**
   * Can't include both a column name and a wildcard.
   */

  @Test
  public void testErrorColumnAndWildcard() {
    try {
      new ScanLevelProjection(
          RowSetTestUtils.projectList("a", SchemaPath.DYNAMIC_STAR),
          ScanTestUtils.parsers());
      fail();
    } catch (IllegalArgumentException e) {
      // Expected
    }
  }

  /**
   * Can't include a wildcard twice.
   * <p>
   * Note: Drill actually allows this, but the work should be done
   * in the project operator; scan should see at most one wildcard.
   */

  @Test
  public void testErrorTwoWildcards() {
    try {
      new ScanLevelProjection(
          RowSetTestUtils.projectList(SchemaPath.DYNAMIC_STAR, SchemaPath.DYNAMIC_STAR),
          ScanTestUtils.parsers());
      fail();
    } catch (UserException e) {
      // Expected
    }
  }
}
