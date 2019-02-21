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
package org.apache.drill.exec.physical.impl.scan.project;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.drill.categories.RowSetTests;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.impl.scan.ScanTestUtils;
import org.apache.drill.exec.physical.rowSet.impl.RowSetTestUtils;
import org.apache.drill.exec.physical.rowSet.project.RequestedTuple.RequestedColumn;
import org.apache.drill.exec.record.metadata.ProjectionType;
import org.apache.drill.test.SubOperatorTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test the level of projection done at the level of the scan as a whole;
 * before knowledge of table "implicit" columns or the specific table schema.
 */

@Category(RowSetTests.class)
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

    final ScanLevelProjection scanProj = new ScanLevelProjection(
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

  /**
   * Map projection occurs when a query contains project-list items with
   * a dot, such as "a.b". We may not know the type of "b", but have
   * just learned that "a" must be a map.
   */

  @Test
  public void testMap() {
    final ScanLevelProjection scanProj = new ScanLevelProjection(
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

    final RequestedColumn a = ((UnresolvedColumn) scanProj.columns().get(0)).element();
    assertTrue(a.isTuple());
    assertEquals(ProjectionType.UNSPECIFIED, a.mapProjection().projectionType("x"));
    assertEquals(ProjectionType.UNSPECIFIED, a.mapProjection().projectionType("y"));
    assertEquals(ProjectionType.UNPROJECTED,  a.mapProjection().projectionType("z"));

    final RequestedColumn c = ((UnresolvedColumn) scanProj.columns().get(2)).element();
    assertTrue(c.isSimple());
  }

  /**
   * Similar to maps, if the project list contains "a[1]" then we've learned that
   * a is an array, but we don't know what type.
   */

  @Test
  public void testArray() {
    final ScanLevelProjection scanProj = new ScanLevelProjection(
        RowSetTestUtils.projectList("a[1]", "a[3]"),
        ScanTestUtils.parsers());
    assertFalse(scanProj.projectAll());
    assertFalse(scanProj.projectNone());

    assertEquals(1, scanProj.columns().size());
    assertEquals("a", scanProj.columns().get(0).name());

    // Verify column type

    assertEquals(UnresolvedColumn.UNRESOLVED, scanProj.columns().get(0).nodeType());

    // Map structure

    final RequestedColumn a = ((UnresolvedColumn) scanProj.columns().get(0)).element();
    assertTrue(a.isArray());
    assertFalse(a.hasIndex(0));
    assertTrue(a.hasIndex(1));
    assertFalse(a.hasIndex(2));
    assertTrue(a.hasIndex(3));
  }

  /**
   * Simulate a SELECT * query by passing "*" as a column name.
   */

  @Test
  public void testWildcard() {
    final ScanLevelProjection scanProj = new ScanLevelProjection(
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
    final ScanLevelProjection scanProj = new ScanLevelProjection(
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
    } catch (final IllegalArgumentException e) {
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
    } catch (final IllegalArgumentException e) {
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
    } catch (final UserException e) {
      // Expected
    }
  }
}
