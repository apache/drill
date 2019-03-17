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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.drill.categories.RowSetTests;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.impl.scan.ScanTestUtils;
import org.apache.drill.exec.physical.impl.scan.project.AbstractUnresolvedColumn.UnresolvedColumn;
import org.apache.drill.exec.physical.impl.scan.project.AbstractUnresolvedColumn.UnresolvedSchemaColumn;
import org.apache.drill.exec.physical.impl.scan.project.AbstractUnresolvedColumn.UnresolvedWildcardColumn;
import org.apache.drill.exec.physical.impl.scan.project.ScanLevelProjection.ScanProjectionType;
import org.apache.drill.exec.physical.rowSet.impl.RowSetTestUtils;
import org.apache.drill.exec.physical.rowSet.project.ImpliedTupleRequest;
import org.apache.drill.exec.physical.rowSet.project.RequestedTuple;
import org.apache.drill.exec.physical.rowSet.project.RequestedTuple.RequestedColumn;
import org.apache.drill.exec.record.metadata.ProjectionType;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
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

    assertTrue(scanProj.columns().get(0) instanceof UnresolvedColumn);

    // Verify tuple projection

    RequestedTuple outputProj = scanProj.rootProjection();
    assertEquals(3, outputProj.projections().size());
    assertNotNull(outputProj.get("a"));
    assertTrue(outputProj.get("a").isSimple());

    RequestedTuple readerProj = scanProj.readerProjection();
    assertEquals(3, readerProj.projections().size());
    assertNotNull(readerProj.get("a"));
    assertEquals(ProjectionType.UNSPECIFIED, readerProj.projectionType("a"));
    assertEquals(ProjectionType.UNPROJECTED, readerProj.projectionType("d"));
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

    assertTrue(scanProj.columns().get(0) instanceof UnresolvedColumn);

    // Map structure

    final RequestedColumn a = ((UnresolvedColumn) scanProj.columns().get(0)).element();
    assertTrue(a.isTuple());
    assertEquals(ProjectionType.UNSPECIFIED, a.mapProjection().projectionType("x"));
    assertEquals(ProjectionType.UNSPECIFIED, a.mapProjection().projectionType("y"));
    assertEquals(ProjectionType.UNPROJECTED,  a.mapProjection().projectionType("z"));

    final RequestedColumn c = ((UnresolvedColumn) scanProj.columns().get(2)).element();
    assertTrue(c.isSimple());

    // Verify tuple projection

    RequestedTuple outputProj = scanProj.rootProjection();
    assertEquals(3, outputProj.projections().size());
    assertNotNull(outputProj.get("a"));
    assertTrue(outputProj.get("a").isTuple());

    RequestedTuple readerProj = scanProj.readerProjection();
    assertEquals(3, readerProj.projections().size());
    assertNotNull(readerProj.get("a"));
    assertEquals(ProjectionType.TUPLE, readerProj.projectionType("a"));
    assertEquals(ProjectionType.UNSPECIFIED, readerProj.projectionType("c"));
    assertEquals(ProjectionType.UNPROJECTED, readerProj.projectionType("d"));
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

    assertTrue(scanProj.columns().get(0) instanceof UnresolvedColumn);

    // Map structure

    final RequestedColumn a = ((UnresolvedColumn) scanProj.columns().get(0)).element();
    assertTrue(a.isArray());
    assertFalse(a.hasIndex(0));
    assertTrue(a.hasIndex(1));
    assertFalse(a.hasIndex(2));
    assertTrue(a.hasIndex(3));

    // Verify tuple projection

    RequestedTuple outputProj = scanProj.rootProjection();
    assertEquals(1, outputProj.projections().size());
    assertNotNull(outputProj.get("a"));
    assertTrue(outputProj.get("a").isArray());

    RequestedTuple readerProj = scanProj.readerProjection();
    assertEquals(1, readerProj.projections().size());
    assertNotNull(readerProj.get("a"));
    assertEquals(ProjectionType.ARRAY, readerProj.projectionType("a"));
    assertEquals(ProjectionType.UNPROJECTED, readerProj.projectionType("c"));
  }

  /**
   * Simulate a SELECT * query by passing "**" (Drill's internal representation
   * of the wildcard) as a column name.
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

    assertTrue(scanProj.columns().get(0) instanceof UnresolvedWildcardColumn);

    // Verify tuple projection

    RequestedTuple outputProj = scanProj.rootProjection();
    assertEquals(1, outputProj.projections().size());
    assertNotNull(outputProj.get("**"));
    assertTrue(outputProj.get("**").isWildcard());

    RequestedTuple readerProj = scanProj.readerProjection();
    assertTrue(readerProj instanceof ImpliedTupleRequest);
    assertEquals(ProjectionType.UNSPECIFIED, readerProj.projectionType("a"));
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

    // Verify tuple projection

    RequestedTuple outputProj = scanProj.rootProjection();
    assertEquals(0, outputProj.projections().size());

    RequestedTuple readerProj = scanProj.readerProjection();
    assertTrue(readerProj instanceof ImpliedTupleRequest);
    assertEquals(ProjectionType.UNPROJECTED, readerProj.projectionType("a"));
  }

  /**
   * Can include both a wildcard and a column name. The Project
   * operator will fill in the column, the scan framework just ignores
   * the extra column.
   */

  @Test
  public void testWildcardAndColumns() {
    ScanLevelProjection scanProj = new ScanLevelProjection(
          RowSetTestUtils.projectList(SchemaPath.DYNAMIC_STAR, "a"),
          ScanTestUtils.parsers());

    assertTrue(scanProj.projectAll());
    assertFalse(scanProj.projectNone());
    assertEquals(2, scanProj.requestedCols().size());
    assertEquals(1, scanProj.columns().size());

    // Verify tuple projection

    RequestedTuple outputProj = scanProj.rootProjection();
    assertEquals(2, outputProj.projections().size());
    assertNotNull(outputProj.get("**"));
    assertTrue(outputProj.get("**").isWildcard());
    assertNotNull(outputProj.get("a"));

    RequestedTuple readerProj = scanProj.readerProjection();
    assertTrue(readerProj instanceof ImpliedTupleRequest);
    assertEquals(ProjectionType.UNSPECIFIED, readerProj.projectionType("a"));
    assertEquals(ProjectionType.UNSPECIFIED, readerProj.projectionType("c"));
  }

  /**
   * Test a column name and a wildcard.
   */

  @Test
  public void testColumnAndWildcard() {
    ScanLevelProjection scanProj = new ScanLevelProjection(
          RowSetTestUtils.projectList("a", SchemaPath.DYNAMIC_STAR),
          ScanTestUtils.parsers());

    assertTrue(scanProj.projectAll());
    assertFalse(scanProj.projectNone());
    assertEquals(2, scanProj.requestedCols().size());
    assertEquals(1, scanProj.columns().size());
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

  @Test
  public void testEmptyOutputSchema() {
    TupleMetadata outputSchema = new SchemaBuilder().buildSchema();

    // Simulate SELECT a

    final ScanLevelProjection scanProj = new ScanLevelProjection(
        RowSetTestUtils.projectList("a"),
        ScanTestUtils.parsers(),
        outputSchema);

    assertEquals(ScanProjectionType.EXPLICIT, scanProj.projectionType());

    assertEquals(1, scanProj.columns().size());
    assertEquals("a", scanProj.columns().get(0).name());
    assertTrue(scanProj.columns().get(0) instanceof UnresolvedColumn);
  }

  @Test
  public void testOutputSchemaWildcard() {
    TupleMetadata outputSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("b", MinorType.BIGINT)
        .buildSchema();

    final ScanLevelProjection scanProj = new ScanLevelProjection(
        RowSetTestUtils.projectAll(),
        ScanTestUtils.parsers(),
        outputSchema);

    assertEquals(ScanProjectionType.SCHEMA_WILDCARD, scanProj.projectionType());

    assertEquals(2, scanProj.columns().size());
    assertEquals("a", scanProj.columns().get(0).name());
    assertTrue(scanProj.columns().get(0) instanceof UnresolvedSchemaColumn);
    assertEquals("b", scanProj.columns().get(1).name());
    assertTrue(scanProj.columns().get(1) instanceof UnresolvedSchemaColumn);

    RequestedTuple readerProj = scanProj.readerProjection();
    assertEquals(2, readerProj.projections().size());
    assertEquals(ProjectionType.SCALAR, readerProj.projectionType("a"));
    assertEquals(ProjectionType.SCALAR, readerProj.projectionType("b"));
  }

  /**
   * Wildcard projection with a strict schema is the same as a non-strict
   * schema, except that the projection type is different.
   */
  @Test
  public void testStrictOutputSchemaWildcard() {
    TupleMetadata outputSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("b", MinorType.BIGINT)
        .buildSchema();
    outputSchema.setProperty(TupleMetadata.IS_STRICT_SCHEMA_PROP, Boolean.TRUE.toString());

    final ScanLevelProjection scanProj = new ScanLevelProjection(
        RowSetTestUtils.projectAll(),
        ScanTestUtils.parsers(),
        outputSchema);

    assertEquals(ScanProjectionType.STRICT_SCHEMA_WILDCARD, scanProj.projectionType());

    assertEquals(2, scanProj.columns().size());
    assertEquals("a", scanProj.columns().get(0).name());
    assertTrue(scanProj.columns().get(0) instanceof UnresolvedSchemaColumn);
    assertEquals("b", scanProj.columns().get(1).name());
    assertTrue(scanProj.columns().get(1) instanceof UnresolvedSchemaColumn);

    RequestedTuple readerProj = scanProj.readerProjection();
    assertEquals(2, readerProj.projections().size());
    assertEquals(ProjectionType.SCALAR, readerProj.projectionType("a"));
    assertEquals(ProjectionType.SCALAR, readerProj.projectionType("b"));
  }
}
