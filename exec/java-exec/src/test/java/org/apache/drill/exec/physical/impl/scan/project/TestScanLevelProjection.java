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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.drill.categories.RowSetTests;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.impl.scan.ScanTestUtils;
import org.apache.drill.exec.physical.impl.scan.project.AbstractUnresolvedColumn.UnresolvedColumn;
import org.apache.drill.exec.physical.impl.scan.project.AbstractUnresolvedColumn.UnresolvedWildcardColumn;
import org.apache.drill.exec.physical.impl.scan.project.ScanLevelProjection.ScanProjectionType;
import org.apache.drill.exec.physical.resultSet.ProjectionSet;
import org.apache.drill.exec.physical.resultSet.ProjectionSet.ColumnReadProjection;
import org.apache.drill.exec.physical.resultSet.impl.RowSetTestUtils;
import org.apache.drill.exec.physical.resultSet.project.ProjectionType;
import org.apache.drill.exec.physical.resultSet.project.RequestedTuple;
import org.apache.drill.exec.physical.resultSet.project.RequestedTuple.RequestedColumn;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
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

    final ScanLevelProjection scanProj = ScanLevelProjection.build(
        RowSetTestUtils.projectList("a", "b", "c"),
        ScanTestUtils.parsers());

    assertFalse(scanProj.projectAll());
    assertFalse(scanProj.isEmptyProjection());

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

    // Make up a reader schema and test the projection set.

    TupleMetadata readerSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("b", MinorType.INT)
        .add("c", MinorType.INT)
        .add("d", MinorType.INT)
        .buildSchema();

    ProjectionSet projSet = scanProj.projectionSet().build();
    assertTrue(projSet.readProjection(readerSchema.metadata("a")).isProjected());
    assertFalse(projSet.readProjection(readerSchema.metadata("d")).isProjected());
  }

  /**
   * Map projection occurs when a query contains project-list items with
   * a dot, such as "a.b". We may not know the type of "b", but have
   * just learned that "a" must be a map.
   */

  @Test
  public void testMap() {

    // SELECT a.x, b.x, a.y, b.y, c
    // We infer a and b are maps.

    final ScanLevelProjection scanProj = ScanLevelProjection.build(
        RowSetTestUtils.projectList("a.x", "b.x", "a.y", "b.y", "c"),
        ScanTestUtils.parsers());

    assertFalse(scanProj.projectAll());
    assertFalse(scanProj.isEmptyProjection());

    assertEquals(3, scanProj.columns().size());
    assertEquals("a", scanProj.columns().get(0).name());
    assertEquals("b", scanProj.columns().get(1).name());
    assertEquals("c", scanProj.columns().get(2).name());

    // Verify column type

    assertTrue(scanProj.columns().get(0) instanceof UnresolvedColumn);

    // Inferred map structure

    final RequestedColumn a = ((UnresolvedColumn) scanProj.columns().get(0)).element();
    assertTrue(a.isTuple());
    assertEquals(ProjectionType.GENERAL, a.mapProjection().projectionType("x"));
    assertEquals(ProjectionType.GENERAL, a.mapProjection().projectionType("y"));
    assertEquals(ProjectionType.UNPROJECTED,  a.mapProjection().projectionType("z"));

    final RequestedColumn c = ((UnresolvedColumn) scanProj.columns().get(2)).element();
    assertTrue(c.isSimple());

    // Verify tuple projection

    RequestedTuple outputProj = scanProj.rootProjection();
    assertEquals(3, outputProj.projections().size());
    assertNotNull(outputProj.get("a"));
    assertTrue(outputProj.get("a").isTuple());

    // Make up a reader schema and test the projection set.

    TupleMetadata readerSchema = new SchemaBuilder()
        .addMap("a")
          .add("x", MinorType.INT)
          .add("y", MinorType.INT)
          .resumeSchema()
        .addMap("b")
          .add("x", MinorType.INT)
          .add("y", MinorType.INT)
          .resumeSchema()
        .add("c", MinorType.INT)
        .add("d", MinorType.INT)
        .buildSchema();

    // Verify the projection set as if we were a reader. Note that the
    // projection type is used here for testing; should not be used by
    // an actual reader.

    ProjectionSet projSet = scanProj.projectionSet().build();
    ColumnReadProjection aProj = projSet.readProjection(readerSchema.metadata("a"));
    assertTrue(aProj.isProjected());
    assertEquals(ProjectionType.TUPLE, aProj.projectionType());
    ColumnReadProjection cProj = projSet.readProjection(readerSchema.metadata("c"));
    assertTrue(cProj.isProjected());
    assertEquals(ProjectionType.GENERAL, cProj.projectionType());
    assertFalse(projSet.readProjection(readerSchema.metadata("d")).isProjected());
  }

  /**
   * Similar to maps, if the project list contains "a[1]" then we've learned that
   * a is an array, but we don't know what type.
   */

  @Test
  public void testArray() {
    final ScanLevelProjection scanProj = ScanLevelProjection.build(
        RowSetTestUtils.projectList("a[1]", "a[3]"),
        ScanTestUtils.parsers());

    assertFalse(scanProj.projectAll());
    assertFalse(scanProj.isEmptyProjection());

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

    // Make up a reader schema and test the projection set.

    TupleMetadata readerSchema = new SchemaBuilder()
        .addArray("a", MinorType.INT)
        .add("c", MinorType.INT)
        .buildSchema();

    ProjectionSet projSet = scanProj.projectionSet().build();
    ColumnReadProjection aProj = projSet.readProjection(readerSchema.metadata("a"));
    assertTrue(aProj.isProjected());
    assertEquals(ProjectionType.ARRAY, aProj.projectionType());
    assertFalse(projSet.readProjection(readerSchema.metadata("c")).isProjected());
  }

  /**
   * Simulate a SELECT * query by passing "**" (Drill's internal representation
   * of the wildcard) as a column name.
   */

  @Test
  public void testWildcard() {
    final ScanLevelProjection scanProj = ScanLevelProjection.build(
        RowSetTestUtils.projectAll(),
        ScanTestUtils.parsers());

    assertTrue(scanProj.projectAll());
    assertFalse(scanProj.isEmptyProjection());
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
    assertNotNull(outputProj.get(SchemaPath.DYNAMIC_STAR));
    assertTrue(outputProj.get(SchemaPath.DYNAMIC_STAR).isWildcard());

    // Make up a reader schema and test the projection set.

    TupleMetadata readerSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("c", MinorType.INT)
        .buildSchema();

    ProjectionSet projSet = scanProj.projectionSet().build();
    assertTrue(projSet.readProjection(readerSchema.metadata("a")).isProjected());
    assertTrue(projSet.readProjection(readerSchema.metadata("c")).isProjected());
  }

  /**
   * Test an empty projection which occurs in a
   * SELECT COUNT(*) query.
   */

  @Test
  public void testEmptyProjection() {
    final ScanLevelProjection scanProj = ScanLevelProjection.build(
        RowSetTestUtils.projectList(),
        ScanTestUtils.parsers());

    assertFalse(scanProj.projectAll());
    assertTrue(scanProj.isEmptyProjection());
    assertEquals(0, scanProj.requestedCols().size());

    // Verify tuple projection

    RequestedTuple outputProj = scanProj.rootProjection();
    assertEquals(0, outputProj.projections().size());

    // Make up a reader schema and test the projection set.

    TupleMetadata readerSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .buildSchema();

    ProjectionSet projSet = scanProj.projectionSet().build();
    assertFalse(projSet.readProjection(readerSchema.metadata("a")).isProjected());
  }

  /**
   * Can include both a wildcard and a column name. The Project
   * operator will fill in the column, the scan framework just ignores
   * the extra column.
   */

  @Test
  public void testWildcardAndColumns() {
    ScanLevelProjection scanProj = ScanLevelProjection.build(
          RowSetTestUtils.projectList(SchemaPath.DYNAMIC_STAR, "a"),
          ScanTestUtils.parsers());

    assertTrue(scanProj.projectAll());
    assertFalse(scanProj.isEmptyProjection());
    assertEquals(2, scanProj.requestedCols().size());
    assertEquals(1, scanProj.columns().size());

    // Verify tuple projection

    RequestedTuple outputProj = scanProj.rootProjection();
    assertEquals(2, outputProj.projections().size());
    assertNotNull(outputProj.get(SchemaPath.DYNAMIC_STAR));
    assertTrue(outputProj.get(SchemaPath.DYNAMIC_STAR).isWildcard());
    assertNotNull(outputProj.get("a"));

    // Make up a reader schema and test the projection set.

    TupleMetadata readerSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("c", MinorType.INT)
        .buildSchema();

    ProjectionSet projSet = scanProj.projectionSet().build();
    assertTrue(projSet.readProjection(readerSchema.metadata("a")).isProjected());
    assertTrue(projSet.readProjection(readerSchema.metadata("c")).isProjected());
  }

  /**
   * Test a column name and a wildcard.
   */

  @Test
  public void testColumnAndWildcard() {
    ScanLevelProjection scanProj = ScanLevelProjection.build(
          RowSetTestUtils.projectList("a", SchemaPath.DYNAMIC_STAR),
          ScanTestUtils.parsers());

    assertTrue(scanProj.projectAll());
    assertFalse(scanProj.isEmptyProjection());
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
      ScanLevelProjection.build(
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

    final ScanLevelProjection scanProj = ScanLevelProjection.build(
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

    final ScanLevelProjection scanProj = ScanLevelProjection.build(
        RowSetTestUtils.projectAll(),
        ScanTestUtils.parsers(),
        outputSchema);

    assertEquals(ScanProjectionType.SCHEMA_WILDCARD, scanProj.projectionType());

    assertEquals(2, scanProj.columns().size());
    ColumnProjection aCol = scanProj.columns().get(0);
    assertEquals("a", aCol.name());
    assertTrue(aCol instanceof UnresolvedColumn);
    assertSame(outputSchema.metadata("a"), ((UnresolvedColumn) aCol).metadata());
    ColumnProjection bCol = scanProj.columns().get(1);
    assertEquals("b", bCol.name());
    assertTrue(bCol instanceof UnresolvedColumn);
    assertSame(outputSchema.metadata("b"), ((UnresolvedColumn) bCol).metadata());

    ProjectionSet projSet = scanProj.projectionSet().build();
    assertTrue(projSet.readProjection(outputSchema.metadata("a")).isProjected());
    assertTrue(projSet.readProjection(outputSchema.metadata("b")).isProjected());
  }

  @Test
  public void testOutputSchemaWildcardSpecialCols() {
    TupleMetadata outputSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("b", MinorType.BIGINT)
        .add("c", MinorType.VARCHAR)
        .buildSchema();

    // Mark b as special; not expanded in wildcard.

    outputSchema.metadata("b").setBooleanProperty(ColumnMetadata.EXCLUDE_FROM_WILDCARD, true);

    final ScanLevelProjection scanProj = ScanLevelProjection.build(
        RowSetTestUtils.projectAll(),
        ScanTestUtils.parsers(),
        outputSchema);

    assertEquals(ScanProjectionType.SCHEMA_WILDCARD, scanProj.projectionType());

    assertEquals(2, scanProj.columns().size());
    assertEquals("a", scanProj.columns().get(0).name());
    assertEquals("c", scanProj.columns().get(1).name());
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

    final ScanLevelProjection scanProj = ScanLevelProjection.build(
        RowSetTestUtils.projectAll(),
        ScanTestUtils.parsers(),
        outputSchema);

    assertEquals(ScanProjectionType.STRICT_SCHEMA_WILDCARD, scanProj.projectionType());

    assertEquals(2, scanProj.columns().size());
    assertEquals("a", scanProj.columns().get(0).name());
    assertTrue(scanProj.columns().get(0) instanceof UnresolvedColumn);
    assertEquals("b", scanProj.columns().get(1).name());
    assertTrue(scanProj.columns().get(1) instanceof UnresolvedColumn);

    // Make up a reader schema and test the projection set.

    TupleMetadata readerSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("b", MinorType.INT)
        .add("c", MinorType.INT)
        .buildSchema();

    ProjectionSet projSet = scanProj.projectionSet().build();
    assertTrue(projSet.readProjection(readerSchema.metadata("a")).isProjected());
    assertTrue(projSet.readProjection(readerSchema.metadata("b")).isProjected());
    assertFalse(projSet.readProjection(readerSchema.metadata("c")).isProjected());
  }
}
