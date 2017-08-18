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
package org.apache.drill.exec.physical.rowSet.impl;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.rowSet.ResultSetLoader;
import org.apache.drill.exec.physical.rowSet.RowSetLoader;
import org.apache.drill.exec.physical.rowSet.impl.ResultSetLoaderImpl.ResultSetOptions;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.ColumnMetadata;
import org.apache.drill.exec.record.TupleMetadata;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.RowSet;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.apache.drill.test.rowSet.RowSet.SingleRowSet;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * Test of the basics of the projection mechanism.
 */

public class TestResultSetLoaderProjection extends SubOperatorTest {

  @Test
  public void testProjectionMap() {

    // Null map means everything is projected

    {
      ProjectionSet projSet = ProjectionSetImpl.parse(null);
      assertTrue(projSet instanceof NullProjectionSet);
      assertTrue(projSet.isProjected("foo"));
    }

    // Empty list means everything is projected

    {
      ProjectionSet projSet = ProjectionSetImpl.parse(new ArrayList<SchemaPath>());
      assertTrue(projSet instanceof NullProjectionSet);
      assertTrue(projSet.isProjected("foo"));
    }

    // Simple non-map columns

    {
      List<SchemaPath> projCols = new ArrayList<>();
      projCols.add(SchemaPath.getSimplePath("foo"));
      projCols.add(SchemaPath.getSimplePath("bar"));
      ProjectionSet projSet = ProjectionSetImpl.parse(projCols);
      assertTrue(projSet instanceof ProjectionSetImpl);
      assertTrue(projSet.isProjected("foo"));
      assertTrue(projSet.isProjected("bar"));
      assertFalse(projSet.isProjected("mumble"));
    }

    // Whole-map projection (note, fully projected maps are
    // identical to projected simple columns at this level of
    // abstraction.)

    {
      List<SchemaPath> projCols = new ArrayList<>();
      projCols.add(SchemaPath.getSimplePath("map"));
      ProjectionSet projSet = ProjectionSetImpl.parse(projCols);
      assertTrue(projSet instanceof ProjectionSetImpl);
      assertTrue(projSet.isProjected("map"));
      assertFalse(projSet.isProjected("another"));
      ProjectionSet mapProj = projSet.mapProjection("map");
      assertNotNull(mapProj);
      assertTrue(mapProj instanceof NullProjectionSet);
      assertTrue(mapProj.isProjected("foo"));
      assertNotNull(projSet.mapProjection("another"));
      assertFalse(projSet.mapProjection("another").isProjected("anyCol"));
    }

    // Selected map projection, multiple levels, full projection
    // at leaf level.

    {
      List<SchemaPath> projCols = new ArrayList<>();
      projCols.add(SchemaPath.getCompoundPath("map", "a"));
      projCols.add(SchemaPath.getCompoundPath("map", "b"));
      projCols.add(SchemaPath.getCompoundPath("map", "map2", "x"));
      ProjectionSet projSet = ProjectionSetImpl.parse(projCols);
      assertTrue(projSet instanceof ProjectionSetImpl);
      assertTrue(projSet.isProjected("map"));

      // Map: an explicit map at top level

      ProjectionSet mapProj = projSet.mapProjection("map");
      assertTrue(mapProj instanceof ProjectionSetImpl);
      assertTrue(mapProj.isProjected("a"));
      assertTrue(mapProj.isProjected("b"));
      assertTrue(mapProj.isProjected("map2"));
      assertFalse(projSet.isProjected("bogus"));

      // Map b: an implied nested map

      ProjectionSet bMapProj = mapProj.mapProjection("b");
      assertNotNull(bMapProj);
      assertTrue(bMapProj instanceof NullProjectionSet);
      assertTrue(bMapProj.isProjected("foo"));

      // Map2, an nested map, has an explicit projection

      ProjectionSet map2Proj = mapProj.mapProjection("map2");
      assertNotNull(map2Proj);
      assertTrue(map2Proj instanceof ProjectionSetImpl);
      assertTrue(map2Proj.isProjected("x"));
      assertFalse(map2Proj.isProjected("bogus"));
    }
  }

  /**
   * Test imposing a selection mask between the client and the underlying
   * vector container.
   */

  @Test
  public void testProjectionStatic() {
    List<SchemaPath> selection = Lists.newArrayList(
        SchemaPath.getSimplePath("c"),
        SchemaPath.getSimplePath("b"),
        SchemaPath.getSimplePath("e"));
    TupleMetadata schema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("b", MinorType.INT)
        .add("c", MinorType.INT)
        .add("d", MinorType.INT)
        .buildSchema();
    ResultSetOptions options = new OptionBuilder()
        .setProjection(selection)
        .setSchema(schema)
        .build();
    ResultSetLoader rsLoader = new ResultSetLoaderImpl(fixture.allocator(), options);

    doProjectionTest(rsLoader);
  }

  @Test
  public void testProjectionDynamic() {
    List<SchemaPath> selection = Lists.newArrayList(
        SchemaPath.getSimplePath("c"),
        SchemaPath.getSimplePath("b"),
        SchemaPath.getSimplePath("e"));
    ResultSetOptions options = new OptionBuilder()
        .setProjection(selection)
        .build();
    ResultSetLoader rsLoader = new ResultSetLoaderImpl(fixture.allocator(), options);
    RowSetLoader rootWriter = rsLoader.writer();
    rootWriter.addColumn(SchemaBuilder.columnSchema("a", MinorType.INT, DataMode.REQUIRED));
    rootWriter.addColumn(SchemaBuilder.columnSchema("b", MinorType.INT, DataMode.REQUIRED));
    rootWriter.addColumn(SchemaBuilder.columnSchema("c", MinorType.INT, DataMode.REQUIRED));
    rootWriter.addColumn(SchemaBuilder.columnSchema("d", MinorType.INT, DataMode.REQUIRED));

    doProjectionTest(rsLoader);
  }

  private void doProjectionTest(ResultSetLoader rsLoader) {
    RowSetLoader rootWriter = rsLoader.writer();

    // All columns appear, including non-projected ones.

    TupleMetadata actualSchema = rootWriter.schema();
    assertEquals(4, actualSchema.size());
    assertEquals("a", actualSchema.column(0).getName());
    assertEquals("b", actualSchema.column(1).getName());
    assertEquals("c", actualSchema.column(2).getName());
    assertEquals("d", actualSchema.column(3).getName());
    assertEquals(0, actualSchema.index("A"));
    assertEquals(3, actualSchema.index("d"));
    assertEquals(-1, actualSchema.index("e"));

    // Non-projected columns identify themselves via metadata

    assertFalse(actualSchema.metadata("a").isProjected());
    assertTrue(actualSchema.metadata("b").isProjected());
    assertTrue(actualSchema.metadata("c").isProjected());
    assertFalse(actualSchema.metadata("d").isProjected());

    // Write some data. Doesn't need much.

    rsLoader.startBatch();
    for (int i = 1; i < 3; i++) {
      rootWriter.start();
      rootWriter.scalar(0).setInt(i * 5);
      rootWriter.scalar(1).setInt(i);
      rootWriter.scalar(2).setInt(i * 10);
      rootWriter.scalar(3).setInt(i * 20);
      rootWriter.save();
    }

    // Verify. Result should only have the projected
    // columns, only if defined by the loader, in the order
    // of definition.

    BatchSchema expectedSchema = new SchemaBuilder()
        .add("b", MinorType.INT)
        .add("c", MinorType.INT)
        .build();
    SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addRow(1, 10)
        .addRow(2, 20)
        .build();
    RowSet actual = fixture.wrap(rsLoader.harvest());
//    actual.print();
    new RowSetComparison(expected)
        .verifyAndClearAll(actual);
    rsLoader.close();
  }

  @Test
  public void testMapProjection() {
    List<SchemaPath> selection = Lists.newArrayList(
        SchemaPath.getSimplePath("m1"),
        SchemaPath.getCompoundPath("m2", "d"));
    TupleMetadata schema = new SchemaBuilder()
        .addMap("m1")
          .add("a", MinorType.INT)
          .add("b", MinorType.INT)
          .buildMap()
        .addMap("m2")
          .add("c", MinorType.INT)
          .add("d", MinorType.INT)
          .buildMap()
        .addMap("m3")
          .add("e", MinorType.INT)
          .add("f", MinorType.INT)
          .buildMap()
        .buildSchema();
    ResultSetOptions options = new OptionBuilder()
        .setProjection(selection)
        .setSchema(schema)
        .build();
    ResultSetLoader rsLoader = new ResultSetLoaderImpl(fixture.allocator(), options);
    RowSetLoader rootWriter = rsLoader.writer();

    // Verify the projected columns

    TupleMetadata actualSchema = rootWriter.schema();
    ColumnMetadata m1Md = actualSchema.metadata("m1");
    assertTrue(m1Md.isMap());
    assertTrue(m1Md.isProjected());
    assertEquals(2, m1Md.mapSchema().size());
    assertTrue(m1Md.mapSchema().metadata("a").isProjected());
    assertTrue(m1Md.mapSchema().metadata("b").isProjected());

    ColumnMetadata m2Md = actualSchema.metadata("m2");
    assertTrue(m2Md.isMap());
    assertTrue(m2Md.isProjected());
    assertEquals(2, m2Md.mapSchema().size());
    assertFalse(m2Md.mapSchema().metadata("c").isProjected());
    assertTrue(m2Md.mapSchema().metadata("d").isProjected());

    ColumnMetadata m3Md = actualSchema.metadata("m3");
    assertTrue(m3Md.isMap());
    assertFalse(m3Md.isProjected());
    assertEquals(2, m3Md.mapSchema().size());
    assertFalse(m3Md.mapSchema().metadata("e").isProjected());
    assertFalse(m3Md.mapSchema().metadata("f").isProjected());

    // Write a couple of rows.

    rsLoader.startBatch();
    rootWriter.start();
    rootWriter.tuple("m1").scalar("a").setInt(1);
    rootWriter.tuple("m1").scalar("b").setInt(2);
    rootWriter.tuple("m2").scalar("c").setInt(3);
    rootWriter.tuple("m2").scalar("d").setInt(4);
    rootWriter.tuple("m3").scalar("e").setInt(5);
    rootWriter.tuple("m3").scalar("f").setInt(6);
    rootWriter.save();

    rootWriter.start();
    rootWriter.tuple("m1").scalar("a").setInt(11);
    rootWriter.tuple("m1").scalar("b").setInt(12);
    rootWriter.tuple("m2").scalar("c").setInt(13);
    rootWriter.tuple("m2").scalar("d").setInt(14);
    rootWriter.tuple("m3").scalar("e").setInt(15);
    rootWriter.tuple("m3").scalar("f").setInt(16);
    rootWriter.save();

    // Verify. Only the projected columns appear in the result set.

    BatchSchema expectedSchema = new SchemaBuilder()
      .addMap("m1")
        .add("a", MinorType.INT)
        .add("b", MinorType.INT)
        .buildMap()
      .addMap("m2")
        .add("d", MinorType.INT)
        .buildMap()
      .build();
    SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
      .addRow(new Object[] {1, 2}, new Object[] {4})
      .addRow(new Object[] {11, 12}, new Object[] {14})
      .build();
    new RowSetComparison(expected)
        .verifyAndClearAll(fixture.wrap(rsLoader.harvest()));
    rsLoader.close();
  }

  /**
   * Test a map array. Use the convenience methods to set values.
   * Only the projected array members should appear in the harvested
   * results.
   */

  @Test
  public void testMapArrayProjection() {
    List<SchemaPath> selection = Lists.newArrayList(
        SchemaPath.getSimplePath("m1"),
        SchemaPath.getCompoundPath("m2", "d"));
    TupleMetadata schema = new SchemaBuilder()
        .addMapArray("m1")
          .add("a", MinorType.INT)
          .add("b", MinorType.INT)
          .buildMap()
        .addMapArray("m2")
          .add("c", MinorType.INT)
          .add("d", MinorType.INT)
          .buildMap()
        .addMapArray("m3")
          .add("e", MinorType.INT)
          .add("f", MinorType.INT)
          .buildMap()
        .buildSchema();
    ResultSetOptions options = new OptionBuilder()
        .setProjection(selection)
        .setSchema(schema)
        .build();
    ResultSetLoader rsLoader = new ResultSetLoaderImpl(fixture.allocator(), options);
    RowSetLoader rootWriter = rsLoader.writer();

    // Write a couple of rows.

    rsLoader.startBatch();
    rootWriter.addRow(
        new Object[] { new Object[] {10, 20}, new Object[] {11, 21}},
        new Object[] { new Object[] {30, 40}, new Object[] {31, 42}},
        new Object[] { new Object[] {50, 60}, new Object[] {51, 62}});
    rootWriter.addRow(
        new Object[] { new Object[] {110, 120}, new Object[] {111, 121}},
        new Object[] { new Object[] {130, 140}, new Object[] {131, 142}},
        new Object[] { new Object[] {150, 160}, new Object[] {151, 162}});

    // Verify. Only the projected columns appear in the result set.

    BatchSchema expectedSchema = new SchemaBuilder()
      .addMapArray("m1")
        .add("a", MinorType.INT)
        .add("b", MinorType.INT)
        .buildMap()
      .addMapArray("m2")
        .add("d", MinorType.INT)
        .buildMap()
      .build();
    SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
      .addRow(
          new Object[] { new Object[] {10, 20}, new Object[] {11, 21}},
          new Object[] { new Object[] {40}, new Object[] {42}})
      .addRow(
          new Object[] { new Object[] {110, 120}, new Object[] {111, 121}},
          new Object[] { new Object[] {140}, new Object[] {142}})
      .build();
    new RowSetComparison(expected)
        .verifyAndClearAll(fixture.wrap(rsLoader.harvest()));
    rsLoader.close();
  }

  /**
   * Verify that the projection code plays nice with vector overflow. Overflow
   * is the most complex operation in this subsystem with many specialized
   * methods that must work together flawlessly. This test ensures that
   * non-projected columns stay in the background and don't interfere
   * with overflow logic.
   */

  @Test
  public void testProjectWithOverflow() {
    List<SchemaPath> selection = Lists.newArrayList(
        SchemaPath.getSimplePath("small"),
        SchemaPath.getSimplePath("dummy"));
    TupleMetadata schema = new SchemaBuilder()
        .add("big", MinorType.VARCHAR)
        .add("small", MinorType.VARCHAR)
        .buildSchema();
    ResultSetOptions options = new OptionBuilder()
        .setRowCountLimit(ValueVector.MAX_ROW_COUNT)
        .setProjection(selection)
        .setSchema(schema)
        .build();
    ResultSetLoader rsLoader = new ResultSetLoaderImpl(fixture.allocator(), options);
    RowSetLoader rootWriter = rsLoader.writer();

    byte big[] = new byte[600];
    Arrays.fill(big, (byte) 'X');
    byte small[] = new byte[512];
    Arrays.fill(small, (byte) 'X');

    rsLoader.startBatch();
    int count = 0;
    while (! rootWriter.isFull()) {
      rootWriter.start();
      rootWriter.scalar(0).setBytes(big, big.length);
      rootWriter.scalar(1).setBytes(small, small.length);
      rootWriter.save();
      count++;
    }

    // Number of rows should be driven by size of the
    // projected vector ("small"), not by the larger, unprojected
    // "big" vector.
    // Our row count should include the overflow row

    int expectedCount = ValueVector.MAX_BUFFER_SIZE / small.length;
    assertEquals(expectedCount + 1, count);

    // Loader's row count should include only "visible" rows

    assertEquals(expectedCount, rootWriter.rowCount());

    // Total count should include invisible and look-ahead rows.

    assertEquals(expectedCount + 1, rsLoader.totalRowCount());

    // Result should exclude the overflow row

    RowSet result = fixture.wrap(rsLoader.harvest());
    assertEquals(expectedCount, result.rowCount());
    result.clear();

    // Next batch should start with the overflow row

    rsLoader.startBatch();
    assertEquals(1, rootWriter.rowCount());
    assertEquals(expectedCount + 1, rsLoader.totalRowCount());
    result = fixture.wrap(rsLoader.harvest());
    assertEquals(1, result.rowCount());
    result.clear();

    rsLoader.close();
  }
}
