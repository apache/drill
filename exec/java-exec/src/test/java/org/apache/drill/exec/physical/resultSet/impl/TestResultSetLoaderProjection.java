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
package org.apache.drill.exec.physical.resultSet.impl;

import static org.apache.drill.test.rowSet.RowSetUtilities.intArray;
import static org.apache.drill.test.rowSet.RowSetUtilities.map;
import static org.apache.drill.test.rowSet.RowSetUtilities.mapValue;
import static org.apache.drill.test.rowSet.RowSetUtilities.objArray;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.List;

import org.apache.drill.categories.RowSetTests;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.impl.scan.project.projSet.ProjectionSetFactory;
import org.apache.drill.exec.physical.resultSet.ResultSetLoader;
import org.apache.drill.exec.physical.resultSet.RowSetLoader;
import org.apache.drill.exec.physical.resultSet.impl.ResultSetLoaderImpl.ResultSetOptions;
import org.apache.drill.exec.proto.UserBitShared.DrillPBError.ErrorType;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.DictWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.physical.rowSet.RowSet.SingleRowSet;
import org.apache.drill.test.rowSet.RowSetUtilities;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test of the basics of the projection mechanism.
 */

@Category(RowSetTests.class)
public class TestResultSetLoaderProjection extends SubOperatorTest {

  /**
   * Test imposing a selection mask between the client and the underlying
   * vector container.
   */

  @Test
  public void testProjectionStatic() {
    List<SchemaPath> selection = RowSetTestUtils.projectList("c", "b", "e");
    TupleMetadata schema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("b", MinorType.INT)
        .add("c", MinorType.INT)
        .add("d", MinorType.INT)
        .buildSchema();
    ResultSetOptions options = new OptionBuilder()
        .setProjection(ProjectionSetFactory.build(selection))
        .setSchema(schema)
        .build();
    ResultSetLoader rsLoader = new ResultSetLoaderImpl(fixture.allocator(), options);

    doProjectionTest(rsLoader);
  }

  @Test
  public void testProjectionDynamic() {
    List<SchemaPath> selection = RowSetTestUtils.projectList("c", "b", "e");
    ResultSetOptions options = new OptionBuilder()
        .setProjection(ProjectionSetFactory.build(selection))
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

    TupleMetadata actualSchema = rootWriter.tupleSchema();
    assertEquals(4, actualSchema.size());
    assertEquals("a", actualSchema.column(0).getName());
    assertEquals("b", actualSchema.column(1).getName());
    assertEquals("c", actualSchema.column(2).getName());
    assertEquals("d", actualSchema.column(3).getName());
    assertEquals(0, actualSchema.index("A"));
    assertEquals(3, actualSchema.index("d"));
    assertEquals(-1, actualSchema.index("e"));

    // Non-projected columns identify themselves

    assertFalse(rootWriter.column("a").isProjected());
    assertTrue(rootWriter.column("b").isProjected());
    assertTrue(rootWriter.column("c").isProjected());
    assertFalse(rootWriter.column("d").isProjected());

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

    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("b", MinorType.INT)
        .add("c", MinorType.INT)
        .buildSchema();
    SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addRow(1, 10)
        .addRow(2, 20)
        .build();
    RowSet actual = fixture.wrap(rsLoader.harvest());
//    actual.print();
    RowSetUtilities.verify(expected, actual);
    rsLoader.close();
  }

  @Test
  public void testArrayProjection() {
    List<SchemaPath> selection = RowSetTestUtils.projectList("a1", "a2[0]");
    TupleMetadata schema = new SchemaBuilder()
        .addArray("a1", MinorType.INT)
        .addArray("a2", MinorType.INT)
        .addArray("a3", MinorType.INT)
        .buildSchema();
    ResultSetOptions options = new OptionBuilder()
        .setProjection(ProjectionSetFactory.build(selection))
        .setSchema(schema)
        .build();
    ResultSetLoader rsLoader = new ResultSetLoaderImpl(fixture.allocator(), options);
    RowSetLoader rootWriter = rsLoader.writer();

    // Verify the projected columns

    TupleMetadata actualSchema = rootWriter.tupleSchema();
    assertTrue(actualSchema.metadata("a1").isArray());
    assertTrue(rootWriter.column("a1").isProjected());

    assertTrue(actualSchema.metadata("a2").isArray());
    assertTrue(rootWriter.column("a2").isProjected());

    assertTrue(actualSchema.metadata("a3").isArray());
    assertFalse(rootWriter.column("a3").isProjected());

    // Write a couple of rows.

    rsLoader.startBatch();
    rootWriter.start();
    rootWriter
      .addRow(intArray(10, 100), intArray(20, 200), intArray(30, 300))
      .addRow(intArray(11, 101), intArray(21, 201), intArray(31, 301));

    // Verify. Only the projected columns appear in the result set.

    TupleMetadata expectedSchema = new SchemaBuilder()
        .addArray("a1", MinorType.INT)
        .addArray("a2", MinorType.INT)
      .buildSchema();
    SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
      .addRow(intArray(10, 100), intArray(20, 200))
      .addRow(intArray(11, 101), intArray(21, 201))
      .build();
    RowSetUtilities.verify(expected, fixture.wrap(rsLoader.harvest()));
    rsLoader.close();
  }

  @Test
  public void testMapProjection() {
    List<SchemaPath> selection = RowSetTestUtils.projectList("m1", "m2.d");
    TupleMetadata schema = new SchemaBuilder()
        .addMap("m1")
          .add("a", MinorType.INT)
          .add("b", MinorType.INT)
          .resumeSchema()
        .addMap("m2")
          .add("c", MinorType.INT)
          .add("d", MinorType.INT)
          .resumeSchema()
        .addMap("m3")
          .add("e", MinorType.INT)
          .add("f", MinorType.INT)
          .resumeSchema()
        .buildSchema();
    ResultSetOptions options = new OptionBuilder()
        .setProjection(ProjectionSetFactory.build(selection))
        .setSchema(schema)
        .build();
    ResultSetLoader rsLoader = new ResultSetLoaderImpl(fixture.allocator(), options);
    RowSetLoader rootWriter = rsLoader.writer();

    // Verify the projected columns

    TupleMetadata actualSchema = rootWriter.tupleSchema();
    ColumnMetadata m1Md = actualSchema.metadata("m1");
    TupleWriter m1Writer = rootWriter.tuple("m1");
    assertTrue(m1Md.isMap());
    assertTrue(m1Writer.isProjected());
    assertEquals(2, m1Md.tupleSchema().size());
    assertTrue(m1Writer.column("a").isProjected());
    assertTrue(m1Writer.column("b").isProjected());

    ColumnMetadata m2Md = actualSchema.metadata("m2");
    TupleWriter m2Writer = rootWriter.tuple("m2");
    assertTrue(m2Md.isMap());
    assertTrue(m2Writer.isProjected());
    assertEquals(2, m2Md.tupleSchema().size());
    assertFalse(m2Writer.column("c").isProjected());
    assertTrue(m2Writer.column("d").isProjected());

    ColumnMetadata m3Md = actualSchema.metadata("m3");
    TupleWriter m3Writer = rootWriter.tuple("m3");
    assertTrue(m3Md.isMap());
    assertFalse(m3Writer.isProjected());
    assertEquals(2, m3Md.tupleSchema().size());
    assertFalse(m3Writer.column("e").isProjected());
    assertFalse(m3Writer.column("f").isProjected());

    // Write a couple of rows.

    rsLoader.startBatch();
    rootWriter.start();
    rootWriter
      .addRow(mapValue( 1,  2), mapValue( 3,  4), mapValue( 5,  6))
      .addRow(mapValue(11, 12), mapValue(13, 14), mapValue(15, 16));

    // Verify. Only the projected columns appear in the result set.

    TupleMetadata expectedSchema = new SchemaBuilder()
      .addMap("m1")
        .add("a", MinorType.INT)
        .add("b", MinorType.INT)
        .resumeSchema()
      .addMap("m2")
        .add("d", MinorType.INT)
        .resumeSchema()
      .buildSchema();
    SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
      .addRow(mapValue( 1,  2), mapValue( 4))
      .addRow(mapValue(11, 12), mapValue(14))
      .build();
    RowSetUtilities.verify(expected, fixture.wrap(rsLoader.harvest()));
    rsLoader.close();
  }

  @Test
  public void testMapProjectionMemberAndMap() {

    // SELECT m1, m1.b
    // This really means project all of m1; m1.b is along for the ride.

    List<SchemaPath> selection = RowSetTestUtils.projectList("m1", "m1.b");

    // Define an "early" reader schema consistent with the projection.

    TupleMetadata schema = new SchemaBuilder()
        .addMap("m1")
          .add("a", MinorType.INT)
          .add("b", MinorType.INT)
          .resumeSchema()
        .buildSchema();
    ResultSetOptions options = new OptionBuilder()
        .setProjection(ProjectionSetFactory.build(selection))
        .setSchema(schema)
        .build();
    ResultSetLoader rsLoader = new ResultSetLoaderImpl(fixture.allocator(), options);
    RowSetLoader rootWriter = rsLoader.writer();

    // Verify the projected columns

    TupleMetadata actualSchema = rootWriter.tupleSchema();
    ColumnMetadata m1Md = actualSchema.metadata("m1");
    TupleWriter m1Writer = rootWriter.tuple("m1");
    assertTrue(m1Md.isMap());
    assertTrue(m1Writer.isProjected());
    assertEquals(2, m1Md.tupleSchema().size());
    assertTrue(m1Writer.column("a").isProjected());
    assertTrue(m1Writer.column("b").isProjected());

    // Write a couple of rows.

    rsLoader.startBatch();
    rootWriter.start();
    rootWriter
      .addSingleCol(mapValue( 1,  2))
      .addSingleCol(mapValue(11, 12));

    // Verify. The whole map appears in the result set because the
    // project list included the whole map as well as a map member.

    SingleRowSet expected = fixture.rowSetBuilder(schema)
      .addSingleCol(mapValue( 1,  2))
      .addSingleCol(mapValue(11, 12))
      .build();
    RowSetUtilities.verify(expected, fixture.wrap(rsLoader.harvest()));
    rsLoader.close();
  }

  /**
   * Test a map array. Use the convenience methods to set values.
   * Only the projected array members should appear in the harvested
   * results.
   */

  @Test
  public void testMapArrayProjection() {
    List<SchemaPath> selection = RowSetTestUtils.projectList("m1", "m2.d");
    TupleMetadata schema = new SchemaBuilder()
        .addMapArray("m1")
          .add("a", MinorType.INT)
          .add("b", MinorType.INT)
          .resumeSchema()
        .addMapArray("m2")
          .add("c", MinorType.INT)
          .add("d", MinorType.INT)
          .resumeSchema()
        .addMapArray("m3")
          .add("e", MinorType.INT)
          .add("f", MinorType.INT)
          .resumeSchema()
        .buildSchema();
    ResultSetOptions options = new OptionBuilder()
        .setProjection(ProjectionSetFactory.build(selection))
        .setSchema(schema)
        .build();
    ResultSetLoader rsLoader = new ResultSetLoaderImpl(fixture.allocator(), options);
    RowSetLoader rootWriter = rsLoader.writer();

    // Write a couple of rows.

    rsLoader.startBatch();
    rootWriter.addRow(
        objArray(objArray(10, 20), objArray(11, 21)),
        objArray(objArray(30, 40), objArray(31, 42)),
        objArray(objArray(50, 60), objArray(51, 62)));
    rootWriter.addRow(
        objArray(objArray(110, 120), objArray(111, 121)),
        objArray(objArray(130, 140), objArray(131, 142)),
        objArray(objArray(150, 160), objArray(151, 162)));

    // Verify. Only the projected columns appear in the result set.

    TupleMetadata expectedSchema = new SchemaBuilder()
      .addMapArray("m1")
        .add("a", MinorType.INT)
        .add("b", MinorType.INT)
        .resumeSchema()
      .addMapArray("m2")
        .add("d", MinorType.INT)
        .resumeSchema()
      .buildSchema();
    SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
      .addRow(
          objArray(objArray(10, 20), objArray(11, 21)),
          objArray(objArray(40), objArray(42)))
      .addRow(
          objArray(objArray(110, 120), objArray(111, 121)),
          objArray(objArray(140), objArray(142)))
      .build();
    RowSetUtilities.verify(expected, fixture.wrap(rsLoader.harvest()));
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
    List<SchemaPath> selection = RowSetTestUtils.projectList("small", "dummy");
    TupleMetadata schema = new SchemaBuilder()
        .add("big", MinorType.VARCHAR)
        .add("small", MinorType.VARCHAR)
        .buildSchema();
    ResultSetOptions options = new OptionBuilder()
        .setRowCountLimit(ValueVector.MAX_ROW_COUNT)
        .setProjection(ProjectionSetFactory.build(selection))
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

  @Test
  public void testScalarArrayConflict() {
    List<SchemaPath> selection = RowSetTestUtils.projectList("col[0]");
    TupleMetadata schema = new SchemaBuilder()
        .add("col", MinorType.VARCHAR)
        .buildSchema();
    ResultSetOptions options = new OptionBuilder()
        .setProjection(ProjectionSetFactory.build(selection))
        .setSchema(schema)
        .build();
    try {
      new ResultSetLoaderImpl(fixture.allocator(), options);
      fail();
    } catch (UserException e) {
      assertTrue(e.getErrorType() == ErrorType.VALIDATION);
    }
  }

  @Test
  public void testScalarMapConflict() {
    List<SchemaPath> selection = RowSetTestUtils.projectList("col.child");
    TupleMetadata schema = new SchemaBuilder()
        .add("col", MinorType.VARCHAR)
        .buildSchema();
    ResultSetOptions options = new OptionBuilder()
        .setProjection(ProjectionSetFactory.build(selection))
        .setSchema(schema)
        .build();
    try {
      new ResultSetLoaderImpl(fixture.allocator(), options);
      fail();
    } catch (UserException e) {
      assertTrue(e.getErrorType() == ErrorType.VALIDATION);
    }
  }

  @Test
  public void testScalarMapArrayConflict() {
    List<SchemaPath> selection = RowSetTestUtils.projectList("col[0].child");
    TupleMetadata schema = new SchemaBuilder()
        .add("col", MinorType.VARCHAR)
        .buildSchema();
    ResultSetOptions options = new OptionBuilder()
        .setProjection(ProjectionSetFactory.build(selection))
        .setSchema(schema)
        .build();
    try {
      new ResultSetLoaderImpl(fixture.allocator(), options);
      fail();
    } catch (UserException e) {
      assertTrue(e.getErrorType() == ErrorType.VALIDATION);
    }
  }

  @Test
  public void testArrayMapConflict() {
    List<SchemaPath> selection = RowSetTestUtils.projectList("col.child");
    TupleMetadata schema = new SchemaBuilder()
        .addArray("col", MinorType.VARCHAR)
        .buildSchema();
    ResultSetOptions options = new OptionBuilder()
        .setProjection(ProjectionSetFactory.build(selection))
        .setSchema(schema)
        .build();
    try {
      new ResultSetLoaderImpl(fixture.allocator(), options);
      fail();
    } catch (UserException e) {
      assertTrue(e.getErrorType() == ErrorType.VALIDATION);
    }
  }

  @Test
  public void testArrayMapArrayConflict() {
    List<SchemaPath> selection = RowSetTestUtils.projectList("col[0].child");
    TupleMetadata schema = new SchemaBuilder()
        .addArray("col", MinorType.VARCHAR)
        .buildSchema();
    ResultSetOptions options = new OptionBuilder()
        .setProjection(ProjectionSetFactory.build(selection))
        .setSchema(schema)
        .build();
    try {
      new ResultSetLoaderImpl(fixture.allocator(), options);
      fail();
    } catch (UserException e) {
      assertTrue(e.getErrorType() == ErrorType.VALIDATION);
    }
  }

  @Test
  public void testMapArrayConflict() {
    List<SchemaPath> selection = RowSetTestUtils.projectList("col[0]");
    TupleMetadata schema = new SchemaBuilder()
        .addMap("col")
          .add("child", MinorType.VARCHAR)
          .resumeSchema()
        .buildSchema();
    ResultSetOptions options = new OptionBuilder()
        .setProjection(ProjectionSetFactory.build(selection))
        .setSchema(schema)
        .build();
    try {
      new ResultSetLoaderImpl(fixture.allocator(), options);
      fail();
    } catch (UserException e) {
      assertTrue(e.getErrorType() == ErrorType.VALIDATION);
    }
  }


  @Test
  public void testMapMapArrayConflict() {
    List<SchemaPath> selection = RowSetTestUtils.projectList("col[0].child");
    TupleMetadata schema = new SchemaBuilder()
        .addMap("col")
          .add("child", MinorType.VARCHAR)
          .resumeSchema()
        .buildSchema();
    ResultSetOptions options = new OptionBuilder()
        .setProjection(ProjectionSetFactory.build(selection))
        .setSchema(schema)
        .build();
    try {
      new ResultSetLoaderImpl(fixture.allocator(), options);
      fail();
    } catch (UserException e) {
      assertTrue(e.getErrorType() == ErrorType.VALIDATION);
    }
  }

  @Test
  public void testDictProjection() {

    final String dictName1 = "d1";
    final String dictName2 = "d2";

    // There is no test for case when obtaining a value by key as this is not as simple projection
    // as it is in case of map - there is a need to find a value corresponding to a key
    // (the functionality is currently present in DictReader) and final column schema should be
    // changed from dict structure with `key` and `value` children to a simple `value`.
    List<SchemaPath> selection = RowSetTestUtils.projectList(dictName1);
    TupleMetadata schema = new SchemaBuilder()
        .addDict(dictName1, MinorType.VARCHAR)
          .value(MinorType.INT)
          .resumeSchema()
        .addDict(dictName2, MinorType.VARCHAR)
          .value(MinorType.INT)
          .resumeSchema()
        .buildSchema();
    ResultSetOptions options = new OptionBuilder()
        .setProjection(ProjectionSetFactory.build(selection))
        .setSchema(schema)
        .build();
    ResultSetLoader rsLoader = new ResultSetLoaderImpl(fixture.allocator(), options);
    RowSetLoader rootWriter = rsLoader.writer();

    // Verify the projected columns

    TupleMetadata actualSchema = rootWriter.tupleSchema();
    ColumnMetadata dictMetadata1 = actualSchema.metadata(dictName1);
    DictWriter dictWriter1 = rootWriter.dict(dictName1);
    assertTrue(dictMetadata1.isDict());
    assertTrue(dictWriter1.isProjected());
    assertEquals(2, dictMetadata1.tupleSchema().size());
    assertTrue(dictWriter1.keyWriter().isProjected());
    assertTrue(dictWriter1.valueWriter().isProjected());

    ColumnMetadata dictMetadata2 = actualSchema.metadata(dictName2);
    DictWriter dictWriter2 = rootWriter.dict(dictName2);
    assertTrue(dictMetadata2.isDict());
    assertFalse(dictWriter2.isProjected());
    assertEquals(2, dictMetadata2.tupleSchema().size());
    assertFalse(dictWriter2.keyWriter().isProjected());
    assertFalse(dictWriter2.valueWriter().isProjected());

    // Write a couple of rows.

    rsLoader.startBatch();
    rootWriter.start();
    rootWriter
        .addRow(map( "a", 1, "b", 2), map( "c", 3, "d", 4))
        .addRow(map("a", 11, "b", 12), map("c", 13, "d", 14));

    // Verify. Only the projected columns appear in the result set.

    TupleMetadata expectedSchema = new SchemaBuilder()
        .addDict(dictName1, MinorType.VARCHAR)
          .value(MinorType.INT)
          .resumeSchema()
        .buildSchema();
    SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addRow(map( "a", 1, "b", 2))
        .addRow(map("a", 11, "b", 12))
        .build();
    RowSetUtilities.verify(expected, fixture.wrap(rsLoader.harvest()));
    rsLoader.close();
  }

  @Test
  public void testDictStringKeyAccess() {
    List<SchemaPath> selection = RowSetTestUtils.projectList("col.a"); // the same as col['a'], but number is expected in brackets ([])
    TupleMetadata schema = new SchemaBuilder()
        .addDict("col", MinorType.VARCHAR)
          .value(MinorType.INT)
          .resumeSchema()
        .buildSchema();
    ResultSetOptions options = new OptionBuilder()
        .setProjection(ProjectionSetFactory.build(selection))
        .setSchema(schema)
        .build();
    new ResultSetLoaderImpl(fixture.allocator(), options); // no validation error
  }

  @Test
  public void testDictNumericKeyAccess() {
    List<SchemaPath> selection = RowSetTestUtils.projectList("col[0]");
    TupleMetadata schema = new SchemaBuilder()
        .addDict("col", MinorType.INT)
        .value(MinorType.VARCHAR)
        .resumeSchema()
        .buildSchema();
    ResultSetOptions options = new OptionBuilder()
        .setProjection(ProjectionSetFactory.build(selection))
        .setSchema(schema)
        .build();
    new ResultSetLoaderImpl(fixture.allocator(), options); // no validation error
  }
}
