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
package org.apache.drill.test.rowSet.test;

import static org.apache.drill.test.rowSet.RowSetUtilities.intArray;
import static org.apache.drill.test.rowSet.RowSetUtilities.objArray;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.ArrayReader;
import org.apache.drill.exec.vector.accessor.ArrayWriter;
import org.apache.drill.exec.vector.accessor.ObjectType;
import org.apache.drill.exec.vector.accessor.ScalarReader;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.apache.drill.exec.vector.accessor.TupleReader;
import org.apache.drill.exec.vector.accessor.TupleWriter;
import org.apache.drill.exec.vector.accessor.ValueType;
import org.apache.drill.exec.vector.complex.MapVector;
import org.apache.drill.exec.vector.complex.RepeatedMapVector;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.DirectRowSet;
import org.apache.drill.test.rowSet.RowSet.ExtendableRowSet;
import org.apache.drill.test.rowSet.RowSet.SingleRowSet;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.apache.drill.test.rowSet.RowSetReader;
import org.apache.drill.test.rowSet.RowSetWriter;
import org.apache.drill.test.rowSet.schema.SchemaBuilder;
import org.junit.Test;

/**
 * Test row sets. Since row sets are a thin wrapper around vectors,
 * readers and writers, this is also a test of those constructs.
 * <p>
 * Tests basic protocol of the writers: <pre><code>
 * row : tuple
 * tuple : column *
 * column : scalar obj | array obj | tuple obj | variant obj
 * scalar obj : scalar
 * array obj : array
 * array : index --> column
 * element : column
 * tuple obj : tuple
 * tuple : name --> column (also index --> column)
 * variant obj : variant
 * variant : type --> column</code></pre>
 * <p>
 * A list is an array of variants. Variants are tested elsewhere.
 */

public class RowSetTest extends SubOperatorTest {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RowSetTest.class);

  /**
   * Test the simplest constructs: a row with top-level scalar
   * columns.
   * <p>
   * The focus here is the structure of the readers and writers, along
   * with the row set loader and verifier that use those constructs.
   * That is, while this test uses the int vector, this test is not
   * focused on that vector.
   */

  @Test
  public void testScalarStructure() {
    final TupleMetadata schema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .buildSchema();
    final ExtendableRowSet rowSet = fixture.rowSet(schema);
    final RowSetWriter writer = rowSet.writer();

    // Required Int
    // Verify the invariants of the "full" and "simple" access paths

    assertEquals(ObjectType.SCALAR, writer.column("a").type());
    assertSame(writer.column("a"), writer.column(0));
    assertSame(writer.scalar("a"), writer.scalar(0));
    assertSame(writer.column("a").scalar(), writer.scalar("a"));
    assertSame(writer.column(0).scalar(), writer.scalar(0));
    assertEquals(ValueType.INTEGER, writer.scalar(0).valueType());

    // Sanity checks

    try {
      writer.column(0).array();
      fail();
    } catch (final UnsupportedOperationException e) {
      // Expected
    }
    try {
      writer.column(0).tuple();
      fail();
    } catch (final UnsupportedOperationException e) {
      // Expected
    }

    // Test the various ways to get at the scalar writer.

    writer.column("a").scalar().setInt(10);
    writer.save();
    writer.scalar("a").setInt(20);
    writer.save();
    writer.column(0).scalar().setInt(30);
    writer.save();
    writer.scalar(0).setInt(40);
    writer.save();

    // Finish the row set and get a reader.

    final SingleRowSet actual = writer.done();
    final RowSetReader reader = actual.reader();

    // Verify invariants

    assertEquals(ObjectType.SCALAR, reader.column(0).type());
    assertSame(reader.column("a"), reader.column(0));
    assertSame(reader.scalar("a"), reader.scalar(0));
    assertSame(reader.column("a").scalar(), reader.scalar("a"));
    assertSame(reader.column(0).scalar(), reader.scalar(0));
    assertEquals(ValueType.INTEGER, reader.scalar(0).valueType());
    assertTrue(schema.metadata("a").isEquivalent(reader.column("a").schema()));

    // Test various accessors: full and simple

    assertTrue(reader.next());
    assertFalse(reader.column("a").scalar().isNull());
    assertEquals(10, reader.column("a").scalar().getInt());
    assertTrue(reader.next());
    assertFalse(reader.scalar("a").isNull());
    assertEquals(20, reader.scalar("a").getInt());
    assertTrue(reader.next());
    assertFalse(reader.column(0).scalar().isNull());
    assertEquals(30, reader.column(0).scalar().getInt());
    assertTrue(reader.next());
    assertFalse(reader.column(0).scalar().isNull());
    assertEquals(40, reader.scalar(0).getInt());
    assertFalse(reader.next());

    // Test the above again via the writer and reader
    // utility classes.

    final SingleRowSet expected = fixture.rowSetBuilder(schema)
        .addRow(10)
        .addRow(20)
        .addRow(30)
        .addRow(40)
        .build();
    new RowSetComparison(expected).verifyAndClearAll(actual);
  }

  /**
   * Test a record with a top level array. The focus here is on the
   * scalar array structure.
   *
   * @throws VectorOverflowException should never occur
   */

  @Test
  public void testScalarArrayStructure() {
    final TupleMetadata schema = new SchemaBuilder()
        .addArray("a", MinorType.INT)
        .buildSchema();
    final ExtendableRowSet rowSet = fixture.rowSet(schema);
    final RowSetWriter writer = rowSet.writer();

    // Repeated Int
    // Verify the invariants of the "full" and "simple" access paths

    assertEquals(ObjectType.ARRAY, writer.column("a").type());

    assertSame(writer.column("a"), writer.column(0));
    assertSame(writer.array("a"), writer.array(0));
    assertSame(writer.column("a").array(), writer.array("a"));
    assertSame(writer.column(0).array(), writer.array(0));

    assertEquals(ObjectType.SCALAR, writer.column("a").array().entry().type());
    assertEquals(ObjectType.SCALAR, writer.column("a").array().entryType());
    assertSame(writer.array(0).entry().scalar(), writer.array(0).scalar());
    assertEquals(ValueType.INTEGER, writer.array(0).scalar().valueType());

    // Sanity checks

    try {
      writer.column(0).scalar();
      fail();
    } catch (final UnsupportedOperationException e) {
      // Expected
    }
    try {
      writer.column(0).tuple();
      fail();
    } catch (final UnsupportedOperationException e) {
      // Expected
    }

    // Write some data

    final ScalarWriter intWriter = writer.array("a").scalar();
    intWriter.setInt(10);
    intWriter.setInt(11);
    writer.save();
    intWriter.setInt(20);
    intWriter.setInt(21);
    intWriter.setInt(22);
    writer.save();
    intWriter.setInt(30);
    writer.save();
    intWriter.setInt(40);
    intWriter.setInt(41);
    writer.save();

    // Finish the row set and get a reader.

    final SingleRowSet actual = writer.done();
    final RowSetReader reader = actual.reader();

    // Verify the invariants of the "full" and "simple" access paths

    assertEquals(ObjectType.ARRAY, writer.column("a").type());

    assertSame(reader.column("a"), reader.column(0));
    assertSame(reader.array("a"), reader.array(0));
    assertSame(reader.column("a").array(), reader.array("a"));
    assertSame(reader.column(0).array(), reader.array(0));

    assertEquals(ObjectType.SCALAR, reader.column("a").array().entryType());
    assertEquals(ValueType.INTEGER, reader.array(0).scalar().valueType());

    // Read and verify the rows

    final ArrayReader arrayReader = reader.array(0);
    final ScalarReader intReader = arrayReader.scalar();
    assertTrue(reader.next());
    assertFalse(arrayReader.isNull());
    assertEquals(2, arrayReader.size());

    assertTrue(arrayReader.next());
    assertEquals(10, intReader.getInt());
    assertTrue(arrayReader.next());
    assertEquals(11, intReader.getInt());
    assertFalse(arrayReader.next());

    assertTrue(reader.next());
    assertFalse(arrayReader.isNull());
    assertEquals(3, arrayReader.size());

    assertTrue(arrayReader.next());
    assertEquals(20, intReader.getInt());
    assertTrue(arrayReader.next());
    assertEquals(21, intReader.getInt());
    assertTrue(arrayReader.next());
    assertEquals(22, intReader.getInt());
    assertFalse(arrayReader.next());

    assertTrue(reader.next());
    assertFalse(arrayReader.isNull());
    assertEquals(1, arrayReader.size());
    assertTrue(arrayReader.next());
    assertEquals(30, intReader.getInt());
    assertFalse(arrayReader.next());

    assertTrue(reader.next());
    assertFalse(arrayReader.isNull());
    assertEquals(2, arrayReader.size());
    assertTrue(arrayReader.next());
    assertEquals(40, intReader.getInt());
    assertTrue(arrayReader.next());
    assertEquals(41, intReader.getInt());
    assertFalse(arrayReader.next());

    assertFalse(reader.next());

    // Test the above again via the writer and reader
    // utility classes.

    final SingleRowSet expected = fixture.rowSetBuilder(schema)
        .addSingleCol(intArray(10, 11))
        .addSingleCol(intArray(20, 21, 22))
        .addSingleCol(intArray(30))
        .addSingleCol(intArray(40, 41))
        .build();
    new RowSetComparison(expected)
      .verifyAndClearAll(actual);
  }

  /**
   * Test a simple map structure at the top level of a row.
   *
   * @throws VectorOverflowException should never occur
   */

  @Test
  public void testMapStructure() {
    final TupleMetadata schema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .addMap("m")
          .addArray("b", MinorType.INT)
          .resumeSchema()
        .buildSchema();
    final ExtendableRowSet rowSet = fixture.rowSet(schema);
    final RowSetWriter writer = rowSet.writer();

    // Map and Int
    // Test Invariants

    assertEquals(ObjectType.SCALAR, writer.column("a").type());
    assertEquals(ObjectType.SCALAR, writer.column(0).type());
    assertEquals(ObjectType.TUPLE, writer.column("m").type());
    assertEquals(ObjectType.TUPLE, writer.column(1).type());
    assertSame(writer.column(1).tuple(), writer.tuple(1));

    final TupleWriter mapWriter = writer.column(1).tuple();
    assertEquals(ObjectType.SCALAR, mapWriter.column("b").array().entry().type());
    assertEquals(ObjectType.SCALAR, mapWriter.column("b").array().entryType());

    final ScalarWriter aWriter = writer.column("a").scalar();
    final ScalarWriter bWriter = writer.column("m").tuple().column("b").array().entry().scalar();
    assertSame(bWriter, writer.tuple(1).array(0).scalar());
    assertEquals(ValueType.INTEGER, bWriter.valueType());

    // Sanity checks

    try {
      writer.column(1).scalar();
      fail();
    } catch (final UnsupportedOperationException e) {
      // Expected
    }
    try {
      writer.column(1).array();
      fail();
    } catch (final UnsupportedOperationException e) {
      // Expected
    }

    // Write data

    aWriter.setInt(10);
    bWriter.setInt(11);
    bWriter.setInt(12);
    writer.save();
    aWriter.setInt(20);
    bWriter.setInt(21);
    bWriter.setInt(22);
    writer.save();
    aWriter.setInt(30);
    bWriter.setInt(31);
    bWriter.setInt(32);
    writer.save();

    // Finish the row set and get a reader.

    final SingleRowSet actual = writer.done();
    final RowSetReader reader = actual.reader();

    assertEquals(ObjectType.SCALAR, reader.column("a").type());
    assertEquals(ObjectType.SCALAR, reader.column(0).type());
    assertEquals(ObjectType.TUPLE, reader.column("m").type());
    assertEquals(ObjectType.TUPLE, reader.column(1).type());
    assertSame(reader.column(1).tuple(), reader.tuple(1));

    final ScalarReader aReader = reader.column(0).scalar();
    final TupleReader mReader = reader.column(1).tuple();
    final ArrayReader bArray = mReader.column("b").array();
    assertEquals(ObjectType.SCALAR, bArray.entryType());
    final ScalarReader bReader = bArray.scalar();
    assertEquals(ValueType.INTEGER, bReader.valueType());

    // Row 1: (10, {[11, 12]})

    assertTrue(reader.next());
    assertEquals(10, aReader.getInt());
    assertFalse(mReader.isNull());

    assertTrue(bArray.next());
    assertFalse(bReader.isNull());
    assertEquals(11, bReader.getInt());
    assertTrue(bArray.next());
    assertFalse(bReader.isNull());
    assertEquals(12, bReader.getInt());
    assertFalse(bArray.next());

    // Row 2: (20, {[21, 22]})

    assertTrue(reader.next());
    assertEquals(20, aReader.getInt());
    assertFalse(mReader.isNull());

    assertTrue(bArray.next());
    assertEquals(21, bReader.getInt());
    assertTrue(bArray.next());
    assertEquals(22, bReader.getInt());

    // Row 3: (30, {[31, 32]})

    assertTrue(reader.next());
    assertEquals(30, aReader.getInt());
    assertFalse(mReader.isNull());

    assertTrue(bArray.next());
    assertEquals(31, bReader.getInt());
    assertTrue(bArray.next());
    assertEquals(32, bReader.getInt());

    assertFalse(reader.next());

    // Verify that the map accessor's value count was set.

    final MapVector mapVector = (MapVector) actual.container().getValueVector(1).getValueVector();
    assertEquals(actual.rowCount(), mapVector.getAccessor().getValueCount());

    final SingleRowSet expected = fixture.rowSetBuilder(schema)
        .addRow(10, objArray(intArray(11, 12)))
        .addRow(20, objArray(intArray(21, 22)))
        .addRow(30, objArray(intArray(31, 32)))
        .build();
    new RowSetComparison(expected)
      .verifyAndClearAll(actual);
  }

  @Test
  public void testRepeatedMapStructure() {
    final TupleMetadata schema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .addMapArray("m")
          .add("b", MinorType.INT)
          .add("c", MinorType.INT)
          .resumeSchema()
        .buildSchema();
    final ExtendableRowSet rowSet = fixture.rowSet(schema);
    final RowSetWriter writer = rowSet.writer();

    // Map and Int
    // Pick out components and lightly test. (Assumes structure
    // tested earlier is still valid, so no need to exhaustively
    // test again.)

    assertEquals(ObjectType.SCALAR, writer.column("a").type());
    assertEquals(ObjectType.ARRAY, writer.column("m").type());

    final ArrayWriter maWriter = writer.column(1).array();
    assertEquals(ObjectType.TUPLE, maWriter.entryType());

    final TupleWriter mapWriter = maWriter.tuple();
    assertEquals(ObjectType.SCALAR, mapWriter.column("b").type());
    assertEquals(ObjectType.SCALAR, mapWriter.column("c").type());

    final ScalarWriter aWriter = writer.column("a").scalar();
    final ScalarWriter bWriter = mapWriter.scalar("b");
    final ScalarWriter cWriter = mapWriter.scalar("c");
    assertEquals(ValueType.INTEGER, aWriter.valueType());
    assertEquals(ValueType.INTEGER, bWriter.valueType());
    assertEquals(ValueType.INTEGER, cWriter.valueType());

    // Write data

    aWriter.setInt(10);
    bWriter.setInt(101);
    cWriter.setInt(102);
    maWriter.save(); // Advance to next array position
    bWriter.setInt(111);
    cWriter.setInt(112);
    maWriter.save();
    writer.save();

    aWriter.setInt(20);
    bWriter.setInt(201);
    cWriter.setInt(202);
    maWriter.save();
    bWriter.setInt(211);
    cWriter.setInt(212);
    maWriter.save();
    writer.save();

    aWriter.setInt(30);
    bWriter.setInt(301);
    cWriter.setInt(302);
    maWriter.save();
    bWriter.setInt(311);
    cWriter.setInt(312);
    maWriter.save();
    writer.save();

    // Finish the row set and get a reader.

    final SingleRowSet actual = writer.done();
    final RowSetReader reader = actual.reader();

    // Verify reader structure

    assertEquals(ObjectType.SCALAR, reader.column("a").type());
    assertEquals(ObjectType.ARRAY, reader.column("m").type());

    final ArrayReader maReader = reader.column(1).array();
    assertEquals(ObjectType.TUPLE, maReader.entryType());

    final TupleReader mapReader = maReader.tuple();
    assertEquals(ObjectType.SCALAR, mapReader.column("b").type());
    assertEquals(ObjectType.SCALAR, mapReader.column("c").type());

    final ScalarReader aReader = reader.column("a").scalar();
    final ScalarReader bReader = mapReader.scalar("b");
    final ScalarReader cReader = mapReader.scalar("c");
    assertEquals(ValueType.INTEGER, aReader.valueType());
    assertEquals(ValueType.INTEGER, bReader.valueType());
    assertEquals(ValueType.INTEGER, cReader.valueType());

    // Row 1: Use iterator-like accessors

    assertTrue(reader.next());
    assertEquals(10, aReader.getInt());
    assertFalse(maReader.isNull()); // Array itself is not null

    assertTrue(maReader.next());
    assertFalse(mapReader.isNull()); // Tuple 0 is not null
    assertEquals(101, mapReader.scalar(0).getInt());
    assertEquals(102, mapReader.scalar(1).getInt());

    assertTrue(maReader.next());
    assertEquals(111, mapReader.scalar(0).getInt());
    assertEquals(112, mapReader.scalar(1).getInt());

    // Row 2: use explicit positioning,
    // but access scalars through the map reader.

    assertTrue(reader.next());
    assertEquals(20, aReader.getInt());
    maReader.setPosn(0);
    assertEquals(201, mapReader.scalar(0).getInt());
    assertEquals(202, mapReader.scalar(1).getInt());
    maReader.setPosn(1);
    assertEquals(211, mapReader.scalar(0).getInt());
    assertEquals(212, mapReader.scalar(1).getInt());

    // Row 3: use scalar accessor

    assertTrue(reader.next());
    assertEquals(30, aReader.getInt());

    assertTrue(maReader.next());
    assertEquals(301, bReader.getInt());
    assertEquals(302, cReader.getInt());

    assertTrue(maReader.next());
    assertEquals(311, bReader.getInt());
    assertEquals(312, cReader.getInt());

    assertFalse(reader.next());

    // Verify that the map accessor's value count was set.

    final RepeatedMapVector mapVector = (RepeatedMapVector) actual.container().getValueVector(1).getValueVector();
    assertEquals(3, mapVector.getAccessor().getValueCount());

    // Verify the readers and writers again using the testing tools.

    final SingleRowSet expected = fixture.rowSetBuilder(schema)
        .addRow(10, objArray(objArray(101, 102), objArray(111, 112)))
        .addRow(20, objArray(objArray(201, 202), objArray(211, 212)))
        .addRow(30, objArray(objArray(301, 302), objArray(311, 312)))
        .build();
    new RowSetComparison(expected)
      .verifyAndClearAll(actual);
  }

  /**
   * Test an array of ints (as an example fixed-width type)
   * at the top level of a schema.
   */

  @Test
  public void testTopFixedWidthArray() {
    final BatchSchema batchSchema = new SchemaBuilder()
        .add("c", MinorType.INT)
        .addArray("a", MinorType.INT)
        .build();

    final ExtendableRowSet rs1 = fixture.rowSet(batchSchema);
    final RowSetWriter writer = rs1.writer();
    writer.scalar(0).setInt(10);
    final ScalarWriter array = writer.array(1).scalar();
    array.setInt(100);
    array.setInt(110);
    writer.save();
    writer.scalar(0).setInt(20);
    array.setInt(200);
    array.setInt(120);
    array.setInt(220);
    writer.save();
    writer.scalar(0).setInt(30);
    writer.save();

    final SingleRowSet result = writer.done();

    final RowSetReader reader = result.reader();
    final ArrayReader arrayReader = reader.array(1);
    final ScalarReader elementReader = arrayReader.scalar();

    assertTrue(reader.next());
    assertEquals(10, reader.scalar(0).getInt());
    assertEquals(2, arrayReader.size());

    assertTrue(arrayReader.next());
    assertEquals(100, elementReader.getInt());
    assertTrue(arrayReader.next());
    assertEquals(110, elementReader.getInt());

    assertTrue(reader.next());
    assertEquals(20, reader.scalar(0).getInt());
    assertEquals(3, arrayReader.size());
    assertTrue(arrayReader.next());
    assertEquals(200, elementReader.getInt());
    assertTrue(arrayReader.next());
    assertEquals(120, elementReader.getInt());
    assertTrue(arrayReader.next());
    assertEquals(220, elementReader.getInt());

    assertTrue(reader.next());
    assertEquals(30, reader.scalar(0).getInt());
    assertEquals(0, arrayReader.size());
    assertFalse(reader.next());

    final SingleRowSet rs2 = fixture.rowSetBuilder(batchSchema)
      .addRow(10, intArray(100, 110))
      .addRow(20, intArray(200, 120, 220))
      .addRow(30, null)
      .build();

    new RowSetComparison(rs1)
      .verifyAndClearAll(rs2);
  }
  /**
   * Test filling a row set up to the maximum number of rows.
   * Values are small enough to prevent filling to the
   * maximum buffer size.
   */

  @Test
  public void testRowBounds() {
    final BatchSchema batchSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .build();

    final ExtendableRowSet rs = fixture.rowSet(batchSchema);
    final RowSetWriter writer = rs.writer();
    int count = 0;
    while (! writer.isFull()) {
      writer.scalar(0).setInt(count++);
      writer.save();
    }
    writer.done();

    assertEquals(ValueVector.MAX_ROW_COUNT, count);
    // The writer index points past the writable area.
    // But, this is fine, the valid() method says we can't
    // write at this location.
    assertEquals(ValueVector.MAX_ROW_COUNT, writer.rowIndex());
    assertEquals(ValueVector.MAX_ROW_COUNT, rs.rowCount());
    rs.clear();
  }

  /**
   * Test filling a row set up to the maximum vector size.
   * Values in the first column are small enough to prevent filling to the
   * maximum buffer size, but values in the second column
   * will reach maximum buffer size before maximum row size.
   * The result should be the number of rows that fit, with the
   * partial last row not counting. (A complete application would
   * reload the partial row into a new row set.)
   */

  @Test
  public void testBufferBounds() {
    final BatchSchema batchSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("b", MinorType.VARCHAR)
        .build();

    String varCharValue;
    try {
      final byte rawValue[] = new byte[512];
      Arrays.fill(rawValue, (byte) 'X');
      varCharValue = new String(rawValue, "UTF-8");
    } catch (final UnsupportedEncodingException e) {
      throw new IllegalStateException(e);
    }

    final ExtendableRowSet rs = fixture.rowSet(batchSchema);
    final RowSetWriter writer = rs.writer();
    int count = 0;
    try {

      // Test overflow. This is not a typical use case: don't want to
      // hit overflow without overflow handling. In this case, we throw
      // away the last row because the row set abstraction does not
      // implement vector overflow other than throwing an exception.

      for (;;) {
        writer.scalar(0).setInt(count);
        writer.scalar(1).setString(varCharValue);

        // Won't get here on overflow.
        writer.save();
        count++;
      }
    } catch (final Exception e) {
      assertTrue(e.getMessage().contains("Overflow"));
    }
    writer.done();

    assertTrue(count < ValueVector.MAX_ROW_COUNT);
    assertEquals(count, writer.rowIndex());
    assertEquals(count, rs.rowCount());
    rs.clear();
  }

  /**
   * The code below is not a test. Rather, it is a simple example of
   * how to write a batch of data using writers, then read it using
   * readers.
   */

  @Test
  public void example() {

    // Step 1: Define a schema. In a real app, this
    // will be provided by a reader, by an incoming batch,
    // etc.

    final BatchSchema schema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .addArray("b", MinorType.INT)
        .addMap("c")
          .add("c1", MinorType.INT)
          .add("c2", MinorType.VARCHAR)
          .resumeSchema()
        .build();

    // Step 2: Create a batch. Done here because this is
    // a batch-oriented test. Done automatically in the
    // result set loader.

    final DirectRowSet drs = DirectRowSet.fromSchema(fixture.allocator(), schema);

    // Step 3: Create the writer.

    final RowSetWriter writer = drs.writer();

    // Step 4: Populate data. Here we do it the way an app would:
    // using the individual accessors. See tests above for the many
    // ways this can be done depending on the need of the app.
    //
    // Write two rows:
    // ("fred", [10, 11], {12, "wilma"})
    // ("barney", [20, 21], {22, "betty"})
    //
    // This example uses Java strings for Varchar. Real code might
    // use byte arrays.

    writer.scalar("a").setString("fred");
    final ArrayWriter bWriter = writer.array("b");
    bWriter.scalar().setInt(10);
    bWriter.scalar().setInt(11);
    final TupleWriter cWriter = writer.tuple("c");
    cWriter.scalar("c1").setInt(12);
    cWriter.scalar("c2").setString("wilma");
    writer.save();

    writer.scalar("a").setString("barney");
    bWriter.scalar().setInt(20);
    bWriter.scalar().setInt(21);
    cWriter.scalar("c1").setInt(22);
    cWriter.scalar("c2").setString("betty");
    writer.save();

    // Step 5: "Harvest" the batch. Done differently in the
    // result set loader.

    final SingleRowSet rowSet = writer.done();

    // Step 5: Create a reader.

    final RowSetReader reader = rowSet.reader();

    // Step 6: Retrieve the data. Here we just print the
    // values.

    while (reader.next()) {
      final StringBuilder sb = new StringBuilder();
      sb.append(print(reader.scalar("a").getString()));
      final ArrayReader bReader = reader.array("b");
      while (bReader.next()) {
        sb.append(print(bReader.scalar().getInt()));
      }
      final TupleReader cReader = reader.tuple("c");
      sb.append(print(cReader.scalar("c1").getInt()));
      sb.append(print(cReader.scalar("c2").getString()));
      logger.debug(sb.toString());
    }

    // Step 7: Free memory.

    rowSet.clear();
  }

  public String print(Object obj) {
    final StringBuilder sb = new StringBuilder();

    if (obj instanceof String) {
      sb.append("\"");
      sb.append(obj);
      sb.append("\"");
    } else {
      sb.append(obj);
    }
    sb.append(" ");
    return sb.toString();
  }
}
