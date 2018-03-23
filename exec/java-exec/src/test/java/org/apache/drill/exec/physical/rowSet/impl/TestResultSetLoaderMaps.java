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

import static org.apache.drill.test.rowSet.RowSetUtilities.intArray;
import static org.apache.drill.test.rowSet.RowSetUtilities.objArray;
import static org.apache.drill.test.rowSet.RowSetUtilities.strArray;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.rowSet.ResultSetLoader;
import org.apache.drill.exec.physical.rowSet.RowSetLoader;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.apache.drill.exec.vector.accessor.TupleReader;
import org.apache.drill.exec.vector.accessor.TupleWriter;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.RowSet;
import org.apache.drill.test.rowSet.RowSet.SingleRowSet;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.apache.drill.test.rowSet.RowSetReader;
import org.apache.drill.test.rowSet.schema.SchemaBuilder;
import org.junit.Test;

/**
 * Test (non-array) map support in the result set loader and related classes.
 */

public class TestResultSetLoaderMaps extends SubOperatorTest {

  @Test
  public void testBasics() {
    TupleMetadata schema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .addMap("m")
          .add("c", MinorType.INT)
          .add("d", MinorType.VARCHAR)
          .resumeSchema()
        .add("e", MinorType.VARCHAR)
        .buildSchema();
    ResultSetLoaderImpl.ResultSetOptions options = new OptionBuilder()
        .setSchema(schema)
        .build();
    ResultSetLoader rsLoader = new ResultSetLoaderImpl(fixture.allocator(), options);
    RowSetLoader rootWriter = rsLoader.writer();

    // Verify structure and schema

    assertEquals(5, rsLoader.schemaVersion());
    TupleMetadata actualSchema = rootWriter.schema();
    assertEquals(3, actualSchema.size());
    assertTrue(actualSchema.metadata(1).isMap());
    assertEquals(2, actualSchema.metadata("m").mapSchema().size());
    assertEquals(2, actualSchema.column("m").getChildren().size());

    rsLoader.startBatch();

    // Write a row the way that clients will do.

    ScalarWriter aWriter = rootWriter.scalar("a");
    TupleWriter mWriter = rootWriter.tuple("m");
    ScalarWriter cWriter = mWriter.scalar("c");
    ScalarWriter dWriter = mWriter.scalar("d");
    ScalarWriter eWriter = rootWriter.scalar("e");

    rootWriter.start();
    aWriter.setInt(10);
    cWriter.setInt(110);
    dWriter.setString("fred");
    eWriter.setString("pebbles");
    rootWriter.save();

    // Try adding a duplicate column.

    try {
      mWriter.addColumn(SchemaBuilder.columnSchema("c", MinorType.INT, DataMode.OPTIONAL));
      fail();
    } catch (IllegalArgumentException e) {
      // Expected
    }

    // Write another using the test-time conveniences

    rootWriter.addRow(20, objArray(210, "barney"), "bam-bam");

    // Harvest the batch

    RowSet actual = fixture.wrap(rsLoader.harvest());
    assertEquals(5, rsLoader.schemaVersion());
    assertEquals(2, actual.rowCount());

    // Validate data

    SingleRowSet expected = fixture.rowSetBuilder(schema)
        .addRow(10, objArray(110, "fred"), "pebbles")
        .addRow(20, objArray(210, "barney"), "bam-bam")
        .build();

    new RowSetComparison(expected).verifyAndClearAll(actual);
    rsLoader.close();
  }

  /**
   * Create schema with a map, then add columns to the map
   * after delivering the first batch. The new columns should appear
   * in the second-batch output.
   */

  @Test
  public void testMapEvolution() {
    TupleMetadata schema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .addMap("m")
          .add("b", MinorType.VARCHAR)
          .resumeSchema()
        .buildSchema();
    ResultSetLoaderImpl.ResultSetOptions options = new OptionBuilder()
        .setSchema(schema)
        .build();
    ResultSetLoader rsLoader = new ResultSetLoaderImpl(fixture.allocator(), options);
    assertEquals(3, rsLoader.schemaVersion());
    RowSetLoader rootWriter = rsLoader.writer();

    rsLoader.startBatch();
    rootWriter
      .addRow(10, objArray("fred"))
      .addRow(20, objArray("barney"));

    RowSet actual = fixture.wrap(rsLoader.harvest());
    assertEquals(3, rsLoader.schemaVersion());
    assertEquals(2, actual.rowCount());

    // Validate first batch

    SingleRowSet expected = fixture.rowSetBuilder(schema)
        .addRow(10, objArray("fred"))
        .addRow(20, objArray("barney"))
        .build();

    new RowSetComparison(expected).verifyAndClearAll(actual);

    // Add three columns in the second batch. One before
    // the batch starts, one before the first row, and one after
    // the first row.

    TupleWriter mapWriter = rootWriter.tuple("m");
    mapWriter.addColumn(SchemaBuilder.columnSchema("c", MinorType.INT, DataMode.REQUIRED));

    rsLoader.startBatch();
    mapWriter.addColumn(SchemaBuilder.columnSchema("d", MinorType.BIGINT, DataMode.REQUIRED));

    rootWriter.addRow(30, objArray("wilma", 130, 130_000L));

    mapWriter.addColumn(SchemaBuilder.columnSchema("e", MinorType.VARCHAR, DataMode.REQUIRED));
    rootWriter.addRow(40, objArray("betty", 140, 140_000L, "bam-bam"));

    actual = fixture.wrap(rsLoader.harvest());
    assertEquals(6, rsLoader.schemaVersion());
    assertEquals(2, actual.rowCount());

    // Validate first batch

    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .addMap("m")
          .add("b", MinorType.VARCHAR)
          .add("c", MinorType.INT)
          .add("d", MinorType.BIGINT)
          .add("e", MinorType.VARCHAR)
          .resumeSchema()
        .buildSchema();
    expected = fixture.rowSetBuilder(expectedSchema)
        .addRow(30, objArray("wilma", 130, 130_000L, ""))
        .addRow(40, objArray("betty", 140, 140_000L, "bam-bam"))
        .build();

    new RowSetComparison(expected).verifyAndClearAll(actual);

    rsLoader.close();
  }

  /**
   * Test adding a map to a loader after writing the first row.
   */

  @Test
  public void testMapAddition() {
    TupleMetadata schema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .buildSchema();
    ResultSetLoaderImpl.ResultSetOptions options = new OptionBuilder()
        .setSchema(schema)
        .build();
    ResultSetLoader rsLoader = new ResultSetLoaderImpl(fixture.allocator(), options);
    assertEquals(1, rsLoader.schemaVersion());
    RowSetLoader rootWriter = rsLoader.writer();

    // Start without the map. Add a map after the first row.

    rsLoader.startBatch();
    rootWriter.addRow(10);

    int mapIndex = rootWriter.addColumn(SchemaBuilder.columnSchema("m", MinorType.MAP, DataMode.REQUIRED));
    TupleWriter mapWriter = rootWriter.tuple(mapIndex);

    // Add a column to the map with the same name as the top-level column.
    // Verifies that the name spaces are independent.

    mapWriter.addColumn(SchemaBuilder.columnSchema("a", MinorType.VARCHAR, DataMode.REQUIRED));

    rootWriter
      .addRow(20, objArray("fred"))
      .addRow(30, objArray("barney"));

    RowSet actual = fixture.wrap(rsLoader.harvest());
    assertEquals(3, rsLoader.schemaVersion());
    assertEquals(3, actual.rowCount());

    // Validate first batch

    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .addMap("m")
          .add("a", MinorType.VARCHAR)
          .resumeSchema()
        .buildSchema();
    SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addRow(10, objArray(""))
        .addRow(20, objArray("fred"))
        .addRow(30, objArray("barney"))
        .build();

    new RowSetComparison(expected).verifyAndClearAll(actual);

    rsLoader.close();
  }

  /**
   * Test adding an empty map to a loader after writing the first row.
   * Then add columns in another batch. Yes, this is a bizarre condition,
   * but we must check it anyway for robustness.
   */

  @Test
  public void testEmptyMapAddition() {
    TupleMetadata schema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .buildSchema();
    ResultSetLoaderImpl.ResultSetOptions options = new OptionBuilder()
        .setSchema(schema)
        .build();
    ResultSetLoader rsLoader = new ResultSetLoaderImpl(fixture.allocator(), options);
    assertEquals(1, rsLoader.schemaVersion());
    RowSetLoader rootWriter = rsLoader.writer();

    // Start without the map. Add a map after the first row.

    rsLoader.startBatch();
    rootWriter.addRow(10);

    int mapIndex = rootWriter.addColumn(SchemaBuilder.columnSchema("m", MinorType.MAP, DataMode.REQUIRED));
    TupleWriter mapWriter = rootWriter.tuple(mapIndex);

    rootWriter
      .addRow(20, objArray())
      .addRow(30, objArray());

    RowSet actual = fixture.wrap(rsLoader.harvest());
    assertEquals(2, rsLoader.schemaVersion());
    assertEquals(3, actual.rowCount());

    // Validate first batch

    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .addMap("m")
          .resumeSchema()
        .buildSchema();
    SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addRow(10, objArray())
        .addRow(20, objArray())
        .addRow(30, objArray())
        .build();

    new RowSetComparison(expected).verifyAndClearAll(actual);

    // Now add another column to the map

    rsLoader.startBatch();
    mapWriter.addColumn(SchemaBuilder.columnSchema("a", MinorType.VARCHAR, DataMode.REQUIRED));

    rootWriter
      .addRow(40, objArray("fred"))
      .addRow(50, objArray("barney"));

    actual = fixture.wrap(rsLoader.harvest());
    assertEquals(3, rsLoader.schemaVersion());
    assertEquals(2, actual.rowCount());

    // Validate first batch

    expectedSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .addMap("m")
          .add("a", MinorType.VARCHAR)
          .resumeSchema()
        .buildSchema();
    expected = fixture.rowSetBuilder(expectedSchema)
        .addRow(40, objArray("fred"))
        .addRow(50, objArray("barney"))
        .build();

    new RowSetComparison(expected).verifyAndClearAll(actual);

    rsLoader.close();
  }

  /**
   * Create nested maps. Then, add columns to each map
   * on the fly. Use required, variable-width columns since
   * those require the most processing and are most likely to
   * fail if anything is out of place.
   */

  @Test
  public void testNestedMapsRequired() {
    TupleMetadata schema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .addMap("m1")
          .add("b", MinorType.VARCHAR)
          .addMap("m2")
            .add("c", MinorType.VARCHAR)
            .resumeMap()
          .resumeSchema()
        .buildSchema();
    ResultSetLoaderImpl.ResultSetOptions options = new OptionBuilder()
        .setSchema(schema)
        .build();
    ResultSetLoader rsLoader = new ResultSetLoaderImpl(fixture.allocator(), options);
    assertEquals(5, rsLoader.schemaVersion());
    RowSetLoader rootWriter = rsLoader.writer();

    rsLoader.startBatch();
    rootWriter.addRow(10, objArray("b1", objArray("c1")));

    // Validate first batch

    RowSet actual = fixture.wrap(rsLoader.harvest());
    assertEquals(5, rsLoader.schemaVersion());
    SingleRowSet expected = fixture.rowSetBuilder(schema)
        .addRow(10, objArray("b1", objArray("c1")))
        .build();

    new RowSetComparison(expected).verifyAndClearAll(actual);

    // Now add columns in the second batch.

    rsLoader.startBatch();
    rootWriter.addRow(20, objArray("b2", objArray("c2")));

    TupleWriter m1Writer = rootWriter.tuple("m1");
    m1Writer.addColumn(SchemaBuilder.columnSchema("d", MinorType.VARCHAR, DataMode.REQUIRED));
    TupleWriter m2Writer = m1Writer.tuple("m2");
    m2Writer.addColumn(SchemaBuilder.columnSchema("e", MinorType.VARCHAR, DataMode.REQUIRED));

    rootWriter.addRow(30, objArray("b3", objArray("c3", "e3"), "d3"));

    // And another set while the write proceeds.

    m1Writer.addColumn(SchemaBuilder.columnSchema("f", MinorType.VARCHAR, DataMode.REQUIRED));
    m2Writer.addColumn(SchemaBuilder.columnSchema("g", MinorType.VARCHAR, DataMode.REQUIRED));

    rootWriter.addRow(40, objArray("b4", objArray("c4", "e4", "g4"), "d4", "e4"));

    // Validate second batch

    actual = fixture.wrap(rsLoader.harvest());
    assertEquals(9, rsLoader.schemaVersion());

    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .addMap("m1")
          .add("b", MinorType.VARCHAR)
          .addMap("m2")
            .add("c", MinorType.VARCHAR)
            .add("e", MinorType.VARCHAR)
            .add("g", MinorType.VARCHAR)
            .resumeMap()
          .add("d", MinorType.VARCHAR)
          .add("f", MinorType.VARCHAR)
          .resumeSchema()
        .buildSchema();
    expected = fixture.rowSetBuilder(expectedSchema)
        .addRow(20, objArray("b2", objArray("c2", "",   ""  ), "",    "" ))
        .addRow(30, objArray("b3", objArray("c3", "e3", ""  ), "d3",  "" ))
        .addRow(40, objArray("b4", objArray("c4", "e4", "g4"), "d4", "e4"))
        .build();

    new RowSetComparison(expected).verifyAndClearAll(actual);

    rsLoader.close();
  }

  /**
   * Create nested maps. Then, add columns to each map
   * on the fly. This time, with nullable types.
   */

  @Test
  public void testNestedMapsNullable() {
    TupleMetadata schema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .addMap("m1")
          .addNullable("b", MinorType.VARCHAR)
          .addMap("m2")
            .addNullable("c", MinorType.VARCHAR)
            .resumeMap()
          .resumeSchema()
        .buildSchema();
    ResultSetLoaderImpl.ResultSetOptions options = new OptionBuilder()
        .setSchema(schema)
        .build();
    ResultSetLoader rsLoader = new ResultSetLoaderImpl(fixture.allocator(), options);
    RowSetLoader rootWriter = rsLoader.writer();

    rsLoader.startBatch();
    rootWriter.addRow(10, objArray("b1", objArray("c1")));

    // Validate first batch

    RowSet actual = fixture.wrap(rsLoader.harvest());
    SingleRowSet expected = fixture.rowSetBuilder(schema)
        .addRow(10, objArray("b1", objArray("c1")))
        .build();
//    actual.print();
//    expected.print();

    new RowSetComparison(expected).verifyAndClearAll(actual);

    // Now add columns in the second batch.

    rsLoader.startBatch();
    rootWriter.addRow(20, objArray("b2", objArray("c2")));

    TupleWriter m1Writer = rootWriter.tuple("m1");
    m1Writer.addColumn(SchemaBuilder.columnSchema("d", MinorType.VARCHAR, DataMode.OPTIONAL));
    TupleWriter m2Writer = m1Writer.tuple("m2");
    m2Writer.addColumn(SchemaBuilder.columnSchema("e", MinorType.VARCHAR, DataMode.OPTIONAL));

    rootWriter.addRow(30, objArray("b3", objArray("c3", "e3"), "d3"));

    // And another set while the write proceeds.

    m1Writer.addColumn(SchemaBuilder.columnSchema("f", MinorType.VARCHAR, DataMode.OPTIONAL));
    m2Writer.addColumn(SchemaBuilder.columnSchema("g", MinorType.VARCHAR, DataMode.OPTIONAL));

    rootWriter.addRow(40, objArray("b4", objArray("c4", "e4", "g4"), "d4", "e4"));

    // Validate second batch

    actual = fixture.wrap(rsLoader.harvest());
    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .addMap("m1")
          .addNullable("b", MinorType.VARCHAR)
          .addMap("m2")
            .addNullable("c", MinorType.VARCHAR)
            .addNullable("e", MinorType.VARCHAR)
            .addNullable("g", MinorType.VARCHAR)
            .resumeMap()
          .addNullable("d", MinorType.VARCHAR)
          .addNullable("f", MinorType.VARCHAR)
          .resumeSchema()
        .buildSchema();
    expected = fixture.rowSetBuilder(expectedSchema)
        .addRow(20, objArray("b2", objArray("c2", null, null), null, null))
        .addRow(30, objArray("b3", objArray("c3", "e3", null), "d3", null))
        .addRow(40, objArray("b4", objArray("c4", "e4", "g4"), "d4", "e4"))
        .build();

    new RowSetComparison(expected).verifyAndClearAll(actual);

    rsLoader.close();
  }

  /**
   * Test a map that contains a scalar array. No reason to suspect that this
   * will have problem as the array writer is fully tested in the accessor
   * subsystem. Still, need to test the cardinality methods of the loader
   * layer.
   */

  @Test
  public void testMapWithArray() {
    TupleMetadata schema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .addMap("m")
          .addArray("c", MinorType.INT)
          .addArray("d", MinorType.VARCHAR)
          .resumeSchema()
        .buildSchema();
    ResultSetLoaderImpl.ResultSetOptions options = new OptionBuilder()
        .setSchema(schema)
        .build();
    ResultSetLoader rsLoader = new ResultSetLoaderImpl(fixture.allocator(), options);
    RowSetLoader rootWriter = rsLoader.writer();

    // Write some rows

    rsLoader.startBatch();
    rootWriter
      .addRow(10, objArray(intArray(110, 120, 130),
                                strArray("d1.1", "d1.2", "d1.3", "d1.4")))
      .addRow(20, objArray(intArray(210), strArray()))
      .addRow(30, objArray(intArray(), strArray("d3.1")))
      ;

    // Validate first batch

    RowSet actual = fixture.wrap(rsLoader.harvest());
    SingleRowSet expected = fixture.rowSetBuilder(schema)
        .addRow(10, objArray(intArray(110, 120, 130),
                                  strArray("d1.1", "d1.2", "d1.3", "d1.4")))
        .addRow(20, objArray(intArray(210), strArray()))
        .addRow(30, objArray(intArray(), strArray("d3.1")))
        .build();

    new RowSetComparison(expected).verifyAndClearAll(actual);

    // Add another array after the first row in the second batch.

    rsLoader.startBatch();
    rootWriter
      .addRow(40, objArray(intArray(410, 420), strArray("d4.1", "d4.2")))
      .addRow(50, objArray(intArray(510), strArray("d5.1")))
      ;

    TupleWriter mapWriter = rootWriter.tuple("m");
    mapWriter.addColumn(SchemaBuilder.columnSchema("e", MinorType.VARCHAR, DataMode.REPEATED));
    rootWriter
      .addRow(60, objArray(intArray(610, 620), strArray("d6.1", "d6.2"), strArray("e6.1", "e6.2")))
      .addRow(70, objArray(intArray(710), strArray(), strArray("e7.1", "e7.2")))
      ;

    // Validate first batch. The new array should have been back-filled with
    // empty offsets for the missing rows.

    actual = fixture.wrap(rsLoader.harvest());
//    System.out.println(actual.schema().toString());
    expected = fixture.rowSetBuilder(actual.schema())
        .addRow(40, objArray(intArray(410, 420), strArray("d4.1", "d4.2"), strArray()))
        .addRow(50, objArray(intArray(510), strArray("d5.1"), strArray()))
        .addRow(60, objArray(intArray(610, 620), strArray("d6.1", "d6.2"), strArray("e6.1", "e6.2")))
        .addRow(70, objArray(intArray(710), strArray(), strArray("e7.1", "e7.2")))
        .build();
//    expected.print();

    new RowSetComparison(expected).verifyAndClearAll(actual);

    rsLoader.close();
  }

  /**
   * Create a schema with a map, then trigger an overflow on one of the columns
   * in the map. Proper overflow handling should occur regardless of nesting
   * depth.
   */

  @Test
  public void testMapWithOverflow() {
    TupleMetadata schema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .addMap("m1")
          .add("b", MinorType.INT)
          .addMap("m2")
            .add("c", MinorType.INT) // Before overflow, written
            .add("d", MinorType.VARCHAR)
            .add("e", MinorType.INT) // After overflow, not yet written
            .resumeMap()
          .resumeSchema()
        .buildSchema();
    ResultSetLoaderImpl.ResultSetOptions options = new OptionBuilder()
        .setSchema(schema)
        .setRowCountLimit(ValueVector.MAX_ROW_COUNT)
        .build();
    ResultSetLoader rsLoader = new ResultSetLoaderImpl(fixture.allocator(), options);
    RowSetLoader rootWriter = rsLoader.writer();

    byte value[] = new byte[512];
    Arrays.fill(value, (byte) 'X');
    int count = 0;
    rsLoader.startBatch();
    while (! rootWriter.isFull()) {
      rootWriter.addRow(count, objArray(count * 10, objArray(count * 100, value, count * 1000)));
      count++;
    }

    // Our row count should include the overflow row

    int expectedCount = ValueVector.MAX_BUFFER_SIZE / value.length;
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

  /**
   * Test the case in which a new column is added during the overflow row. Unlike
   * the top-level schema case, internally we must create a copy of the map, and
   * move vectors across only when the result is to include the schema version
   * of the target column. For overflow, the new column is added after the
   * first batch; it is added in the second batch that contains the overflow
   * row in which the column was added.
   */

  @Test
  public void testMapOverflowWithNewColumn() {
    TupleMetadata schema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .addMap("m")
          .add("b", MinorType.INT)
          .add("c", MinorType.VARCHAR)
          .resumeSchema()
        .buildSchema();
    ResultSetLoaderImpl.ResultSetOptions options = new OptionBuilder()
        .setSchema(schema)
        .setRowCountLimit(ValueVector.MAX_ROW_COUNT)
        .build();
    ResultSetLoader rsLoader = new ResultSetLoaderImpl(fixture.allocator(), options);
    assertEquals(4, rsLoader.schemaVersion());
    RowSetLoader rootWriter = rsLoader.writer();

    // Can't use the shortcut to populate rows when doing a schema
    // change.

    ScalarWriter aWriter = rootWriter.scalar("a");
    TupleWriter mWriter = rootWriter.tuple("m");
    ScalarWriter bWriter = mWriter.scalar("b");
    ScalarWriter cWriter = mWriter.scalar("c");

    byte value[] = new byte[512];
    Arrays.fill(value, (byte) 'X');
    int count = 0;
    rsLoader.startBatch();
    while (! rootWriter.isFull()) {
      rootWriter.start();
      aWriter.setInt(count);
      bWriter.setInt(count * 10);
      cWriter.setBytes(value, value.length);
      if (rootWriter.isFull()) {

        // Overflow just occurred. Add another column.

        mWriter.addColumn(SchemaBuilder.columnSchema("d", MinorType.INT, DataMode.OPTIONAL));
        mWriter.scalar("d").setInt(count * 100);
      }
      rootWriter.save();
      count++;
    }

    // Result set should include the original columns, but not d.

    RowSet result = fixture.wrap(rsLoader.harvest());

    assertEquals(4, rsLoader.schemaVersion());
    assertTrue(schema.isEquivalent(result.schema()));
    BatchSchema expectedSchema = new BatchSchema(SelectionVectorMode.NONE, schema.toFieldList());
    assertTrue(expectedSchema.isEquivalent(result.batchSchema()));

    // Use a reader to validate row-by-row. Too large to create an expected
    // result set.

    RowSetReader reader = result.reader();
    TupleReader mapReader = reader.tuple("m");
    int rowId = 0;
    while (reader.next()) {
      assertEquals(rowId, reader.scalar("a").getInt());
      assertEquals(rowId * 10, mapReader.scalar("b").getInt());
      assertTrue(Arrays.equals(value, mapReader.scalar("c").getBytes()));
      rowId++;
    }
    result.clear();

    // Next batch should start with the overflow row

    rsLoader.startBatch();
    assertEquals(1, rootWriter.rowCount());
    result = fixture.wrap(rsLoader.harvest());
    assertEquals(1, result.rowCount());

    reader = result.reader();
    mapReader = reader.tuple("m");
    while (reader.next()) {
      assertEquals(rowId, reader.scalar("a").getInt());
      assertEquals(rowId * 10, mapReader.scalar("b").getInt());
      assertTrue(Arrays.equals(value, mapReader.scalar("c").getBytes()));
      assertEquals(rowId * 100, mapReader.scalar("d").getInt());
    }
    result.clear();

    rsLoader.close();
  }

  /**
   * Version of the {#link TestResultSetLoaderProtocol#testOverwriteRow()} test
   * that uses nested columns.
   */

  @Test
  public void testOverwriteRow() {
    TupleMetadata schema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .addMap("m")
          .add("b", MinorType.INT)
          .add("c", MinorType.VARCHAR)
        .resumeSchema()
      .buildSchema();
    ResultSetLoaderImpl.ResultSetOptions options = new OptionBuilder()
        .setSchema(schema)
        .setRowCountLimit(ValueVector.MAX_ROW_COUNT)
        .build();
    ResultSetLoader rsLoader = new ResultSetLoaderImpl(fixture.allocator(), options);
    RowSetLoader rootWriter = rsLoader.writer();

    // Can't use the shortcut to populate rows when doing overwrites.

    ScalarWriter aWriter = rootWriter.scalar("a");
    TupleWriter mWriter = rootWriter.tuple("m");
    ScalarWriter bWriter = mWriter.scalar("b");
    ScalarWriter cWriter = mWriter.scalar("c");

    // Write 100,000 rows, overwriting 99% of them. This will cause vector
    // overflow and data corruption if overwrite does not work; but will happily
    // produce the correct result if everything works as it should.

    byte value[] = new byte[512];
    Arrays.fill(value, (byte) 'X');
    int count = 0;
    rsLoader.startBatch();
    while (count < 100_000) {
      rootWriter.start();
      count++;
      aWriter.setInt(count);
      bWriter.setInt(count * 10);
      cWriter.setBytes(value, value.length);
      if (count % 100 == 0) {
        rootWriter.save();
      }
    }

    // Verify using a reader.

    RowSet result = fixture.wrap(rsLoader.harvest());
    assertEquals(count / 100, result.rowCount());
    RowSetReader reader = result.reader();
    TupleReader mReader = reader.tuple("m");
    int rowId = 1;
    while (reader.next()) {
      assertEquals(rowId * 100, reader.scalar("a").getInt());
      assertEquals(rowId * 1000, mReader.scalar("b").getInt());
      assertTrue(Arrays.equals(value, mReader.scalar("c").getBytes()));
      rowId++;
    }

    result.clear();
    rsLoader.close();
  }
}
