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

import static org.apache.drill.test.rowSet.RowSetUtilities.objArray;
import static org.apache.drill.test.rowSet.RowSetUtilities.strArray;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.rowSet.ResultSetLoader;
import org.apache.drill.exec.physical.rowSet.RowSetLoader;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.ArrayReader;
import org.apache.drill.exec.vector.accessor.ArrayWriter;
import org.apache.drill.exec.vector.accessor.ScalarElementReader;
import org.apache.drill.exec.vector.accessor.ScalarReader;
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
 * Test map array support in the result set loader.
 * <p>
 * The tests here should be considered in the "extra for experts"
 * category: run and/or debug these tests only after the scalar
 * tests work. Maps, and especially repeated maps, are very complex
 * constructs not to be tackled lightly.
 */

public class TestResultSetLoaderMapArray extends SubOperatorTest {

  @Test
  public void testBasics() {
    TupleMetadata schema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .addMapArray("m")
          .add("c", MinorType.INT)
          .add("d", MinorType.VARCHAR)
          .resumeSchema()
        .buildSchema();
    ResultSetLoaderImpl.ResultSetOptions options = new OptionBuilder()
        .setSchema(schema)
        .build();
    ResultSetLoader rsLoader = new ResultSetLoaderImpl(fixture.allocator(), options);
    RowSetLoader rootWriter = rsLoader.writer();

    // Verify structure and schema

    TupleMetadata actualSchema = rootWriter.schema();
    assertEquals(2, actualSchema.size());
    assertTrue(actualSchema.metadata(1).isArray());
    assertTrue(actualSchema.metadata(1).isMap());
    assertEquals(2, actualSchema.metadata("m").mapSchema().size());
    assertEquals(2, actualSchema.column("m").getChildren().size());

    // Write a couple of rows with arrays.

    rsLoader.startBatch();
    rootWriter
      .addRow(10, objArray(
          objArray(110, "d1.1"),
          objArray(120, "d2.2")))
      .addRow(20, objArray())
      .addRow(30, objArray(
          objArray(310, "d3.1"),
          objArray(320, "d3.2"),
          objArray(330, "d3.3")))
      ;

    // Verify the first batch

    RowSet actual = fixture.wrap(rsLoader.harvest());
    SingleRowSet expected = fixture.rowSetBuilder(schema)
        .addRow(10, objArray(
            objArray(110, "d1.1"),
            objArray(120, "d2.2")))
        .addRow(20, objArray())
        .addRow(30, objArray(
            objArray(310, "d3.1"),
            objArray(320, "d3.2"),
            objArray(330, "d3.3")))
        .build();
    new RowSetComparison(expected).verifyAndClearAll(actual);

    // In the second, create a row, then add a map member.
    // Should be back-filled to empty for the first row.

    rsLoader.startBatch();
    rootWriter
      .addRow(40, objArray(
          objArray(410, "d4.1"),
          objArray(420, "d4.2")));

    TupleWriter mapWriter = rootWriter.array("m").tuple();
    mapWriter.addColumn(SchemaBuilder.columnSchema("e", MinorType.VARCHAR, DataMode.OPTIONAL));

    rootWriter
      .addRow(50, objArray(
          objArray(510, "d5.1", "e5.1"),
          objArray(520, "d5.2", null)))
      .addRow(60, objArray(
          objArray(610, "d6.1", "e6.1"),
          objArray(620, "d6.2", null),
          objArray(630, "d6.3", "e6.3")))
      ;

    // Verify the second batch

    actual = fixture.wrap(rsLoader.harvest());
    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .addMapArray("m")
          .add("c", MinorType.INT)
          .add("d", MinorType.VARCHAR)
          .addNullable("e", MinorType.VARCHAR)
          .resumeSchema()
        .buildSchema();
    expected = fixture.rowSetBuilder(expectedSchema)
        .addRow(40, objArray(
            objArray(410, "d4.1", null),
            objArray(420, "d4.2", null)))
        .addRow(50, objArray(
            objArray(510, "d5.1", "e5.1"),
            objArray(520, "d5.2", null)))
        .addRow(60, objArray(
            objArray(610, "d6.1", "e6.1"),
            objArray(620, "d6.2", null),
            objArray(630, "d6.3", "e6.3")))
        .build();
    new RowSetComparison(expected).verifyAndClearAll(actual);

    rsLoader.close();
  }

  @Test
  public void testNestedArray() {
    TupleMetadata schema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .addMapArray("m")
          .add("c", MinorType.INT)
          .addArray("d", MinorType.VARCHAR)
          .resumeSchema()
        .buildSchema();
    ResultSetLoaderImpl.ResultSetOptions options = new OptionBuilder()
        .setSchema(schema)
        .build();
    ResultSetLoader rsLoader = new ResultSetLoaderImpl(fixture.allocator(), options);
    RowSetLoader rootWriter = rsLoader.writer();

    // Write a couple of rows with arrays within arrays.
    // (And, of course, the Varchar is actually an array of
    // bytes, so that's three array levels.)

    rsLoader.startBatch();
    rootWriter
      .addRow(10, objArray(
          objArray(110, strArray("d1.1.1", "d1.1.2")),
          objArray(120, strArray("d1.2.1", "d1.2.2"))))
      .addRow(20, objArray())
      .addRow(30, objArray(
          objArray(310, strArray("d3.1.1", "d3.2.2")),
          objArray(320, strArray()),
          objArray(330, strArray("d3.3.1", "d1.2.2"))))
      ;

    // Verify the batch

    RowSet actual = fixture.wrap(rsLoader.harvest());
    SingleRowSet expected = fixture.rowSetBuilder(schema)
        .addRow(10, objArray(
            objArray(110, strArray("d1.1.1", "d1.1.2")),
            objArray(120, strArray("d1.2.1", "d1.2.2"))))
        .addRow(20, objArray())
        .addRow(30, objArray(
            objArray(310, strArray("d3.1.1", "d3.2.2")),
            objArray(320, strArray()),
            objArray(330, strArray("d3.3.1", "d1.2.2"))))
        .build();
    new RowSetComparison(expected).verifyAndClearAll(actual);

    rsLoader.close();
  }

  /**
   * Test a doubly-nested arrays of maps.
   */

  @Test
  public void testDoubleNestedArray() {
    TupleMetadata schema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .addMapArray("m1")
          .add("b", MinorType.INT)
          .addMapArray("m2")
            .add("c", MinorType.INT)
            .addArray("d", MinorType.VARCHAR)
            .resumeMap()
          .resumeSchema()
        .buildSchema();
    ResultSetLoaderImpl.ResultSetOptions options = new OptionBuilder()
        .setSchema(schema)
        .build();
    ResultSetLoader rsLoader = new ResultSetLoaderImpl(fixture.allocator(), options);
    RowSetLoader rootWriter = rsLoader.writer();
    rsLoader.startBatch();

    ScalarWriter aWriter = rootWriter.scalar("a");
    ArrayWriter a1Writer = rootWriter.array("m1");
    TupleWriter m1Writer = a1Writer.tuple();
    ScalarWriter bWriter = m1Writer.scalar("b");
    ArrayWriter a2Writer = m1Writer.array("m2");
    TupleWriter m2Writer = a2Writer.tuple();
    ScalarWriter cWriter = m2Writer.scalar("c");
    ScalarWriter dWriter = m2Writer.array("d").scalar();

    for (int i = 0; i < 5; i++) {
      rootWriter.start();
      aWriter.setInt(i);
      for (int j = 0; j < 4; j++) {
        int a1Key = i + 10 + j;
        bWriter.setInt(a1Key);
        for (int k = 0; k < 3; k++) {
          int a2Key = a1Key * 10 + k;
          cWriter.setInt(a2Key);
          for (int l = 0; l < 2; l++) {
            dWriter.setString("d-" + (a2Key * 10 + l));
          }
          a2Writer.save();
        }
        a1Writer.save();
      }
      rootWriter.save();
    }

    RowSet results = fixture.wrap(rsLoader.harvest());
    RowSetReader reader = results.reader();

    ScalarReader aReader = reader.scalar("a");
    ArrayReader a1Reader = reader.array("m1");
    TupleReader m1Reader = a1Reader.tuple();
    ScalarReader bReader = m1Reader.scalar("b");
    ArrayReader a2Reader = m1Reader.array("m2");
    TupleReader m2Reader = a2Reader.tuple();
    ScalarReader cReader = m2Reader.scalar("c");
    ScalarElementReader dReader = m2Reader.elements("d");

    for (int i = 0; i < 5; i++) {
      reader.next();
      assertEquals(i, aReader.getInt());
      for (int j = 0; j < 4; j++) {
        a1Reader.setPosn(j);
        int a1Key = i + 10 + j;
        assertEquals(a1Key, bReader.getInt());
        for (int k = 0; k < 3; k++) {
          a2Reader.setPosn(k);
          int a2Key = a1Key * 10 + k;
          assertEquals(a2Key, cReader.getInt());
          for (int l = 0; l < 2; l++) {
            assertEquals("d-" + (a2Key * 10 + l), dReader.getString(l));
          }
        }
      }
    }
    rsLoader.close();
  }

  /**
   * Version of the {#link TestResultSetLoaderProtocol#testOverwriteRow()} test
   * that uses nested columns inside an array of maps. Here we must call
   * <tt>start()</tt> to reset the array back to the initial start position after
   * each "discard."
   */

  @Test
  public void testOverwriteRow() {
    TupleMetadata schema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .addMapArray("m")
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
    ArrayWriter maWriter = rootWriter.array("m");
    TupleWriter mWriter = maWriter.tuple();
    ScalarWriter bWriter = mWriter.scalar("b");
    ScalarWriter cWriter = mWriter.scalar("c");

    // Write 100,000 rows, overwriting 99% of them. This will cause vector
    // overflow and data corruption if overwrite does not work; but will happily
    // produce the correct result if everything works as it should.

    byte value[] = new byte[512];
    Arrays.fill(value, (byte) 'X');
    int count = 0;
    rsLoader.startBatch();
    while (count < 10_000) {
      rootWriter.start();
      count++;
      aWriter.setInt(count);
      for (int i = 0; i < 10; i++) {
        bWriter.setInt(count * 10 + i);
        cWriter.setBytes(value, value.length);
        maWriter.save();
      }
      if (count % 100 == 0) {
        rootWriter.save();
      }
    }

    // Verify using a reader.

    RowSet result = fixture.wrap(rsLoader.harvest());
    assertEquals(count / 100, result.rowCount());
    RowSetReader reader = result.reader();
    ArrayReader maReader = reader.array("m");
    TupleReader mReader = maReader.tuple();
    int rowId = 1;
    while (reader.next()) {
      assertEquals(rowId * 100, reader.scalar("a").getInt());
      assertEquals(10, maReader.size());
      for (int i = 0; i < 10; i++) {
        maReader.setPosn(i);
        assertEquals(rowId * 1000 + i, mReader.scalar("b").getInt());
        assertTrue(Arrays.equals(value, mReader.scalar("c").getBytes()));
      }
      rowId++;
    }

    result.clear();
    rsLoader.close();
  }

  /**
   * Check that the "fill-empties" logic descends down into
   * a repeated map.
   */

  @Test
  public void testOmittedValues() {
    TupleMetadata schema = new SchemaBuilder()
        .add("id", MinorType.INT)
        .addMapArray("m")
          .addNullable("a", MinorType.INT)
          .addNullable("b", MinorType.VARCHAR)
          .resumeSchema()
        .buildSchema();
    ResultSetLoaderImpl.ResultSetOptions options = new OptionBuilder()
        .setSchema(schema)
        .setRowCountLimit(ValueVector.MAX_ROW_COUNT)
        .build();
    ResultSetLoader rsLoader = new ResultSetLoaderImpl(fixture.allocator(), options);
    RowSetLoader rootWriter = rsLoader.writer();

    int mapSkip = 5;
    int entrySkip = 3;
    int rowCount = 1000;
    int entryCount = 10;

    rsLoader.startBatch();
    ArrayWriter maWriter = rootWriter.array("m");
    TupleWriter mWriter = maWriter.tuple();
    for (int i = 0; i < rowCount; i++) {
      rootWriter.start();
      rootWriter.scalar(0).setInt(i);
      if (i % mapSkip != 0) {
        for (int j = 0; j < entryCount; j++) {
          if (j % entrySkip != 0) {
            mWriter.scalar(0).setInt(i * entryCount + j);
            mWriter.scalar(1).setString("b-" + i + "." + j);
          }
          maWriter.save();
        }
      }
      rootWriter.save();
    }

    RowSet result = fixture.wrap(rsLoader.harvest());
    assertEquals(rowCount, result.rowCount());
    RowSetReader reader = result.reader();
    ArrayReader maReader = reader.array("m");
    TupleReader mReader = maReader.tuple();
    for (int i = 0; i < rowCount; i++) {
      assertTrue(reader.next());
      assertEquals(i, reader.scalar(0).getInt());
      if (i % mapSkip == 0) {
        assertEquals(0, maReader.size());
        continue;
      }
      assertEquals(entryCount, maReader.size());
      for (int j = 0; j < entryCount; j++) {
        maReader.setPosn(j);
        if (j % entrySkip == 0) {
          assertTrue(mReader.scalar(0).isNull());
          assertTrue(mReader.scalar(1).isNull());
        } else {
          assertFalse(mReader.scalar(0).isNull());
          assertFalse(mReader.scalar(1).isNull());
          assertEquals(i * entryCount + j, mReader.scalar(0).getInt());
          assertEquals("b-" + i + "." + j, mReader.scalar(1).getString());
        }
      }
    }
    result.clear();
    rsLoader.close();
  }

  /**
   * Test that memory is released if the loader is closed with an active
   * batch (that is, before the batch is harvested.)
   */

  @Test
  public void testCloseWithoutHarvest() {
    TupleMetadata schema = new SchemaBuilder()
        .addMapArray("m")
          .add("a", MinorType.INT)
          .add("b", MinorType.VARCHAR)
          .resumeSchema()
        .buildSchema();
    ResultSetLoaderImpl.ResultSetOptions options = new OptionBuilder()
        .setSchema(schema)
        .setRowCountLimit(ValueVector.MAX_ROW_COUNT)
        .build();
    ResultSetLoader rsLoader = new ResultSetLoaderImpl(fixture.allocator(), options);
    RowSetLoader rootWriter = rsLoader.writer();

    ArrayWriter maWriter = rootWriter.array("m");
    TupleWriter mWriter = maWriter.tuple();
    rsLoader.startBatch();
    for (int i = 0; i < 40; i++) {
      rootWriter.start();
      for (int j = 0; j < 3; j++) {
        mWriter.scalar("a").setInt(i);
        mWriter.scalar("b").setString("b-" + i);
        maWriter.save();
      }
      rootWriter.save();
    }

    // Don't harvest the batch. Allocator will complain if the
    // loader does not release memory.

    rsLoader.close();
  }
}
