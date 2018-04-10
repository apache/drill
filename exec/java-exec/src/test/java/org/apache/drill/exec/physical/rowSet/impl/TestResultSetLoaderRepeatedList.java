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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.physical.rowSet.ResultSetLoader;
import org.apache.drill.exec.physical.rowSet.RowSetLoader;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.ColumnMetadata.StructureType;
import org.apache.drill.exec.record.metadata.RepeatedListColumnMetadata;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.ArrayReader;
import org.apache.drill.exec.vector.accessor.ArrayWriter;
import org.apache.drill.exec.vector.accessor.ObjectType;
import org.apache.drill.exec.vector.accessor.ObjectWriter;
import org.apache.drill.exec.vector.accessor.ScalarReader;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.apache.drill.exec.vector.accessor.ValueType;
import org.apache.drill.exec.vector.accessor.writer.RepeatedListWriter;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.RowSet;
import org.apache.drill.test.rowSet.RowSetReader;
import org.apache.drill.test.rowSet.RowSetUtilities;
import org.apache.drill.test.rowSet.schema.SchemaBuilder;
import org.junit.Test;

import com.google.common.base.Charsets;

/**
 * Tests repeated list support. Repeated lists add another layer of dimensionality
 * on top of other repeated types. Since a repeated list can wrap another repeated
 * list, the repeated list allows creating 2D, 3D or higher dimensional arrays (lists,
 * actually, since the different "slices" need not have the same length...)
 * Repeated lists appear to be used only by JSON.
 */

public class TestResultSetLoaderRepeatedList extends SubOperatorTest {

  @Test
  public void test2DEarlySchema() {
    TupleMetadata schema = new SchemaBuilder()
        .add("id", MinorType.INT)
        .addRepeatedList("list2")
          .addArray(MinorType.VARCHAR)
          .resumeSchema()
        .buildSchema();

    ResultSetLoaderImpl.ResultSetOptions options = new OptionBuilder()
        .setSchema(schema)
        .build();
    ResultSetLoader rsLoader = new ResultSetLoaderImpl(fixture.allocator(), options);

    do2DTest(schema, rsLoader);
    rsLoader.close();
  }

  private void do2DTest(TupleMetadata schema, ResultSetLoader rsLoader) {
    RowSetLoader writer = rsLoader.writer();

    // Sanity check of writer structure

    assertEquals(2, writer.size());
    ObjectWriter listObj = writer.column("list2");
    assertEquals(ObjectType.ARRAY, listObj.type());
    ArrayWriter listWriter = listObj.array();
    assertEquals(ObjectType.ARRAY, listWriter.entryType());
    ArrayWriter innerWriter = listWriter.array();
    assertEquals(ObjectType.SCALAR, innerWriter.entryType());
    ScalarWriter strWriter = innerWriter.scalar();
    assertEquals(ValueType.STRING, strWriter.valueType());

    // Sanity test of schema

    TupleMetadata rowSchema = writer.tupleSchema();
    assertEquals(2, rowSchema.size());
    ColumnMetadata listSchema = rowSchema.metadata(1);
    assertEquals(MinorType.LIST, listSchema.type());
    assertEquals(DataMode.REPEATED, listSchema.mode());
    assertTrue(listSchema instanceof RepeatedListColumnMetadata);
    assertEquals(StructureType.MULTI_ARRAY, listSchema.structureType());
    assertNotNull(listSchema.childSchema());

    ColumnMetadata elementSchema = listSchema.childSchema();
    assertEquals(listSchema.name(), elementSchema.name());
    assertEquals(MinorType.VARCHAR, elementSchema.type());
    assertEquals(DataMode.REPEATED, elementSchema.mode());

    // Write values

    rsLoader.startBatch();
    writer
        .addRow(1, objArray(strArray("a", "b"), strArray("c", "d")))
        .addRow(2, objArray(strArray("e"), strArray(), strArray("f", "g", "h")))
        .addRow(3, objArray())
        .addRow(4, objArray(strArray(), strArray("i"), strArray()));

    // Verify the values.
    // (Relies on the row set level repeated list tests having passed.)

    RowSet expected = fixture.rowSetBuilder(schema)
        .addRow(1, objArray(strArray("a", "b"), strArray("c", "d")))
        .addRow(2, objArray(strArray("e"), strArray(), strArray("f", "g", "h")))
        .addRow(3, objArray())
        .addRow(4, objArray(strArray(), strArray("i"), strArray()))
        .build();

    RowSetUtilities.verify(expected, fixture.wrap(rsLoader.harvest()));
  }

  @Test
  public void test2DLateSchema() {
    TupleMetadata schema = new SchemaBuilder()
        .add("id", MinorType.INT)
        .addRepeatedList("list2")
          .addArray(MinorType.VARCHAR)
          .resumeSchema()
        .buildSchema();

    ResultSetLoaderImpl.ResultSetOptions options = new OptionBuilder()
        .build();
    ResultSetLoader rsLoader = new ResultSetLoaderImpl(fixture.allocator(), options);
    RowSetLoader writer = rsLoader.writer();

    // Add columns dynamically

    writer.addColumn(schema.metadata(0));
    writer.addColumn(schema.metadata(1).cloneEmpty());

    // Yes, this is ugly. The whole repeated array idea is awkward.
    // The only place it is used at present is in JSON where the
    // awkwardness is mixed in with a logs of JSON complexity.
    // Consider improving this API in the future.

    ((RepeatedListWriter) writer.array(1)).defineElement(schema.metadata(1).childSchema());

    do2DTest(schema, rsLoader);
    rsLoader.close();
  }

  @Test
  public void test2DLateSchemaIncremental() {
    TupleMetadata schema = new SchemaBuilder()
        .add("id", MinorType.INT)
        .addRepeatedList("list2")
          .addArray(MinorType.VARCHAR)
          .resumeSchema()
        .buildSchema();

    ResultSetLoaderImpl.ResultSetOptions options = new OptionBuilder()
        .build();
    ResultSetLoader rsLoader = new ResultSetLoaderImpl(fixture.allocator(), options);
    RowSetLoader writer = rsLoader.writer();

    // Add columns dynamically

    writer.addColumn(schema.metadata(0));

    // Write a row without the array.

    rsLoader.startBatch();
    writer.addRow(1);

    // Add the repeated list, but without contents.

    writer.addColumn(schema.metadata(1).cloneEmpty());

    // Sanity check of writer structure

    assertEquals(2, writer.size());
    ObjectWriter listObj = writer.column("list2");
    assertEquals(ObjectType.ARRAY, listObj.type());
    ArrayWriter listWriter = listObj.array();

    // No child defined yet. A dummy child is inserted instead.

    assertEquals(MinorType.NULL, listWriter.entry().schema().type());
    assertEquals(ObjectType.ARRAY, listWriter.entryType());
    assertEquals(ObjectType.SCALAR, listWriter.array().entryType());
    assertEquals(ValueType.NULL, listWriter.array().scalar().valueType());

    // Although we don't know the type of the inner, we can still
    // create null (empty) elements in the outer array.

    writer
      .addRow(2, null)
      .addRow(3, objArray())
      .addRow(4, objArray(objArray(), null));

    // Define the inner type.

    RepeatedListWriter listWriterImpl = (RepeatedListWriter) listWriter;
    listWriterImpl.defineElement(MaterializedField.create("list2", Types.repeated(MinorType.VARCHAR)));

    // Sanity check of completed structure

    assertEquals(ObjectType.ARRAY, listWriter.entryType());
    ArrayWriter innerWriter = listWriter.array();
    assertEquals(ObjectType.SCALAR, innerWriter.entryType());
    ScalarWriter strWriter = innerWriter.scalar();
    assertEquals(ValueType.STRING, strWriter.valueType());

    // Write values

    writer
        .addRow(5, objArray(strArray("a", "b"), strArray("c", "d")));

    // Verify the values.
    // (Relies on the row set level repeated list tests having passed.)

    RowSet expected = fixture.rowSetBuilder(schema)
        .addRow(1, objArray())
        .addRow(2, objArray())
        .addRow(3, objArray())
        .addRow(4, objArray(objArray(), null))
        .addRow(5, objArray(strArray("a", "b"), strArray("c", "d")))
        .build();

    RowSetUtilities.verify(expected, fixture.wrap(rsLoader.harvest()));
    rsLoader.close();
  }

  @Test
  public void test2DOverflow() {
    TupleMetadata schema = new SchemaBuilder()
        .add("id", MinorType.INT)
        .addRepeatedList("list2")
          .addArray(MinorType.VARCHAR)
          .resumeSchema()
        .buildSchema();

    ResultSetLoaderImpl.ResultSetOptions options = new OptionBuilder()
        .setRowCountLimit(ValueVector.MAX_ROW_COUNT)
        .setSchema(schema)
        .build();
    ResultSetLoader rsLoader = new ResultSetLoaderImpl(fixture.allocator(), options);
    RowSetLoader writer = rsLoader.writer();

    // Fill the batch with enough data to cause overflow.
    // Data must be large enough to cause overflow before 64K rows
    // Make a bit bigger to overflow early.

    final int outerSize = 7;
    final int innerSize = 5;
    final int strLength = ValueVector.MAX_BUFFER_SIZE / ValueVector.MAX_ROW_COUNT / outerSize / innerSize + 20;
    byte value[] = new byte[strLength - 6];
    Arrays.fill(value, (byte) 'X');
    String strValue = new String(value, Charsets.UTF_8);
    int rowCount = 0;
    int elementCount = 0;

    ArrayWriter outerWriter = writer.array(1);
    ArrayWriter innerWriter = outerWriter.array();
    ScalarWriter elementWriter = innerWriter.scalar();
    rsLoader.startBatch();
    while (! writer.isFull()) {
      writer.start();
      writer.scalar(0).setInt(rowCount);
      for (int j = 0; j < outerSize; j++) {
        for (int k = 0; k < innerSize; k++) {
          elementWriter.setString(String.format("%s%06d", strValue, elementCount));
          elementCount++;
        }
        outerWriter.save();
      }
      writer.save();
      rowCount++;
    }

    // Number of rows should be driven by vector size.
    // Our row count should include the overflow row

    int expectedCount = ValueVector.MAX_BUFFER_SIZE / (strLength * innerSize * outerSize);
    assertEquals(expectedCount + 1, rowCount);

    // Loader's row count should include only "visible" rows

    assertEquals(expectedCount, writer.rowCount());

    // Total count should include invisible and look-ahead rows.

    assertEquals(expectedCount + 1, rsLoader.totalRowCount());

    // Result should exclude the overflow row

    RowSet result = fixture.wrap(rsLoader.harvest());
    assertEquals(expectedCount, result.rowCount());

    // Verify the data.

    RowSetReader reader = result.reader();
    ArrayReader outerReader = reader.array(1);
    ArrayReader innerReader = outerReader.array();
    ScalarReader strReader = innerReader.scalar();
    int readRowCount = 0;
    int readElementCount = 0;
    while (reader.next()) {
      assertEquals(readRowCount, reader.scalar(0).getInt());
      for (int i = 0; i < outerSize; i++) {
        assertTrue(outerReader.next());
        for (int j = 0; j < innerSize; j++) {
          assertTrue(innerReader.next());
          assertEquals(String.format("%s%06d", strValue, readElementCount),
              strReader.getString());
          readElementCount++;
        }
        assertFalse(innerReader.next());
      }
      assertFalse(outerReader.next());
      readRowCount++;
    }
    assertEquals(readRowCount, result.rowCount());
    result.clear();

    // Write a few more rows to verify the overflow row.

    rsLoader.startBatch();
    for (int i = 0; i < 1000; i++) {
      writer.start();
      writer.scalar(0).setInt(rowCount);
      for (int j = 0; j < outerSize; j++) {
        for (int k = 0; k < innerSize; k++) {
          elementWriter.setString(String.format("%s%06d", strValue, elementCount));
          elementCount++;
        }
        outerWriter.save();
      }
      writer.save();
      rowCount++;
    }

    result = fixture.wrap(rsLoader.harvest());
    assertEquals(1001, result.rowCount());

    int startCount = readRowCount;
    reader = result.reader();
    outerReader = reader.array(1);
    innerReader = outerReader.array();
    strReader = innerReader.scalar();
    while (reader.next()) {
      assertEquals(readRowCount, reader.scalar(0).getInt());
      for (int i = 0; i < outerSize; i++) {
        assertTrue(outerReader.next());
        for (int j = 0; j < innerSize; j++) {
          assertTrue(innerReader.next());
          elementWriter.setString(String.format("%s%06d", strValue, readElementCount));
          assertEquals(String.format("%s%06d", strValue, readElementCount),
              strReader.getString());
          readElementCount++;
        }
        assertFalse(innerReader.next());
      }
      assertFalse(outerReader.next());
      readRowCount++;
    }
    assertEquals(readRowCount - startCount, result.rowCount());
    result.clear();
    rsLoader.close();
  }

  // TODO: Test union list as inner
  // TODO: Test repeated map as inner
}
