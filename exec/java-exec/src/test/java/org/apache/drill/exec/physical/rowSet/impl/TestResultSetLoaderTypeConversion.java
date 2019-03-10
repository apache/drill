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
import static org.apache.drill.test.rowSet.RowSetUtilities.strArray;
import org.apache.drill.categories.RowSetTests;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.rowSet.ResultSetLoader;
import org.apache.drill.exec.physical.rowSet.RowSetLoader;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.RowSet;
import org.apache.drill.test.rowSet.RowSetUtilities;
import org.apache.drill.test.rowSet.RowSet.SingleRowSet;
import org.apache.drill.test.rowSet.test.TestColumnConverter;
import org.apache.drill.test.rowSet.test.TestColumnConverter.ConverterFactory;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(RowSetTests.class)
public class TestResultSetLoaderTypeConversion extends SubOperatorTest {

  /**
   * Test the use of a column type converter in the result set loader for
   * required, nullable and repeated columns.
   * <p>
   * This tests the simple case: keeping the same column type, just
   * inserting a conversion "shim" on top.
   */

  @Test
  public void testConversionShim() {
    TupleMetadata schema = new SchemaBuilder()
        .add("n1", MinorType.INT)
        .addNullable("n2", MinorType.INT)
        .addArray("n3", MinorType.INT)
        .buildSchema();

    // Add a type converter. Passed in as a factory
    // since we must create a new one for each row set writer.

    TestColumnConverter.setConverterProp(schema.metadata("n1"),
        TestColumnConverter.CONVERT_TO_INT);
    TestColumnConverter.setConverterProp(schema.metadata("n2"),
        TestColumnConverter.CONVERT_TO_INT);
    TestColumnConverter.setConverterProp(schema.metadata("n3"),
        TestColumnConverter.CONVERT_TO_INT);

    SchemaTransformer schemaTransform = new DefaultSchemaTransformer(new ConverterFactory());
    ResultSetLoaderImpl.ResultSetOptions options = new OptionBuilder()
        .setSchema(schema)
        .setRowCountLimit(ValueVector.MAX_ROW_COUNT)
        .setSchemaTransform(schemaTransform)
        .build();
    ResultSetLoader rsLoader = new ResultSetLoaderImpl(fixture.allocator(), options);
    rsLoader.startBatch();

    // Write data as both a string as an integer

    RowSetLoader rootWriter = rsLoader.writer();
    rootWriter.addRow("123", "12", strArray("123", "124"));
    rootWriter.addRow(234, 23, intArray(234, 235));
    RowSet actual = fixture.wrap(rsLoader.harvest());

    // Build the expected vector without a type converter.

    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("n1", MinorType.INT)
        .addNullable("n2", MinorType.INT)
        .addArray("n3", MinorType.INT)
        .buildSchema();
    final SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addRow(123, 12, intArray(123, 124))
        .addRow(234, 23, intArray(234, 235))
        .build();

    // Compare

    RowSetUtilities.verify(expected, actual);
  }

  /**
   * Test full-blown type conversion using the standard Drill properties.
   */

  @Test
  public void testTypeConversion() {
    TupleMetadata outputSchema = new SchemaBuilder()
        .add("n1", MinorType.INT)
        .addNullable("n2", MinorType.INT)
        .addArray("n3", MinorType.INT)
        .buildSchema();

    TupleMetadata inputSchema = new SchemaBuilder()
        .add("n1", MinorType.VARCHAR)
        .addNullable("n2", MinorType.VARCHAR)
        .addArray("n3", MinorType.VARCHAR)
        .buildSchema();

    ResultSetLoaderImpl.ResultSetOptions options = new OptionBuilder()
        .setSchema(inputSchema)
        .setRowCountLimit(ValueVector.MAX_ROW_COUNT)
        .setSchemaTransform(new SchemaTransformerImpl(outputSchema))
        .build();
    ResultSetLoader rsLoader = new ResultSetLoaderImpl(fixture.allocator(), options);
    rsLoader.startBatch();

    // Write data as both a string as an integer

    RowSetLoader rootWriter = rsLoader.writer();
    rootWriter.addRow("123", "12", strArray("123", "124"));
    rootWriter.addRow(234, 23, intArray(234, 235));
    RowSet actual = fixture.wrap(rsLoader.harvest());

    // Build the expected vector without a type converter.

    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("n1", MinorType.INT)
        .addNullable("n2", MinorType.INT)
        .addArray("n3", MinorType.INT)
        .buildSchema();
    final SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addRow(123, 12, intArray(123, 124))
        .addRow(234, 23, intArray(234, 235))
        .build();

    // Compare

    RowSetUtilities.verify(expected, actual);
  }
}
