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

import static org.apache.drill.test.rowSet.RowSetUtilities.strArray;
import static org.apache.drill.test.rowSet.RowSetUtilities.intArray;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.vector.accessor.ColumnConversionFactory;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.apache.drill.exec.vector.accessor.writer.AbstractWriteConvertor;
import org.apache.drill.exec.vector.accessor.writer.ConcreteWriter;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.RowSet;
import org.apache.drill.test.rowSet.RowSetBuilder;
import org.apache.drill.test.rowSet.RowSetUtilities;
import org.apache.drill.test.rowSet.RowSet.SingleRowSet;
import org.junit.Test;

/**
 * Tests the column type convertor feature of the column metadata
 * and of the RowSetWriter.
 */

public class TestColumnConvertor extends SubOperatorTest {

  /**
   * Simple type converter that allows string-to-int conversions.
   * Inherits usual int value support from the base writer.
   */
  public static class TestConvertor extends AbstractWriteConvertor {

    public TestConvertor(ScalarWriter baseWriter) {
      super(baseWriter);
    }

    @Override
    public void setString(String value) {
      setInt(Integer.parseInt(value));
    }

    public static ColumnConversionFactory factory() {
      return new ColumnConversionFactory() {
        @Override
        public ConcreteWriter newWriter(ColumnMetadata colDefn,
            ConcreteWriter baseWriter) {
           return new TestConvertor(baseWriter);
        }
      };
    }
  }

  @Test
  public void testScalarConvertor() {

    // Create the schema

    TupleMetadata schema = new SchemaBuilder()
        .add("n1", MinorType.INT)
        .addNullable("n2", MinorType.INT)
        .buildSchema();

    // Add a type convertor. Passed in as a factory
    // since we must create a new one for each row set writer.

    schema.metadata("n1").setTypeConverter(TestConvertor.factory());
    schema.metadata("n2").setTypeConverter(TestConvertor.factory());

    // Write data as both a string as an integer

    RowSet actual = new RowSetBuilder(fixture.allocator(), schema)
        .addRow("123", "12")
        .addRow(234, 23)
        .build();

    // Build the expected vector without a type convertor.

    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("n1", MinorType.INT)
        .addNullable("n2", MinorType.INT)
        .buildSchema();
    final SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addRow(123, 12)
        .addRow(234, 23)
        .build();

    // Compare

    RowSetUtilities.verify(expected, actual);
  }

  @Test
  public void testArrayConvertor() {

    // Create the schema

    TupleMetadata schema = new SchemaBuilder()
        .addArray("n", MinorType.INT)
        .buildSchema();

    // Add a type convertor. Passed in as a factory
    // since we must create a new one for each row set writer.

    schema.metadata("n").setTypeConverter(TestConvertor.factory());

    // Write data as both a string as an integer

    RowSet actual = new RowSetBuilder(fixture.allocator(), schema)
        .addSingleCol(strArray("123", "124"))
        .addSingleCol(intArray(234, 235))
        .build();

    // Build the expected vector without a type convertor.

    TupleMetadata expectedSchema = new SchemaBuilder()
        .addArray("n", MinorType.INT)
        .buildSchema();
    final SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addSingleCol(intArray(123, 124))
        .addSingleCol(intArray(234, 235))
        .build();

    // Compare

    RowSetUtilities.verify(expected, actual);
  }
}
