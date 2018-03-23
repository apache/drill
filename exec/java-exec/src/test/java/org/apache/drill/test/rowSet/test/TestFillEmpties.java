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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.vector.accessor.ScalarElementReader;
import org.apache.drill.exec.vector.accessor.ScalarReader;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.apache.drill.exec.vector.accessor.ValueType;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.RowSet.ExtendableRowSet;
import org.apache.drill.test.rowSet.RowSet.SingleRowSet;
import org.apache.drill.test.rowSet.schema.SchemaBuilder;
import org.apache.drill.test.rowSet.RowSetReader;
import org.apache.drill.test.rowSet.RowSetUtilities;
import org.apache.drill.test.rowSet.RowSetWriter;
import org.junit.Test;

/**
 * Test the "fill empties" logic for all types for all modes.
 * This test exploits the dynamic typing ability of the
 * accessors. Creating an object per value is too slow for
 * production code, but very handy for tests such as this.
 * <p>
 * Note that this test also has the handy side-effect of testing
 * null handling in the accessor classes.
 */

public class TestFillEmpties extends SubOperatorTest {

  public static final int ROW_COUNT = 1000;

  /**
   * Test "fill empties" for required types. Here, the fill value
   * is more of a convention: 0 (fixed-width) or an empty
   * entry (variable width.) Some fill value is required to avoid
   * the alternatives which are either 1) leave the value as
   * garbage, or 2) raise an exception about the missing value.
   */

  @Test
  public void testFillEmptiesRequired() {
    testFillEmpties(DataMode.REQUIRED);
  }

  /**
   * Test "fill empties" for nullable types which are the most
   * "natural" type for omitted values.
   * Nullable vectors fill empties with nulls.
   */

  @Test
  public void testFillEmptiesNullable() {
    testFillEmpties(DataMode.OPTIONAL);
  }

  /**
   * Test "fill empties" for repeated types.
   * Drill defines a null (omitted) array as the same thing as
   * a zero-length array.
   */

  @Test
  public void testFillEmptiesRepeated() {
    testFillEmpties(DataMode.REPEATED);
  }

  private void testFillEmpties(DataMode mode) {
    for (MinorType type : MinorType.values()) {
      switch (type) {
      case DECIMAL28DENSE:
      case DECIMAL38DENSE:
        // Not yet supported
        break;
      case GENERIC_OBJECT:
      case LATE:
      case LIST:
      case MAP:
      case NULL:
      case UNION:
        // Writer N/A
        break;
      case BIT:
      case FIXEDBINARY:
      case FIXEDCHAR:
      case FIXED16CHAR:
      case MONEY:
      case TIMESTAMPTZ:
      case TIMETZ:
        // Not supported in Drill
        break;
      case DECIMAL18:
      case DECIMAL28SPARSE:
      case DECIMAL9:
      case DECIMAL38SPARSE:
        doFillEmptiesTest(type, mode, 9, 2);
        break;
      default:
        doFillEmptiesTest(type, mode);
      }
    }
  }

  private void doFillEmptiesTest(MinorType type, DataMode mode, int prec, int scale) {
    MajorType majorType = MajorType.newBuilder()
        .setMinorType(type)
        .setMode(mode)
        .setPrecision(prec)
        .setScale(scale)
        .build();
    doFillEmptiesTest(majorType);
  }

  private void doFillEmptiesTest(MinorType type, DataMode mode) {
    MajorType majorType = MajorType.newBuilder()
        .setMinorType(type)
        .setMode(mode)
        .build();
    doFillEmptiesTest(majorType);
  }

  private void doFillEmptiesTest(MajorType majorType) {
    if (majorType.getMode() == DataMode.REPEATED) {
      dofillEmptiesRepeated(majorType);
    } else {
      doFillEmptiesScalar(majorType);
    }
  }

  private void doFillEmptiesScalar(MajorType majorType) {
    TupleMetadata schema = new SchemaBuilder()
        .add("a", majorType)
        .buildSchema();
    ExtendableRowSet rs = fixture.rowSet(schema);
    RowSetWriter writer = rs.writer();
    ScalarWriter colWriter = writer.scalar(0);
    ValueType valueType = colWriter.valueType();
    boolean nullable = majorType.getMode() == DataMode.OPTIONAL;
    for (int i = 0; i < ROW_COUNT; i++) {
      if (i % 5 == 0) {
        colWriter.setObject(RowSetUtilities.testDataFromInt(valueType, majorType, i));
      }
      writer.save();
    }
    SingleRowSet result = writer.done();
    RowSetReader reader = result.reader();
    ScalarReader colReader = reader.scalar(0);
    MinorType type = majorType.getMinorType();
    boolean isVariable = (type == MinorType.VARCHAR ||
                          type == MinorType.VAR16CHAR ||
                          type == MinorType.VARBINARY);
    for (int i = 0; i < ROW_COUNT; i++) {
      assertTrue(reader.next());
      if (i % 5 != 0) {
        if (nullable) {
          // Nullable types fill with nulls.

          assertTrue(colReader.isNull());
          continue;
        }
        if (isVariable) {
          // Variable width types fill with a zero-length value.

          assertEquals(0, colReader.getBytes().length);
          continue;
        }
      }

      // All other types fill with zero-bytes, interpreted as some form
      // of zero for each type.

      Object actual = colReader.getObject();
      Object expected = RowSetUtilities.testDataFromInt(valueType, majorType,
          i % 5 == 0 ? i : 0);
      RowSetUtilities.assertEqualValues(
          majorType.toString().replace('\n', ' ') + "[" + i + "]",
          valueType, expected, actual);
    }
    result.clear();
  }

  private void dofillEmptiesRepeated(MajorType majorType) {
    TupleMetadata schema = new SchemaBuilder()
        .add("a", majorType)
        .buildSchema();
    ExtendableRowSet rs = fixture.rowSet(schema);
    RowSetWriter writer = rs.writer();
    ScalarWriter colWriter = writer.array(0).scalar();
    ValueType valueType = colWriter.valueType();
    for (int i = 0; i < ROW_COUNT; i++) {
      if (i % 5 == 0) {
        // Write two values so we can exercise a bit of the array logic.

        colWriter.setObject(RowSetUtilities.testDataFromInt(valueType, majorType, i));
        colWriter.setObject(RowSetUtilities.testDataFromInt(valueType, majorType, i+1));
      }
      writer.save();
    }
    SingleRowSet result = writer.done();
    RowSetReader reader = result.reader();
    ScalarElementReader colReader = reader.array(0).elements();
    for (int i = 0; i < ROW_COUNT; i++) {
      assertTrue(reader.next());
      if (i % 5 != 0) {
        // Empty arrays are defined to be the same as a zero-length array.

        assertEquals(0, colReader.size());
      } else {
        for (int j = 0; j < 2; j++) {
          Object actual = colReader.getObject(j);
          Object expected = RowSetUtilities.testDataFromInt(valueType, majorType, i + j);
          RowSetUtilities.assertEqualValues(
              majorType.toString().replace('\n', ' ') + "[" + i + "][" + j + "]",
              valueType, expected, actual);
        }
      }
    }
    result.clear();
  }
}
