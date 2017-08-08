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
package org.apache.drill.exec.physical.rowSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.rowSet.impl.ResultSetLoaderImpl;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.RowSet;
import org.apache.drill.test.rowSet.RowSetReader;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.junit.Test;

public class TestOfTestDataConverter extends SubOperatorTest {

  public void runRequiredTest(MinorType type, int maxValue) {
    ResultSetLoader loader = new ResultSetLoaderImpl(fixture.allocator());

    BatchSchema schema = new SchemaBuilder()
        .add("a", type)
        .build();
    loader.writer().schema().setSchema(schema);

    loader.startBatch();
    TestDataConverter converter = new TestDataConverter(loader.writer());
    int count = 10;
    for (int i = 0; i < count; i++) {
      int value = i % maxValue;
      loader.startRow();
      converter.column(0).setInt(value);
      loader.saveRow();
    }

    RowSet results = fixture.wrap(loader.harvest());
    RowSetReader reader = results.reader();
    for (int i = 0; i < count; i++) {
      int value = i % maxValue;
      assertTrue(reader.next());
      assertEquals(converter.column(0).toObject(value), reader.column(0).getObject());
    }
    assertFalse(reader.next());
    results.clear();
    loader.close();
  }

  public void runOptionalTest(MinorType type, int maxValue) {
    ResultSetLoader loader = new ResultSetLoaderImpl(fixture.allocator());

    BatchSchema schema = new SchemaBuilder()
        .addNullable("a", type)
        .build();
    TupleLoader writer = loader.writer();
    writer.schema().setSchema(schema);

    loader.startBatch();
    TestDataConverter converter = new TestDataConverter(writer);
    int count = 10;
    for (int i = 0; i < count; i++) {
      int value = i % maxValue;
      loader.startRow();
      if (i % 3 == 0) {
        writer.column(0).setNull();
      } else {
        converter.column(0).setInt(value);
      }
      loader.saveRow();
    }

    RowSet results = fixture.wrap(loader.harvest());
    RowSetReader reader = results.reader();
    for (int i = 0; i < count; i++) {
      int value = i % maxValue;
      assertTrue(reader.next());
      if (i % 3 == 0) {
        assertTrue(reader.column(0).isNull());
      } else {
        assertFalse(reader.column(0).isNull());
        assertEquals(converter.column(0).toObject(value), reader.column(0).getObject());
      }
    }
    assertFalse(reader.next());
    results.clear();
    loader.close();
  }

  public void runTest(MinorType type, int maxValue) {
    runRequiredTest(type, maxValue);
    runOptionalTest(type, maxValue);
  }

  public void runTest(MinorType type) {
    runTest(type, Integer.MAX_VALUE);
  }

  @Test
  public void testTypes() {
    runTest(MinorType.BIT, 1);
    runTest(MinorType.TINYINT, Byte.MAX_VALUE);
    runTest(MinorType.SMALLINT);
    runTest(MinorType.INT);
    runTest(MinorType.BIGINT);
    runTest(MinorType.UINT1);
    runTest(MinorType.UINT2);
    runTest(MinorType.UINT4);
    runTest(MinorType.UINT8);
    runTest(MinorType.FLOAT4);
    runTest(MinorType.FLOAT8);
    runTest(MinorType.DECIMAL9);
    runTest(MinorType.DECIMAL18);
    //runTest(MinorType.DECIMAL28SPARSE); // Not supported in writer yet
    //runTest(MinorType.DECIMAL38SPARSE); // Not supported in writer yet
    //runTest(MinorType.DECIMAL28DENSE); // Not supported in writer yet
    //runTest(MinorType.DECIMAL38DENSE); // Not supported in writer yet
    //runTest(MinorType.MONEY); // Not supported in Drill
    //runTest(MinorType.DATE); // Not supported in writer yet
    //runTest(MinorType.TIME); // Not supported in writer yet
    //runTest(MinorType.TIMETZ); // Not supported in Drill
    //runTest(MinorType.TIMESTAMPTZ); // Not supported in Drill
    //runTest(MinorType.TIMESTAMP); // Not supported in writer yet
    runTest(MinorType.INTERVAL);
    runTest(MinorType.INTERVALYEAR);
    runTest(MinorType.INTERVALDAY);
    runTest(MinorType.VARCHAR);
    // runTest(MinorType.VAR16CHAR); // Some kind of problem
    //runTest(MinorType.VARBINARY); // Tries to compare array identity?
  }

  // TODO: Dates
  // TODO: VAR16Char
  // TODO: VARBINARY
  // TODO: Repeated types
  // TODO: sparse/dense decimal

}
