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
package org.apache.drill.exec.cache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.test.DrillTest;
import org.apache.drill.test.OperatorFixture;
import org.apache.drill.test.rowSet.RowSet;
import org.apache.drill.test.rowSet.RowSet.ExtendableRowSet;
import org.apache.drill.test.rowSet.RowSet.RowSetWriter;
import org.apache.drill.test.rowSet.RowSet.SingleRowSet;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.apache.drill.test.rowSet.RowSetUtilities;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestBatchSerialization extends DrillTest {

  public static OperatorFixture fixture;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    fixture = OperatorFixture.builder().build();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    fixture.close();
  }

  public SingleRowSet makeRowSet(BatchSchema schema, int rowCount) {
    ExtendableRowSet rowSet = fixture.rowSet(schema);
    RowSetWriter writer = rowSet.writer(rowCount);
    for (int i = 0; i < rowCount; i++) {
      RowSetUtilities.setFromInt(writer, 0, i);
      writer.save();
    }
    writer.done();
    return rowSet;
  }

  public SingleRowSet makeNullableRowSet(BatchSchema schema, int rowCount) {
    ExtendableRowSet rowSet = fixture.rowSet(schema);
    RowSetWriter writer = rowSet.writer(rowCount);
    for (int i = 0; i < rowCount; i++) {
      if (i % 2 == 0) {
        RowSetUtilities.setFromInt(writer, 0, i);
      } else {
        writer.column(0).setNull();
      }
      writer.save();
    }
    writer.done();
    return rowSet;
  }

  public void testType(MinorType type) throws IOException {
    testNonNullType(type);
    testNullableType(type);
  }

  public void testNonNullType(MinorType type) throws IOException {
    BatchSchema schema = new SchemaBuilder( )
        .add("col", type)
        .build();
    int rowCount = 20;
    verifySerialize(makeRowSet(schema, rowCount),
                    makeRowSet(schema, rowCount));
  }

  public void testNullableType(MinorType type) throws IOException {
    BatchSchema schema = new SchemaBuilder( )
        .addNullable("col", type)
        .build();
    int rowCount = 20;
    verifySerialize(makeNullableRowSet(schema, rowCount),
                    makeNullableRowSet(schema, rowCount));
  }

  /**
   * Verify serialize and deserialize. Need to pass both the
   * input and expected (even though the expected should be the same
   * data as the input) because the act of serializing clears the
   * input for obscure historical reasons.
   *
   * @param rowSet
   * @param expected
   * @throws IOException
   */
  private void verifySerialize(SingleRowSet rowSet, SingleRowSet expected) throws IOException {

    long origSize = rowSet.size();

    File dir = OperatorFixture.getTempDir("serial");
    File outFile = new File(dir, "serialze.dat");
    try (OutputStream out = new BufferedOutputStream(new FileOutputStream(outFile))) {
      VectorSerializer.writer(fixture.allocator(), out)
        .write(rowSet.container(), rowSet.getSv2());
    }

    RowSet result;
    try (InputStream in = new BufferedInputStream(new FileInputStream(outFile))) {
      result = fixture.wrap(
        VectorSerializer.reader(fixture.allocator(), in)
          .read());
    }

    assertTrue(origSize >= result.size());
    new RowSetComparison(expected)
      .verifyAndClearAll(result);
    outFile.delete();
  }

  @Test
  public void testTypes() throws IOException {
    testType(MinorType.TINYINT);
    testType(MinorType.UINT1);
    testType(MinorType.SMALLINT);
    testType(MinorType.UINT2);
    testType(MinorType.INT);
    testType(MinorType.UINT4);
    testType(MinorType.BIGINT);
    testType(MinorType.UINT8);
    testType(MinorType.FLOAT4);
    testType(MinorType.FLOAT8);
    testType(MinorType.DECIMAL9);
    testType(MinorType.DECIMAL18);
    testType(MinorType.DECIMAL28SPARSE);
    testType(MinorType.DECIMAL38SPARSE);
//  testType(MinorType.DECIMAL28DENSE); No writer
//  testType(MinorType.DECIMAL38DENSE); No writer
    testType(MinorType.DATE);
    testType(MinorType.TIME);
    testType(MinorType.TIMESTAMP);
    testType(MinorType.INTERVAL);
    testType(MinorType.INTERVALYEAR);
    testType(MinorType.INTERVALDAY);
  }

  private SingleRowSet buildMapSet(BatchSchema schema) {
    return fixture.rowSetBuilder(schema)
        .add(1, 100, "first")
        .add(2, 200, "second")
        .add(3, 300, "third")
        .build();
  }

  private SingleRowSet buildArraySet(BatchSchema schema) {
    return fixture.rowSetBuilder(schema)
        .add(1, new String[] { "first, second, third" } )
        .add(2, null)
        .add(3, new String[] { "third, fourth, fifth" } )
        .build();
  }

  /**
   * Tests a map type and an SV2.
   *
   * @throws IOException
   */

  @Test
  public void testMap() throws IOException {
    BatchSchema schema = new SchemaBuilder()
        .add("top", MinorType.INT)
        .addMap("map")
          .add("key", MinorType.INT)
          .add("value", MinorType.VARCHAR)
          .buildMap()
        .build();

    verifySerialize(buildMapSet(schema).toIndirect(),
                    buildMapSet(schema));
  }

  @Test
  public void testArray() throws IOException {
    BatchSchema schema = new SchemaBuilder()
        .add("top", MinorType.INT)
        .addArray("arr", MinorType.VARCHAR)
        .build();

    verifySerialize(buildArraySet(schema).toIndirect(),
                    buildArraySet(schema));
  }
}
