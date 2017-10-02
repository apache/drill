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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.vector.accessor.ArrayReader;
import org.apache.drill.exec.vector.accessor.ArrayWriter;
import org.apache.drill.exec.vector.accessor.TupleAccessor.TupleSchema;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.RowSet.ExtendableRowSet;
import org.apache.drill.test.rowSet.RowSet.RowSetReader;
import org.apache.drill.test.rowSet.RowSet.RowSetWriter;
import org.apache.drill.test.rowSet.RowSet.SingleRowSet;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.apache.drill.test.rowSet.RowSetSchema;
import org.apache.drill.test.rowSet.RowSetSchema.FlattenedSchema;
import org.apache.drill.test.rowSet.RowSetSchema.PhysicalSchema;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.junit.Test;

import com.google.common.base.Splitter;

public class RowSetTest extends SubOperatorTest {

  /**
   * Test a simple physical schema with no maps.
   */

//  @Test
//  public void testSchema() {
//    BatchSchema batchSchema = new SchemaBuilder()
//        .add("c", MinorType.INT)
//        .add("a", MinorType.INT, DataMode.REPEATED)
//        .addNullable("b", MinorType.VARCHAR)
//        .build();
//
//    assertEquals("c", batchSchema.getColumn(0).getName());
//    assertEquals("a", batchSchema.getColumn(1).getName());
//    assertEquals("b", batchSchema.getColumn(2).getName());
//
//    RowSetSchema schema = new RowSetSchema(batchSchema);
//    TupleSchema access = schema.hierarchicalAccess();
//    assertEquals(3, access.count());
//
//    crossCheck(access, 0, "c", MinorType.INT);
//    assertEquals(DataMode.REQUIRED, access.column(0).getDataMode());
//    assertEquals(DataMode.REQUIRED, access.column(0).getType().getMode());
//    assertTrue(! access.column(0).isNullable());
//
//    crossCheck(access, 1, "a", MinorType.INT);
//    assertEquals(DataMode.REPEATED, access.column(1).getDataMode());
//    assertEquals(DataMode.REPEATED, access.column(1).getType().getMode());
//    assertTrue(! access.column(1).isNullable());
//
//    crossCheck(access, 2, "b", MinorType.VARCHAR);
//    assertEquals(MinorType.VARCHAR, access.column(2).getType().getMinorType());
//    assertEquals(DataMode.OPTIONAL, access.column(2).getDataMode());
//    assertEquals(DataMode.OPTIONAL, access.column(2).getType().getMode());
//    assertTrue(access.column(2).isNullable());
//
//    // No maps: physical schema is the same as access schema.
//
//    PhysicalSchema physical = schema.physical();
//    assertEquals(3, physical.count());
//    assertEquals("c", physical.column(0).field().getName());
//    assertEquals("a", physical.column(1).field().getName());
//    assertEquals("b", physical.column(2).field().getName());
//  }

  /**
   * Validate that the actual column metadata is as expected by
   * cross-checking: validate that the column at the index and
   * the column at the column name are both correct.
   *
   * @param schema the schema for the row set
   * @param index column index
   * @param fullName expected column name
   * @param type expected type
   */

//  public void crossCheck(TupleSchema schema, int index, String fullName, MinorType type) {
//    String name = null;
//    for (String part : Splitter.on(".").split(fullName)) {
//      name = part;
//    }
//    assertEquals(name, schema.column(index).getName());
//    assertEquals(index, schema.columnIndex(fullName));
//    assertSame(schema.column(index), schema.column(fullName));
//    assertEquals(type, schema.column(index).getType().getMinorType());
//  }

  /**
   * Verify that a nested map schema works as expected.
   */

//  @Test
//  public void testMapSchema() {
//    BatchSchema batchSchema = new SchemaBuilder()
//        .add("c", MinorType.INT)
//        .addMap("a")
//          .addNullable("b", MinorType.VARCHAR)
//          .add("d", MinorType.INT)
//          .addMap("e")
//            .add("f", MinorType.VARCHAR)
//            .buildMap()
//          .add("g", MinorType.INT)
//          .buildMap()
//        .add("h", MinorType.BIGINT)
//        .build();
//
//    RowSetSchema schema = new RowSetSchema(batchSchema);
//
//    // Access schema: flattened with maps removed
//
//    FlattenedSchema access = schema.flatAccess();
//    assertEquals(6, access.count());
//    crossCheck(access, 0, "c", MinorType.INT);
//    crossCheck(access, 1, "a.b", MinorType.VARCHAR);
//    crossCheck(access, 2, "a.d", MinorType.INT);
//    crossCheck(access, 3, "a.e.f", MinorType.VARCHAR);
//    crossCheck(access, 4, "a.g", MinorType.INT);
//    crossCheck(access, 5, "h", MinorType.BIGINT);
//
//    // Should have two maps.
//
//    assertEquals(2, access.mapCount());
//    assertEquals("a", access.map(0).getName());
//    assertEquals("e", access.map(1).getName());
//    assertEquals(0, access.mapIndex("a"));
//    assertEquals(1, access.mapIndex("a.e"));
//
//    // Verify physical schema: should mirror the schema created above.
//
//    PhysicalSchema physical = schema.physical();
//    assertEquals(3, physical.count());
//    assertEquals("c", physical.column(0).field().getName());
//    assertEquals("c", physical.column(0).fullName());
//    assertFalse(physical.column(0).isMap());
//    assertNull(physical.column(0).mapSchema());
//
//    assertEquals("a", physical.column(1).field().getName());
//    assertEquals("a", physical.column(1).fullName());
//    assertTrue(physical.column(1).isMap());
//    assertNotNull(physical.column(1).mapSchema());
//
//    assertEquals("h", physical.column(2).field().getName());
//    assertEquals("h", physical.column(2).fullName());
//    assertFalse(physical.column(2).isMap());
//    assertNull(physical.column(2).mapSchema());
//
//    PhysicalSchema aSchema = physical.column(1).mapSchema();
//    assertEquals(4, aSchema.count());
//    assertEquals("b", aSchema.column(0).field().getName());
//    assertEquals("a.b", aSchema.column(0).fullName());
//    assertEquals("d", aSchema.column(1).field().getName());
//    assertEquals("e", aSchema.column(2).field().getName());
//    assertEquals("g", aSchema.column(3).field().getName());
//
//    PhysicalSchema eSchema = aSchema.column(2).mapSchema();
//    assertEquals(1, eSchema.count());
//    assertEquals("f", eSchema.column(0).field().getName());
//    assertEquals("a.e.f", eSchema.column(0).fullName());
//  }

  /**
   * Verify that simple scalar (non-repeated) column readers
   * and writers work as expected. This is for tiny ints.
   */

  @Test
  public void testTinyIntRW() {
    BatchSchema batchSchema = new SchemaBuilder()
        .add("col", MinorType.TINYINT)
        .build();
    SingleRowSet rs = fixture.rowSetBuilder(batchSchema)
        .add(0)
        .add(Byte.MAX_VALUE)
        .add(Byte.MIN_VALUE)
        .build();
    assertEquals(3, rs.rowCount());
    RowSetReader reader = rs.reader();
    assertTrue(reader.next());
    assertEquals(0, reader.column(0).getInt());
    assertTrue(reader.next());
    assertEquals(Byte.MAX_VALUE, reader.column(0).getInt());
    assertEquals((int) Byte.MAX_VALUE, reader.column(0).getObject());
    assertTrue(reader.next());
    assertEquals(Byte.MIN_VALUE, reader.column(0).getInt());
    assertFalse(reader.next());
    rs.clear();
  }

  @Test
  public void testSmallIntRW() {
    BatchSchema batchSchema = new SchemaBuilder()
        .add("col", MinorType.SMALLINT)
        .build();
    SingleRowSet rs = fixture.rowSetBuilder(batchSchema)
        .add(0)
        .add(Short.MAX_VALUE)
        .add(Short.MIN_VALUE)
        .build();
    RowSetReader reader = rs.reader();
    assertTrue(reader.next());
    assertEquals(0, reader.column(0).getInt());
    assertTrue(reader.next());
    assertEquals(Short.MAX_VALUE, reader.column(0).getInt());
    assertEquals((int) Short.MAX_VALUE, reader.column(0).getObject());
    assertTrue(reader.next());
    assertEquals(Short.MIN_VALUE, reader.column(0).getInt());
    assertFalse(reader.next());
    rs.clear();
  }

  @Test
  public void testIntRW() {
    BatchSchema batchSchema = new SchemaBuilder()
        .add("col", MinorType.INT)
        .build();
    SingleRowSet rs = fixture.rowSetBuilder(batchSchema)
        .add(0)
        .add(Integer.MAX_VALUE)
        .add(Integer.MIN_VALUE)
        .build();
    RowSetReader reader = rs.reader();
    assertTrue(reader.next());
    assertEquals(0, reader.column(0).getInt());
    assertTrue(reader.next());
    assertEquals(Integer.MAX_VALUE, reader.column(0).getInt());
    assertEquals(Integer.MAX_VALUE, reader.column(0).getObject());
    assertTrue(reader.next());
    assertEquals(Integer.MIN_VALUE, reader.column(0).getInt());
    assertFalse(reader.next());
    rs.clear();
  }

  @Test
  public void testLongRW() {
    BatchSchema batchSchema = new SchemaBuilder()
        .add("col", MinorType.BIGINT)
        .build();
    SingleRowSet rs = fixture.rowSetBuilder(batchSchema)
        .add(0L)
        .add(Long.MAX_VALUE)
        .add(Long.MIN_VALUE)
        .build();
    RowSetReader reader = rs.reader();
    assertTrue(reader.next());
    assertEquals(0, reader.column(0).getLong());
    assertTrue(reader.next());
    assertEquals(Long.MAX_VALUE, reader.column(0).getLong());
    assertEquals(Long.MAX_VALUE, reader.column(0).getObject());
    assertTrue(reader.next());
    assertEquals(Long.MIN_VALUE, reader.column(0).getLong());
    assertFalse(reader.next());
    rs.clear();
  }

  @Test
  public void testFloatRW() {
    BatchSchema batchSchema = new SchemaBuilder()
        .add("col", MinorType.FLOAT4)
        .build();
    SingleRowSet rs = fixture.rowSetBuilder(batchSchema)
        .add(0F)
        .add(Float.MAX_VALUE)
        .add(Float.MIN_VALUE)
        .build();
    RowSetReader reader = rs.reader();
    assertTrue(reader.next());
    assertEquals(0, reader.column(0).getDouble(), 0.000001);
    assertTrue(reader.next());
    assertEquals((double) Float.MAX_VALUE, reader.column(0).getDouble(), 0.000001);
    assertEquals((double) Float.MAX_VALUE, (double) reader.column(0).getObject(), 0.000001);
    assertTrue(reader.next());
    assertEquals((double) Float.MIN_VALUE, reader.column(0).getDouble(), 0.000001);
    assertFalse(reader.next());
    rs.clear();
  }

  @Test
  public void testDoubleRW() {
    BatchSchema batchSchema = new SchemaBuilder()
        .add("col", MinorType.FLOAT8)
        .build();
    SingleRowSet rs = fixture.rowSetBuilder(batchSchema)
        .add(0D)
        .add(Double.MAX_VALUE)
        .add(Double.MIN_VALUE)
        .build();
    RowSetReader reader = rs.reader();
    assertTrue(reader.next());
    assertEquals(0, reader.column(0).getDouble(), 0.000001);
    assertTrue(reader.next());
    assertEquals(Double.MAX_VALUE, reader.column(0).getDouble(), 0.000001);
    assertEquals(Double.MAX_VALUE, (double) reader.column(0).getObject(), 0.000001);
    assertTrue(reader.next());
    assertEquals(Double.MIN_VALUE, reader.column(0).getDouble(), 0.000001);
    assertFalse(reader.next());
    rs.clear();
  }

  @Test
  public void testStringRW() {
    BatchSchema batchSchema = new SchemaBuilder()
        .add("col", MinorType.VARCHAR)
        .build();
    SingleRowSet rs = fixture.rowSetBuilder(batchSchema)
        .add("")
        .add("abcd")
        .build();
    RowSetReader reader = rs.reader();
    assertTrue(reader.next());
    assertEquals("", reader.column(0).getString());
    assertTrue(reader.next());
    assertEquals("abcd", reader.column(0).getString());
    assertEquals("abcd", reader.column(0).getObject());
    assertFalse(reader.next());
    rs.clear();
  }

  /**
   * Test writing to and reading from a row set with nested maps.
   * Map fields are flattened into a logical schema.
   */

//  @Test
//  public void testMap() {
//    BatchSchema batchSchema = new SchemaBuilder()
//        .add("a", MinorType.INT)
//        .addMap("b")
//          .add("c", MinorType.INT)
//          .add("d", MinorType.INT)
//          .buildMap()
//        .build();
//    SingleRowSet rs = fixture.rowSetBuilder(batchSchema)
//        .add(10, 20, 30)
//        .add(40, 50, 60)
//        .build();
//    RowSetReader reader = rs.reader();
//    assertTrue(reader.next());
//    assertEquals(10, reader.column(0).getInt());
//    assertEquals(20, reader.column(1).getInt());
//    assertEquals(30, reader.column(2).getInt());
//    assertEquals(10, reader.column("a").getInt());
//    assertEquals(30, reader.column("b.d").getInt());
//    assertTrue(reader.next());
//    assertEquals(40, reader.column(0).getInt());
//    assertEquals(50, reader.column(1).getInt());
//    assertEquals(60, reader.column(2).getInt());
//    assertFalse(reader.next());
//    rs.clear();
//  }

  /**
   * Test an array of ints (as an example fixed-width type)
   * at the top level of a schema.
   */

  @Test
  public void TestTopFixedWidthArray() {
    BatchSchema batchSchema = new SchemaBuilder()
        .add("c", MinorType.INT)
        .addArray("a", MinorType.INT)
        .build();

    ExtendableRowSet rs1 = fixture.rowSet(batchSchema);
    RowSetWriter writer = rs1.writer();
    writer.column(0).setInt(10);
    ArrayWriter array = writer.column(1).array();
    array.setInt(100);
    array.setInt(110);
    writer.save();
    writer.column(0).setInt(20);
    array = writer.column(1).array();
    array.setInt(200);
    array.setInt(120);
    array.setInt(220);
    writer.save();
    writer.column(0).setInt(30);
    writer.save();
    writer.done();

    RowSetReader reader = rs1.reader();
    assertTrue(reader.next());
    assertEquals(10, reader.column(0).getInt());
    ArrayReader arrayReader = reader.column(1).array();
    assertEquals(2, arrayReader.size());
    assertEquals(100, arrayReader.getInt(0));
    assertEquals(110, arrayReader.getInt(1));
    assertTrue(reader.next());
    assertEquals(20, reader.column(0).getInt());
    arrayReader = reader.column(1).array();
    assertEquals(3, arrayReader.size());
    assertEquals(200, arrayReader.getInt(0));
    assertEquals(120, arrayReader.getInt(1));
    assertEquals(220, arrayReader.getInt(2));
    assertTrue(reader.next());
    assertEquals(30, reader.column(0).getInt());
    arrayReader = reader.column(1).array();
    assertEquals(0, arrayReader.size());
    assertFalse(reader.next());

    SingleRowSet rs2 = fixture.rowSetBuilder(batchSchema)
      .add(10, new int[] {100, 110})
      .add(20, new int[] {200, 120, 220})
      .add(30, null)
      .build();

    new RowSetComparison(rs1)
      .verifyAndClearAll(rs2);
  }

}
