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

import static org.junit.Assert.*;

import java.math.BigDecimal;
import java.util.Arrays;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.vector.accessor.ScalarElementReader;
import org.apache.drill.exec.vector.accessor.ScalarReader;
import org.apache.drill.exec.vector.accessor.ValueType;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.RowSetReader;
import org.joda.time.Period;
import org.apache.drill.test.rowSet.RowSet.SingleRowSet;
import org.apache.drill.test.rowSet.schema.SchemaBuilder;
import org.junit.Test;

/**
 * Verify that simple scalar (non-repeated) column readers
 * and writers work as expected. The focus is on the generated
 * and type-specific functions for each type.
 */

// The following types are not fully supported in Drill
// TODO: Var16Char
// TODO: Bit
// TODO: Decimal28Sparse
// TODO: Decimal38Sparse

public class TestScalarAccessors extends SubOperatorTest {

  @Test
  public void testTinyIntRW() {
    BatchSchema batchSchema = new SchemaBuilder()
        .add("col", MinorType.TINYINT)
        .build();
    SingleRowSet rs = fixture.rowSetBuilder(batchSchema)
        .addRow(0)
        .addRow(Byte.MAX_VALUE)
        .addRow(Byte.MIN_VALUE)
        .build();
    assertEquals(3, rs.rowCount());

    RowSetReader reader = rs.reader();
    ScalarReader colReader = reader.scalar(0);
    assertEquals(ValueType.INTEGER, colReader.valueType());

    assertTrue(reader.next());
    assertFalse(colReader.isNull());
    assertEquals(0, colReader.getInt());

    assertTrue(reader.next());
    assertEquals(Byte.MAX_VALUE, colReader.getInt());
    assertEquals((int) Byte.MAX_VALUE, colReader.getObject());
    assertEquals(Byte.toString(Byte.MAX_VALUE), colReader.getAsString());

    assertTrue(reader.next());
    assertEquals(Byte.MIN_VALUE, colReader.getInt());

    assertFalse(reader.next());
    rs.clear();
  }

  private void nullableIntTester(MinorType type) {
    BatchSchema batchSchema = new SchemaBuilder()
        .addNullable("col", type)
        .build();
    SingleRowSet rs = fixture.rowSetBuilder(batchSchema)
        .addRow(10)
        .addSingleCol(null)
        .addRow(30)
        .build();
    assertEquals(3, rs.rowCount());

    RowSetReader reader = rs.reader();
    ScalarReader colReader = reader.scalar(0);

    assertTrue(reader.next());
    assertFalse(colReader.isNull());
    assertEquals(10, colReader.getInt());

    assertTrue(reader.next());
    assertTrue(colReader.isNull());
    assertNull(colReader.getObject());
    assertEquals("null", colReader.getAsString());
    // Data value is undefined, may be garbage

    assertTrue(reader.next());
    assertEquals(30, colReader.getInt());

    assertFalse(reader.next());
    rs.clear();
  }

  @Test
  public void testNullableTinyInt() {
    nullableIntTester(MinorType.TINYINT);
  }

  private void intArrayTester(MinorType type) {
    BatchSchema batchSchema = new SchemaBuilder()
        .addArray("col", type)
        .build();
    SingleRowSet rs = fixture.rowSetBuilder(batchSchema)
        .addSingleCol(new int[] {})
        .addSingleCol(new int[] {0, 20, 30})
        .build();
    assertEquals(2, rs.rowCount());

    RowSetReader reader = rs.reader();
    ScalarElementReader colReader = reader.elements(0);
    assertEquals(ValueType.INTEGER, colReader.valueType());

    assertTrue(reader.next());
    assertEquals(0, colReader.size());

    assertTrue(reader.next());
    assertEquals(3, colReader.size());
    assertEquals(0, colReader.getInt(0));
    assertEquals(20, colReader.getInt(1));
    assertEquals(30, colReader.getInt(2));
    assertEquals(0, colReader.getObject(0));
    assertEquals(20, colReader.getObject(1));
    assertEquals(30, colReader.getObject(2));
    assertEquals("0", colReader.getAsString(0));
    assertEquals("20", colReader.getAsString(1));
    assertEquals("30", colReader.getAsString(2));

    assertFalse(reader.next());
    rs.clear();
  }

  @Test
  public void testTinyIntArray() {
    intArrayTester(MinorType.TINYINT);
  }

  @Test
  public void testSmallIntRW() {
    BatchSchema batchSchema = new SchemaBuilder()
        .add("col", MinorType.SMALLINT)
        .build();
    SingleRowSet rs = fixture.rowSetBuilder(batchSchema)
        .addRow(0)
        .addRow(Short.MAX_VALUE)
        .addRow(Short.MIN_VALUE)
        .build();
    assertEquals(3, rs.rowCount());

    RowSetReader reader = rs.reader();
    ScalarReader colReader = reader.scalar(0);
    assertEquals(ValueType.INTEGER, colReader.valueType());

    assertTrue(reader.next());
    assertFalse(colReader.isNull());
    assertEquals(0, colReader.getInt());

    assertTrue(reader.next());
    assertEquals(Short.MAX_VALUE, colReader.getInt());
    assertEquals((int) Short.MAX_VALUE, colReader.getObject());
    assertEquals(Short.toString(Short.MAX_VALUE), colReader.getAsString());

    assertTrue(reader.next());
    assertEquals(Short.MIN_VALUE, colReader.getInt());

    assertFalse(reader.next());
    rs.clear();
  }

  @Test
  public void testNullableSmallInt() {
    nullableIntTester(MinorType.SMALLINT);
  }

  @Test
  public void testSmallArray() {
    intArrayTester(MinorType.SMALLINT);
  }

  @Test
  public void testIntRW() {
    BatchSchema batchSchema = new SchemaBuilder()
        .add("col", MinorType.INT)
        .build();
    SingleRowSet rs = fixture.rowSetBuilder(batchSchema)
        .addRow(0)
        .addRow(Integer.MAX_VALUE)
        .addRow(Integer.MIN_VALUE)
        .build();
    assertEquals(3, rs.rowCount());

    RowSetReader reader = rs.reader();
    ScalarReader colReader = reader.scalar(0);
    assertEquals(ValueType.INTEGER, colReader.valueType());

    assertTrue(reader.next());
    assertFalse(colReader.isNull());
    assertEquals(0, reader.scalar(0).getInt());

    assertTrue(reader.next());
    assertEquals(Integer.MAX_VALUE, colReader.getInt());
    assertEquals(Integer.MAX_VALUE, colReader.getObject());
    assertEquals(Integer.toString(Integer.MAX_VALUE), colReader.getAsString());

    assertTrue(reader.next());
    assertEquals(Integer.MIN_VALUE, colReader.getInt());

    assertFalse(reader.next());
    rs.clear();
  }

  @Test
  public void testNullableInt() {
    nullableIntTester(MinorType.INT);
  }

  @Test
  public void testIntArray() {
    intArrayTester(MinorType.INT);
  }

  private void longRWTester(MinorType type) {
    BatchSchema batchSchema = new SchemaBuilder()
        .add("col", type)
        .build();
    SingleRowSet rs = fixture.rowSetBuilder(batchSchema)
        .addRow(0L)
        .addRow(Long.MAX_VALUE)
        .addRow(Long.MIN_VALUE)
        .build();
    assertEquals(3, rs.rowCount());

    RowSetReader reader = rs.reader();
    ScalarReader colReader = reader.scalar(0);
    assertEquals(ValueType.LONG, colReader.valueType());

    assertTrue(reader.next());
    assertFalse(colReader.isNull());
    assertEquals(0, colReader.getLong());

    assertTrue(reader.next());
    assertEquals(Long.MAX_VALUE, colReader.getLong());
    assertEquals(Long.MAX_VALUE, colReader.getObject());
    assertEquals(Long.toString(Long.MAX_VALUE), colReader.getAsString());

    assertTrue(reader.next());
    assertEquals(Long.MIN_VALUE, colReader.getLong());

    assertFalse(reader.next());
    rs.clear();
  }

  @Test
  public void testLongRW() {
    longRWTester(MinorType.BIGINT);
  }

  private void nullableLongTester(MinorType type) {
    BatchSchema batchSchema = new SchemaBuilder()
        .addNullable("col", type)
        .build();
    SingleRowSet rs = fixture.rowSetBuilder(batchSchema)
        .addRow(10L)
        .addSingleCol(null)
        .addRow(30L)
        .build();
    assertEquals(3, rs.rowCount());

    RowSetReader reader = rs.reader();
    ScalarReader colReader = reader.scalar(0);

    assertTrue(reader.next());
    assertFalse(colReader.isNull());
    assertEquals(10, colReader.getLong());

    assertTrue(reader.next());
    assertTrue(colReader.isNull());
    assertNull(colReader.getObject());
    assertEquals("null", colReader.getAsString());
    // Data value is undefined, may be garbage

    assertTrue(reader.next());
    assertEquals(30, colReader.getLong());

    assertFalse(reader.next());
    rs.clear();
  }

  @Test
  public void testNullableLong() {
    nullableLongTester(MinorType.BIGINT);
  }

  private void longArrayTester(MinorType type) {
    BatchSchema batchSchema = new SchemaBuilder()
        .addArray("col", type)
        .build();
    SingleRowSet rs = fixture.rowSetBuilder(batchSchema)
        .addSingleCol(new long[] {})
        .addSingleCol(new long[] {0, 20, 30})
        .build();
    assertEquals(2, rs.rowCount());

    RowSetReader reader = rs.reader();
    ScalarElementReader colReader = reader.elements(0);
    assertEquals(ValueType.LONG, colReader.valueType());

    assertTrue(reader.next());
    assertEquals(0, colReader.size());

    assertTrue(reader.next());
    assertEquals(3, colReader.size());
    assertEquals(0, colReader.getLong(0));
    assertEquals(20, colReader.getLong(1));
    assertEquals(30, colReader.getLong(2));
    assertEquals(0L, colReader.getObject(0));
    assertEquals(20L, colReader.getObject(1));
    assertEquals(30L, colReader.getObject(2));
    assertEquals("0", colReader.getAsString(0));
    assertEquals("20", colReader.getAsString(1));
    assertEquals("30", colReader.getAsString(2));

    assertFalse(reader.next());
    rs.clear();
  }

  @Test
  public void testLongArray() {
    longArrayTester(MinorType.BIGINT);
  }

  @Test
  public void testFloatRW() {
    BatchSchema batchSchema = new SchemaBuilder()
        .add("col", MinorType.FLOAT4)
        .build();
    SingleRowSet rs = fixture.rowSetBuilder(batchSchema)
        .addRow(0F)
        .addRow(Float.MAX_VALUE)
        .addRow(Float.MIN_VALUE)
        .addRow(100F)
        .build();
    assertEquals(4, rs.rowCount());

    RowSetReader reader = rs.reader();
    ScalarReader colReader = reader.scalar(0);
    assertEquals(ValueType.DOUBLE, colReader.valueType());

    assertTrue(reader.next());
    assertFalse(colReader.isNull());
    assertEquals(0, colReader.getDouble(), 0.000001);

    assertTrue(reader.next());
    assertEquals(Float.MAX_VALUE, colReader.getDouble(), 0.000001);
    assertEquals((double) Float.MAX_VALUE, (double) colReader.getObject(), 0.000001);

    assertTrue(reader.next());
    assertEquals(Float.MIN_VALUE, colReader.getDouble(), 0.000001);

    assertTrue(reader.next());
    assertEquals(100, colReader.getDouble(), 0.000001);
    assertEquals("100.0", colReader.getAsString());

    assertFalse(reader.next());
    rs.clear();
  }

  private void nullableDoubleTester(MinorType type) {
    BatchSchema batchSchema = new SchemaBuilder()
        .addNullable("col", type)
        .build();
    SingleRowSet rs = fixture.rowSetBuilder(batchSchema)
        .addRow(10F)
        .addSingleCol(null)
        .addRow(30F)
        .build();
    assertEquals(3, rs.rowCount());

    RowSetReader reader = rs.reader();
    ScalarReader colReader = reader.scalar(0);

    assertTrue(reader.next());
    assertFalse(colReader.isNull());
    assertEquals(10, colReader.getDouble(), 0.000001);

    assertTrue(reader.next());
    assertTrue(colReader.isNull());
    assertNull(colReader.getObject());
    assertEquals("null", colReader.getAsString());
    // Data value is undefined, may be garbage

    assertTrue(reader.next());
    assertEquals(30, colReader.getDouble(), 0.000001);

    assertFalse(reader.next());
    rs.clear();
  }

  @Test
  public void testNullableFloat() {
    nullableDoubleTester(MinorType.FLOAT4);
  }

  private void doubleArrayTester(MinorType type) {
    BatchSchema batchSchema = new SchemaBuilder()
        .addArray("col", type)
        .build();
    SingleRowSet rs = fixture.rowSetBuilder(batchSchema)
        .addSingleCol(new double[] {})
        .addSingleCol(new double[] {0, 20.5, 30.0})
        .build();
    assertEquals(2, rs.rowCount());

    RowSetReader reader = rs.reader();
    ScalarElementReader colReader = reader.elements(0);
    assertEquals(ValueType.DOUBLE, colReader.valueType());

    assertTrue(reader.next());
    assertEquals(0, colReader.size());

    assertTrue(reader.next());
    assertEquals(3, colReader.size());
    assertEquals(0, colReader.getDouble(0), 0.00001);
    assertEquals(20.5, colReader.getDouble(1), 0.00001);
    assertEquals(30.0, colReader.getDouble(2), 0.00001);
    assertEquals(0, (double) colReader.getObject(0), 0.00001);
    assertEquals(20.5, (double) colReader.getObject(1), 0.00001);
    assertEquals(30.0, (double) colReader.getObject(2), 0.00001);
    assertEquals("0.0", colReader.getAsString(0));
    assertEquals("20.5", colReader.getAsString(1));
    assertEquals("30.0", colReader.getAsString(2));

    assertFalse(reader.next());
    rs.clear();
  }

  @Test
  public void testFloatArray() {
    doubleArrayTester(MinorType.FLOAT4);
  }

  @Test
  public void testDoubleRW() {
    BatchSchema batchSchema = new SchemaBuilder()
        .add("col", MinorType.FLOAT8)
        .build();
    SingleRowSet rs = fixture.rowSetBuilder(batchSchema)
        .addRow(0D)
        .addRow(Double.MAX_VALUE)
        .addRow(Double.MIN_VALUE)
        .addRow(100D)
        .build();
    assertEquals(4, rs.rowCount());

    RowSetReader reader = rs.reader();
    ScalarReader colReader = reader.scalar(0);
    assertEquals(ValueType.DOUBLE, colReader.valueType());

    assertTrue(reader.next());
    assertFalse(colReader.isNull());
    assertEquals(0, colReader.getDouble(), 0.000001);

    assertTrue(reader.next());
    assertEquals(Double.MAX_VALUE, colReader.getDouble(), 0.000001);
    assertEquals(Double.MAX_VALUE, (double) colReader.getObject(), 0.000001);

    assertTrue(reader.next());
    assertEquals(Double.MIN_VALUE, colReader.getDouble(), 0.000001);

    assertTrue(reader.next());
    assertEquals(100, colReader.getDouble(), 0.000001);
    assertEquals("100.0", colReader.getAsString());

    assertFalse(reader.next());
    rs.clear();
  }

  @Test
  public void testNullableDouble() {
    nullableDoubleTester(MinorType.FLOAT8);
  }

  @Test
  public void testDoubleArray() {
    doubleArrayTester(MinorType.FLOAT8);
  }

  @Test
  public void testStringRW() {
    BatchSchema batchSchema = new SchemaBuilder()
        .add("col", MinorType.VARCHAR)
        .build();
    SingleRowSet rs = fixture.rowSetBuilder(batchSchema)
        .addRow("")
        .addRow("abcd")
        .build();
    assertEquals(2, rs.rowCount());

    RowSetReader reader = rs.reader();
    ScalarReader colReader = reader.scalar(0);
    assertEquals(ValueType.STRING, colReader.valueType());

    assertTrue(reader.next());
    assertFalse(colReader.isNull());
    assertEquals("", colReader.getString());

    assertTrue(reader.next());
    assertEquals("abcd", colReader.getString());
    assertEquals("abcd", colReader.getObject());
    assertEquals("\"abcd\"", colReader.getAsString());

    assertFalse(reader.next());
    rs.clear();
  }

  @Test
  public void testNullableString() {
    BatchSchema batchSchema = new SchemaBuilder()
        .addNullable("col", MinorType.VARCHAR)
        .build();
    SingleRowSet rs = fixture.rowSetBuilder(batchSchema)
        .addRow("")
        .addSingleCol(null)
        .addRow("abcd")
        .build();
    assertEquals(3, rs.rowCount());

    RowSetReader reader = rs.reader();
    ScalarReader colReader = reader.scalar(0);

    assertTrue(reader.next());
    assertFalse(colReader.isNull());
    assertEquals("", colReader.getString());

    assertTrue(reader.next());
    assertTrue(colReader.isNull());
    assertNull(colReader.getObject());
    assertEquals("null", colReader.getAsString());

    assertTrue(reader.next());
    assertEquals("abcd", colReader.getString());
    assertEquals("abcd", colReader.getObject());
    assertEquals("\"abcd\"", colReader.getAsString());

    assertFalse(reader.next());
    rs.clear();
  }

  @Test
  public void testStringArray() {
    BatchSchema batchSchema = new SchemaBuilder()
        .addArray("col", MinorType.VARCHAR)
        .build();
    SingleRowSet rs = fixture.rowSetBuilder(batchSchema)
        .addSingleCol(new String[] {})
        .addSingleCol(new String[] {"fred", "", "wilma"})
        .build();
    assertEquals(2, rs.rowCount());

    RowSetReader reader = rs.reader();
    ScalarElementReader colReader = reader.elements(0);
    assertEquals(ValueType.STRING, colReader.valueType());

    assertTrue(reader.next());
    assertEquals(0, colReader.size());

    assertTrue(reader.next());
    assertEquals(3, colReader.size());
    assertEquals("fred", colReader.getString(0));
    assertEquals("", colReader.getString(1));
    assertEquals("wilma", colReader.getString(2));
    assertEquals("fred", colReader.getObject(0));
    assertEquals("", colReader.getObject(1));
    assertEquals("wilma", colReader.getObject(2));
    assertEquals("\"fred\"", colReader.getAsString(0));
    assertEquals("\"\"", colReader.getAsString(1));
    assertEquals("\"wilma\"", colReader.getAsString(2));

    assertFalse(reader.next());
    rs.clear();
  }

  @Test
  public void testIntervalYearRW() {
    BatchSchema batchSchema = new SchemaBuilder()
        .add("col", MinorType.INTERVALYEAR)
        .build();

    Period p1 = Period.years(0);
    Period p2 = Period.years(2).plusMonths(3);
    Period p3 = Period.years(1234).plusMonths(11);

    SingleRowSet rs = fixture.rowSetBuilder(batchSchema)
        .addRow(p1)
        .addRow(p2)
        .addRow(p3)
        .build();
    assertEquals(3, rs.rowCount());

    RowSetReader reader = rs.reader();
    ScalarReader colReader = reader.scalar(0);
    assertEquals(ValueType.PERIOD, colReader.valueType());

    assertTrue(reader.next());
    assertFalse(colReader.isNull());
    assertEquals(p1, colReader.getPeriod());

    assertTrue(reader.next());
    assertEquals(p2, colReader.getPeriod());
    assertEquals(p2, colReader.getObject());
    assertEquals(p2.toString(), colReader.getAsString());

    assertTrue(reader.next());
    assertEquals(p3, colReader.getPeriod());

    assertFalse(reader.next());
    rs.clear();
  }

  @Test
  public void testNullableIntervalYear() {
    BatchSchema batchSchema = new SchemaBuilder()
        .addNullable("col", MinorType.INTERVALYEAR)
        .build();

    Period p1 = Period.years(0);
    Period p2 = Period.years(2).plusMonths(3);

    SingleRowSet rs = fixture.rowSetBuilder(batchSchema)
        .addRow(p1)
        .addSingleCol(null)
        .addRow(p2)
        .build();
    assertEquals(3, rs.rowCount());

    RowSetReader reader = rs.reader();
    ScalarReader colReader = reader.scalar(0);
    assertEquals(ValueType.PERIOD, colReader.valueType());

    assertTrue(reader.next());
    assertFalse(colReader.isNull());
    assertEquals(p1, colReader.getPeriod());

    assertTrue(reader.next());
    assertTrue(colReader.isNull());
    assertNull(colReader.getPeriod());
    assertNull(colReader.getObject());
    assertEquals("null", colReader.getAsString());

    assertTrue(reader.next());
    assertEquals(p2, colReader.getPeriod());

    assertFalse(reader.next());
    rs.clear();
  }

  @Test
  public void testIntervalYearArray() {
    BatchSchema batchSchema = new SchemaBuilder()
        .addArray("col", MinorType.INTERVALYEAR)
        .build();

    Period p1 = Period.years(0);
    Period p2 = Period.years(2).plusMonths(3);
    Period p3 = Period.years(1234).plusMonths(11);

    SingleRowSet rs = fixture.rowSetBuilder(batchSchema)
        .addSingleCol(new Period[] {})
        .addSingleCol(new Period[] {p1, p2, p3})
        .build();
    assertEquals(2, rs.rowCount());

    RowSetReader reader = rs.reader();
    ScalarElementReader colReader = reader.elements(0);
    assertEquals(ValueType.PERIOD, colReader.valueType());

    assertTrue(reader.next());
    assertEquals(0, colReader.size());

    assertTrue(reader.next());
    assertEquals(3, colReader.size());
    assertEquals(p1, colReader.getPeriod(0));
    assertEquals(p2, colReader.getPeriod(1));
    assertEquals(p3, colReader.getPeriod(2));
    assertEquals(p2, colReader.getObject(1));
    assertEquals(p2.toString(), colReader.getAsString(1));

    assertFalse(reader.next());
    rs.clear();
  }

  @Test
  public void testIntervalDayRW() {
    BatchSchema batchSchema = new SchemaBuilder()
        .add("col", MinorType.INTERVALDAY)
        .build();

    Period p1 = Period.days(0);
    Period p2 = Period.days(3).plusHours(4).plusMinutes(5).plusSeconds(23);
    Period p3 = Period.days(999).plusHours(23).plusMinutes(59).plusSeconds(59);

    SingleRowSet rs = fixture.rowSetBuilder(batchSchema)
        .addRow(p1)
        .addRow(p2)
        .addRow(p3)
        .build();
    assertEquals(3, rs.rowCount());

    RowSetReader reader = rs.reader();
    ScalarReader colReader = reader.scalar(0);
    assertEquals(ValueType.PERIOD, colReader.valueType());

    assertTrue(reader.next());
    assertFalse(colReader.isNull());
    // The normalizedStandard() call is a hack. See DRILL-5689.
    assertEquals(p1, colReader.getPeriod().normalizedStandard());

    assertTrue(reader.next());
    assertEquals(p2, colReader.getPeriod().normalizedStandard());
    assertEquals(p2, ((Period) colReader.getObject()).normalizedStandard());
    assertEquals(p2.toString(), colReader.getAsString());

    assertTrue(reader.next());
    assertEquals(p3.normalizedStandard(), colReader.getPeriod().normalizedStandard());

    assertFalse(reader.next());
    rs.clear();
  }

  @Test
  public void testNullableIntervalDay() {
    BatchSchema batchSchema = new SchemaBuilder()
        .addNullable("col", MinorType.INTERVALDAY)
        .build();

    Period p1 = Period.years(0);
    Period p2 = Period.days(3).plusHours(4).plusMinutes(5).plusSeconds(23);

    SingleRowSet rs = fixture.rowSetBuilder(batchSchema)
        .addRow(p1)
        .addSingleCol(null)
        .addRow(p2)
        .build();
    assertEquals(3, rs.rowCount());

    RowSetReader reader = rs.reader();
    ScalarReader colReader = reader.scalar(0);
    assertEquals(ValueType.PERIOD, colReader.valueType());

    assertTrue(reader.next());
    assertFalse(colReader.isNull());
    assertEquals(p1, colReader.getPeriod().normalizedStandard());

    assertTrue(reader.next());
    assertTrue(colReader.isNull());
    assertNull(colReader.getPeriod());
    assertNull(colReader.getObject());
    assertEquals("null", colReader.getAsString());

    assertTrue(reader.next());
    assertEquals(p2, colReader.getPeriod().normalizedStandard());

    assertFalse(reader.next());
    rs.clear();
  }

  @Test
  public void testIntervalDayArray() {
    BatchSchema batchSchema = new SchemaBuilder()
        .addArray("col", MinorType.INTERVALDAY)
        .build();

    Period p1 = Period.days(0);
    Period p2 = Period.days(3).plusHours(4).plusMinutes(5).plusSeconds(23);
    Period p3 = Period.days(999).plusHours(23).plusMinutes(59).plusSeconds(59);

    SingleRowSet rs = fixture.rowSetBuilder(batchSchema)
        .addSingleCol(new Period[] {})
        .addSingleCol(new Period[] {p1, p2, p3})
        .build();
    assertEquals(2, rs.rowCount());

    RowSetReader reader = rs.reader();
    ScalarElementReader colReader = reader.elements(0);
    assertEquals(ValueType.PERIOD, colReader.valueType());

    assertTrue(reader.next());
    assertEquals(0, colReader.size());

    assertTrue(reader.next());
    assertEquals(3, colReader.size());
    assertEquals(p1, colReader.getPeriod(0).normalizedStandard());
    assertEquals(p2, colReader.getPeriod(1).normalizedStandard());
    assertEquals(p3.normalizedStandard(), colReader.getPeriod(2).normalizedStandard());
    assertEquals(p2, ((Period) colReader.getObject(1)).normalizedStandard());
    assertEquals(p2.toString(), colReader.getAsString(1));

    assertFalse(reader.next());
    rs.clear();
  }

  @Test
  public void testIntervalRW() {
    BatchSchema batchSchema = new SchemaBuilder()
        .add("col", MinorType.INTERVAL)
        .build();

    Period p1 = Period.days(0);
    Period p2 = Period.years(7).plusMonths(8)
                      .plusDays(3).plusHours(4)
                      .plusMinutes(5).plusSeconds(23);
    Period p3 = Period.years(9999).plusMonths(11)
                      .plusDays(365).plusHours(23)
                      .plusMinutes(59).plusSeconds(59);

    SingleRowSet rs = fixture.rowSetBuilder(batchSchema)
        .addRow(p1)
        .addRow(p2)
        .addRow(p3)
        .build();
    assertEquals(3, rs.rowCount());

    RowSetReader reader = rs.reader();
    ScalarReader colReader = reader.scalar(0);
    assertEquals(ValueType.PERIOD, colReader.valueType());

    assertTrue(reader.next());
    assertFalse(colReader.isNull());
    // The normalizedStandard() call is a hack. See DRILL-5689.
    assertEquals(p1, colReader.getPeriod().normalizedStandard());

    assertTrue(reader.next());
    assertEquals(p2, colReader.getPeriod().normalizedStandard());
    assertEquals(p2, ((Period) colReader.getObject()).normalizedStandard());
    assertEquals(p2.toString(), colReader.getAsString());

    assertTrue(reader.next());
    assertEquals(p3.normalizedStandard(), colReader.getPeriod().normalizedStandard());

    assertFalse(reader.next());
    rs.clear();
  }

  @Test
  public void testNullableInterval() {
    BatchSchema batchSchema = new SchemaBuilder()
        .addNullable("col", MinorType.INTERVAL)
        .build();

    Period p1 = Period.years(0);
    Period p2 = Period.years(7).plusMonths(8)
                      .plusDays(3).plusHours(4)
                      .plusMinutes(5).plusSeconds(23);

    SingleRowSet rs = fixture.rowSetBuilder(batchSchema)
        .addRow(p1)
        .addSingleCol(null)
        .addRow(p2)
        .build();
    assertEquals(3, rs.rowCount());

    RowSetReader reader = rs.reader();
    ScalarReader colReader = reader.scalar(0);
    assertEquals(ValueType.PERIOD, colReader.valueType());

    assertTrue(reader.next());
    assertFalse(colReader.isNull());
    assertEquals(p1, colReader.getPeriod().normalizedStandard());

    assertTrue(reader.next());
    assertTrue(colReader.isNull());
    assertNull(colReader.getPeriod());
    assertNull(colReader.getObject());
    assertEquals("null", colReader.getAsString());

    assertTrue(reader.next());
    assertEquals(p2, colReader.getPeriod().normalizedStandard());

    assertFalse(reader.next());
    rs.clear();
  }

  @Test
  public void testIntervalArray() {
    BatchSchema batchSchema = new SchemaBuilder()
        .addArray("col", MinorType.INTERVAL)
        .build();

    Period p1 = Period.days(0);
    Period p2 = Period.years(7).plusMonths(8)
                      .plusDays(3).plusHours(4)
                      .plusMinutes(5).plusSeconds(23);
    Period p3 = Period.years(9999).plusMonths(11)
                      .plusDays(365).plusHours(23)
                      .plusMinutes(59).plusSeconds(59);

    SingleRowSet rs = fixture.rowSetBuilder(batchSchema)
        .addSingleCol(new Period[] {})
        .addSingleCol(new Period[] {p1, p2, p3})
        .build();
    assertEquals(2, rs.rowCount());

    RowSetReader reader = rs.reader();
    ScalarElementReader colReader = reader.elements(0);
    assertEquals(ValueType.PERIOD, colReader.valueType());

    assertTrue(reader.next());
    assertEquals(0, colReader.size());

    assertTrue(reader.next());
    assertEquals(3, colReader.size());
    assertEquals(p1, colReader.getPeriod(0).normalizedStandard());
    assertEquals(p2, colReader.getPeriod(1).normalizedStandard());
    assertEquals(p3.normalizedStandard(), colReader.getPeriod(2).normalizedStandard());
    assertEquals(p2, ((Period) colReader.getObject(1)).normalizedStandard());
    assertEquals(p2.toString(), colReader.getAsString(1));

    assertFalse(reader.next());
    rs.clear();
  }

  @Test
  public void testDecimal9RW() {
    MajorType type = MajorType.newBuilder()
        .setMinorType(MinorType.DECIMAL9)
        .setScale(3)
        .setPrecision(9)
        .setMode(DataMode.REQUIRED)
        .build();
    BatchSchema batchSchema = new SchemaBuilder()
        .add("col", type)
        .build();

    BigDecimal v1 = BigDecimal.ZERO;
    BigDecimal v2 = BigDecimal.valueOf(123_456_789, 3);
    BigDecimal v3 = BigDecimal.valueOf(999_999_999, 3);

    SingleRowSet rs = fixture.rowSetBuilder(batchSchema)
        .addRow(v1)
        .addRow(v2)
        .addRow(v3)
        .build();
    assertEquals(3, rs.rowCount());

    RowSetReader reader = rs.reader();
    ScalarReader colReader = reader.scalar(0);
    assertEquals(ValueType.DECIMAL, colReader.valueType());

    assertTrue(reader.next());
    assertFalse(colReader.isNull());
    assertEquals(0, v1.compareTo(colReader.getDecimal()));

    assertTrue(reader.next());
    assertEquals(0, v2.compareTo(colReader.getDecimal()));
    assertEquals(0, v2.compareTo((BigDecimal) colReader.getObject()));
    assertEquals(v2.toString(), colReader.getAsString());

    assertTrue(reader.next());
    assertEquals(0, v3.compareTo(colReader.getDecimal()));

    assertFalse(reader.next());
    rs.clear();
  }

  private void nullableDecimalTester(MinorType type, int precision) {
    MajorType majorType = MajorType.newBuilder()
        .setMinorType(type)
        .setScale(3)
        .setPrecision(precision)
        .setMode(DataMode.OPTIONAL)
        .build();
    BatchSchema batchSchema = new SchemaBuilder()
        .add("col", majorType)
        .build();

    BigDecimal v1 = BigDecimal.ZERO;
    BigDecimal v2 = BigDecimal.valueOf(123_456_789, 3);

    SingleRowSet rs = fixture.rowSetBuilder(batchSchema)
        .addRow(v1)
        .addSingleCol(null)
        .addRow(v2)
        .build();
    assertEquals(3, rs.rowCount());

    RowSetReader reader = rs.reader();
    ScalarReader colReader = reader.scalar(0);
    assertEquals(ValueType.DECIMAL, colReader.valueType());

    assertTrue(reader.next());
    assertFalse(colReader.isNull());
    assertEquals(0, v1.compareTo(colReader.getDecimal()));

    assertTrue(reader.next());
    assertTrue(colReader.isNull());
    assertNull(colReader.getObject());
    assertEquals("null", colReader.getAsString());

    assertTrue(reader.next());
    assertEquals(0, v2.compareTo(colReader.getDecimal()));

    assertFalse(reader.next());
    rs.clear();
  }

  @Test
  public void testNullableDecimal9() {
    nullableDecimalTester(MinorType.DECIMAL9, 9);
  }

  private void decimalArrayTester(MinorType type, int precision) {
    MajorType majorType = MajorType.newBuilder()
        .setMinorType(type)
        .setScale(3)
        .setPrecision(precision)
        .setMode(DataMode.REPEATED)
        .build();
    BatchSchema batchSchema = new SchemaBuilder()
        .add("col", majorType)
        .build();

    BigDecimal v1 = BigDecimal.ZERO;
    BigDecimal v2 = BigDecimal.valueOf(123_456_789, 3);
    BigDecimal v3 = BigDecimal.TEN;

    SingleRowSet rs = fixture.rowSetBuilder(batchSchema)
        .addSingleCol(new BigDecimal[] {})
        .addSingleCol(new BigDecimal[] {v1, v2, v3})
        .build();
    assertEquals(2, rs.rowCount());

    RowSetReader reader = rs.reader();
    ScalarElementReader colReader = reader.elements(0);
    assertEquals(ValueType.DECIMAL, colReader.valueType());

    assertTrue(reader.next());
    assertEquals(0, colReader.size());

    assertTrue(reader.next());
    assertEquals(3, colReader.size());
    assertEquals(0, v1.compareTo(colReader.getDecimal(0)));
    assertEquals(0, v2.compareTo(colReader.getDecimal(1)));
    assertEquals(0, v3.compareTo(colReader.getDecimal(2)));
    assertEquals(0, v2.compareTo((BigDecimal) colReader.getObject(1)));
    assertEquals(v2.toString(), colReader.getAsString(1));

    assertFalse(reader.next());
    rs.clear();
  }

  @Test
  public void testDecimal9Array() {
    decimalArrayTester(MinorType.DECIMAL9, 9);
  }

  @Test
  public void testDecimal18RW() {
    MajorType type = MajorType.newBuilder()
        .setMinorType(MinorType.DECIMAL18)
        .setScale(3)
        .setPrecision(9)
        .setMode(DataMode.REQUIRED)
        .build();
    BatchSchema batchSchema = new SchemaBuilder()
        .add("col", type)
        .build();

    BigDecimal v1 = BigDecimal.ZERO;
    BigDecimal v2 = BigDecimal.valueOf(123_456_789_123_456_789L, 3);
    BigDecimal v3 = BigDecimal.valueOf(999_999_999_999_999_999L, 3);

    SingleRowSet rs = fixture.rowSetBuilder(batchSchema)
        .addRow(v1)
        .addRow(v2)
        .addRow(v3)
        .build();
    assertEquals(3, rs.rowCount());

    RowSetReader reader = rs.reader();
    ScalarReader colReader = reader.scalar(0);
    assertEquals(ValueType.DECIMAL, colReader.valueType());

    assertTrue(reader.next());
    assertFalse(colReader.isNull());
    assertEquals(0, v1.compareTo(colReader.getDecimal()));

    assertTrue(reader.next());
    assertEquals(0, v2.compareTo(colReader.getDecimal()));
    assertEquals(0, v2.compareTo((BigDecimal) colReader.getObject()));
    assertEquals(v2.toString(), colReader.getAsString());

    assertTrue(reader.next());
    assertEquals(0, v3.compareTo(colReader.getDecimal()));

    assertFalse(reader.next());
    rs.clear();
  }

  @Test
  public void testNullableDecimal18() {
    nullableDecimalTester(MinorType.DECIMAL18, 9);
  }

  @Test
  public void testDecimal18Array() {
    decimalArrayTester(MinorType.DECIMAL18, 9);
  }

  // From the perspective of the vector, a date vector is just a long.

  @Test
  public void testDateRW() {
    longRWTester(MinorType.DATE);
  }

  @Test
  public void testNullableDate() {
    nullableLongTester(MinorType.DATE);
  }

  @Test
  public void testDateArray() {
    longArrayTester(MinorType.DATE);
  }

  // From the perspective of the vector, a timestamp vector is just a long.

  @Test
  public void testTimestampRW() {
    longRWTester(MinorType.TIMESTAMP);
  }

  @Test
  public void testNullableTimestamp() {
    nullableLongTester(MinorType.TIMESTAMP);
  }

  @Test
  public void testTimestampArray() {
    longArrayTester(MinorType.TIMESTAMP);
  }

  @Test
  public void testVarBinaryRW() {
    BatchSchema batchSchema = new SchemaBuilder()
        .add("col", MinorType.VARBINARY)
        .build();

    byte v1[] = new byte[] {};
    byte v2[] = new byte[] { (byte) 0x00, (byte) 0x7f, (byte) 0x80, (byte) 0xFF};

    SingleRowSet rs = fixture.rowSetBuilder(batchSchema)
        .addRow(v1)
        .addRow(v2)
        .build();
    assertEquals(2, rs.rowCount());

    RowSetReader reader = rs.reader();
    ScalarReader colReader = reader.scalar(0);
    assertEquals(ValueType.BYTES, colReader.valueType());

    assertTrue(reader.next());
    assertFalse(colReader.isNull());
    assertTrue(Arrays.equals(v1, colReader.getBytes()));

    assertTrue(reader.next());
    assertTrue(Arrays.equals(v2, colReader.getBytes()));
    assertTrue(Arrays.equals(v2, (byte[]) colReader.getObject()));
    assertEquals("[00, 7f, 80, ff]", colReader.getAsString());

    assertFalse(reader.next());
    rs.clear();
  }

  @Test
  public void testNullableVarBinary() {
    BatchSchema batchSchema = new SchemaBuilder()
        .addNullable("col", MinorType.VARBINARY)
        .build();

    byte v1[] = new byte[] {};
    byte v2[] = new byte[] { (byte) 0x00, (byte) 0x7f, (byte) 0x80, (byte) 0xFF};

    SingleRowSet rs = fixture.rowSetBuilder(batchSchema)
        .addRow(v1)
        .addSingleCol(null)
        .addRow(v2)
        .build();
    assertEquals(3, rs.rowCount());

    RowSetReader reader = rs.reader();
    ScalarReader colReader = reader.scalar(0);
    assertEquals(ValueType.BYTES, colReader.valueType());

    assertTrue(reader.next());
    assertFalse(colReader.isNull());
    assertTrue(Arrays.equals(v1, colReader.getBytes()));

    assertTrue(reader.next());
    assertTrue(colReader.isNull());
    assertNull(colReader.getObject());
    assertEquals("null", colReader.getAsString());

    assertTrue(reader.next());
    assertTrue(Arrays.equals(v2, colReader.getBytes()));

    assertFalse(reader.next());
    rs.clear();
  }

  @Test
  public void testVarBinaryArray() {
    BatchSchema batchSchema = new SchemaBuilder()
        .addArray("col", MinorType.VARBINARY)
        .build();

    byte v1[] = new byte[] {};
    byte v2[] = new byte[] { (byte) 0x00, (byte) 0x7f, (byte) 0x80, (byte) 0xFF};
    byte v3[] = new byte[] { (byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xAF};

    SingleRowSet rs = fixture.rowSetBuilder(batchSchema)
        .addSingleCol(new byte[][] {})
        .addSingleCol(new byte[][] {v1, v2, v3})
        .build();
    assertEquals(2, rs.rowCount());

    RowSetReader reader = rs.reader();
    ScalarElementReader colReader = reader.elements(0);
    assertEquals(ValueType.BYTES, colReader.valueType());

    assertTrue(reader.next());
    assertEquals(0, colReader.size());

    assertTrue(reader.next());
    assertEquals(3, colReader.size());
    assertTrue(Arrays.equals(v1, colReader.getBytes(0)));
    assertTrue(Arrays.equals(v2, colReader.getBytes(1)));
    assertTrue(Arrays.equals(v3, colReader.getBytes(2)));
    assertTrue(Arrays.equals(v2, (byte[]) colReader.getObject(1)));
    assertEquals("[00, 7f, 80, ff]", colReader.getAsString(1));

    assertFalse(reader.next());
    rs.clear();
  }
}
