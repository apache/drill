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
package org.apache.drill.exec.store.accumulo;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.apache.drill.exec.store.accumulo.schema.AccumuloColumnType;
import org.apache.drill.test.BaseTest;
import org.junit.Test;

/**
 * Unit tests for AccumuloTypeConverter.
 */
public class AccumuloTypeConverterTest extends BaseTest {

  @Test
  public void testConvertVarchar() {
    byte[] bytes = "hello world".getBytes(StandardCharsets.UTF_8);
    Object result = AccumuloTypeConverter.convert(bytes, AccumuloColumnType.VARCHAR);
    assertEquals("hello world", result);
  }

  @Test
  public void testConvertVarcharEmpty() {
    byte[] bytes = new byte[0];
    Object result = AccumuloTypeConverter.convert(bytes, AccumuloColumnType.VARCHAR);
    assertNull(result);
  }

  @Test
  public void testConvertVarcharNull() {
    Object result = AccumuloTypeConverter.convert(null, AccumuloColumnType.VARCHAR);
    assertNull(result);
  }

  @Test
  public void testConvertIntegerFromString() {
    byte[] bytes = "42".getBytes(StandardCharsets.UTF_8);
    Object result = AccumuloTypeConverter.convert(bytes, AccumuloColumnType.INT);
    assertEquals(42, result);
  }

  @Test
  public void testConvertIntegerNegative() {
    byte[] bytes = "-123".getBytes(StandardCharsets.UTF_8);
    Object result = AccumuloTypeConverter.convert(bytes, AccumuloColumnType.INTEGER);
    assertEquals(-123, result);
  }

  @Test
  public void testConvertIntegerFromBinary() {
    // Use a value that produces non-ASCII bytes so it falls through to binary parsing
    ByteBuffer buffer = ByteBuffer.allocate(4);
    buffer.putInt(0x80000001); // Has high bit set, produces non-printable chars
    Object result = AccumuloTypeConverter.convert(buffer.array(), AccumuloColumnType.INT);
    assertEquals(0x80000001, result);
  }

  @Test
  public void testConvertIntegerInvalid() {
    byte[] bytes = "not a number".getBytes(StandardCharsets.UTF_8);
    Object result = AccumuloTypeConverter.convert(bytes, AccumuloColumnType.INT);
    assertNull(result);
  }

  @Test
  public void testConvertLongFromString() {
    byte[] bytes = "9223372036854775807".getBytes(StandardCharsets.UTF_8);
    Object result = AccumuloTypeConverter.convert(bytes, AccumuloColumnType.BIGINT);
    assertEquals(Long.MAX_VALUE, result);
  }

  @Test
  public void testConvertLongFromBinary() {
    ByteBuffer buffer = ByteBuffer.allocate(8);
    buffer.putLong(123456789L);
    Object result = AccumuloTypeConverter.convert(buffer.array(), AccumuloColumnType.LONG);
    assertEquals(123456789L, result);
  }

  @Test
  public void testConvertFloat() {
    byte[] bytes = "3.14".getBytes(StandardCharsets.UTF_8);
    Object result = AccumuloTypeConverter.convert(bytes, AccumuloColumnType.FLOAT);
    assertEquals(3.14f, (Float) result, 0.001);
  }

  @Test
  public void testConvertFloatFromBinary() {
    ByteBuffer buffer = ByteBuffer.allocate(4);
    buffer.putFloat(3.14f);
    Object result = AccumuloTypeConverter.convert(buffer.array(), AccumuloColumnType.FLOAT);
    assertEquals(3.14f, (Float) result, 0.001);
  }

  @Test
  public void testConvertDouble() {
    byte[] bytes = "3.14159265359".getBytes(StandardCharsets.UTF_8);
    Object result = AccumuloTypeConverter.convert(bytes, AccumuloColumnType.DOUBLE);
    assertEquals(3.14159265359, (Double) result, 0.00000000001);
  }

  @Test
  public void testConvertDoubleFromBinary() {
    ByteBuffer buffer = ByteBuffer.allocate(8);
    buffer.putDouble(3.14159265359);
    Object result = AccumuloTypeConverter.convert(buffer.array(), AccumuloColumnType.DOUBLE);
    assertEquals(3.14159265359, (Double) result, 0.00000000001);
  }

  @Test
  public void testConvertDecimal() {
    byte[] bytes = "123456.789".getBytes(StandardCharsets.UTF_8);
    Object result = AccumuloTypeConverter.convert(bytes, AccumuloColumnType.DECIMAL);
    assertEquals(new BigDecimal("123456.789"), result);
  }

  @Test
  public void testConvertBooleanTrue() {
    byte[] bytes = "true".getBytes(StandardCharsets.UTF_8);
    Object result = AccumuloTypeConverter.convert(bytes, AccumuloColumnType.BOOLEAN);
    assertTrue((Boolean) result);
  }

  @Test
  public void testConvertBooleanFalse() {
    byte[] bytes = "false".getBytes(StandardCharsets.UTF_8);
    Object result = AccumuloTypeConverter.convert(bytes, AccumuloColumnType.BOOLEAN);
    assertFalse((Boolean) result);
  }

  @Test
  public void testConvertBooleanOne() {
    byte[] bytes = "1".getBytes(StandardCharsets.UTF_8);
    Object result = AccumuloTypeConverter.convert(bytes, AccumuloColumnType.BOOLEAN);
    assertTrue((Boolean) result);
  }

  @Test
  public void testConvertBooleanZero() {
    byte[] bytes = "0".getBytes(StandardCharsets.UTF_8);
    Object result = AccumuloTypeConverter.convert(bytes, AccumuloColumnType.BOOLEAN);
    assertFalse((Boolean) result);
  }

  @Test
  public void testConvertBooleanYes() {
    byte[] bytes = "YES".getBytes(StandardCharsets.UTF_8);
    Object result = AccumuloTypeConverter.convert(bytes, AccumuloColumnType.BOOLEAN);
    assertTrue((Boolean) result);
  }

  @Test
  public void testConvertBooleanBinary() {
    byte[] bytes = new byte[]{1};
    Object result = AccumuloTypeConverter.convert(bytes, AccumuloColumnType.BOOLEAN);
    assertTrue((Boolean) result);

    bytes = new byte[]{0};
    result = AccumuloTypeConverter.convert(bytes, AccumuloColumnType.BOOLEAN);
    assertFalse((Boolean) result);
  }

  @Test
  public void testConvertDateIso() {
    byte[] bytes = "2024-01-15".getBytes(StandardCharsets.UTF_8);
    Long result = (Long) AccumuloTypeConverter.convert(bytes, AccumuloColumnType.DATE);
    // 2024-01-15 00:00:00 UTC
    assertEquals(1705276800000L, result.longValue());
  }

  @Test
  public void testConvertTimeIso() {
    byte[] bytes = "12:30:45".getBytes(StandardCharsets.UTF_8);
    Integer result = (Integer) AccumuloTypeConverter.convert(bytes, AccumuloColumnType.TIME);
    // 12:30:45 = 12*3600*1000 + 30*60*1000 + 45*1000 = 45045000 ms
    assertEquals(45045000, result.intValue());
  }

  @Test
  public void testConvertTimestampIso() {
    byte[] bytes = "2024-01-15T12:30:45Z".getBytes(StandardCharsets.UTF_8);
    Long result = (Long) AccumuloTypeConverter.convert(bytes, AccumuloColumnType.TIMESTAMP);
    assertEquals(1705321845000L, result.longValue());
  }

  @Test
  public void testConvertTimestampEpoch() {
    byte[] bytes = "1705321845000".getBytes(StandardCharsets.UTF_8);
    Long result = (Long) AccumuloTypeConverter.convert(bytes, AccumuloColumnType.TIMESTAMP);
    assertEquals(1705321845000L, result.longValue());
  }

  @Test
  public void testConvertVarbinary() {
    byte[] bytes = new byte[]{0x01, 0x02, 0x03, (byte) 0xFF};
    Object result = AccumuloTypeConverter.convert(bytes, AccumuloColumnType.VARBINARY);
    assertArrayEquals(bytes, (byte[]) result);
  }

  @Test
  public void testConvertAny() {
    byte[] bytes = "some value".getBytes(StandardCharsets.UTF_8);
    Object result = AccumuloTypeConverter.convert(bytes, AccumuloColumnType.ANY);
    assertEquals("some value", result);
  }

  @Test
  public void testToDisplayStringPrintable() {
    byte[] bytes = "Hello World".getBytes(StandardCharsets.UTF_8);
    String result = AccumuloTypeConverter.toDisplayString(bytes);
    assertEquals("Hello World", result);
  }

  @Test
  public void testToDisplayStringBinary() {
    byte[] bytes = new byte[]{0x01, 0x02, 0x03};
    String result = AccumuloTypeConverter.toDisplayString(bytes);
    assertEquals("0x010203", result);
  }

  @Test
  public void testToDisplayStringNull() {
    String result = AccumuloTypeConverter.toDisplayString(null);
    assertEquals("null", result);
  }

  @Test
  public void testToDisplayStringEmpty() {
    String result = AccumuloTypeConverter.toDisplayString(new byte[0]);
    assertEquals("", result);
  }

  @Test
  public void testConvertShort() {
    byte[] bytes = "32767".getBytes(StandardCharsets.UTF_8);
    Object result = AccumuloTypeConverter.convert(bytes, AccumuloColumnType.SMALLINT);
    assertEquals((short) 32767, result);
  }

  @Test
  public void testConvertByte() {
    byte[] bytes = "127".getBytes(StandardCharsets.UTF_8);
    Object result = AccumuloTypeConverter.convert(bytes, AccumuloColumnType.TINYINT);
    assertEquals((byte) 127, result);
  }
}
