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
package org.apache.drill.test.rowSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;

import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.apache.drill.exec.vector.accessor.ValueType;
import org.bouncycastle.util.Arrays;
import org.joda.time.Duration;
import org.joda.time.Period;

/**
 * Various utilities useful for working with row sets, especially for testing.
 */

public class RowSetUtilities {

  private RowSetUtilities() { }

  /**
   * Reverse a row set by reversing the entries in an SV2. This is a quick
   * and easy way to reverse the sort order of an expected-value row set.
   * @param sv2 the SV2 which is reversed in place
   */

  public static void reverse(SelectionVector2 sv2) {
    int count = sv2.getCount();
    for (int i = 0; i < count / 2; i++) {
      char temp = sv2.getIndex(i);
      int dest = count - 1 - i;
      sv2.setIndex(i, sv2.getIndex(dest));
      sv2.setIndex(dest, temp);
    }
  }

  /**
   * Set a test data value from an int. Uses the type information of the
   * column to handle interval types. Else, uses the value type of the
   * accessor. The value set here is purely for testing; the mapping
   * from ints to intervals has no real meaning.
   *
   * @param rowWriter
   * @param index
   * @param value
   */

  public static void setFromInt(RowSetWriter rowWriter, int index, int value) {
    ScalarWriter writer = rowWriter.scalar(index);
    MaterializedField field = rowWriter.schema().column(index);
    writer.setObject(testDataFromInt(writer.valueType(), field.getType(), value));
  }

  public static Object testDataFromInt(ValueType valueType, MajorType dataType, int value) {
    switch (valueType) {
    case BYTES:
      return Integer.toHexString(value).getBytes();
    case DOUBLE:
      return (double) value;
    case INTEGER:
      switch (dataType.getMinorType()) {
      case BIT:
        return value & 0x01;
      case SMALLINT:
        return value & 0x7FFF;
      case UINT2:
        return value & 0xFFFF;
      case TINYINT:
        return value & 0x7F;
      case UINT1:
        return value & 0xFF;
      default:
        return value;
      }
    case LONG:
      return (long) value;
    case STRING:
      return Integer.toString(value);
    case DECIMAL:
      return BigDecimal.valueOf(value, dataType.getScale());
    case PERIOD:
      return periodFromInt(dataType.getMinorType(), value);
    default:
      throw new IllegalStateException("Unknown writer type: " + valueType);
    }
  }

  /**
   * Ad-hoc, test-only method to set a Period from an integer. Periods are made up of
   * months and millseconds. There is no mapping from one to the other, so a period
   * requires at least two number. Still, we are given just one (typically from a test
   * data generator.) Use that int value to "spread" some value across the two kinds
   * of fields. The result has no meaning, but has the same comparison order as the
   * original ints.
   *
   * @param writer column writer for a period column
   * @param minorType the Drill data type
   * @param value the integer value to apply
   * @throws VectorOverflowException
   */

  public static Period periodFromInt(MinorType minorType, int value) {
    switch (minorType) {
    case INTERVAL:
      return Duration.millis(value).toPeriod();
    case INTERVALYEAR:
      return Period.years(value / 12).withMonths(value % 12);
    case INTERVALDAY:
      int sec = value % 60;
      value = value / 60;
      int min = value % 60;
      value = value / 60;
      return Period.days(value).withMinutes(min).withSeconds(sec);
    default:
      throw new IllegalArgumentException("Writer is not an interval: " + minorType);
    }
  }

  public static void assertEqualValues(ValueType type, Object expectedObj, Object actualObj) {
    assertEqualValues(type.toString(), type, expectedObj, actualObj);
  }

  public static void assertEqualValues(String msg, ValueType type, Object expectedObj, Object actualObj) {
    switch (type) {
    case BYTES: {
        byte expected[] = (byte[]) expectedObj;
        byte actual[] = (byte[]) actualObj;
        assertEquals(msg + " - byte lengths differ", expected.length, actual.length);
        assertTrue(msg, Arrays.areEqual(expected, actual));
        break;
     }
     case DOUBLE:
       assertEquals(msg, (double) expectedObj, (double) actualObj, 0.0001);
       break;
     case INTEGER:
     case LONG:
     case STRING:
     case DECIMAL:
       assertEquals(msg, expectedObj, actualObj);
       break;
     case PERIOD: {
       Period expected = (Period) expectedObj;
       Period actual = (Period) actualObj;
       assertEquals(msg, expected.normalizedStandard(), actual.normalizedStandard());
       break;
     }
     default:
        throw new IllegalStateException( "Unexpected type: " + type);
    }
  }

  public static Object[] mapValue(Object... members) {
    return members;
  }

  public static Object[] singleMap(Object member) {
    return new Object[] { member };
  }

  public static byte[] byteArray(Integer... elements) {
    byte array[] = new byte[elements.length];
    for (int i = 0; i < elements.length; i++) {
      array[i] = (byte) (int) elements[i];
    }
    return array;
  }

  public static Long[] longArray(Long... elements) {
    return elements;
  }

  public static double[] doubleArray(Double... elements) {
    double array[] = new double[elements.length];
    for (int i = 0; i < elements.length; i++) {
      array[i] = elements[i];
    }
    return array;
  }

  public static String[] strArray(String... elements) {
    return elements;
  }

  public static int[] intArray(Integer... elements) {
    int array[] = new int[elements.length];
    for (int i = 0; i < elements.length; i++) {
      array[i] = elements[i];
    }
    return array;
  }

  public static Object[][] mapArray(Object[]... elements) {
    return elements;
  }

  public static Object[] variantArray(Object... elements) {
    return elements;
  }

  public static Object[] listValue(Object... elements) {
    return elements;
  }

  public static Object[] singleList(Object element) {
    return new Object[] { element };
  }

  public static Object[] objArray(Object... elements) {
    return elements;
  }

  public static Object[] singleObjArray(Object element) {
    return new Object[] {element};
  }

  public static void verify(RowSet expected, RowSet actual) {
    new RowSetComparison(expected).verifyAndClearAll(actual);
  }

}
