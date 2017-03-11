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
package org.apache.drill.exec.vector.accessor.impl;

import java.math.BigDecimal;

import org.apache.drill.exec.vector.accessor.AccessorUtilities;
import org.apache.drill.exec.vector.accessor.ArrayWriter;
import org.apache.drill.exec.vector.accessor.ColumnAccessor.ValueType;
import org.apache.drill.exec.vector.accessor.ColumnWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter;
import org.joda.time.Period;

/**
 * Implementation for a writer for a tuple (a row or a map.) Provides access to each
 * column using either a name or a numeric index.
 */

public class TupleWriterImpl extends AbstractTupleAccessor implements TupleWriter {

  private final AbstractColumnWriter writers[];

  public TupleWriterImpl(TupleSchema schema, AbstractColumnWriter writers[]) {
    super(schema);
    this.writers = writers;
  }

  public void start() {
    for (int i = 0; i < writers.length;  i++) {
      writers[i].start();
    }
  }

  @Override
  public ColumnWriter column(int colIndex) {
    return writers[colIndex];
  }

  @Override
  public ColumnWriter column(String colName) {
    int index = schema.columnIndex(colName);
    if (index == -1) {
      return null; }
    return writers[index];
  }

  @Override
  public void set(int colIndex, Object value) {
    ColumnWriter colWriter = column(colIndex);
    if (value == null) {
      // Arrays have no null concept, just an empty array.
      if (colWriter.valueType() != ValueType.ARRAY) {
        colWriter.setNull();
      }
    } else if (value instanceof Integer) {
      colWriter.setInt((Integer) value);
    } else if (value instanceof Long) {
      colWriter.setLong((Long) value);
    } else if (value instanceof String) {
      colWriter.setString((String) value);
    } else if (value instanceof BigDecimal) {
      colWriter.setDecimal((BigDecimal) value);
    } else if (value instanceof Period) {
      colWriter.setPeriod((Period) value);
    } else if (value instanceof byte[]) {
      colWriter.setBytes((byte[]) value);
    } else if (value instanceof Byte) {
      colWriter.setInt((Byte) value);
    } else if (value instanceof Short) {
      colWriter.setInt((Short) value);
    } else if (value instanceof Double) {
      colWriter.setDouble((Double) value);
    } else if (value instanceof Float) {
      colWriter.setDouble((Float) value);
    } else if (value.getClass().getName().startsWith("[")) {
      setArray(colIndex, value);
    } else {
      throw new IllegalArgumentException("Unsupported type " +
                value.getClass().getSimpleName() + " for column " + colIndex);
    }
  }

  public void setArray(int colIndex, Object value) {
    if (value == null) {
      // Assume null means a 0-element array since Drill does
      // not support null for the whole array.

      return;
    }
    String objClass = value.getClass().getName();
    if (!objClass.startsWith("[")) {
      throw new IllegalArgumentException("Argument is not an array");
    }

    ColumnWriter colWriter = column(colIndex);
    if (colWriter.valueType() != ValueType.ARRAY) {
      throw new IllegalArgumentException("Column is not an array");
    }

    ArrayWriter arrayWriter = colWriter.array();

    // Figure out type

    char second = objClass.charAt( 1 );
    switch ( second ) {
    case  'B':
      AccessorUtilities.setByteArray(arrayWriter, (byte[]) value );
      break;
    case  'S':
      AccessorUtilities.setShortArray(arrayWriter, (short[]) value );
      break;
    case  'I':
      AccessorUtilities.setIntArray(arrayWriter, (int[]) value );
      break;
    case  'J':
      AccessorUtilities.setLongArray(arrayWriter, (long[]) value );
      break;
    case  'F':
      AccessorUtilities.setFloatArray(arrayWriter, (float[]) value );
      break;
    case  'D':
      AccessorUtilities.setDoubleArray(arrayWriter, (double[]) value );
      break;
    case  'Z':
      AccessorUtilities.setBooleanArray(arrayWriter, (boolean[]) value );
      break;
    case 'L':
     int posn = objClass.indexOf(';');

      // If the array is of type Object, then we have no type info.

      String memberClassName = objClass.substring( 2, posn );
      if (memberClassName.equals(String.class.getName())) {
        AccessorUtilities.setStringArray(arrayWriter, (String[]) value );
      } else if (memberClassName.equals(Period.class.getName())) {
        AccessorUtilities.setPeriodArray(arrayWriter, (Period[]) value );
      } else if (memberClassName.equals(BigDecimal.class.getName())) {
        AccessorUtilities.setBigDecimalArray(arrayWriter, (BigDecimal[]) value );
      } else {
        throw new IllegalArgumentException( "Unknown Java array type: " + memberClassName );
      }
      break;
    default:
      throw new IllegalArgumentException( "Unknown Java array type: " + second );
    }
  }
}
