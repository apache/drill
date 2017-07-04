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

import java.math.BigDecimal;

import org.apache.drill.exec.physical.rowSet.ArrayLoader;
import org.apache.drill.exec.vector.accessor.ArrayWriter;
import org.apache.drill.exec.vector.accessor.ColumnAccessor.ValueType;
import org.apache.drill.exec.vector.accessor.impl.AbstractColumnWriter;
import org.joda.time.Period;

/**
 * Represents an array (repeated) column. An array column returns an array
 * loader to handle writing. Thus, there are two parts: the column itself,
 * the value of which is an array. The array is a structure, so another
 * loader writes the array members.
 */

public class ArrayColumnLoader extends AbstractStructuredLoader {

  /**
   * Represents each array member. Wraps a generated repeated column writer.
   */

  public static class ArrayMemberLoader extends AbstractScalarLoader implements ArrayLoader {

    private final ArrayWriter arrayWriter;

    protected ArrayMemberLoader(WriterIndexImpl index, ArrayWriter writer) {
      super(index, writer);
      arrayWriter = writer;
    }

    @Override
    public int size() { return arrayWriter.size(); }

    @Override
    public void setNull() {
      // Arrays have no null concept, just an empty array.
    }

    @Override
    public void setArray(Object value) {
      String objClass = value.getClass().getName();
      if (!objClass.startsWith("[")) {
        throw new IllegalArgumentException("Argument is not an array");
      }

      // Figure out type

      char second = objClass.charAt( 1 );
      switch ( second ) {
      case  'B':
        setByteArray((byte[]) value );
        break;
      case  'S':
        setShortArray((short[]) value );
        break;
      case  'I':
        setIntArray((int[]) value );
        break;
      case  'J':
        setLongArray((long[]) value );
        break;
      case  'F':
        setFloatArray((float[]) value );
        break;
      case  'D':
        setDoubleArray((double[]) value );
        break;
      case  'Z':
        setBooleanArray((boolean[]) value );
        break;
      case 'L':
       int posn = objClass.indexOf(';');

        // If the array is of type Object, then we have no type info.

        String memberClassName = objClass.substring( 2, posn );
        if (memberClassName.equals(String.class.getName())) {
          setStringArray((String[]) value );
        } else if (memberClassName.equals(Period.class.getName())) {
          setPeriodArray((Period[]) value );
        } else if (memberClassName.equals(BigDecimal.class.getName())) {
          setBigDecimalArray((BigDecimal[]) value );
        } else {
          throw new IllegalArgumentException( "Unknown Java array type: " + memberClassName );
        }
        break;
      default:
        throw new IllegalArgumentException( "Unknown Java array type: " + second );
      }
    }

    public void setBooleanArray(boolean[] value) {
      for (int i = 0; i < value.length; i++) {
        setInt(value[i] ? 1 : 0);
      }
    }

    public void setByteArray(byte[] value) {
      for (int i = 0; i < value.length; i++) {
        setInt(value[i]);
      }
    }

    public void setShortArray(short[] value) {
      for (int i = 0; i < value.length; i++) {
        setInt(value[i]);
      }
    }

    public void setIntArray(int[] value) {
      for (int i = 0; i < value.length; i++) {
        setInt(value[i]);
      }
    }

    public void setLongArray(long[] value) {
      for (int i = 0; i < value.length; i++) {
        setLong(value[i]);
      }
    }

    public void setFloatArray(float[] value) {
      for (int i = 0; i < value.length; i++) {
        setDouble(value[i]);
      }
    }

    public void setDoubleArray(double[] value) {
      for (int i = 0; i < value.length; i++) {
        setDouble(value[i]);
      }
    }

    public void setStringArray(String[] value) {
      for (int i = 0; i < value.length; i++) {
        setString(value[i]);
      }
    }

    public void setPeriodArray(Period[] value) {
      for (int i = 0; i < value.length; i++) {
        setPeriod(value[i]);
      }
    }

    public void setBigDecimalArray(BigDecimal[] value) {
      for (int i = 0; i < value.length; i++) {
        setDecimal(value[i]);
      }
    }
  }

  private final AbstractColumnWriter writer;
  private final ArrayMemberLoader member;

  public ArrayColumnLoader(WriterIndexImpl writerIndex, AbstractColumnWriter writer) {
    this.writer = writer;
    member = new ArrayMemberLoader(writerIndex, writer.array());
  }

  @Override
  public ValueType valueType() { return writer.valueType(); }

  @Override
  public int writeIndex() { return writer.lastWriteIndex(); }

  @Override
  public void reset() { writer.reset(); }

  @Override
  public void resetTo(int dest) { writer.reset(dest); }

  @Override
  public ArrayLoader array() { return member; }

  @Override
  public void set(Object value) {
    member.setArray(value);
  }
}
