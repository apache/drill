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
package org.apache.drill.exec.vector.accessor.writer;

import java.math.BigDecimal;

import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.vector.accessor.ColumnWriterIndex;
import org.apache.drill.exec.vector.accessor.ScalarWriter.ColumnWriterListener;
import org.apache.drill.exec.vector.accessor.writer.AbstractArrayWriter.BaseArrayWriter;
import org.apache.drill.exec.vector.accessor.writer.AbstractScalarWriter.ScalarObjectWriter;
import org.apache.drill.exec.vector.complex.RepeatedValueVector;
import org.joda.time.Period;

/**
 * Writer for a column that holds an array of scalars. This writer manages
 * the array itself. A type-specific child writer manages the elements within
 * the array. The overall row index (usually) provides the index into
 * the offset vector. An array-specific element index provides the index
 * into elements.
 * <p>
 * This class manages the offset vector directly. Doing so saves one read and
 * one write to direct memory per element value.
 * <p>
 * Provides generic write methods for testing and other times when
 * convenience is more important than speed.
 * <p>
 * The scalar writer for array-valued columns appends values: once a value
 * is written, it cannot be changed. As a result, writer methods have no item index;
 * each set advances the array to the next position. This is an abstract base class;
 * subclasses are generated for each repeated value vector type.
 */

public class ScalarArrayWriter extends BaseArrayWriter {

  /**
   * For scalar arrays, incrementing the element index and
   * committing the current value is done automatically since
   * there is exactly one value per array element.
   */

  public class ScalarElementWriterIndex extends ArrayElementWriterIndex {

    @Override
    public final void nextElement() { next(); }
  }

  private final BaseScalarWriter elementWriter;

  public ScalarArrayWriter(ColumnMetadata schema,
      RepeatedValueVector vector, BaseScalarWriter elementWriter) {
    super(vector.getOffsetVector(),
        new ScalarObjectWriter(schema, elementWriter));
    this.elementWriter = elementWriter;
  }

  public static ArrayObjectWriter build(ColumnMetadata schema,
      RepeatedValueVector repeatedVector, BaseScalarWriter elementWriter) {
    return new ArrayObjectWriter(schema,
        new ScalarArrayWriter(schema, repeatedVector, elementWriter));
  }

  @Override
  public void bindIndex(ColumnWriterIndex index) {
    elementIndex = new ScalarElementWriterIndex();
    super.bindIndex(index);
    elementWriter.bindIndex(elementIndex);
  }

  @Override
  public void bindListener(ColumnWriterListener listener) {
    elementWriter.bindListener(listener);
  }

  @Override
  public void save() {
    // No-op: done when writing each scalar value
  }

  @Override
  public void set(Object... values) {
    for (Object value : values) {
      entry().set(value);
    }
  }

  @Override
  public void setObject(Object array) {
    if (array == null) {
      // Assume null means a 0-element array since Drill does
      // not support null for the whole array.

      return;
    }
    String objClass = array.getClass().getName();
    if (! objClass.startsWith("[")) {
      throw new IllegalArgumentException("Argument must be an array");
    }

    // Figure out type

    char second = objClass.charAt(1);
    switch ( second ) {
    case  '[':
      // bytes is represented as an array of byte arrays.

      char third = objClass.charAt(2);
      switch (third) {
      case 'B':
        setBytesArray((byte[][]) array);
        break;
      default:
        throw new IllegalArgumentException( "Unknown Java array type: " + objClass );
      }
      break;
    case  'S':
      setShortArray((short[]) array );
      break;
    case  'I':
      setIntArray((int[]) array );
      break;
    case  'J':
      setLongArray((long[]) array );
      break;
    case  'F':
      setFloatArray((float[]) array );
      break;
    case  'D':
      setDoubleArray((double[]) array );
      break;
    case  'Z':
      setBooleanArray((boolean[]) array );
      break;
    case 'L':
      int posn = objClass.indexOf(';');

      // If the array is of type Object, then we have no type info.

      String memberClassName = objClass.substring( 2, posn );
      if (memberClassName.equals(String.class.getName())) {
        setStringArray((String[]) array );
      } else if (memberClassName.equals(Period.class.getName())) {
        setPeriodArray((Period[]) array );
      } else if (memberClassName.equals(BigDecimal.class.getName())) {
        setBigDecimalArray((BigDecimal[]) array );
      } else {
        throw new IllegalArgumentException( "Unknown Java array type: " + memberClassName );
      }
      break;
    default:
      throw new IllegalArgumentException( "Unknown Java array type: " + objClass );
    }
  }

  public void setBooleanArray(boolean[] value) {
    for (int i = 0; i < value.length; i++) {
      elementWriter.setInt(value[i] ? 1 : 0);
    }
  }

  public void setBytesArray(byte[][] value) {
    for (int i = 0; i < value.length; i++) {
      elementWriter.setBytes(value[i], value[i].length);
    }
  }

  public void setShortArray(short[] value) {
    for (int i = 0; i < value.length; i++) {
      elementWriter.setInt(value[i]);
    }
  }

  public void setIntArray(int[] value) {
    for (int i = 0; i < value.length; i++) {
      elementWriter.setInt(value[i]);
    }
  }

  public void setLongArray(long[] value) {
    for (int i = 0; i < value.length; i++) {
      elementWriter.setLong(value[i]);
    }
  }

  public void setFloatArray(float[] value) {
    for (int i = 0; i < value.length; i++) {
      elementWriter.setDouble(value[i]);
    }
  }

  public void setDoubleArray(double[] value) {
    for (int i = 0; i < value.length; i++) {
      elementWriter.setDouble(value[i]);
    }
  }

  public void setStringArray(String[] value) {
    for (int i = 0; i < value.length; i++) {
      elementWriter.setString(value[i]);
    }
  }

  public void setPeriodArray(Period[] value) {
    for (int i = 0; i < value.length; i++) {
      elementWriter.setPeriod(value[i]);
    }
  }

  public void setBigDecimalArray(BigDecimal[] value) {
    for (int i = 0; i < value.length; i++) {
      elementWriter.setDecimal(value[i]);
    }
  }
}
