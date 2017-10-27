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

import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.ArrayWriter;
import org.apache.drill.exec.vector.complex.BaseRepeatedValueVector;
import org.joda.time.Period;

/**
 * Writer for an array-valued column. This writer appends values: once a value
 * is written, it cannot be changed. As a result, writer methods have no item index;
 * each set advances the array to the next position. This is an abstract base class;
 * subclasses are generated for each repeated value vector type.
 */

public abstract class AbstractArrayWriter extends AbstractColumnAccessor implements ArrayWriter {

  /**
   * Column writer that provides access to an array column by returning a
   * separate writer specifically for that array. That is, writing an array
   * is a two-part process:<pre><code>
   * tupleWriter.column("arrayCol").array().setInt(2);</code></pre>
   * This pattern is used to avoid overloading the column reader with
   * both scalar and array access. Also, this pattern mimics the way
   * that nested tuples (Drill maps) are handled.
   */

  public static class ArrayColumnWriter extends AbstractColumnWriter {

    private final AbstractArrayWriter arrayWriter;

    public ArrayColumnWriter(AbstractArrayWriter arrayWriter) {
      this.arrayWriter = arrayWriter;
    }

    @Override
    public ValueType valueType() {
      return ValueType.ARRAY;
    }

    @Override
    public void bind(RowIndex rowIndex, ValueVector vector) {
      arrayWriter.bind(rowIndex, vector);
      vectorIndex = rowIndex;
    }

    @Override
    public ArrayWriter array() {
      return arrayWriter;
    }

    /**
     * Arrays require a start step for each row, regardless of
     * whether any values are written for that row.
     */

    public void start() {
      arrayWriter.mutator().startNewValue(vectorIndex.index());
    }
  }

  protected abstract BaseRepeatedValueVector.BaseRepeatedMutator mutator();

  @Override
  public int size() {
    return mutator().getInnerValueCountAt(vectorIndex.index());
  }

  @Override
  public boolean valid() {
    // Not implemented yet
    return true;
  }

  @Override
  public void setInt(int value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setLong(long value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setDouble(double value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setString(String value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setBytes(byte[] value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setDecimal(BigDecimal value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setPeriod(Period value) {
    throw new UnsupportedOperationException();
  }
}
