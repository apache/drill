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

import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.ArrayReader;
import org.apache.drill.exec.vector.accessor.TupleReader;
import org.apache.drill.exec.vector.accessor.impl.AbstractColumnReader.VectorAccessor;
import org.joda.time.Period;

/**
 * Reader for an array-valued column. This reader provides access to specific
 * array members via an array index. This is an abstract base class;
 * subclasses are generated for each repeated value vector type.
 */

public abstract class AbstractArrayReader extends AbstractColumnAccessor implements ArrayReader {

  /**
   * Column reader that provides access to an array column by returning a
   * separate reader specifically for that array. That is, reading a column
   * is a two-part process:<pre><code>
   * tupleReader.column("arrayCol").array().getInt(2);</code></pre>
   * This pattern is used to avoid overloading the column reader with
   * both scalar and array access. Also, this pattern mimics the way
   * that nested tuples (Drill maps) are handled.
   */

  public static class ArrayColumnReader extends AbstractColumnReader {

    private final AbstractArrayReader arrayReader;

    public ArrayColumnReader(AbstractArrayReader arrayReader) {
      this.arrayReader = arrayReader;
    }

    @Override
    public ValueType valueType() {
       return ValueType.ARRAY;
    }

    @Override
    public void bind(RowIndex rowIndex, ValueVector vector) {
      arrayReader.bind(rowIndex, vector);
      vectorIndex = rowIndex;
    }

    @Override
    public ArrayReader array() {
      return arrayReader;
    }
  }

  protected VectorAccessor vectorAccessor;

  public void bind(RowIndex rowIndex, MaterializedField field, VectorAccessor va) {
    bind(rowIndex);
    vectorAccessor = va;
  }

  @Override
  public boolean isNull(int index) {
    return false;
  }

  @Override
  public int getInt(int index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getLong(int index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public double getDouble(int index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getString(int index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte[] getBytes(int index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public BigDecimal getDecimal(int index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Period getPeriod(int index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public TupleReader map(int index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ArrayReader array(int index) {
    throw new UnsupportedOperationException();
  }
}
