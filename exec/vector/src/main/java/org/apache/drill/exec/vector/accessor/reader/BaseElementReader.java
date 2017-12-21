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
package org.apache.drill.exec.vector.accessor.reader;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.ColumnReaderIndex;
import org.apache.drill.exec.vector.accessor.ObjectType;
import org.apache.drill.exec.vector.accessor.ScalarElementReader;
import org.apache.drill.exec.vector.accessor.impl.AccessorUtilities;
import org.joda.time.Period;

public abstract class BaseElementReader implements ScalarElementReader {

  public static class ScalarElementObjectReader extends AbstractObjectReader {

    private BaseElementReader elementReader;

    public ScalarElementObjectReader(BaseElementReader elementReader) {
      this.elementReader = elementReader;
    }

    @Override
    public void bindIndex(ColumnReaderIndex index) {
      elementReader.bindIndex((ElementReaderIndex) index);
    }

    @Override
    public ObjectType type() {
      return ObjectType.SCALAR;
    }

    @Override
    public ScalarElementReader elements() {
      return elementReader;
    }

    @Override
    public Object getObject() {
      // Simple: return elements as an object list.
      // If really needed, could return as a typed array, but that
      // is a bit of a hassle.

      List<Object> elements = new ArrayList<>();
      for (int i = 0; i < elementReader.size(); i++) {
        elements.add(elementReader.getObject(i));
      }
      return elements;
    }

    @Override
    public String getAsString() {
      StringBuilder buf = new StringBuilder();
      buf.append("[");
      for (int i = 0; i < elementReader.size(); i++) {
        if (i > 0) {
          buf.append( ", " );
        }
        buf.append(elementReader.getAsString(i));
      }
      buf.append("]");
      return buf.toString();
    }
  }

  protected ElementReaderIndex vectorIndex;
  protected VectorAccessor vectorAccessor;

  public abstract void bindVector(ValueVector vector);

  public void bindVector(MajorType majorType, VectorAccessor va) {
    vectorAccessor = va;
  }

  protected void bindIndex(ElementReaderIndex rowIndex) {
    this.vectorIndex = rowIndex;
  }

  @Override
  public int size() { return vectorIndex.size(); }

  @Override
  public Object getObject(int index) {
    if (isNull(index)) {
      return "null";
    }
    switch (valueType()) {
    case BYTES:
      return getBytes(index);
    case DECIMAL:
      return getDecimal(index);
    case DOUBLE:
      return getDouble(index);
    case INTEGER:
      return getInt(index);
    case LONG:
      return getLong(index);
    case PERIOD:
      return getPeriod(index);
    case STRING:
      return getString(index);
    default:
      throw new IllegalStateException("Unexpected type: " + valueType());
    }
  }

  @Override
  public String getAsString(int index) {
    switch (valueType()) {
    case BYTES:
      return AccessorUtilities.bytesToString(getBytes(index));
    case DOUBLE:
      return Double.toString(getDouble(index));
    case INTEGER:
      return Integer.toString(getInt(index));
    case LONG:
      return Long.toString(getLong(index));
    case STRING:
      return "\"" + getString(index) + "\"";
    case DECIMAL:
      return getDecimal(index).toPlainString();
    case PERIOD:
      return getPeriod(index).normalizedStandard().toString();
    default:
      throw new IllegalArgumentException("Unsupported type " + valueType());
    }
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
}
