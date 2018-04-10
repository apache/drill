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

import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.ColumnReaderIndex;
import org.apache.drill.exec.vector.accessor.ObjectType;
import org.apache.drill.exec.vector.accessor.ScalarReader;
import org.apache.drill.exec.vector.accessor.impl.AccessorUtilities;
import org.joda.time.Period;

/**
 * Column reader implementation that acts as the basis for the
 * generated, vector-specific implementations. All set methods
 * throw an exception; subclasses simply override the supported
 * method(s).
 */

public abstract class BaseScalarReader implements ScalarReader {

  public static class ScalarObjectReader extends AbstractObjectReader {

    private BaseScalarReader scalarReader;

    public ScalarObjectReader(BaseScalarReader scalarReader) {
      this.scalarReader = scalarReader;
    }

    @Override
    public void bindIndex(ColumnReaderIndex index) {
      scalarReader.bindIndex(index);
    }

    @Override
    public ObjectType type() {
      return ObjectType.SCALAR;
    }

    @Override
    public ScalarReader scalar() {
      return scalarReader;
    }

    @Override
    public Object getObject() {
      return scalarReader.getObject();
    }

    @Override
    public String getAsString() {
      return scalarReader.getAsString();
    }
  }

  protected ColumnReaderIndex vectorIndex;
  protected VectorAccessor vectorAccessor;

  public static ScalarObjectReader build(ValueVector vector, BaseScalarReader reader) {
    reader.bindVector(vector);
    return new ScalarObjectReader(reader);
  }

  public static AbstractObjectReader build(MajorType majorType, VectorAccessor va,
                                           BaseScalarReader reader) {
    reader.bindVector(majorType, va);
    return new ScalarObjectReader(reader);
  }

  public abstract void bindVector(ValueVector vector);

  protected void bindIndex(ColumnReaderIndex rowIndex) {
    this.vectorIndex = rowIndex;
    if (vectorAccessor != null) {
      vectorAccessor.bind(rowIndex);
    }
  }

  public void bindVector(MajorType majorType, VectorAccessor va) {
    vectorAccessor = va;
  }

  @Override
  public Object getObject() {
    if (isNull()) {
      return null;
    }
    switch (valueType()) {
    case BYTES:
      return getBytes();
    case DECIMAL:
      return getDecimal();
    case DOUBLE:
      return getDouble();
    case INTEGER:
      return getInt();
    case LONG:
      return getLong();
    case PERIOD:
      return getPeriod();
    case STRING:
      return getString();
    default:
      throw new IllegalStateException("Unexpected type: " + valueType());
    }
  }

  @Override
  public String getAsString() {
    if (isNull()) {
      return "null";
    }
    switch (valueType()) {
    case BYTES:
      return AccessorUtilities.bytesToString(getBytes());
    case DOUBLE:
      return Double.toString(getDouble());
    case INTEGER:
      return Integer.toString(getInt());
    case LONG:
      return Long.toString(getLong());
    case STRING:
      return "\"" + getString() + "\"";
    case DECIMAL:
      return getDecimal().toPlainString();
    case PERIOD:
      return getPeriod().normalizedStandard().toString();
    default:
      throw new IllegalArgumentException("Unsupported type " + valueType());
    }
  }

  @Override
  public boolean isNull() {
    return false;
  }

  @Override
  public int getInt() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getLong() {
    throw new UnsupportedOperationException();
  }

  @Override
  public double getDouble() {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getString() {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte[] getBytes() {
    throw new UnsupportedOperationException();
  }

  @Override
  public BigDecimal getDecimal() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Period getPeriod() {
    throw new UnsupportedOperationException();
  }
}
