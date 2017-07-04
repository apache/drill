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
import org.apache.drill.exec.vector.VectorOverflowException;
import org.apache.drill.exec.vector.accessor.ArrayWriter;
import org.apache.drill.exec.vector.accessor.ColumnWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter;
import org.apache.drill.exec.vector.accessor.ColumnWriterIndex;
import org.joda.time.Period;

/**
 * Column writer implementation that acts as the basis for the
 * generated, vector-specific implementations. All set methods
 * throw an exception; subclasses simply override the supported
 * method(s).
 */

public abstract class AbstractColumnWriter implements ColumnWriter {

  protected ColumnWriterIndex vectorIndex;
  protected int lastWriteIndex;

  public abstract void bind(ColumnWriterIndex rowIndex, ValueVector vector);

  protected void bind(ColumnWriterIndex rowIndex) {
    vectorIndex = rowIndex;
  }

  public ColumnWriterIndex vectorIndex() { return vectorIndex; }

  public void start() { }
  public void reset() { lastWriteIndex = -1; }
  public void reset(int index) { lastWriteIndex = index - 1; }
  public int lastWriteIndex() { return lastWriteIndex; }

  public abstract void finishBatch() throws VectorOverflowException;

  @Override
  public void setNull() throws VectorOverflowException {
    throw new UnsupportedOperationException("setNull");
  }

  @Override
  public void setInt(int value) throws VectorOverflowException {
    throw new UnsupportedOperationException("setInt");
  }

  @Override
  public void setLong(long value) throws VectorOverflowException {
    throw new UnsupportedOperationException("setLong");
  }

  @Override
  public void setDouble(double value) throws VectorOverflowException {
    throw new UnsupportedOperationException("setDouble");
  }

  @Override
  public void setString(String value) throws VectorOverflowException {
    throw new UnsupportedOperationException("setString");
  }

  @Override
  public void setBytes(byte[] value, int len) throws VectorOverflowException {
    throw new UnsupportedOperationException("setBytes");
  }

  @Override
  public void setDecimal(BigDecimal value) throws VectorOverflowException {
    throw new UnsupportedOperationException("setDecimal");
  }

  @Override
  public void setPeriod(Period value) throws VectorOverflowException {
    throw new UnsupportedOperationException("setPeriod");
  }

  @Override
  public TupleWriter map() {
    throw new UnsupportedOperationException("map");
  }

  @Override
  public ArrayWriter array() {
    throw new UnsupportedOperationException("array");
  }
}
