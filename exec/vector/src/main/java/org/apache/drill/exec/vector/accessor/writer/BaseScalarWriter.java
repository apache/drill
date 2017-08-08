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

import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.ColumnWriterIndex;
import org.joda.time.Period;

/**
 * Column writer implementation that acts as the basis for the
 * generated, vector-specific implementations. All set methods
 * throw an exception; subclasses simply override the supported
 * method(s).
 */

public abstract class BaseScalarWriter extends AbstractScalarWriter {

  protected ColumnWriterIndex vectorIndex;
  protected int lastWriteIndex;
  protected long bufAddr;
  protected int capacity;

  public static ScalarObjectWriter build(ValueVector vector, BaseScalarWriter writer) {
    writer.bindVector(vector);
    return new ScalarObjectWriter(writer);
  }

  @Override
  public void bindIndex(ColumnWriterIndex vectorIndex) {
    this.vectorIndex = vectorIndex;
  }

  @Override
  public void startWrite() { lastWriteIndex = -1; }
  public int lastWriteIndex() { return lastWriteIndex; }
  public void setLastWriteIndex(int index) { lastWriteIndex = index; }

  @Override
  public void setNull() {
    throw new UnsupportedOperationException("Vector is not nullable");
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
  public void setBytes(byte[] value, int len) {
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
