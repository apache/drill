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

import org.apache.drill.exec.vector.NullableVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.ColumnAccessors.UInt1ColumnWriter;
import org.apache.drill.exec.vector.accessor.ColumnWriterIndex;
import org.apache.drill.exec.vector.accessor.ValueType;
import org.joda.time.Period;

public class NullableScalarWriter extends AbstractScalarWriter {

  private final UInt1ColumnWriter isSetWriter = new UInt1ColumnWriter();
  private final BaseScalarWriter baseWriter;

  public NullableScalarWriter(BaseScalarWriter baseWriter) {
    this.baseWriter = baseWriter;
  }

  public static ScalarObjectWriter build(ValueVector vector, BaseScalarWriter baseWriter) {
    NullableScalarWriter writer = new NullableScalarWriter(baseWriter);
    writer.bindVector(vector);
    return new ScalarObjectWriter(writer);
  }

  @Override
  public void bindVector(ValueVector vector) {
    NullableVector nullableVector = (NullableVector) vector;
    baseWriter.bindVector(nullableVector.getValuesVector());
    isSetWriter.bindVector(nullableVector.getBitsVector());
  }

  @Override
  public void bindIndex(ColumnWriterIndex index) {
    isSetWriter.bindIndex(index);
    baseWriter.bindIndex(index);
  }

  @Override
  public void finish() {
    isSetWriter.finish();
    baseWriter.setLastWriteIndex(isSetWriter.lastWriteIndex());
    baseWriter.finish();
  }

  @Override
  public ValueType valueType() {
    return baseWriter.valueType();
  }

  @Override
  public void setNull() {
    isSetWriter.setInt(0);
    baseWriter.setLastWriteIndex(isSetWriter.lastWriteIndex());
  }

  @Override
  public void setInt(int value) {
    isSetWriter.setInt(1);
    baseWriter.setInt(value);
  }

  @Override
  public void setLong(long value) {
    isSetWriter.setInt(1);
    baseWriter.setLong(value);
  }

  @Override
  public void setDouble(double value) {
    isSetWriter.setInt(1);
    baseWriter.setDouble(value);
  }

  @Override
  public void setString(String value) {
    isSetWriter.setInt(1);
    baseWriter.setString(value);
  }

  @Override
  public void setBytes(byte[] value, int len) {
    isSetWriter.setInt(1);
    baseWriter.setBytes(value, len);
  }

  @Override
  public void setDecimal(BigDecimal value) {
    isSetWriter.setInt(1);
    baseWriter.setDecimal(value);
  }

  @Override
  public void setPeriod(Period value) {
    isSetWriter.setInt(1);
    baseWriter.setPeriod(value);
  }
}
