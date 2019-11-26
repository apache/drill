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
package org.apache.drill.exec.vector.accessor.convert;

import java.math.BigDecimal;

import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.vector.accessor.ColumnReader;
import org.apache.drill.exec.vector.accessor.ObjectType;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.apache.drill.exec.vector.accessor.ValueType;
import org.apache.drill.exec.vector.accessor.writer.AbstractScalarWriter;
import org.joda.time.Instant;
import org.joda.time.LocalDate;
import org.joda.time.LocalTime;
import org.joda.time.Period;

/**
 * Base class for type converting scalar column writers. All methods
 * pass through to the base writer. Override selected "set" methods to
 * perform the type conversion, such as overriding "setString" to convert
 * from a string representation of a value to the actual format.
 * <p>
 * The {@link #setObject(Object)} method works here: the object is passed
 * to this class's set methods, allowing, say, setting a string object
 * for an int column in the case above.
 */

public abstract class AbstractWriteConverter extends AbstractScalarWriter {

  protected final ScalarWriter baseWriter;

  public AbstractWriteConverter(ScalarWriter baseWriter) {
    this.baseWriter = baseWriter;
  }

  @Override
  public ValueType valueType() {
    return baseWriter.valueType();
  }

  @Override
  public ObjectType type() {
    return baseWriter.type();
  }

  @Override
  public boolean isProjected() {
    return baseWriter.isProjected();
  }

  @Override
  public boolean nullable() {
    return baseWriter.nullable();
  }

  @Override
  public ColumnMetadata schema() {
    return baseWriter.schema();
  }

  @Override
  public void setDefaultValue(Object value) {
    throw new IllegalStateException(
        "Cannot set a default value through a shim; types conflict: " + value);
  }

  @Override
  public void setNull() {
    baseWriter.setNull();
  }

  @Override
  public void setBoolean(boolean value) {
    baseWriter.setBoolean(value);
  }

  @Override
  public void setInt(int value) {
    baseWriter.setInt(value);
  }

  @Override
  public void setLong(long value) {
    baseWriter.setLong(value);
  }

  @Override
  public void setDouble(double value) {
    baseWriter.setDouble(value);
  }

  @Override
  public void setString(String value) {
    baseWriter.setString(value);
  }

  @Override
  public void setBytes(byte[] value, int len) {
    baseWriter.setBytes(value, len);
  }

  @Override
  public void appendBytes(byte[] value, int len) {
    throw conversionError("bytes");
  }

  @Override
  public void setDecimal(BigDecimal value) {
    baseWriter.setDecimal(value);
  }

  @Override
  public void setPeriod(Period value) {
    baseWriter.setPeriod(value);
  }

  @Override
  public void setDate(LocalDate value) {
    baseWriter.setDate(value);
  }

  @Override
  public void setTime(LocalTime value) {
    baseWriter.setTime(value);
  }

  @Override
  public void setTimestamp(Instant value) {
    baseWriter.setTimestamp(value);
  }

  @Override
  public final void copy(ColumnReader from) {
    throw new UnsupportedOperationException("Cannot copy values through a type converter");
  }
}
