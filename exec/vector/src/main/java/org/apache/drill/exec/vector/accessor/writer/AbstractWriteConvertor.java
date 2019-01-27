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
import org.apache.drill.exec.vector.accessor.ObjectType;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.apache.drill.exec.vector.accessor.ValueType;
import org.apache.drill.exec.vector.accessor.impl.HierarchicalFormatter;
import org.joda.time.Period;

/**
 * Base class for type converting scalar column writers. All methods
 * pass through to the base writer. Override selected "set" methods to
 * perform the type conversion, such as overriding "setString" to convert
 * from a string representation of a value to the actual format.
 * <p>
 * The {@link #setObject()} method works here: the object is passed
 * to this class's set methods, allowing, say, setting a string object
 * for an int column in the case above.
 */

// TODO: This organization works fine, but is a bit heavy-weight.
// It may be time to think about separating the pure writer aspect of
// a column writer from its plumbing aspects. That is, the base
// ConcreteWriter class combines the public API (ScalarWriter) with
// the internal implementation (WriterEvents) into a single class.
// Might be worth using composition rather than inheritance to keep
// these aspects distinct.

public class AbstractWriteConvertor extends ConcreteWriter {

  private final ConcreteWriter baseWriter;

  public AbstractWriteConvertor(ScalarWriter baseWriter) {
    this.baseWriter = (ConcreteWriter) baseWriter;
  }

  @Override
  public ValueType valueType() {
    return baseWriter.valueType();
  }

  @Override
  public int lastWriteIndex() {
    return baseWriter.lastWriteIndex();
  }

  @Override
  public void restartRow() {
    baseWriter.restartRow();
  }

  @Override
  public void endWrite() {
    baseWriter.endWrite();
  }

  @Override
  public void preRollover() {
    baseWriter.preRollover();
  }

  @Override
  public void postRollover() {
    baseWriter.postRollover();
  }

  @Override
  public ObjectType type() {
    return baseWriter.type();
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
  public void setNull() {
    baseWriter.setNull();
  }

  @Override
  public int rowStartIndex() {
    return baseWriter.rowStartIndex();
  }

  @Override
  public int writeIndex() {
    return baseWriter.writeIndex();
  }

  @Override
  public void bindListener(ColumnWriterListener listener) {
    baseWriter.bindListener(listener);
  }

  @Override
  public void bindIndex(ColumnWriterIndex index) {
    baseWriter.bindIndex(index);
  }

  @Override
  public void startWrite() {
    baseWriter.startWrite();
  }

  @Override
  public void startRow() {
    baseWriter.startRow();
  }

  @Override
  public void endArrayValue() {
    baseWriter.endArrayValue();
  }

  @Override
  public void saveRow() {
    baseWriter.saveRow();
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
  public void setDecimal(BigDecimal value) {
    baseWriter.setDecimal(value);
  }

  @Override
  public void setPeriod(Period value) {
    baseWriter.setPeriod(value);
  }

  @Override
  public void dump(HierarchicalFormatter format) {
    baseWriter.dump(format);
  }
}
