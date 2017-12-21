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
package org.apache.drill.exec.vector.accessor.writer.dummy;

import java.math.BigDecimal;

import org.apache.drill.exec.vector.BaseDataValueVector;
import org.apache.drill.exec.vector.accessor.ColumnWriterIndex;
import org.apache.drill.exec.vector.accessor.ValueType;
import org.apache.drill.exec.vector.accessor.writer.AbstractScalarWriter;
import org.joda.time.Period;

/**
 * Represents a non-projected column. The writer accepts data, but
 * discards it. The writer does not participate in writer events,
 * nor is it backed by a real vector, index or type.
 */

public class DummyScalarWriter extends AbstractScalarWriter {

  @Override
  public void bindListener(ColumnWriterListener listener) { }

  @Override
  public ValueType valueType() { return null; }

  @Override
  public void setNull() { }

  @Override
  public void setInt(int value) { }

  @Override
  public void setLong(long value) { }

  @Override
  public void setDouble(double value) { }

  @Override
  public void setString(String value) { }

  @Override
  public void setBytes(byte[] value, int len) { }

  @Override
  public void setDecimal(BigDecimal value) { }

  @Override
  public void setPeriod(Period value) { }

  @Override
  public void bindIndex(ColumnWriterIndex index) { }

  @Override
  public ColumnWriterIndex writerIndex() { return null; }

  @Override
  public void restartRow() { }

  @Override
  public void endWrite() { }

  @Override
  public void preRollover() { }

  @Override
  public void postRollover() { }

  @Override
  public int lastWriteIndex() { return 0; }

  @Override
  public BaseDataValueVector vector() { return null; }
}
