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
package org.apache.drill.exec.physical.rowSet.impl;

import java.math.BigDecimal;

import org.apache.drill.exec.physical.rowSet.ArrayLoader;
import org.apache.drill.exec.physical.rowSet.TupleLoader;
import org.joda.time.Period;

/**
 * Abstract base class for structured types (maps and lists). Primarly marks
 * the scalar methods as unsupported.
 */

public abstract class AbstractStructuredLoader implements ColumnLoaderImpl {

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

  @Override
  public TupleLoader map() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ArrayLoader array() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setNull() {
    throw new UnsupportedOperationException();
  }
}
