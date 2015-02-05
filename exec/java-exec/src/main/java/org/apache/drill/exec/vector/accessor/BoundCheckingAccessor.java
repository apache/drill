/**
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
package org.apache.drill.exec.vector.accessor;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

import org.apache.drill.exec.vector.ValueVector;

/**
 * A decorating accessor that returns null for indices that is beyond underlying vector's capacity.
 */
public class BoundCheckingAccessor implements SqlAccessor {
  private final ValueVector vector;
  private final SqlAccessor delegate;

  public BoundCheckingAccessor(ValueVector vector, SqlAccessor inner) {
    this.vector = vector;
    this.delegate = inner;
  }

  @Override
  public boolean isNull(int index) {
    return delegate.isNull(index);
  }

  @Override
  public BigDecimal getBigDecimal(int index) throws AbstractSqlAccessor.InvalidAccessException {
    return delegate.getBigDecimal(index);
  }

  @Override
  public boolean getBoolean(int index) throws AbstractSqlAccessor.InvalidAccessException {
    return delegate.getBoolean(index);
  }

  @Override
  public byte getByte(int index) throws AbstractSqlAccessor.InvalidAccessException {
    return delegate.getByte(index);
  }

  @Override
  public byte[] getBytes(int index) throws AbstractSqlAccessor.InvalidAccessException {
    return delegate.getBytes(index);
  }

  @Override
  public Date getDate(int index) throws AbstractSqlAccessor.InvalidAccessException {
    return delegate.getDate(index);
  }

  @Override
  public double getDouble(int index) throws AbstractSqlAccessor.InvalidAccessException {
    return delegate.getDouble(index);
  }

  @Override
  public float getFloat(int index) throws AbstractSqlAccessor.InvalidAccessException {
    return delegate.getFloat(index);
  }

  @Override
  public char getChar(int index) throws AbstractSqlAccessor.InvalidAccessException {
    return delegate.getChar(index);
  }

  @Override
  public int getInt(int index) throws AbstractSqlAccessor.InvalidAccessException {
    return delegate.getInt(index);
  }

  @Override
  public long getLong(int index) throws AbstractSqlAccessor.InvalidAccessException {
    return delegate.getLong(index);
  }

  @Override
  public short getShort(int index) throws AbstractSqlAccessor.InvalidAccessException {
    return delegate.getShort(index);
  }

  @Override
  public InputStream getStream(int index) throws AbstractSqlAccessor.InvalidAccessException {
    return delegate.getStream(index);
  }

  @Override
  public Reader getReader(int index) throws AbstractSqlAccessor.InvalidAccessException {
    return delegate.getReader(index);
  }

  @Override
  public String getString(int index) throws AbstractSqlAccessor.InvalidAccessException {
    return delegate.getString(index);
  }

  @Override
  public Time getTime(int index) throws AbstractSqlAccessor.InvalidAccessException {
    return delegate.getTime(index);
  }

  @Override
  public Timestamp getTimestamp(int index) throws AbstractSqlAccessor.InvalidAccessException {
    return delegate.getTimestamp(index);
  }

  /**
   * Returns an instance sitting at the given index if exists, null otherwise.
   *
   * @see org.apache.drill.exec.vector.accessor.SqlAccessor#getObject(int)
   */
  @Override
  public Object getObject(int index) throws AbstractSqlAccessor.InvalidAccessException {
    // in case some vectors have less values than others, callee invokes this method with index >= #getValueCount
    // this should still yield null.
    final ValueVector.Accessor accessor = vector.getAccessor();
    if (index < accessor.getValueCount()) {
      return delegate.getObject(index);
    }
    return null;
  }
}
