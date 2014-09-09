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

import org.apache.drill.exec.vector.accessor.AbstractSqlAccessor.InvalidAccessException;

public interface SqlAccessor {

  public abstract boolean isNull(int index);

  public abstract BigDecimal getBigDecimal(int index) throws InvalidAccessException;

  public abstract boolean getBoolean(int index) throws InvalidAccessException;

  public abstract byte getByte(int index) throws InvalidAccessException;

  public abstract byte[] getBytes(int index) throws InvalidAccessException;

  public abstract Date getDate(int index) throws InvalidAccessException;

  public abstract double getDouble(int index) throws InvalidAccessException;

  public abstract float getFloat(int index) throws InvalidAccessException;

  public abstract char getChar(int index) throws InvalidAccessException;

  public abstract int getInt(int index) throws InvalidAccessException;

  public abstract long getLong(int index) throws InvalidAccessException;

  public abstract short getShort(int index) throws InvalidAccessException;

  public abstract InputStream getStream(int index) throws InvalidAccessException;

  public abstract Reader getReader(int index) throws InvalidAccessException;

  public abstract String getString(int index) throws InvalidAccessException;

  public abstract Time getTime(int index) throws InvalidAccessException;

  public abstract Timestamp getTimestamp(int index) throws InvalidAccessException;

  public abstract Object getObject(int index) throws InvalidAccessException;

}