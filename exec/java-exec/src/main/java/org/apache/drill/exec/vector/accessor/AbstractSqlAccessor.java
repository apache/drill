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
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;

import org.apache.drill.common.types.TypeProtos.MajorType;

abstract class AbstractSqlAccessor implements SqlAccessor {

  @Override
  public abstract boolean isNull(int index);

  @Override
  public BigDecimal getBigDecimal(int index) throws InvalidAccessException{
    throw new InvalidAccessException("BigDecimal");
  }

  @Override
  public boolean getBoolean(int index) throws InvalidAccessException{
    throw new InvalidAccessException("boolean");
  }

  @Override
  public byte getByte(int index) throws InvalidAccessException{
    throw new InvalidAccessException("byte");
  }

  @Override
  public byte[] getBytes(int index) throws InvalidAccessException{
    throw new InvalidAccessException("byte[]");
  }

  @Override
  public Date getDate(int index) throws InvalidAccessException{
    throw new InvalidAccessException("Date");
  }

  @Override
  public double getDouble(int index) throws InvalidAccessException{
    throw new InvalidAccessException("double");
  }

  @Override
  public float getFloat(int index) throws InvalidAccessException{
    throw new InvalidAccessException("float");
  }

  @Override
  public int getInt(int index) throws InvalidAccessException{
    throw new InvalidAccessException("int");
  }

  @Override
  public long getLong(int index) throws InvalidAccessException{
    throw new InvalidAccessException("long");
  }

  @Override
  public short getShort(int index) throws InvalidAccessException{
    throw new InvalidAccessException("short");
  }

  @Override
  public InputStream getStream(int index) throws InvalidAccessException{
    throw new InvalidAccessException("InputStream");
  }

  @Override
  public char getChar(int index) throws InvalidAccessException{
    throw new InvalidAccessException("Char");
  }

  @Override
  public Reader getReader(int index) throws InvalidAccessException{
    throw new InvalidAccessException("Reader");
  }

  @Override
  public String getString(int index) throws InvalidAccessException{
    return getObject(index).toString();
  }

  @Override
  public Time getTime(int index) throws InvalidAccessException{
    throw new InvalidAccessException("Time");
  }

  @Override
  public Timestamp getTimestamp(int index) throws InvalidAccessException{
    throw new InvalidAccessException("Timestamp");
  }

  abstract MajorType getType();


  public class InvalidAccessException extends SQLException{
    public InvalidAccessException(String name){
      super(String.format("Requesting class of type %s for an object of type %s:%s is not allowed.", name, getType().getMinorType().name(), getType().getMode().name()));
    }
  }
}
