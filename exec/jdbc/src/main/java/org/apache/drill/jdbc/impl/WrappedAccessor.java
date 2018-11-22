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
package org.apache.drill.jdbc.impl;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;

import org.apache.calcite.avatica.util.Cursor.Accessor;

/**
 * Wraps Avatica {@code Accessor} instances to catch convertion exception
 * which are thrown as {@code RuntimeException} and throws {@code SQLException}
 * instead
 *
 */
public class WrappedAccessor implements Accessor {
  private final Accessor delegate;

  public WrappedAccessor(Accessor delegate) {
    this.delegate = delegate;
  }

  @Override
  public boolean wasNull() throws SQLException {
    return delegate.wasNull();
  }

  @Override
  public String getString() throws SQLException {
    try {
      return delegate.getString();
    } catch(RuntimeException e) {
      String message = e.getMessage();
      if (message != null && message.startsWith("cannot convert to")) {
        throw new SQLException(e.getMessage(), e);
      }
      throw e;
    }
  }

  @Override
  public boolean getBoolean() throws SQLException {
    try {
      return delegate.getBoolean();
    } catch(RuntimeException e) {
      String message = e.getMessage();
      if (message != null && message.startsWith("cannot convert to")) {
        throw new SQLException(e.getMessage(), e);
      }
      throw e;
    }
  }

  @Override
  public byte getByte() throws SQLException {
    try {
      return delegate.getByte();
    } catch(RuntimeException e) {
      String message = e.getMessage();
      if (message != null && message.startsWith("cannot convert to")) {
        throw new SQLException(e.getMessage(), e);
      }
      throw e;
    }
  }

  @Override
  public short getShort() throws SQLException {
    try {
      return delegate.getShort();
    } catch(RuntimeException e) {
      String message = e.getMessage();
      if (message != null && message.startsWith("cannot convert to")) {
        throw new SQLException(e.getMessage(), e);
      }
      throw e;
    }
  }

  @Override
  public int getInt() throws SQLException {
    try {
      return delegate.getInt();
    } catch(RuntimeException e) {
      String message = e.getMessage();
      if (message != null && message.startsWith("cannot convert to")) {
        throw new SQLException(e.getMessage(), e);
      }
      throw e;
    }
  }

  @Override
  public long getLong() throws SQLException {
    try {
      return delegate.getLong();
    } catch(RuntimeException e) {
      String message = e.getMessage();
      if (message != null && message.startsWith("cannot convert to")) {
        throw new SQLException(e.getMessage(), e);
      }
      throw e;
    }
  }

  @Override
  public float getFloat() throws SQLException {
    try {
      return delegate.getFloat();
    } catch(RuntimeException e) {
      String message = e.getMessage();
      if (message != null && message.startsWith("cannot convert to")) {
        throw new SQLException(e.getMessage(), e);
      }
      throw e;
    }
  }

  @Override
  public double getDouble() throws SQLException {
    try {
      return delegate.getDouble();
    } catch(RuntimeException e) {
      String message = e.getMessage();
      if (message != null && message.startsWith("cannot convert to")) {
        throw new SQLException(e.getMessage(), e);
      }
      throw e;
    }
  }

  @Override
  public BigDecimal getBigDecimal() throws SQLException {
    try {
      return delegate.getBigDecimal();
    } catch(RuntimeException e) {
      String message = e.getMessage();
      if (message != null && message.startsWith("cannot convert to")) {
        throw new SQLException(e.getMessage(), e);
      }
      throw e;
    }
  }

  @Override
  public BigDecimal getBigDecimal(int scale) throws SQLException {
    try {
      return delegate.getBigDecimal(scale);
    } catch(RuntimeException e) {
      String message = e.getMessage();
      if (message != null && message.startsWith("cannot convert to")) {
        throw new SQLException(e.getMessage(), e);
      }
      throw e;
    }
  }

  @Override
  public byte[] getBytes() throws SQLException {
    try {
      return delegate.getBytes();
    } catch(RuntimeException e) {
      String message = e.getMessage();
      if (message != null && message.startsWith("cannot convert to")) {
        throw new SQLException(e.getMessage(), e);
      }
      throw e;
    }
  }

  @Override
  public InputStream getAsciiStream() throws SQLException {
    try {
      return delegate.getAsciiStream();
    } catch(RuntimeException e) {
      String message = e.getMessage();
      if (message != null && message.startsWith("cannot convert to")) {
        throw new SQLException(e.getMessage(), e);
      }
      throw e;
    }
  }

  @Override
  public InputStream getUnicodeStream() throws SQLException {
    try {
      return delegate.getUnicodeStream();
    } catch(RuntimeException e) {
      String message = e.getMessage();
      if (message != null && message.startsWith("cannot convert to")) {
        throw new SQLException(e.getMessage(), e);
      }
      throw e;
    }
  }

  @Override
  public InputStream getBinaryStream() throws SQLException {
    try {
      return delegate.getBinaryStream();
    } catch(RuntimeException e) {
      String message = e.getMessage();
      if (message != null && message.startsWith("cannot convert to")) {
        throw new SQLException(e.getMessage(), e);
      }
      throw e;
    }
  }

  @Override
  public Object getObject() throws SQLException {
    try {
      return delegate.getObject();
    } catch(RuntimeException e) {
      String message = e.getMessage();
      if (message != null && message.startsWith("cannot convert to")) {
        throw new SQLException(e.getMessage(), e);
      }
      throw e;
    }
  }

  @Override
  public Reader getCharacterStream() throws SQLException {
    try {
      return delegate.getCharacterStream();
    } catch(RuntimeException e) {
      String message = e.getMessage();
      if (message != null && message.startsWith("cannot convert to")) {
        throw new SQLException(e.getMessage(), e);
      }
      throw e;
    }
  }

  @Override
  public Object getObject(Map<String, Class<?>> map) throws SQLException {
    try {
      return delegate.getObject(map);
    } catch(RuntimeException e) {
      String message = e.getMessage();
      if (message != null && message.startsWith("cannot convert to")) {
        throw new SQLException(e.getMessage(), e);
      }
      throw e;
    }
  }

  @Override
  public Ref getRef() throws SQLException {
    try {
      return delegate.getRef();
    } catch(RuntimeException e) {
      String message = e.getMessage();
      if (message != null && message.startsWith("cannot convert to")) {
        throw new SQLException(e.getMessage(), e);
      }
      throw e;
    }
  }

  @Override
  public Blob getBlob() throws SQLException {
    try {
      return delegate.getBlob();
    } catch(RuntimeException e) {
      String message = e.getMessage();
      if (message != null && message.startsWith("cannot convert to")) {
        throw new SQLException(e.getMessage(), e);
      }
      throw e;
    }
  }

  @Override
  public Clob getClob() throws SQLException {
    try {
      return delegate.getClob();
    } catch(RuntimeException e) {
      String message = e.getMessage();
      if (message != null && message.startsWith("cannot convert to")) {
        throw new SQLException(e.getMessage(), e);
      }
      throw e;
    }
  }

  @Override
  public Array getArray() throws SQLException {
    try {
      return delegate.getArray();
    } catch(RuntimeException e) {
      String message = e.getMessage();
      if (message != null && message.startsWith("cannot convert to")) {
        throw new SQLException(e.getMessage(), e);
      }
      throw e;
    }
  }

  @Override
  public Date getDate(Calendar calendar) throws SQLException {
    try {
      return delegate.getDate(calendar);
    } catch(RuntimeException e) {
      String message = e.getMessage();
      if (message != null && message.startsWith("cannot convert to")) {
        throw new SQLException(e.getMessage(), e);
      }
      throw e;
    }
  }

  @Override
  public Time getTime(Calendar calendar) throws SQLException {
    try {
      return delegate.getTime(calendar);
    } catch(RuntimeException e) {
      String message = e.getMessage();
      if (message != null && message.startsWith("cannot convert to")) {
        throw new SQLException(e.getMessage(), e);
      }
      throw e;
    }
  }

  @Override
  public Timestamp getTimestamp(Calendar calendar) throws SQLException {
    try {
      return delegate.getTimestamp(calendar);
    } catch(RuntimeException e) {
      String message = e.getMessage();
      if (message != null && message.startsWith("cannot convert to")) {
        throw new SQLException(e.getMessage(), e);
      }
      throw e;
    }
  }

  @Override
  public URL getURL() throws SQLException {
    try {
      return delegate.getURL();
    } catch(RuntimeException e) {
      String message = e.getMessage();
      if (message != null && message.startsWith("cannot convert to")) {
        throw new SQLException(e.getMessage(), e);
      }
      throw e;
    }
  }

  @Override
  public NClob getNClob() throws SQLException {
    try {
      return delegate.getNClob();
    } catch(RuntimeException e) {
      String message = e.getMessage();
      if (message != null && message.startsWith("cannot convert to")) {
        throw new SQLException(e.getMessage(), e);
      }
      throw e;
    }
  }

  @Override
  public SQLXML getSQLXML() throws SQLException {
    try {
      return delegate.getSQLXML();
    } catch(RuntimeException e) {
      String message = e.getMessage();
      if (message != null && message.startsWith("cannot convert to")) {
        throw new SQLException(e.getMessage(), e);
      }
      throw e;
    }
  }

  @Override
  public String getNString() throws SQLException {
    try {
      return delegate.getNString();
    } catch(RuntimeException e) {
      String message = e.getMessage();
      if (message != null && message.startsWith("cannot convert to")) {
        throw new SQLException(e.getMessage(), e);
      }
      throw e;
    }
  }

  @Override
  public Reader getNCharacterStream() throws SQLException {
    try {
      return delegate.getNCharacterStream();
    } catch(RuntimeException e) {
      String message = e.getMessage();
      if (message != null && message.startsWith("cannot convert to")) {
        throw new SQLException(e.getMessage(), e);
      }
      throw e;
    }
  }

  @Override
  public <T> T getObject(Class<T> type) throws SQLException {
    try {
      return delegate.getObject(type);
    } catch(RuntimeException e) {
      String message = e.getMessage();
      if (message != null && message.startsWith("cannot convert to")) {
        throw new SQLException(e.getMessage(), e);
      }
      throw e;
    }
  }

}
