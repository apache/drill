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
package org.apache.drill.jdbc;

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
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;

import net.hydromatic.avatica.Cursor.Accessor;

import org.apache.drill.exec.vector.accessor.SqlAccessor;


public class AvaticaDrillSqlAccessor implements Accessor{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AvaticaDrillSqlAccessor.class);

  private SqlAccessor a;
  private DrillCursor cursor;

  public AvaticaDrillSqlAccessor(SqlAccessor drillSqlAccessor, DrillCursor cursor) {
    super();
    this.a = drillSqlAccessor;
    this.cursor = cursor;
  }

  private int row(){
    return cursor.currentRecord;
  }

  @Override
  public boolean wasNull() {
    return a.isNull(row());
  }

  @Override
  public String getString() throws SQLException {
    return a.getString(row());
  }

  @Override
  public boolean getBoolean() throws SQLException {
    return a.getBoolean(row());
  }

  @Override
  public byte getByte() throws SQLException {
    return a.getByte(row());
  }

  @Override
  public short getShort() throws SQLException {
    return a.getShort(row());
  }

  @Override
  public int getInt() throws SQLException {
    return a.getInt(row());
  }

  @Override
  public long getLong() throws SQLException {
    return a.getLong(row());
  }

  @Override
  public float getFloat() throws SQLException {
    return a.getFloat(row());
  }

  @Override
  public double getDouble() throws SQLException {
    return a.getDouble(row());
  }

  @Override
  public BigDecimal getBigDecimal() throws SQLException {
    return a.getBigDecimal(row());
  }

  @Override
  public BigDecimal getBigDecimal(int scale) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public byte[] getBytes() throws SQLException {
    return a.getBytes(row());
  }

  @Override
  public InputStream getAsciiStream() throws SQLException {
    return a.getStream(row());
  }

  @Override
  public InputStream getUnicodeStream() throws SQLException {
    return a.getStream(row());
  }

  @Override
  public InputStream getBinaryStream() throws SQLException {
    return a.getStream(row());
  }

  @Override
  public Object getObject() throws SQLException {
    return a.getObject(row());
  }

  @Override
  public Reader getCharacterStream() throws SQLException {
    return a.getReader(row());
  }

  @Override
  public Object getObject(Map<String, Class<?>> map) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Ref getRef() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Blob getBlob() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Clob getClob() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Array getArray() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Date getDate(Calendar calendar) throws SQLException {
    return a.getDate(row());
  }

  @Override
  public Time getTime(Calendar calendar) throws SQLException {
    return a.getTime(row());
  }

  @Override
  public Timestamp getTimestamp(Calendar calendar) throws SQLException {
    return a.getTimestamp(row());
  }

  @Override
  public URL getURL() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public NClob getNClob() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public SQLXML getSQLXML() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String getNString() throws SQLException {
    return a.getString(row());
  }

  @Override
  public Reader getNCharacterStream() throws SQLException {
    return a.getReader(row());
  }

  @Override
  public <T> T getObject(Class<T> type) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

}
