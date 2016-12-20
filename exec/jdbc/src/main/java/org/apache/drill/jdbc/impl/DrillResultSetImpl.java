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
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.apache.calcite.avatica.AvaticaResultSet;
import org.apache.calcite.avatica.AvaticaSite;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.util.Cursor;
import org.apache.calcite.avatica.util.Cursor.Accessor;
import org.apache.drill.jdbc.AlreadyClosedSqlException;
import org.apache.drill.jdbc.DrillResultSet;
import org.apache.drill.jdbc.ExecutionCanceledSqlException;


/**
 * Drill's implementation of {@link ResultSet}.
 */
class DrillResultSetImpl extends AvaticaResultSet implements DrillResultSet {
  @SuppressWarnings("unused")
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(DrillResultSetImpl.class);

  private final DrillConnectionImpl connection;
  private volatile boolean hasPendingCancelationNotification = false;

  DrillResultSetImpl(AvaticaStatement statement, Meta.Signature signature,
                     ResultSetMetaData resultSetMetaData, TimeZone timeZone,
                     Meta.Frame firstFrame) {
    super(statement, signature, resultSetMetaData, timeZone, firstFrame);
    connection = (DrillConnectionImpl) statement.getConnection();
  }

  /**
   * Throws AlreadyClosedSqlException or QueryCanceledSqlException if this
   * ResultSet is closed.
   *
   * @throws  ExecutionCanceledSqlException  if ResultSet is closed because of
   *          cancelation and no QueryCanceledSqlException has been thrown yet
   *          for this ResultSet
   * @throws  AlreadyClosedSqlException  if ResultSet is closed
   * @throws  SQLException  if error in calling {@link #isClosed()}
   */
  private void throwIfClosed() throws AlreadyClosedSqlException,
                                      ExecutionCanceledSqlException,
                                      SQLException {
    if ( isClosed() ) {
      if (cursor instanceof DrillCursor && hasPendingCancelationNotification) {
        hasPendingCancelationNotification = false;
        throw new ExecutionCanceledSqlException(
            "SQL statement execution canceled; ResultSet now closed." );
      }
      else {
        throw new AlreadyClosedSqlException( "ResultSet is already closed." );
      }
    }
  }


  // Note:  Using dynamic proxies would reduce the quantity (450?) of method
  // overrides by eliminating those that exist solely to check whether the
  // object is closed.  It would also eliminate the need to throw non-compliant
  // RuntimeExceptions when Avatica's method declarations won't let us throw
  // proper SQLExceptions. (Check performance before applying to frequently
  // called ResultSet.)

  @Override
  protected void cancel() {
    if (cursor instanceof DrillCursor) {
      hasPendingCancelationNotification = true;
      ((DrillCursor) cursor).cancel();
    } else {
      super.cancel();
    }
  }

  ////////////////////////////////////////
  // ResultSet-defined methods (in same order as in ResultSet):

  // No isWrapperFor(Class<?>) (it doesn't throw SQLException if already closed).
  // No unwrap(Class<T>) (it doesn't throw SQLException if already closed).

  // (Not delegated.)
  @Override
  public boolean next() throws SQLException {
    throwIfClosed();
    // TODO:  Resolve following comments (possibly obsolete because of later
    // addition of preceding call to throwIfClosed.  Also, NOTE that the
    // following check, and maybe some throwIfClosed() calls, probably must
    // synchronize on the statement, per the comment on AvaticaStatement's
    // openResultSet:

    // Next may be called after close has been called (for example after a user
    // cancellation) which in turn sets the cursor to null.  So we must check
    // before we call next.
    // TODO: handle next() after close is called in the Avatica code.
    if (cursor != null) {
      return super.next();
    } else {
      return false;
    }
  }

  @Override
  public void close() {
    // Note:  No already-closed exception for close().
    super.close();
  }

  @Override
  public boolean wasNull() throws SQLException {
    throwIfClosed();
    return super.wasNull();
  }

  // Methods for accessing results by column index
  @Override
  public String getString( int columnIndex ) throws SQLException {
    throwIfClosed();
    return super.getString( columnIndex );
  }

  @Override
  public boolean getBoolean( int columnIndex ) throws SQLException {
    throwIfClosed();
    return super.getBoolean( columnIndex );
  }

  @Override
  public byte getByte( int columnIndex ) throws SQLException {
    throwIfClosed();
    return super.getByte( columnIndex );
  }

  @Override
  public short getShort( int columnIndex ) throws SQLException {
    throwIfClosed();
    return super.getShort( columnIndex );
  }

  @Override
  public int getInt( int columnIndex ) throws SQLException {
    throwIfClosed();
    return super.getInt( columnIndex );
  }

  @Override
  public long getLong( int columnIndex ) throws SQLException {
    throwIfClosed();
    return super.getLong( columnIndex );
  }

  @Override
  public float getFloat( int columnIndex ) throws SQLException {
    throwIfClosed();
    return super.getFloat( columnIndex );
  }

  @Override
  public double getDouble( int columnIndex ) throws SQLException {
    throwIfClosed();
    return super.getDouble( columnIndex );
  }

  @Override
  public BigDecimal getBigDecimal( int columnIndex,
                                   int scale ) throws SQLException {
    throwIfClosed();
    return super.getBigDecimal( columnIndex, scale );
  }

  @Override
  public byte[] getBytes( int columnIndex ) throws SQLException {
    throwIfClosed();
    return super.getBytes( columnIndex );
  }

  @Override
  public Date getDate( int columnIndex ) throws SQLException {
    throwIfClosed();
    return super.getDate( columnIndex );
  }

  @Override
  public Time getTime( int columnIndex ) throws SQLException {
    throwIfClosed();
    return super.getTime( columnIndex );
  }

  @Override
  public Timestamp getTimestamp( int columnIndex ) throws SQLException {
    throwIfClosed();
    return super.getTimestamp( columnIndex );
  }

  @Override
  public InputStream getAsciiStream( int columnIndex ) throws SQLException {
    throwIfClosed();
    return super.getAsciiStream( columnIndex );
  }

  @Override
  public InputStream getUnicodeStream( int columnIndex ) throws SQLException {
    throwIfClosed();
    return super.getUnicodeStream( columnIndex );
  }

  @Override
  public InputStream getBinaryStream( int columnIndex ) throws SQLException {
    throwIfClosed();
    return super.getBinaryStream( columnIndex );
  }

  // Methods for accessing results by column label
  @Override
  public String getString( String columnLabel ) throws SQLException {
    throwIfClosed();
    return super.getString( columnLabel );
  }

  @Override
  public boolean getBoolean( String columnLabel ) throws SQLException {
    throwIfClosed();
    return super.getBoolean( columnLabel );
  }

  @Override
  public byte getByte( String columnLabel ) throws SQLException {
    throwIfClosed();
    return super.getByte( columnLabel );
  }

  @Override
  public short getShort( String columnLabel ) throws SQLException {
    throwIfClosed();
    return super.getShort( columnLabel );
  }

  @Override
  public int getInt( String columnLabel ) throws SQLException {
    throwIfClosed();
    return super.getInt( columnLabel );
  }

  @Override
  public long getLong( String columnLabel ) throws SQLException {
    throwIfClosed();
    return super.getLong( columnLabel );
  }

  @Override
  public float getFloat( String columnLabel ) throws SQLException {
    throwIfClosed();
    return super.getFloat( columnLabel );
  }

  @Override
  public double getDouble( String columnLabel ) throws SQLException {
    throwIfClosed();
    return super.getDouble( columnLabel );
  }

  @Override
  public BigDecimal getBigDecimal( String columnLabel,
                                   int scale ) throws SQLException {
    throwIfClosed();
    return super.getBigDecimal( columnLabel, scale );
  }

  @Override
  public byte[] getBytes( String columnLabel ) throws SQLException {
    throwIfClosed();
    return super.getBytes( columnLabel );
  }

  @Override
  public Date getDate( String columnLabel ) throws SQLException {
    throwIfClosed();
    return super.getDate( columnLabel );
  }

  @Override
  public Time getTime( String columnLabel ) throws SQLException {
    throwIfClosed();
    return super.getTime( columnLabel );
  }

  @Override
  public Timestamp getTimestamp( String columnLabel ) throws SQLException {
    throwIfClosed();
    return super.getTimestamp( columnLabel );
  }

  @Override
  public InputStream getAsciiStream( String columnLabel ) throws SQLException {
    throwIfClosed();
    return super.getAsciiStream( columnLabel );
  }

  @Override
  public InputStream getUnicodeStream( String columnLabel ) throws SQLException {
    throwIfClosed();
    return super.getUnicodeStream( columnLabel );
  }

  @Override
  public InputStream getBinaryStream( String columnLabel ) throws SQLException {
    throwIfClosed();
    return super.getBinaryStream( columnLabel );
  }

  // Advanced features:
  @Override
  public SQLWarning getWarnings() throws SQLException {
    throwIfClosed();
    return super.getWarnings();
  }

  @Override
  public void clearWarnings() throws SQLException {
    throwIfClosed();
    super.clearWarnings();
  }

  @Override
  public String getCursorName() throws SQLException {
    throwIfClosed();
    try {
      return super.getCursorName();
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  // (Not delegated.)
  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    throwIfClosed();
    return super.getMetaData();
  }

  @Override
  public Object getObject( int columnIndex ) throws SQLException {
    throwIfClosed();

    final Cursor.Accessor accessor;
    try {
      accessor = accessorList.get(columnIndex - 1);
    } catch (IndexOutOfBoundsException e) {
      throw new SQLException("invalid column ordinal: " + columnIndex);
    }
    final ColumnMetaData metaData = columnMetaDataList.get(columnIndex - 1);
    // Drill returns a float (4bytes) for a SQL Float whereas Calcite would return a double (8bytes)
    int typeId = (metaData.type.id != Types.FLOAT) ? metaData.type.id : Types.REAL;
    return AvaticaSite.get(accessor, typeId, localCalendar);
  }

  @Override
  public Object getObject( String columnLabel ) throws SQLException {
    throwIfClosed();
    return super.getObject( columnLabel );
  }

  //----------------------------------------------------------------
  @Override
  public int findColumn( String columnLabel ) throws SQLException {
    throwIfClosed();
    return super.findColumn( columnLabel );
  }

  //--------------------------JDBC 2.0-----------------------------------
  //---------------------------------------------------------------------
  // Getters and Setters
  //---------------------------------------------------------------------
  @Override
  public Reader getCharacterStream( int columnIndex ) throws SQLException {
    throwIfClosed();
    return super.getCharacterStream( columnIndex );
  }

  @Override
  public Reader getCharacterStream( String columnLabel ) throws SQLException {
    throwIfClosed();
    return super.getCharacterStream( columnLabel );
  }

  @Override
  public BigDecimal getBigDecimal( int columnIndex ) throws SQLException {
    throwIfClosed();
    return super.getBigDecimal( columnIndex );
  }

  @Override
  public BigDecimal getBigDecimal( String columnLabel ) throws SQLException {
    throwIfClosed();
    return super.getBigDecimal( columnLabel );
  }

  //---------------------------------------------------------------------
  // Traversal/Positioning
  //---------------------------------------------------------------------
  @Override
  public boolean isBeforeFirst() throws SQLException {
    throwIfClosed();
    return super.isBeforeFirst();
  }

  @Override
  public boolean isAfterLast() throws SQLException {
    throwIfClosed();
    return super.isAfterLast();
  }

  @Override
  public boolean isFirst() throws SQLException {
    throwIfClosed();
    return super.isFirst();
  }

  @Override
  public boolean isLast() throws SQLException {
    throwIfClosed();
    try {
      return super.isLast();
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void beforeFirst() throws SQLException {
    throwIfClosed();
    try {
      super.beforeFirst();
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void afterLast() throws SQLException {
    throwIfClosed();
    try {
      super.afterLast();
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public boolean first() throws SQLException {
    throwIfClosed();
    try {
      return super.first();
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public boolean last() throws SQLException {
    throwIfClosed();
    try {
      return super.last();
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public int getRow() throws SQLException {
    throwIfClosed();
    // Map Avatica's erroneous zero-based row numbers to 1-based, and return 0
    // after end, per JDBC:
    return isAfterLast() ? 0 : 1 + super.getRow();
  }

  @Override
  public boolean absolute( int row ) throws SQLException {
    throwIfClosed();
    try {
      return super.absolute( row );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public boolean relative( int rows ) throws SQLException {
    throwIfClosed();
    try {
      return super.relative( rows );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public boolean previous() throws SQLException {
    throwIfClosed();
    try {
      return super.previous();
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  //---------------------------------------------------------------------
  // Properties
  //---------------------------------------------------------------------

  @Override
  public void setFetchDirection( int direction ) throws SQLException {
    throwIfClosed();
    super.setFetchDirection( direction );
  }

  @Override
  public int getFetchDirection() throws SQLException {
    throwIfClosed();
    return super.getFetchDirection();
  }

  @Override
  public void setFetchSize( int rows ) throws SQLException {
    throwIfClosed();
    super.setFetchSize( rows );
  }

  @Override
  public int getFetchSize() throws SQLException {
    throwIfClosed();
    return super.getFetchSize();
  }

  @Override
  public int getType() throws SQLException {
    throwIfClosed();
    return super.getType();
  }

  @Override
  public int getConcurrency() throws SQLException {
    throwIfClosed();
    return super.getConcurrency();
  }

  //---------------------------------------------------------------------
  // Updates
  //---------------------------------------------------------------------
  @Override
  public boolean rowUpdated() throws SQLException {
    throwIfClosed();
    return super.rowUpdated();
  }

  @Override
  public boolean rowInserted() throws SQLException {
    throwIfClosed();
    return super.rowInserted();
  }

  @Override
  public boolean rowDeleted() throws SQLException {
    throwIfClosed();
    return super.rowDeleted();
  }

  @Override
  public void updateNull( int columnIndex ) throws SQLException {
    throwIfClosed();
    try {
      super.updateNull( columnIndex );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void updateBoolean( int columnIndex, boolean x ) throws SQLException {
    throwIfClosed();
    try {
      super.updateBoolean( columnIndex, x );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void updateByte( int columnIndex, byte x ) throws SQLException {
    throwIfClosed();
    try {
      super.updateByte( columnIndex, x );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void updateShort( int columnIndex, short x ) throws SQLException {
    throwIfClosed();
    try {
      super.updateShort( columnIndex, x );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void updateInt( int columnIndex, int x ) throws SQLException {
    throwIfClosed();
    try {
      super.updateInt( columnIndex, x );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void updateLong( int columnIndex, long x ) throws SQLException {
    throwIfClosed();
    try {
      super.updateLong( columnIndex, x );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void updateFloat( int columnIndex, float x ) throws SQLException {
    throwIfClosed();
    try {
      super.updateFloat( columnIndex, x );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void updateDouble( int columnIndex, double x ) throws SQLException {
    throwIfClosed();
    try {
      super.updateDouble( columnIndex, x );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void updateBigDecimal( int columnIndex,
                                BigDecimal x ) throws SQLException {
    throwIfClosed();
    try {
      super.updateBigDecimal( columnIndex, x );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void updateString( int columnIndex, String x ) throws SQLException {
    throwIfClosed();
    try {
      super.updateString( columnIndex, x );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void updateBytes( int columnIndex, byte[] x ) throws SQLException {
    throwIfClosed();
    try {
      super.updateBytes( columnIndex, x );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void updateDate( int columnIndex, Date x ) throws SQLException {
    throwIfClosed();
    try {
      super.updateDate( columnIndex, x );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void updateTime( int columnIndex, Time x ) throws SQLException {
    throwIfClosed();
    try {
      super.updateTime( columnIndex, x );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void updateTimestamp( int columnIndex, Timestamp x ) throws SQLException {
    throwIfClosed();
    try {
      super.updateTimestamp( columnIndex, x );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void updateAsciiStream( int columnIndex, InputStream x,
                                 int length ) throws SQLException {
    throwIfClosed();
    try {
      super.updateAsciiStream( columnIndex, x, length );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void updateBinaryStream( int columnIndex, InputStream x,
                                  int length ) throws SQLException {
    throwIfClosed();
    try {
      super.updateBinaryStream( columnIndex, x, length );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void updateCharacterStream( int columnIndex, Reader x,
                                     int length ) throws SQLException {
    throwIfClosed();
    try {
      super.updateCharacterStream( columnIndex, x, length );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void updateObject( int columnIndex, Object x,
                            int scaleOrLength ) throws SQLException {
    throwIfClosed();
    try {
      super.updateObject( columnIndex, x, scaleOrLength );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void updateObject( int columnIndex, Object x ) throws SQLException {
    throwIfClosed();
    try {
      super.updateObject( columnIndex, x );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void updateNull( String columnLabel ) throws SQLException {
    throwIfClosed();
    try {
      super.updateNull( columnLabel );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void updateBoolean( String columnLabel, boolean x ) throws SQLException {
    throwIfClosed();
    try {
      super.updateBoolean( columnLabel, x );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void updateByte( String columnLabel, byte x ) throws SQLException {
    throwIfClosed();
    try {
      super.updateByte( columnLabel, x );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void updateShort( String columnLabel, short x ) throws SQLException {
    throwIfClosed();
    try {
      super.updateShort( columnLabel, x );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void updateInt( String columnLabel, int x ) throws SQLException {
    throwIfClosed();
    try {
      super.updateInt( columnLabel, x );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void updateLong( String columnLabel, long x ) throws SQLException {
    throwIfClosed();
    try {
      super.updateLong( columnLabel, x );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void updateFloat( String columnLabel, float x ) throws SQLException {
    throwIfClosed();
    try {
      super.updateFloat( columnLabel, x );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void updateDouble( String columnLabel, double x ) throws SQLException {
    throwIfClosed();
    try {
      super.updateDouble( columnLabel, x );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void updateBigDecimal( String columnLabel,
                                BigDecimal x ) throws SQLException {
    throwIfClosed();
    try {
      super.updateBigDecimal( columnLabel, x );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void updateString( String columnLabel, String x ) throws SQLException {
    throwIfClosed();
    try {
      super.updateString( columnLabel, x );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void updateBytes( String columnLabel, byte[] x ) throws SQLException {
    throwIfClosed();
    try {
      super.updateBytes( columnLabel, x );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void updateDate( String columnLabel, Date x ) throws SQLException {
    throwIfClosed();
    try {
      super.updateDate( columnLabel, x );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void updateTime( String columnLabel, Time x ) throws SQLException {
    throwIfClosed();
    try {
      super.updateTime( columnLabel, x );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void updateTimestamp( String columnLabel, Timestamp x ) throws SQLException {
    throwIfClosed();
    try {
      super.updateTimestamp( columnLabel, x );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void updateAsciiStream( String columnLabel, InputStream x,
                                 int length ) throws SQLException {
    throwIfClosed();
    try {
      super.updateAsciiStream( columnLabel, x, length );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void updateBinaryStream( String columnLabel, InputStream x,
                                  int length ) throws SQLException {
    throwIfClosed();
    try {
      super.updateBinaryStream( columnLabel, x, length );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void updateCharacterStream( String columnLabel, Reader reader,
                                     int length ) throws SQLException {
    throwIfClosed();
    try {
      super.updateCharacterStream( columnLabel, reader, length );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void updateObject( String columnLabel, Object x,
                            int scaleOrLength ) throws SQLException {
    throwIfClosed();
    try {
      super.updateObject( columnLabel, x, scaleOrLength );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void updateObject( String columnLabel, Object x ) throws SQLException {
    throwIfClosed();
    try {
      super.updateObject( columnLabel, x );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void insertRow() throws SQLException {
    throwIfClosed();
    try {
      super.insertRow();
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void updateRow() throws SQLException {
    throwIfClosed();
    try {
      super.updateRow();
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void deleteRow() throws SQLException {
    throwIfClosed();
    try {
      super.deleteRow();
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void refreshRow() throws SQLException {
    throwIfClosed();
    try {
      super.refreshRow();
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void cancelRowUpdates() throws SQLException {
    throwIfClosed();
    try {
      super.cancelRowUpdates();
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void moveToInsertRow() throws SQLException {
    throwIfClosed();
    try {
      super.moveToInsertRow();
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void moveToCurrentRow() throws SQLException {
    throwIfClosed();
    try {
      super.moveToCurrentRow();
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public AvaticaStatement getStatement() {
    try {
      throwIfClosed();
    } catch (AlreadyClosedSqlException e) {
      // Can't throw any SQLException because AvaticaConnection's
      // getStatement() is missing "throws SQLException".
      throw new RuntimeException(e.getMessage(), e);
    } catch (SQLException e) {
      throw new RuntimeException(e.getMessage(), e);
    }
    return super.getStatement();
  }

  @Override
  public Object getObject( int columnIndex,
                           Map<String, Class<?>> map ) throws SQLException {
    throwIfClosed();
    return super.getObject( columnIndex, map );
  }

  @Override
  public Ref getRef( int columnIndex ) throws SQLException {
    throwIfClosed();
    return super.getRef( columnIndex );
  }

  @Override
  public Blob getBlob( int columnIndex ) throws SQLException {
    throwIfClosed();
    return super.getBlob( columnIndex );
  }

  @Override
  public Clob getClob( int columnIndex ) throws SQLException {
    throwIfClosed();
    return super.getClob( columnIndex );
  }

  @Override
  public Array getArray( int columnIndex ) throws SQLException {
    throwIfClosed();
    return super.getArray( columnIndex );
  }

  @Override
  public Object getObject( String columnLabel,
                           Map<String,Class<?>> map ) throws SQLException {
    throwIfClosed();
    return super.getObject( columnLabel, map );
  }

  @Override
  public Ref getRef( String columnLabel ) throws SQLException {
    throwIfClosed();
    return super.getRef( columnLabel );
  }

  @Override
  public Blob getBlob( String columnLabel ) throws SQLException {
    throwIfClosed();
    return super.getBlob( columnLabel );
  }

  @Override
  public Clob getClob( String columnLabel ) throws SQLException {
    throwIfClosed();
    return super.getClob( columnLabel );
  }

  @Override
  public Array getArray( String columnLabel ) throws SQLException {
    throwIfClosed();
    return super.getArray( columnLabel );
  }

  @Override
  public Date getDate( int columnIndex, Calendar cal ) throws SQLException {
    throwIfClosed();
    return super.getDate( columnIndex, cal );
  }

  @Override
  public Date getDate( String columnLabel, Calendar cal ) throws SQLException {
    throwIfClosed();
    return super.getDate( columnLabel, cal );
  }

  @Override
  public Time getTime( int columnIndex, Calendar cal ) throws SQLException {
    throwIfClosed();
    return super.getTime( columnIndex, cal );
  }

  @Override
  public Time getTime( String columnLabel, Calendar cal ) throws SQLException {
    throwIfClosed();
    return super.getTime( columnLabel, cal );
  }

  @Override
  public Timestamp getTimestamp( int columnIndex, Calendar cal ) throws SQLException {
    throwIfClosed();
    return super.getTimestamp( columnIndex, cal );
  }

  @Override
  public Timestamp getTimestamp( String columnLabel,
                                 Calendar cal ) throws SQLException {
    throwIfClosed();
    return super.getTimestamp( columnLabel, cal );
  }

  //-------------------------- JDBC 3.0 ----------------------------------------

  @Override
  public URL getURL( int columnIndex ) throws SQLException {
    throwIfClosed();
    return super.getURL( columnIndex );
  }

  @Override
  public URL getURL( String columnLabel ) throws SQLException {
    throwIfClosed();
    return super.getURL( columnLabel );
  }

  @Override
  public void updateRef( int columnIndex, Ref x ) throws SQLException {
    throwIfClosed();
    try {
      super.updateRef( columnIndex, x );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void updateRef( String columnLabel, Ref x ) throws SQLException {
    throwIfClosed();
    try {
      super.updateRef( columnLabel, x );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void updateBlob( int columnIndex, Blob x ) throws SQLException {
    throwIfClosed();
    try {
      super.updateBlob( columnIndex, x );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void updateBlob( String columnLabel, Blob x ) throws SQLException {
    throwIfClosed();
    try {
      super.updateBlob( columnLabel, x );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void updateClob( int columnIndex, Clob x ) throws SQLException {
    throwIfClosed();
    try {
      super.updateClob( columnIndex, x );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void updateClob( String columnLabel, Clob x ) throws SQLException {
    throwIfClosed();
    try {
      super.updateClob( columnLabel, x );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void updateArray( int columnIndex, Array x ) throws SQLException {
    throwIfClosed();
    try {
      super.updateArray( columnIndex, x );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void updateArray( String columnLabel, Array x ) throws SQLException {
    throwIfClosed();
    try {
      super.updateArray( columnLabel, x );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  //------------------------- JDBC 4.0 -----------------------------------
  @Override
  public RowId getRowId( int columnIndex ) throws SQLException {
    throwIfClosed();
    try {
      return super.getRowId( columnIndex );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public RowId getRowId( String columnLabel ) throws SQLException {
    throwIfClosed();
    try {
      return super.getRowId( columnLabel );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void updateRowId( int columnIndex, RowId x ) throws SQLException {
    throwIfClosed();
    try {
      super.updateRowId( columnIndex, x );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void updateRowId( String columnLabel, RowId x ) throws SQLException {
    throwIfClosed();
    try {
      super.updateRowId( columnLabel, x );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public int getHoldability() throws SQLException {
    throwIfClosed();
    return super.getHoldability();
  }

  @Override
  public boolean isClosed() throws SQLException {
    // Note:  No already-closed exception for isClosed().
    return super.isClosed();
  }

  @Override
  public void updateNString( int columnIndex, String nString ) throws SQLException {
    throwIfClosed();
    try {
      super.updateNString( columnIndex, nString );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void updateNString( String columnLabel,
                             String nString ) throws SQLException {
    throwIfClosed();
    try {
      super.updateNString( columnLabel, nString );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void updateNClob( int columnIndex, NClob nClob ) throws SQLException {
    throwIfClosed();
    try {
      super.updateNClob( columnIndex, nClob );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void updateNClob( String columnLabel, NClob nClob ) throws SQLException {
    throwIfClosed();
    try {
      super.updateNClob( columnLabel, nClob );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public NClob getNClob( int columnIndex ) throws SQLException {
    throwIfClosed();
    return super.getNClob( columnIndex );
  }

  @Override
  public NClob getNClob( String columnLabel ) throws SQLException {
    throwIfClosed();
    return super.getNClob( columnLabel );
  }

  @Override
  public SQLXML getSQLXML( int columnIndex ) throws SQLException {
    throwIfClosed();
    return super.getSQLXML( columnIndex );
  }

  @Override
  public SQLXML getSQLXML( String columnLabel ) throws SQLException {
    throwIfClosed();
    return super.getSQLXML( columnLabel );
  }

  @Override
  public void updateSQLXML( int columnIndex,
                            SQLXML xmlObject ) throws SQLException {
    throwIfClosed();
    try {
      super.updateSQLXML( columnIndex, xmlObject );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void updateSQLXML( String columnLabel,
                            SQLXML xmlObject ) throws SQLException {
    throwIfClosed();
    try {
      super.updateSQLXML( columnLabel, xmlObject );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public String getNString( int columnIndex ) throws SQLException {
    throwIfClosed();
    return super.getNString( columnIndex );
  }

  @Override
  public String getNString( String columnLabel ) throws SQLException {
    throwIfClosed();
    return super.getNString( columnLabel );
  }

  @Override
  public Reader getNCharacterStream( int columnIndex ) throws SQLException {
    throwIfClosed();
    return super.getNCharacterStream( columnIndex );
  }

  @Override
  public Reader getNCharacterStream( String columnLabel ) throws SQLException {
    throwIfClosed();
    return super.getNCharacterStream( columnLabel );
  }

  @Override
  public void updateNCharacterStream( int columnIndex, Reader x,
                                      long length ) throws SQLException {
    throwIfClosed();
    try {
      super.updateNCharacterStream( columnIndex, x, length );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void updateNCharacterStream( String columnLabel, Reader reader,
                                      long length ) throws SQLException {
    throwIfClosed();
    try {
      super.updateNCharacterStream( columnLabel, reader, length );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void updateAsciiStream( int columnIndex, InputStream x,
                                 long length ) throws SQLException {
    throwIfClosed();
    try {
      super.updateAsciiStream( columnIndex, x, length );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void updateBinaryStream( int columnIndex, InputStream x,
                                  long length ) throws SQLException {
    throwIfClosed();
    try {
      super.updateBinaryStream( columnIndex, x, length );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void updateCharacterStream( int columnIndex, Reader x,
                                     long length ) throws SQLException {
    throwIfClosed();
    try {
      super.updateCharacterStream( columnIndex, x, length );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void updateAsciiStream( String columnLabel, InputStream x,
                                 long length ) throws SQLException {
    throwIfClosed();
    try {
      super.updateAsciiStream( columnLabel, x, length );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void updateBinaryStream( String columnLabel, InputStream x,
                                  long length ) throws SQLException {
    throwIfClosed();
    try {
      super.updateBinaryStream( columnLabel, x, length );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void updateCharacterStream( String columnLabel, Reader reader,
                                     long length ) throws SQLException {
    throwIfClosed();
    try {
      super.updateCharacterStream( columnLabel, reader, length );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void updateBlob( int columnIndex, InputStream inputStream,
                          long length ) throws SQLException {
    throwIfClosed();
    try {
      super.updateBlob( columnIndex, inputStream, length );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void updateBlob( String columnLabel, InputStream inputStream,
                          long length ) throws SQLException {
    throwIfClosed();
    try {
      super.updateBlob( columnLabel, inputStream, length );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void updateClob( int columnIndex,  Reader reader,
                          long length ) throws SQLException {
    throwIfClosed();
    try {
      super.updateClob( columnIndex, reader, length );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void updateClob( String columnLabel,  Reader reader,
                          long length ) throws SQLException {
    throwIfClosed();
    try {
      super.updateClob( columnLabel, reader, length );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void updateNClob( int columnIndex,  Reader reader,
                           long length ) throws SQLException {
    throwIfClosed();
    try {
      super.updateNClob( columnIndex, reader, length );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void updateNClob( String columnLabel,  Reader reader,
                           long length ) throws SQLException {
    throwIfClosed();
    try {
      super.updateNClob( columnLabel, reader, length );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  //---
  @Override
  public void updateNCharacterStream( int columnIndex,
                                      Reader x ) throws SQLException {
    throwIfClosed();
    try {
      super.updateNCharacterStream( columnIndex, x );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void updateNCharacterStream( String columnLabel,
                                      Reader reader ) throws SQLException {
    throwIfClosed();
    try {
      super.updateNCharacterStream( columnLabel, reader );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void updateAsciiStream( int columnIndex,
                                 InputStream x ) throws SQLException {
    throwIfClosed();
    try {
      super.updateAsciiStream( columnIndex, x );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void updateBinaryStream( int columnIndex,
                                  InputStream x ) throws SQLException {
    throwIfClosed();
    try {
      super.updateBinaryStream( columnIndex, x );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void updateCharacterStream( int columnIndex,
                                     Reader x ) throws SQLException {
    throwIfClosed();
    try {
      super.updateCharacterStream( columnIndex, x );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void updateAsciiStream( String columnLabel,
                                 InputStream x ) throws SQLException {
    throwIfClosed();
    try {
      super.updateAsciiStream( columnLabel, x );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void updateBinaryStream( String columnLabel,
                                  InputStream x ) throws SQLException {
    throwIfClosed();
    try {
      super.updateBinaryStream( columnLabel, x );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void updateCharacterStream( String columnLabel,
                                     Reader reader ) throws SQLException {
    throwIfClosed();
    try {
      super.updateCharacterStream( columnLabel, reader );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void updateBlob( int columnIndex,
                          InputStream inputStream ) throws SQLException {
    throwIfClosed();
    try {
      super.updateBlob( columnIndex, inputStream );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void updateBlob( String columnLabel,
                          InputStream inputStream ) throws SQLException {
    throwIfClosed();
    try {
      super.updateBlob( columnLabel, inputStream );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void updateClob( int columnIndex,  Reader reader ) throws SQLException {
    throwIfClosed();
    try {
      super.updateClob( columnIndex, reader );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void updateClob( String columnLabel,  Reader reader ) throws SQLException {
    throwIfClosed();
    try {
      super.updateClob( columnLabel, reader );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void updateNClob( int columnIndex,  Reader reader ) throws SQLException {
    throwIfClosed();
    try {
      super.updateNClob( columnIndex, reader );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void updateNClob( String columnLabel,  Reader reader ) throws SQLException {
    throwIfClosed();
    try {
      super.updateNClob( columnLabel, reader );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  //------------------------- JDBC 4.1 -----------------------------------
  @Override
  public <T> T getObject( int columnIndex, Class<T> type ) throws SQLException {
    throwIfClosed();
    return super.getObject( columnIndex, type );
  }

  @Override
  public <T> T getObject( String columnLabel, Class<T> type ) throws SQLException {
    throwIfClosed();
    return super.getObject( columnLabel, type );
  }


  ////////////////////////////////////////
  // DrillResultSet methods:

  @Override
  public String getQueryId() throws SQLException {
    throwIfClosed();
    if (cursor instanceof DrillCursor) {
      return ((DrillCursor) cursor).getQueryId();
    }
    return null;
  }


  ////////////////////////////////////////

  @Override
  protected DrillResultSetImpl execute() throws SQLException{
    connection.getDriver().handler.onStatementExecute(statement, null);

    if (signature.cursorFactory != null) {
      // Avatica accessors have to be wrapped to match Drill behaviour regarding exception thrown
      super.execute();
      List<Accessor> wrappedAccessorList = new ArrayList<>(accessorList.size());
      for(Accessor accessor: accessorList) {
        wrappedAccessorList.add(new WrappedAccessor(accessor));
      }
      this.accessorList = wrappedAccessorList;
    }
    else {
      DrillCursor drillCursor = new DrillCursor(connection, statement, signature);
      super.execute2(drillCursor, this.signature.columns);

      // Read first (schema-only) batch to initialize result-set metadata from
      // (initial) schema before Statement.execute...(...) returns result set:
      drillCursor.loadInitialSchema();
    }

    return this;
  }
}
