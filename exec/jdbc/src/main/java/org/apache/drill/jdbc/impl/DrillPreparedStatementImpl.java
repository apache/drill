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

import java.sql.ParameterMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.util.concurrent.Future;

import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.AvaticaPreparedStatement;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.Meta.StatementHandle;
import org.apache.drill.exec.proto.UserProtos.PreparedStatement;
import org.apache.drill.jdbc.AlreadyClosedSqlException;
import org.apache.drill.jdbc.DrillPreparedStatement;
import org.apache.drill.jdbc.InvalidParameterSqlException;
import org.apache.drill.jdbc.SqlTimeoutException;

/**
 * Implementation of {@link java.sql.PreparedStatement} for Drill.
 *
 * <p>
 * This class has sub-classes which implement JDBC 3.0 and JDBC 4.0 APIs; it is
 * instantiated using
 * {@link net.hydromatic.avatica.AvaticaFactory#newPreparedStatement}.
 * </p>
 */
abstract class DrillPreparedStatementImpl extends AvaticaPreparedStatement
    implements DrillPreparedStatement,
               DrillRemoteStatement {

  private final PreparedStatement preparedStatementHandle;
  private TimeoutTrigger timeoutTrigger;
  private Future<?> timeoutTriggerHandle;

  protected DrillPreparedStatementImpl(DrillConnectionImpl connection,
                                       StatementHandle h,
                                       Meta.Signature signature,
                                       PreparedStatement preparedStatementHandle,
                                       int resultSetType,
                                       int resultSetConcurrency,
                                       int resultSetHoldability) throws SQLException {
    super(connection, h, signature,
          resultSetType, resultSetConcurrency, resultSetHoldability);
    connection.openStatementsRegistry.addStatement(this);
    this.preparedStatementHandle = preparedStatementHandle;
    if (preparedStatementHandle != null) {
      ((DrillColumnMetaDataList) signature.columns).updateColumnMetaData(preparedStatementHandle.getColumnsList());
    }
  }



  /**
   * Throws {@link SqlTimeoutException} or {@link AlreadyClosedSqlException} <i>iff</i> this PreparedStatement is closed.
   * @throws SqlTimeoutException        if PreparedStatement has closed due to timeout
   * @throws AlreadyClosedSqlException  if PreparedStatement is closed
   * @throws SQLException               Other unaccounted SQL exceptions
   */
  private void throwIfTimedOutOrClosed() throws SqlTimeoutException, AlreadyClosedSqlException, SQLException {
    throwIfTimedOut();
    throwIfClosed();
  }

  /**
   * Throws AlreadyClosedSqlException <i>iff</i> this PreparedStatement is closed.
   * @throws  AlreadyClosedSqlException  if PreparedStatement is closed
   * @throws  SQLException               Other unaccounted SQL exceptions
   */
  private void throwIfClosed() throws AlreadyClosedSqlException {
    if ( isClosed() ) {
      throw new AlreadyClosedSqlException( "PreparedStatement is already closed." );
    }
  }

  /**
   * Throws {@link SqlTimeoutException} <i>iff</i> this PreparedStatement had closed due to timeout.
   *
   * @throws  SqlTimeoutException  if PreparedStatement has closed due to timeout
   * @throws  SQLException         Other unaccounted SQL exceptions
   */
  private void throwIfTimedOut() throws SqlTimeoutException, SQLException {
    if ( isTimedOut() ) {
      throw new SqlTimeoutException(getQueryTimeout());
    }
  }

  @Override
  public boolean isTimedOut() {
    if (timeoutTrigger != null && timeoutTriggerHandle.isDone()) {
      return true;
    }
    return false;
  }

  // Note:  Using dynamic proxies would reduce the quantity (450?) of method
  // overrides by eliminating those that exist solely to check whether the
  // object is closed.  It would also eliminate the need to throw non-compliant
  // RuntimeExceptions when Avatica's method declarations won't let us throw
  // proper SQLExceptions. (Check performance before applying to frequently
  // called ResultSet.)

  @Override
  public DrillConnectionImpl getConnection() {
    try {
      throwIfClosed();
    } catch (SQLException e) {
      // Can't throw any SQLException because AvaticaConnection's
      // getConnection() is missing "throws SQLException".
      throw new RuntimeException(e.getMessage(), e);
    }
    return (DrillConnectionImpl) super.getConnection();
  }

  PreparedStatement getPreparedStatementHandle() {
    return preparedStatementHandle;
  }

  @Override
  protected AvaticaParameter getParameter(int param) throws SQLException {
    throwIfClosed();
    throw new SQLFeatureNotSupportedException(
        "Prepared-statement dynamic parameters are not supported.");
  }

  @Override
  public void cleanUp() {
    //Cancel Timeout trigger
    if (this.timeoutTrigger != null) {
      timeoutTriggerHandle.cancel(true);
    }
    final DrillConnectionImpl connection1 = (DrillConnectionImpl) connection;
    connection1.openStatementsRegistry.removeStatement(this);
  }

  // Note:  Methods are in same order as in java.sql.PreparedStatement.

  // No isWrapperFor(Class<?>) (it doesn't throw SQLException if already closed).
  // No unwrap(Class<T>) (it doesn't throw SQLException if already closed).

  @Override
  public ResultSet executeQuery(String sql) throws SQLException {
    throwIfClosed();
    try {
      if (timeoutTrigger != null) {
        timeoutTriggerHandle = timeoutTrigger.startCountdown();
      }
      return super.executeQuery(sql);
    }
    catch (UnsupportedOperationException e) {
      if ( isTimedOut() ) {
        throw new SqlTimeoutException(getQueryTimeout());
      }
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public long executeLargeUpdate(String sql) throws SQLException {
    throwIfClosed();
    try {
      if (timeoutTrigger != null) {
        timeoutTriggerHandle = timeoutTrigger.startCountdown();
      }
      return super.executeLargeUpdate(sql);
    }
    catch (UnsupportedOperationException e) {
      if ( isTimedOut() ) {
        throw new SqlTimeoutException(getQueryTimeout());
      }
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  // No close() (it doesn't throw SQLException if already closed).

  @Override
  public int getMaxFieldSize() throws SQLException {
    throwIfClosed();
    try {
      return super.getMaxFieldSize();
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void setMaxFieldSize(int max) throws SQLException {
    throwIfTimedOutOrClosed();
    try {
      super.setMaxFieldSize(max);
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public long getLargeMaxRows() {
    try {
      throwIfClosed();
    } catch (SQLException e) {
      // Can't throw any SQLException because AvaticaConnection's
      // getLargeMaxRows() is missing "throws SQLException".
      throw new RuntimeException(e.getMessage(), e);
    }
    return super.getLargeMaxRows();
  }

  @Override
  public void setLargeMaxRows(long max) throws SQLException {
    throwIfTimedOutOrClosed();
    super.setLargeMaxRows(max);
  }

  @Override
  public void setEscapeProcessing(boolean enable) throws SQLException {
    throwIfTimedOutOrClosed();
    try {
      super.setEscapeProcessing(enable);
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public int getQueryTimeout() throws SQLException {
    throwIfClosed();
    return super.getQueryTimeout();
  }

  @Override
  public void setQueryTimeout(int seconds) throws SQLException {
    throwIfTimedOutOrClosed();
    if ( seconds < 0 ) {
      throw new InvalidParameterSqlException(
          "Invalid (negative) \"seconds\" parameter to setQueryTimeout(...)"
              + " (" + seconds + ")" );
    }
    else {
      if ( 0 < seconds ) {
        timeoutTrigger = new TimeoutTrigger(this, seconds);
      } else {
        //Reset timeout to 0 (i.e. no timeout)
        if (timeoutTrigger != null) {
          //Cancelling existing triggered timeout
          if (timeoutTriggerHandle != null) {
            timeoutTriggerHandle.cancel(true);
          }
          timeoutTrigger = null;
        }
      }
      super.setQueryTimeout(seconds);
    }
  }

  @Override
  public void cancel() throws SQLException {
    throwIfTimedOutOrClosed();
    super.cancel();
  }

  @Override
  public void cancelDueToTimeout() throws SQLException {
    throwIfTimedOutOrClosed();
    cancel();
    throw new SqlTimeoutException(this.getQueryTimeout());
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    throwIfClosed();
    return super.getWarnings();
  }

  @Override
  public void clearWarnings() throws SQLException {
    throwIfTimedOutOrClosed();
    super.clearWarnings();
  }

  @Override
  public void setCursorName(String name) throws SQLException {
    throwIfTimedOutOrClosed();
    try {
      super.setCursorName(name);
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public boolean execute(String sql) throws SQLException {
    throwIfClosed();
    try {
      if (timeoutTrigger != null) {
        timeoutTriggerHandle = timeoutTrigger.startCountdown();
      }
      return super.execute(sql);
    }
    catch (UnsupportedOperationException e) {
      if ( isTimedOut() ) {
        throw new SqlTimeoutException(getQueryTimeout());
      }
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public ResultSet getResultSet() throws SQLException {
    //This getter is checked for timeout
    throwIfTimedOutOrClosed();
    return super.getResultSet();
  }

  @Override
  public int getUpdateCount() throws SQLException {
    throwIfClosed();
    return super.getUpdateCount();
  }

  @Override
  public boolean getMoreResults() throws SQLException {
    throwIfClosed();
    try {
      return super.getMoreResults();
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {
    throwIfTimedOutOrClosed();
    super.setFetchDirection(direction);
  }

  @Override
  public int getFetchDirection(){
    try {
      throwIfClosed();
    } catch (SQLException e) {
      // Can't throw any SQLException because AvaticaConnection's
      // getFetchDirection() is missing "throws SQLException".
      throw new RuntimeException(e.getMessage(), e);
    }
    return super.getFetchDirection();
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {
    throwIfTimedOutOrClosed();
    super.setFetchSize(rows);
  }

  @Override
  public int getFetchSize() {
    try {
      throwIfClosed();
    } catch (SQLException e) {
      // Can't throw any SQLException because AvaticaConnection's
      // getFetchSize() is missing "throws SQLException".
      throw new RuntimeException(e.getMessage(), e);
    }
    return super.getFetchSize();
  }

  @Override
  public int getResultSetConcurrency() throws SQLException {
    throwIfClosed();
    try {
      return super.getResultSetConcurrency();
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public int getResultSetType() throws SQLException {
    throwIfClosed();
    try {
      return super.getResultSetType();
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void addBatch(String sql) throws SQLException {
    throwIfTimedOutOrClosed();
    try {
      super.addBatch(sql);
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void clearBatch() throws SQLException {
    throwIfTimedOutOrClosed();
    try {
      super.clearBatch();
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public int[] executeBatch() throws SQLException {
    throwIfClosed();
    try {
      if (timeoutTrigger != null) {
        timeoutTriggerHandle = timeoutTrigger.startCountdown();
      }
      return super.executeBatch();
    }
    catch (UnsupportedOperationException e) {
      if ( isTimedOut() ) {
        throw new SqlTimeoutException(getQueryTimeout());
      }
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public boolean getMoreResults(int current) throws SQLException {
    throwIfClosed();
    try {
      return super.getMoreResults(current);
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public ResultSet getGeneratedKeys() throws SQLException {
    //This getter is checked for timeout
    throwIfTimedOutOrClosed();
    try {
      return super.getGeneratedKeys();
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
    throwIfClosed();
    try {
      if (timeoutTrigger != null) {
        timeoutTriggerHandle = timeoutTrigger.startCountdown();
      }
      return super.executeUpdate(sql, autoGeneratedKeys);
    }
    catch (UnsupportedOperationException e) {
      if ( isTimedOut() ) {
        throw new SqlTimeoutException(getQueryTimeout());
      }
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public int executeUpdate(String sql, int columnIndexes[]) throws SQLException {
    throwIfClosed();
    try {
      if (timeoutTrigger != null) {
        timeoutTriggerHandle = timeoutTrigger.startCountdown();
      }
      return super.executeUpdate(sql, columnIndexes);
    }
    catch (UnsupportedOperationException e) {
      if ( isTimedOut() ) {
        throw new SqlTimeoutException(getQueryTimeout());
      }
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public int executeUpdate(String sql, String columnNames[]) throws SQLException {
    throwIfClosed();
    try {
      if (timeoutTrigger != null) {
        timeoutTriggerHandle = timeoutTrigger.startCountdown();
      }
      return super.executeUpdate(sql, columnNames);
    }
    catch (UnsupportedOperationException e) {
      if ( isTimedOut() ) {
        throw new SqlTimeoutException(getQueryTimeout());
      }
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
    throwIfClosed();
    try {
      if (timeoutTrigger != null) {
        timeoutTriggerHandle = timeoutTrigger.startCountdown();
      }
      return super.execute(sql, autoGeneratedKeys);
    }
    catch (UnsupportedOperationException e) {
      if ( isTimedOut() ) {
        throw new SqlTimeoutException(getQueryTimeout());
      }
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public boolean execute(String sql, int columnIndexes[]) throws SQLException {
    throwIfClosed();
    try {
      if (timeoutTrigger != null) {
        timeoutTriggerHandle = timeoutTrigger.startCountdown();
      }
      return super.execute(sql, columnIndexes);
    }
    catch (UnsupportedOperationException e) {
      if ( isTimedOut() ) {
        throw new SqlTimeoutException(getQueryTimeout());
      }
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public boolean execute(String sql, String columnNames[]) throws SQLException {
    throwIfClosed();
    try {
      if (timeoutTrigger != null) {
        timeoutTriggerHandle = timeoutTrigger.startCountdown();
      }
      return super.execute(sql, columnNames);
    }
    catch (UnsupportedOperationException e) {
      if ( isTimedOut() ) {
        throw new SqlTimeoutException(getQueryTimeout());
      }
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public int getResultSetHoldability() throws SQLException {
    throwIfClosed();
    try {
      return super.getResultSetHoldability();
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public boolean isClosed() {
    try {
      return super.isClosed();
    } catch (SQLException e) {
      throw new RuntimeException(
          "Unexpected " + e + " from AvaticaPreparedStatement.isClosed" );
    }
  }

  @Override
  public void setPoolable(boolean poolable) throws SQLException {
    throwIfTimedOutOrClosed();
    try {
      super.setPoolable(poolable);
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public boolean isPoolable() throws SQLException {
    throwIfClosed();
    try {
      return super.isPoolable();
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void closeOnCompletion() throws SQLException {
    throwIfTimedOutOrClosed();
    super.closeOnCompletion();
  }

  @Override
  public boolean isCloseOnCompletion() throws SQLException {
    throwIfTimedOutOrClosed();
    return super.isCloseOnCompletion();
  }

  @Override
  public ResultSet executeQuery() throws SQLException {
    throwIfClosed();
    try {
      if (timeoutTrigger != null) {
        timeoutTriggerHandle = timeoutTrigger.startCountdown();
      }
      return super.executeQuery();
    }
    catch (UnsupportedOperationException e) {
      if ( isTimedOut() ) {
        throw new SqlTimeoutException(getQueryTimeout());
      }
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public long executeLargeUpdate() throws SQLException {
    throwIfClosed();
    try {
      if (timeoutTrigger != null) {
        timeoutTriggerHandle = timeoutTrigger.startCountdown();
      }
      return super.executeLargeUpdate();
    }
    catch (UnsupportedOperationException e) {
      if ( isTimedOut() ) {
        throw new SqlTimeoutException(getQueryTimeout());
      }
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  // Covered by superclass methods' calls to getParameter(int):
  // - setNull(int, int)
  // - setBoolean(int, boolean)
  // - setByte(int, byte)
  // - setShort(int, short)
  // - setInt(int, int)
  // - setLong(int, long)
  // - setFloat(int, float)
  // - setDouble(int, double)
  // - setBigDecimal(int, BigDecimal)
  // - setString(int, String)
  // - setBytes(int, byte[])
  // - setDate(int, Date)
  // - setTime(int, Time)
  // - setTimestamp(int, Timestamp)
  // - setAsciiStream(int, InputStream, int)
  // - setUnicodeStream(int, InputStream, int)
  // - setBinaryStream(int, InputStream, int)

  @Override
  public void clearParameters() throws SQLException {
    throwIfTimedOutOrClosed();
    try {
      super.clearParameters();
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  // Covered by superclass methods' calls to getParameter(int):
  // - setObject(int, Object, int)
  // - setObject(int, Object)

  @Override
  public boolean execute() throws SQLException {
    throwIfClosed();
    try {
      if (timeoutTrigger != null) {
        timeoutTriggerHandle = timeoutTrigger.startCountdown();
      }
      return super.execute();
    }
    catch (UnsupportedOperationException e) {
      if ( isTimedOut() ) {
        throw new SqlTimeoutException(getQueryTimeout());
      }
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void addBatch() throws SQLException {
    throwIfTimedOutOrClosed();
    try {
      super.addBatch();
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  // Covered by superclass methods' calls to getParameter(int):
  // - setCharacterStream(int, Reader, int)
  // - setRef(int, Ref)
  // - setBlob(int, Blob)
  // - setClob(int, Clob)
  // - setArray(int, Array)

  @Override
  public ResultSetMetaData getMetaData() {
    try {
      throwIfTimedOutOrClosed();
    } catch (SQLException e) {
      // Can't throw any SQLException because AvaticaConnection's
      // getMetaData() is missing "throws SQLException".
      throw new RuntimeException(e.getMessage(), e);
    }
    return super.getMetaData();
  }

  // Covered by superclass methods' calls to getParameter(int):
  // - setDate(int, Date, Calendar)
  // - setTime(int, Time, Calendar)
  // - setTimestamp(int, Timestamp, Calendar)
  // - setNull(int, int, String)
  // - setURL(int, URL)

  @Override
  public ParameterMetaData getParameterMetaData() throws SQLException {
    throwIfTimedOutOrClosed();
    return super.getParameterMetaData();
  }

  // The following methods are abstract in AvaticaPreparedStatement, and so
  // cannot be overridden here to add throwIfClosed calls.  They are addressed
  // via DrillJdbc41Factory (which calls back to getParameter(int) in here,
  // which calls throwIfClosed()).
  // - setRowId(int, RowId)
  // - setNString(int, String)
  // - setNCharacterStream(int, Reader, long)
  // - setNClob(int, NClob)
  // - setClob(int, Reader, long)
  // - setBlob(int, InputStream, long)
  // - setNClob(int, Reader, long)
  // - setSQLXML(int, SQLXML xmlObject)
  // - setObject(int, Object, int, int)
  // - setAsciiStream(int, InputStream, long)
  // - setBinaryStream(int, InputStream, long)
  // - setCharacterStream(int, Reader, long)
  // - setAsciiStream(int, InputStream)
  // - setBinaryStream(int, InputStream)
  // - setCharacterStream(int, Reader)
  // - setNCharacterStream(int, Reader)
  // - setClob(int, Reader)
  // - setBlob(int, InputStream)
  // - setNClob(int, Reader)

}
