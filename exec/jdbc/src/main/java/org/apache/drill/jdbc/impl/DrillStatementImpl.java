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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.concurrent.Future;

import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.Meta.StatementHandle;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.jdbc.AlreadyClosedSqlException;
import org.apache.drill.jdbc.DrillStatement;
import org.apache.drill.jdbc.InvalidParameterSqlException;
import org.apache.drill.jdbc.SqlTimeoutException;

/**
 * Drill's implementation of {@link Statement}.
 */
// (Was abstract to avoid errors _here_ if newer versions of JDBC added
// interface methods, but now newer versions would probably use Java 8's default
// methods for compatibility.)
class DrillStatementImpl extends AvaticaStatement implements DrillStatement,
                                                             DrillRemoteStatement {

  private final DrillConnectionImpl connection;
  private TimeoutTrigger timeoutTrigger;
  private Future<?> timeoutTriggerHandle;

  DrillStatementImpl(DrillConnectionImpl connection, StatementHandle h, int resultSetType,
                     int resultSetConcurrency, int resultSetHoldability) {
    super(connection, h, resultSetType, resultSetConcurrency, resultSetHoldability);
    this.connection = connection;
    connection.openStatementsRegistry.addStatement(this);
  }

  /**
   * Throws {@link SqlTimeoutException} or {@link AlreadyClosedSqlException} <i>iff</i> this Statement is closed.
   * @throws SqlTimeoutException        if Statement has closed due to timeout
   * @throws AlreadyClosedSqlException  if Statement is closed
   * @throws SQLException               Other unaccounted SQL exceptions
   */
  private void throwIfTimedOutOrClosed() throws SqlTimeoutException, AlreadyClosedSqlException, SQLException {
    throwIfTimedOut();
    throwIfClosed();
  }

  /**
   * Throws AlreadyClosedSqlException <i>iff</i> this Statement is closed.
   * @throws  AlreadyClosedSqlException  if Statement is closed
   * @throws  SQLException               Other unaccounted SQL exceptions
   */
  private void throwIfClosed() throws AlreadyClosedSqlException {
    if ( isClosed() ) {
      throw new AlreadyClosedSqlException( "Statement is already closed." );
    }
  }

  /**
   * Throws {@link SqlTimeoutException} <i>iff</i> this Statement had closed due to timeout.
   *
   * @throws  SqlTimeoutException  if Statement has closed due to timeout
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
    // Can't throw any SQLException because AvaticaConnection's getConnection() is
    // missing "throws SQLException".
    try {
      throwIfClosed();
    } catch (AlreadyClosedSqlException e) {
      throw new RuntimeException(e.getMessage(), e);
    }
    return connection;
  }

  // WORKAROUND:  Work around AvaticaStatement's code that wraps _any_ exception,
  // even if SQLException, by unwrapping to get cause exception so caller can
  // throw it directly if it's a SQLException:
  // TODO:  Any ideas for a better name?
  private SQLException unwrapIfExtra( final SQLException superMethodException ) {
    final SQLException result;
    final Throwable cause = superMethodException.getCause();
    if ( null != cause && cause instanceof SQLException ) {
      result = (SQLException) cause;
    }
    else {
      result = superMethodException;
    }
    return result;
  }

  @Override
  public boolean execute( String sql ) throws SQLException {
    throwIfClosed();
    try {
      if (timeoutTrigger != null) {
        timeoutTriggerHandle = timeoutTrigger.startCountdown();
      }
      return super.execute( sql );
    }
    catch ( final SQLException possiblyExtraWrapperException ) {
      if ( isTimedOut() ) {
        throw new SqlTimeoutException(getQueryTimeout());
      }
      throw unwrapIfExtra( possiblyExtraWrapperException );
    }
  }

  @Override
  public ResultSet executeQuery( String sql ) throws SQLException {
    try {
      throwIfClosed();
      if (timeoutTrigger != null) {
        timeoutTriggerHandle = timeoutTrigger.startCountdown();
      }
      return super.executeQuery( sql );
    }
    catch ( final SQLException possiblyExtraWrapperException ) {
      if ( isTimedOut() ) {
        throw new SqlTimeoutException(getQueryTimeout());
      }
      throw unwrapIfExtra( possiblyExtraWrapperException );
    }
  }

  @Override
  public long executeLargeUpdate( String sql ) throws SQLException {
    throwIfClosed();
    try {
      if (timeoutTrigger != null) {
        timeoutTriggerHandle = timeoutTrigger.startCountdown();
      }
      return super.executeLargeUpdate( sql );
    }
    catch ( final SQLException possiblyExtraWrapperException ) {
      if ( isTimedOut() ) {
        throw new SqlTimeoutException(getQueryTimeout());
      }
      throw unwrapIfExtra( possiblyExtraWrapperException );
    }
  }

  @Override
  public int executeUpdate( String sql, int[] columnIndexes ) throws SQLException {
    throwIfClosed();
    try {
      if (timeoutTrigger != null) {
        timeoutTriggerHandle = timeoutTrigger.startCountdown();
      }
      return super.executeUpdate( sql, columnIndexes );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public int executeUpdate( String sql, String[] columnNames ) throws SQLException {
    throwIfClosed();
    try {
      if ( null != timeoutTrigger ) {
        timeoutTriggerHandle = timeoutTrigger.startCountdown();
      }
      return super.executeUpdate( sql, columnNames );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void cleanUp() {
    //Cancel Timeout trigger
    if (this.timeoutTrigger != null) {
      timeoutTriggerHandle.cancel(true);
    }
    final DrillConnectionImpl connection1 = connection;
    connection1.openStatementsRegistry.removeStatement(this);
  }

  @Override
  public int getQueryTimeout() throws SQLException {
    throwIfClosed();
    return super.getQueryTimeout();
  }

  @Override
  public void setQueryTimeout( int seconds ) throws SQLException {
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
  public boolean isClosed() {
    try {
      return super.isClosed();
    }
    catch ( SQLException e ) {
      // Currently can't happen, since AvaticaStatement.isClosed() never throws
      // SQLException.
      throw new DrillRuntimeException(
          "Unexpected exception from " + getClass().getSuperclass()
          + ".isClosed(): " + e,
          e );
    }
  }

  // Note:  Methods are in same order as in java.sql.Statement.

  // No isWrapperFor(Class<?>) (it doesn't throw SQLException if already closed).
  // No unwrap(Class<T>) (it doesn't throw SQLException if already closed).
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
      // getMaxRows() is missing "throws SQLException".
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
  public int getFetchDirection() {
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
        //Note: JDBC API doesn't specify if the timeout applies for the entire batch or for each. We're assuming it is for the entire batch
        timeoutTriggerHandle = timeoutTrigger.startCountdown();
      }
      return super.executeBatch();
    }
    catch (UnsupportedOperationException e) {
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
    //This getter explicitly checks for a timeout
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

}
