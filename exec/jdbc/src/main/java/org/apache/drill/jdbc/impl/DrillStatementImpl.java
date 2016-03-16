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

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.jdbc.AlreadyClosedSqlException;
import org.apache.drill.jdbc.DrillStatement;
import org.apache.drill.jdbc.InvalidParameterSqlException;

import net.hydromatic.avatica.AvaticaStatement;

/**
 * Drill's implementation of {@link Statement}.
 */
// (Was abstract to avoid errors _here_ if newer versions of JDBC added
// interface methods, but now newer versions would probably use Java 8's default
// methods for compatibility.)
class DrillStatementImpl extends AvaticaStatement implements DrillStatement,
                                                             DrillRemoteStatement {

  private final DrillConnectionImpl connection;

  DrillStatementImpl(DrillConnectionImpl connection, int resultSetType,
                     int resultSetConcurrency, int resultSetHoldability) {
    super(connection, resultSetType, resultSetConcurrency, resultSetHoldability);
    this.connection = connection;
    connection.openStatementsRegistry.addStatement(this);
  }

  /**
   * Throws AlreadyClosedSqlException <i>iff</i> this Statement is closed.
   *
   * @throws  AlreadyClosedSqlException  if Statement is closed
   */
  private void throwIfClosed() throws AlreadyClosedSqlException {
    if ( isClosed() ) {
      throw new AlreadyClosedSqlException( "Statement is already closed." );
    }
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
      return super.execute( sql );
    }
    catch ( final SQLException possiblyExtraWrapperException ) {
      throw unwrapIfExtra( possiblyExtraWrapperException );
    }
  }

  @Override
  public ResultSet executeQuery( String sql ) throws SQLException {
    try {
       throwIfClosed();
       return super.executeQuery( sql );
    }
    catch ( final SQLException possiblyExtraWrapperException ) {
      throw unwrapIfExtra( possiblyExtraWrapperException );
    }
  }

  @Override
  public int executeUpdate( String sql ) throws SQLException {
    throwIfClosed();
    try {
      return super.executeUpdate( sql );
    }
    catch ( final SQLException possiblyExtraWrapperException ) {
      throw unwrapIfExtra( possiblyExtraWrapperException );
    }
  }

  @Override
  public int executeUpdate( String sql, int[] columnIndexes ) throws SQLException {
    throwIfClosed();
    try {
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
      return super.executeUpdate( sql, columnNames );
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void cleanUp() {
    final DrillConnectionImpl connection1 = (DrillConnectionImpl) connection;
    connection1.openStatementsRegistry.removeStatement(this);
  }

  @Override
  public int getQueryTimeout() throws AlreadyClosedSqlException
  {
    throwIfClosed();
    return 0;  // (No no timeout.)
  }

  @Override
  public void setQueryTimeout( int milliseconds )
      throws AlreadyClosedSqlException,
             InvalidParameterSqlException,
             SQLFeatureNotSupportedException {
    throwIfClosed();
    if ( milliseconds < 0 ) {
      throw new InvalidParameterSqlException(
          "Invalid (negative) \"milliseconds\" parameter to setQueryTimeout(...)"
          + " (" + milliseconds + ")" );
    }
    else {
      if ( 0 != milliseconds ) {
        throw new SQLFeatureNotSupportedException(
            "Setting network timeout is not supported." );
      }
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
    throwIfClosed();
    try {
      super.setMaxFieldSize(max);
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public int getMaxRows() {
    try {
      throwIfClosed();
    } catch (AlreadyClosedSqlException e) {
      // Can't throw any SQLException because AvaticaConnection's
      // getMaxRows() is missing "throws SQLException".
      throw new RuntimeException(e.getMessage(), e);
    }
    return super.getMaxRows();
  }

  @Override
  public void setMaxRows(int max) throws SQLException {
    throwIfClosed();
    super.setMaxRows(max);
  }

  @Override
  public void setEscapeProcessing(boolean enable) throws SQLException {
    throwIfClosed();
    try {
      super.setEscapeProcessing(enable);
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void cancel() throws SQLException {
    throwIfClosed();
    super.cancel();
  }

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
  public void setCursorName(String name) throws SQLException {
    throwIfClosed();
    try {
      super.setCursorName(name);
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public ResultSet getResultSet() throws SQLException {
    throwIfClosed();
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
    throwIfClosed();
    super.setFetchDirection(direction);
  }

  @Override
  public int getFetchDirection() {
    try {
      throwIfClosed();
    } catch (AlreadyClosedSqlException e) {
      // Can't throw any SQLException because AvaticaConnection's
      // getFetchDirection() is missing "throws SQLException".
      throw new RuntimeException(e.getMessage(), e);
    }
    return super.getFetchDirection();
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {
    throwIfClosed();
    super.setFetchSize(rows);
  }

  @Override
  public int getFetchSize() {
    try {
      throwIfClosed();
    } catch (AlreadyClosedSqlException e) {
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
    throwIfClosed();
    try {
      super.addBatch(sql);
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void clearBatch() throws SQLException {
    throwIfClosed();
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
    throwIfClosed();
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
    throwIfClosed();
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
    throwIfClosed();
    super.closeOnCompletion();
  }

  @Override
  public boolean isCloseOnCompletion() throws SQLException {
    throwIfClosed();
    return super.isCloseOnCompletion();
  }

}
