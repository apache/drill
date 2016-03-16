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

import org.apache.drill.jdbc.AlreadyClosedSqlException;
import org.apache.drill.jdbc.DrillPreparedStatement;

import java.sql.ParameterMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;

import net.hydromatic.avatica.AvaticaParameter;
import net.hydromatic.avatica.AvaticaPrepareResult;
import net.hydromatic.avatica.AvaticaPreparedStatement;

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

  protected DrillPreparedStatementImpl(DrillConnectionImpl connection,
                                       AvaticaPrepareResult prepareResult,
                                       int resultSetType,
                                       int resultSetConcurrency,
                                       int resultSetHoldability) throws SQLException {
    super(connection, prepareResult,
          resultSetType, resultSetConcurrency, resultSetHoldability);
    connection.openStatementsRegistry.addStatement(this);
  }

  /**
   * Throws AlreadyClosedSqlException <i>iff</i> this PreparedStatement is closed.
   *
   * @throws  AlreadyClosedSqlException  if PreparedStatement is closed
   */
  private void throwIfClosed() throws AlreadyClosedSqlException {
    if (isClosed()) {
      throw new AlreadyClosedSqlException("PreparedStatement is already closed.");
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
    try {
      throwIfClosed();
    } catch (AlreadyClosedSqlException e) {
      // Can't throw any SQLException because AvaticaConnection's
      // getConnection() is missing "throws SQLException".
      throw new RuntimeException(e.getMessage(), e);
    }
    return (DrillConnectionImpl) super.getConnection();
  }

  @Override
  protected AvaticaParameter getParameter(int param) throws SQLException {
    throwIfClosed();
    throw new SQLFeatureNotSupportedException(
        "Prepared-statement dynamic parameters are not supported.");
  }

  @Override
  public void cleanUp() {
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
      return super.executeQuery(sql);
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public int executeUpdate(String sql) throws SQLException {
    throwIfClosed();
    try {
      return super.executeUpdate(sql);
    }
    catch (UnsupportedOperationException e) {
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
  public int getQueryTimeout() throws SQLException {
    throwIfClosed();
    return super.getQueryTimeout();
  }

  @Override
  public void setQueryTimeout(int seconds) throws SQLException {
    throwIfClosed();
    super.setQueryTimeout(seconds);
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
  public boolean execute(String sql) throws SQLException {
    throwIfClosed();
    return super.execute(sql);
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
  public int getFetchDirection(){
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
  public int executeUpdate(String sql, int columnIndexes[]) throws SQLException {
    throwIfClosed();
    try {
      return super.executeUpdate(sql, columnIndexes);
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public int executeUpdate(String sql, String columnNames[]) throws SQLException {
    throwIfClosed();
    try {
      return super.executeUpdate(sql, columnNames);
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

  @Override
  public ResultSet executeQuery() throws SQLException {
    throwIfClosed();
    return super.executeQuery();
  }

  @Override
  public int executeUpdate() throws SQLException {
    throwIfClosed();
    try {
      return super.executeUpdate();
    }
    catch (UnsupportedOperationException e) {
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
    throwIfClosed();
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
      return super.execute();
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void addBatch() throws SQLException {
    throwIfClosed();
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
      throwIfClosed();
    } catch (AlreadyClosedSqlException e) {
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
    throwIfClosed();
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
