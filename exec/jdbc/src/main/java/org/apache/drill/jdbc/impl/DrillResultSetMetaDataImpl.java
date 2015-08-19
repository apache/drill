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

import java.sql.SQLException;
import java.util.List;

import org.apache.drill.jdbc.AlreadyClosedSqlException;

import net.hydromatic.avatica.AvaticaResultSetMetaData;
import net.hydromatic.avatica.AvaticaStatement;
import net.hydromatic.avatica.ColumnMetaData;


public class DrillResultSetMetaDataImpl extends AvaticaResultSetMetaData {

  private final AvaticaStatement statement;


  public DrillResultSetMetaDataImpl( AvaticaStatement statement,
                                     Object query,
                                     List<ColumnMetaData> columnMetaDataList ) {
    super( statement, query, columnMetaDataList );
    this.statement = statement;
  }

  /**
   * Throws AlreadyClosedSqlException if the associated ResultSet is closed.
   *
   * @throws  AlreadyClosedSqlException  if ResultSet is closed
   * @throws  SQLException  if error in checking ResultSet's status
   */
  private void throwIfClosed() throws AlreadyClosedSqlException,
                                      SQLException {
    // Statement.isClosed() call is to avoid exception from getResultSet().
    if ( statement.isClosed()
         || statement.getResultSet().isClosed() ) {
        throw new AlreadyClosedSqlException(
            "ResultSetMetaData's ResultSet is already closed." );
    }
  }


  // Note:  Using dynamic proxies would reduce the quantity (450?) of method
  // overrides by eliminating those that exist solely to check whether the
  // object is closed.  It would also eliminate the need to throw non-compliant
  // RuntimeExceptions when Avatica's method declarations won't let us throw
  // proper SQLExceptions. (Check performance before applying to frequently
  // called ResultSet.)

  // Note:  Methods are in same order as in java.sql.ResultSetMetaData.

  // No isWrapperFor(Class<?>) (it doesn't throw SQLException if already closed).
  // No unwrap(Class<T>) (it doesn't throw SQLException if already closed).

  @Override
  public int getColumnCount() throws SQLException {
    throwIfClosed();
    return super.getColumnCount();
  }

  @Override
  public boolean isAutoIncrement(int column) throws SQLException {
    throwIfClosed();
    return super.isAutoIncrement(column);
  }

  @Override
  public boolean isCaseSensitive(int column) throws SQLException {
    throwIfClosed();
    return super.isCaseSensitive(column);
  }

  @Override
  public boolean isSearchable(int column) throws SQLException {
    throwIfClosed();
    return super.isSearchable(column);
  }

  @Override
  public boolean isCurrency(int column) throws SQLException {
    throwIfClosed();
    return super.isCurrency(column);
  }

  @Override
  public int isNullable(int column) throws SQLException {
    throwIfClosed();
    return super.isNullable(column);
  }

  @Override
  public boolean isSigned(int column) throws SQLException {
    throwIfClosed();
    return super.isSigned(column);
  }

  @Override
  public int getColumnDisplaySize(int column) throws SQLException {
    throwIfClosed();
    return super.getColumnDisplaySize(column);
  }

  @Override
  public String getColumnLabel(int column) throws SQLException {
    throwIfClosed();
    return super.getColumnLabel(column);
  }

  @Override
  public String getColumnName(int column) throws SQLException {
    throwIfClosed();
    return super.getColumnName(column);
  }

  @Override
  public String getSchemaName(int column) throws SQLException {
    throwIfClosed();
    return super.getSchemaName(column);
  }

  @Override
  public int getPrecision(int column) throws SQLException {
    throwIfClosed();
    return super.getPrecision(column);
  }

  @Override
  public int getScale(int column) throws SQLException {
    throwIfClosed();
    return super.getScale(column);
  }

  @Override
  public String getTableName(int column) throws SQLException {
    throwIfClosed();
    return super.getTableName(column);
  }

  @Override
  public String getCatalogName(int column) throws SQLException {
    throwIfClosed();
    return super.getCatalogName(column);
  }

  @Override
  public int getColumnType(int column) throws SQLException {
    throwIfClosed();
    return super.getColumnType(column);
  }

  @Override
  public String getColumnTypeName(int column) throws SQLException {
    throwIfClosed();
    return super.getColumnTypeName(column);
  }

  @Override
  public boolean isReadOnly(int column) throws SQLException {
    throwIfClosed();
    return super.isReadOnly(column);
  }

  @Override
  public boolean isWritable(int column) throws SQLException {
    throwIfClosed();
    return super.isWritable(column);
  }

  @Override
  public boolean isDefinitelyWritable(int column) throws SQLException {
    throwIfClosed();
    return super.isDefinitelyWritable(column);
  }

  @Override
  public String getColumnClassName(int column) throws SQLException {
    throwIfClosed();
    return super.getColumnClassName(column);
  }

}
