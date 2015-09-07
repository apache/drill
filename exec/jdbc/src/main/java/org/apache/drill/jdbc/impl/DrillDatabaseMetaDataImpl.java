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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.drill.jdbc.AlreadyClosedSqlException;
import org.apache.drill.jdbc.DrillDatabaseMetaData;

import net.hydromatic.avatica.AvaticaConnection;
import net.hydromatic.avatica.AvaticaDatabaseMetaData;


/**
 * Drill's implementation of {@link DatabaseMetaData}.
 */
class DrillDatabaseMetaDataImpl extends AvaticaDatabaseMetaData
                                implements DrillDatabaseMetaData {

  protected DrillDatabaseMetaDataImpl( AvaticaConnection connection ) {
    super( connection );
  }

  /**
   * Throws AlreadyClosedSqlException if the associated Connection is closed.
   *
   * @throws AlreadyClosedSqlException if Connection is closed
   * @throws SQLException if error in calling {@link Connection#isClosed()}
   */
  private void checkNotClosed() throws AlreadyClosedSqlException,
                                       SQLException {
    if ( getConnection().isClosed() ) {
      throw new AlreadyClosedSqlException(
          "DatabaseMetaData's Connection is already closed." );
    }
  }


  // For omitted NULLS FIRST/NULLS HIGH, Drill sort NULL sorts as highest value:

  @Override
  public boolean nullsAreSortedHigh() throws SQLException {
    checkNotClosed();
    return true;
  }

  @Override
  public boolean nullsAreSortedLow() throws SQLException {
    checkNotClosed();
    return false;
  }

  @Override
  public boolean nullsAreSortedAtStart() throws SQLException {
    checkNotClosed();
    return false;
  }

  @Override
  public boolean nullsAreSortedAtEnd() throws SQLException {
    checkNotClosed();
    return false;
  }

  // TODO(DRILL-3510):  Update when Drill accepts standard SQL's double quote.
  @Override
  public String getIdentifierQuoteString() throws SQLException {
    checkNotClosed();
    return "`";
  }


  // For now, check whether connection is closed for most important methods
  // (DRILL-2565 (partial fix for DRILL-2489)):


  @Override
  public ResultSet getCatalogs() throws SQLException {
    checkNotClosed();
    return super.getCatalogs();
  }

  @Override
  public ResultSet getSchemas() throws SQLException {
    checkNotClosed();
    return super.getSchemas();
  }

  @Override
  public ResultSet getSchemas( String catalog, String schemaPattern ) throws SQLException {
    checkNotClosed();
    return super.getSchemas( catalog, schemaPattern );
  }

  @Override
  public ResultSet getTables( String catalog,
                              String schemaPattern,
                              String tableNamePattern,
                              String[] types ) throws SQLException {
    checkNotClosed();
    return super.getTables( catalog, schemaPattern,tableNamePattern, types );
  }

  @Override
  public ResultSet getColumns( String catalog, String schema, String table,
                               String columnNamePattern ) throws SQLException {
    checkNotClosed();
    return super.getColumns( catalog, schema, table, columnNamePattern );
  }

}
