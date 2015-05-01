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

import java.sql.ResultSet;
import java.sql.SQLException;

import net.hydromatic.avatica.AvaticaStatement;

public abstract class DrillStatement extends AvaticaStatement
   implements DrillRemoteStatement {

  DrillStatement(DrillConnectionImpl connection, int resultSetType, int resultSetConcurrency, int resultSetHoldability) {
    super(connection, resultSetType, resultSetConcurrency, resultSetHoldability);
    connection.openStatementsRegistry.addStatement(this);
  }

  /**
   * Throws AlreadyClosedSqlException if this Statement is closed.
   *
   * @throws AlreadyClosedSqlException if Statement is closed
   * @throws SQLException if error in calling {@link #isClosed()}
   */
  private void checkNotClosed() throws SQLException {
    if ( isClosed() ) {
      throw new AlreadyClosedSqlException( "Statement is already closed." );
    }
  }

  @Override
  public DrillConnectionImpl getConnection() {
    return (DrillConnectionImpl) connection;
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
    checkNotClosed();
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
       checkNotClosed();
       return super.executeQuery( sql );
    }
    catch ( final SQLException possiblyExtraWrapperException ) {
      throw unwrapIfExtra( possiblyExtraWrapperException );
    }
  }

  @Override
  public int executeUpdate( String sql ) throws SQLException {
    checkNotClosed();
    try {
      return super.executeUpdate( sql );
    }
    catch ( final SQLException possiblyExtraWrapperException ) {
      throw unwrapIfExtra( possiblyExtraWrapperException );
    }
  }

  @Override
  public int executeUpdate( String sql, int[] columnIndexes ) throws SQLException {
    checkNotClosed();
    return super.executeUpdate( sql, columnIndexes );
  }

  @Override
  public int executeUpdate( String sql, String[] columnNames ) throws SQLException {
    checkNotClosed();
    return super.executeUpdate( sql, columnNames );
  }

  @Override
  public void cleanup() {
    final DrillConnectionImpl connection1 = (DrillConnectionImpl) connection;
    connection1.openStatementsRegistry.removeStatement(this);
  }

}