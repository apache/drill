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
  private void checkNotClosed() throws AlreadyClosedSqlException {
    if ( isClosed() ) {
      throw new AlreadyClosedSqlException( "Statement is already closed." );
    }
  }

  @Override
  public DrillConnectionImpl getConnection() {
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
  public void cleanUp() {
    final DrillConnectionImpl connection1 = (DrillConnectionImpl) connection;
    connection1.openStatementsRegistry.removeStatement(this);
  }

  @Override
  public int getQueryTimeout() throws AlreadyClosedSqlException
  {
    checkNotClosed();
    return 0;  // (No no timeout.)
  }

  @Override
  public void setQueryTimeout( int milliseconds )
      throws AlreadyClosedSqlException,
             InvalidParameterSqlException,
             SQLFeatureNotSupportedException {
    checkNotClosed();
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

}
