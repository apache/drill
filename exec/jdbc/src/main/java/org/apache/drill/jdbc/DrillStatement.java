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


  @Override
  public boolean execute( String sql ) throws SQLException {
    checkNotClosed();
    return super.execute( sql );
  }

  @Override
  public ResultSet executeQuery( String sql ) throws SQLException {
    checkNotClosed();
    return super.executeQuery( sql );
  }

  @Override
  public int executeUpdate( String sql ) throws SQLException {
    checkNotClosed();
    return super.executeUpdate( sql );
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