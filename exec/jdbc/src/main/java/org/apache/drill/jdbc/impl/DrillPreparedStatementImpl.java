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

import org.apache.drill.jdbc.DrillPreparedStatement;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

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

  @Override
  public DrillConnectionImpl getConnection() {
    return (DrillConnectionImpl) super.getConnection();
  }

  @Override
  protected AvaticaParameter getParameter(int param) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Prepared-statement dynamic parameters are not supported.");
  }

  @Override
  public void cleanUp() {
    final DrillConnectionImpl connection1 = (DrillConnectionImpl) connection;
    connection1.openStatementsRegistry.removeStatement(this);
  }

}
