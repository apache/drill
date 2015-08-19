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

import java.io.InputStream;
import java.io.Reader;
import java.sql.NClob;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;

import net.hydromatic.avatica.AvaticaConnection;
import net.hydromatic.avatica.AvaticaPrepareResult;
import net.hydromatic.avatica.AvaticaStatement;
import net.hydromatic.avatica.ColumnMetaData;


/**
 * Implementation of {@link net.hydromatic.avatica.AvaticaFactory} for Drill and
 * JDBC 4.1 (corresponds to JDK 1.7).
 */
// Note:  Must be public so net.hydromatic.avatica.UnregisteredDriver can
// (reflectively) call no-args constructor.
public class DrillJdbc41Factory extends DrillFactory {

  /** Creates a factory for JDBC version 4.1. */
  // Note:  Must be public so net.hydromatic.avatica.UnregisteredDriver can
  // (reflectively) call this constructor.
  public DrillJdbc41Factory() {
    this(4, 1);
  }

  /** Creates a JDBC factory with given major/minor version number. */
  protected DrillJdbc41Factory(int major, int minor) {
    super(major, minor);
  }


  @Override
  DrillConnectionImpl newDrillConnection(DriverImpl driver,
                                         DrillFactory factory,
                                         String url,
                                         Properties info) throws SQLException {
    return new DrillConnectionImpl(driver, factory, url, info);
  }

  @Override
  public DrillDatabaseMetaDataImpl newDatabaseMetaData(AvaticaConnection connection) {
    return new DrillDatabaseMetaDataImpl((DrillConnectionImpl) connection);
  }


  @Override
  public DrillStatementImpl newStatement(AvaticaConnection connection,
                                         int resultSetType,
                                         int resultSetConcurrency,
                                         int resultSetHoldability) {
    return new DrillStatementImpl((DrillConnectionImpl) connection,
                                  resultSetType,
                                  resultSetConcurrency,
                                  resultSetHoldability);
  }

  @Override
  public DrillJdbc41PreparedStatement newPreparedStatement(AvaticaConnection connection,
                                                       AvaticaPrepareResult prepareResult,
                                                       int resultSetType,
                                                       int resultSetConcurrency,
                                                       int resultSetHoldability)
      throws SQLException {
    return new DrillJdbc41PreparedStatement((DrillConnectionImpl) connection,
                                            (DrillPrepareResult) prepareResult,
                                            resultSetType,
                                            resultSetConcurrency,
                                            resultSetHoldability);
  }

  @Override
  public DrillResultSetImpl newResultSet(AvaticaStatement statement,
                                         AvaticaPrepareResult prepareResult,
                                         TimeZone timeZone) {
    final ResultSetMetaData metaData =
        newResultSetMetaData(statement, prepareResult.getColumnList());
    return new DrillResultSetImpl(statement, prepareResult, metaData, timeZone);
  }

  @Override
  public ResultSetMetaData newResultSetMetaData(AvaticaStatement statement,
                                                List<ColumnMetaData> columnMetaDataList) {
    return new DrillResultSetMetaDataImpl(statement, null, columnMetaDataList);
  }


  /**
   * JDBC 4.1 version of {@link DrillPreparedStatementImpl}.
   */
  private static class DrillJdbc41PreparedStatement extends DrillPreparedStatementImpl {

    DrillJdbc41PreparedStatement(DrillConnectionImpl connection,
                                 DrillPrepareResult prepareResult,
                                 int resultSetType,
                                 int resultSetConcurrency,
                                 int resultSetHoldability) throws SQLException {
      super(connection, prepareResult,
            resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    // These don't need throwIfClosed(), since getParameter already calls it.

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
      getParameter(parameterIndex).setRowId(x);
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
      getParameter(parameterIndex).setNString(value);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value,
                                    long length) throws SQLException {
      getParameter(parameterIndex).setNCharacterStream(value, length);
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
      getParameter(parameterIndex).setNClob(value);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader,
                        long length) throws SQLException {
      getParameter(parameterIndex).setClob(reader, length);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream,
                        long length) throws SQLException {
      getParameter(parameterIndex).setBlob(inputStream, length);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader,
                         long length) throws SQLException {
      getParameter(parameterIndex).setNClob(reader, length);
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
      getParameter(parameterIndex).setSQLXML(xmlObject);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x,
                               long length) throws SQLException {
      getParameter(parameterIndex).setAsciiStream(x, length);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x,
                                long length) throws SQLException {
      getParameter(parameterIndex).setBinaryStream(x, length);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader,
                                   long length) throws SQLException {
      getParameter(parameterIndex).setCharacterStream(reader, length);
    }

    @Override
    public void setAsciiStream(int parameterIndex,
                               InputStream x) throws SQLException {
      getParameter(parameterIndex).setAsciiStream(x);
    }

    @Override
    public void setBinaryStream(int parameterIndex,
                                InputStream x) throws SQLException {
      getParameter(parameterIndex).setBinaryStream(x);
    }

    @Override
    public void setCharacterStream(int parameterIndex,
                                   Reader reader) throws SQLException {
      getParameter(parameterIndex).setCharacterStream(reader);
    }

    @Override
    public void setNCharacterStream(int parameterIndex,
                                    Reader value) throws SQLException {
      getParameter(parameterIndex).setNCharacterStream(value);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
      getParameter(parameterIndex).setClob(reader);
    }

    @Override
    public void setBlob(int parameterIndex,
                        InputStream inputStream) throws SQLException {
      getParameter(parameterIndex).setBlob(inputStream);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
      getParameter(parameterIndex).setNClob(reader);
    }

  }

}

// End DrillJdbc41Factory.java
