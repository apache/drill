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
import net.hydromatic.avatica.AvaticaDatabaseMetaData;
import net.hydromatic.avatica.AvaticaPrepareResult;
import net.hydromatic.avatica.AvaticaPreparedStatement;
import net.hydromatic.avatica.AvaticaResultSetMetaData;
import net.hydromatic.avatica.AvaticaStatement;
import net.hydromatic.avatica.ColumnMetaData;

/**
 * Implementation of {@link net.hydromatic.avatica.AvaticaFactory} for Drill and JDBC 4.1 (corresponds to JDK 1.7).
 */
@SuppressWarnings("UnusedDeclaration")
public class DrillJdbc41Factory extends DrillFactory {
  /** Creates a factory for JDBC version 4.1. */
  public DrillJdbc41Factory() {
    this(4, 1);
  }

  /** Creates a JDBC factory with given major/minor version number. */
  protected DrillJdbc41Factory(int major, int minor) {
    super(major, minor);
  }

  @Override
  public DrillJdbc41Connection newDrillConnection(Driver driver, DrillFactory factory, String url, Properties info)  throws SQLException{
    return new DrillJdbc41Connection((Driver) driver, factory, url, info);
  }

  @Override
  public DrillJdbc41DatabaseMetaData newDatabaseMetaData(AvaticaConnection connection) {
    return new DrillJdbc41DatabaseMetaData((DrillConnectionImpl) connection);
  }


  @Override
  public DrillJdbc41Statement newStatement(AvaticaConnection connection, int resultSetType, int resultSetConcurrency,
      int resultSetHoldability) {
    return new DrillJdbc41Statement((DrillConnectionImpl) connection, resultSetType, resultSetConcurrency,
        resultSetHoldability);
  }

  @Override
  public AvaticaPreparedStatement newPreparedStatement(AvaticaConnection connection,
      AvaticaPrepareResult prepareResult, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
      throws SQLException {
    return new DrillJdbc41PreparedStatement((DrillConnectionImpl) connection, (DrillPrepareResult) prepareResult,
        resultSetType, resultSetConcurrency, resultSetHoldability);
  }

  @Override
  public DrillResultSet newResultSet(AvaticaStatement statement, AvaticaPrepareResult prepareResult, TimeZone timeZone) {
    final ResultSetMetaData metaData = newResultSetMetaData(statement, prepareResult.getColumnList());
    return new DrillResultSet(statement, (DrillPrepareResult) prepareResult, metaData, timeZone);
  }

  @Override
  public ResultSetMetaData newResultSetMetaData(AvaticaStatement statement, List<ColumnMetaData> columnMetaDataList) {
    return new AvaticaResultSetMetaData(statement, null, columnMetaDataList);
  }

  private static class DrillJdbc41Connection extends DrillConnectionImpl {
    DrillJdbc41Connection(Driver driver, DrillFactory factory, String url, Properties info) throws SQLException {
      super(driver, factory, url, info);
    }

  }

  private static class DrillJdbc41Statement extends DrillStatement {
    public DrillJdbc41Statement(DrillConnectionImpl connection, int resultSetType, int resultSetConcurrency,
        int resultSetHoldability) {
      super(connection, resultSetType, resultSetConcurrency, resultSetHoldability);
    }
  }

  private static class DrillJdbc41PreparedStatement extends DrillPreparedStatement {
    DrillJdbc41PreparedStatement(DrillConnectionImpl connection, DrillPrepareResult prepareResult, int resultSetType,
        int resultSetConcurrency, int resultSetHoldability) throws SQLException {
      super(connection, prepareResult, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    public void setRowId(int parameterIndex, RowId x) throws SQLException {
      getParameter(parameterIndex).setRowId(x);
    }

    public void setNString(int parameterIndex, String value) throws SQLException {
      getParameter(parameterIndex).setNString(value);
    }

    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
      getParameter(parameterIndex).setNCharacterStream(value, length);
    }

    public void setNClob(int parameterIndex, NClob value) throws SQLException {
      getParameter(parameterIndex).setNClob(value);
    }

    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
      getParameter(parameterIndex).setClob(reader, length);
    }

    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
      getParameter(parameterIndex).setBlob(inputStream, length);
    }

    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
      getParameter(parameterIndex).setNClob(reader, length);
    }

    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
      getParameter(parameterIndex).setSQLXML(xmlObject);
    }

    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
      getParameter(parameterIndex).setAsciiStream(x, length);
    }

    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
      getParameter(parameterIndex).setBinaryStream(x, length);
    }

    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
      getParameter(parameterIndex).setCharacterStream(reader, length);
    }

    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
      getParameter(parameterIndex).setAsciiStream(x);
    }

    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
      getParameter(parameterIndex).setBinaryStream(x);
    }

    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
      getParameter(parameterIndex).setCharacterStream(reader);
    }

    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
      getParameter(parameterIndex).setNCharacterStream(value);
    }

    public void setClob(int parameterIndex, Reader reader) throws SQLException {
      getParameter(parameterIndex).setClob(reader);
    }

    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
      getParameter(parameterIndex).setBlob(inputStream);
    }

    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
      getParameter(parameterIndex).setNClob(reader);
    }
  }

  private static class DrillJdbc41DatabaseMetaData extends DrillDatabaseMetaData {
    DrillJdbc41DatabaseMetaData(DrillConnectionImpl connection) {
      super(connection);
    }
  }

}

// End DrillJdbc41Factory.java
