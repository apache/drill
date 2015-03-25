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
package org.apache.drill.jdbc.test;

import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.*;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import org.apache.drill.jdbc.Driver;
import org.apache.drill.jdbc.JdbcTest;
import org.apache.drill.jdbc.AlreadyClosedSqlException;

/**
 * Currently, test for DRILL-2565--for key methods only:  for closed Connection,
 * Statement, ResultSet, etc., object, throw some kind of "object already closed"
 * SQLException,
 *
 * <p>
 * TO BE:  Test for DRILL-2489--on closed Connection, Statement, ResultSet, etc.,
 * objects, most methods must throw some kind of "object already closed" SQLException,
 * but few do.
 * </p>
 * <p>
 *  NOTE:  Test currently covers only {@link Connection}, {@link Statement},
 *  {@link ResultSet} and part of {@link DatabaseMetaData} (but not
 *  {@link Statement} subclasses, {@link ResultsetMetadata}, or any relevant
 *  secondary objects such as {@link Clob} or {@link Array}.
 * </p>
 */
public class Drill2489CallsAfterCloseThrowExceptionsTest extends JdbcTest {

  private static Connection closedConnection;
  private static Statement closedStatement;
  private static ResultSet closedResultSet;
  private static DatabaseMetaData closedDatabaseMetaData;

  @BeforeClass
  public static void setUpConnection() throws Exception {
    final Connection connection = new Driver().connect( "jdbc:drill:zk=local", null );
    final Statement stmt = connection.createStatement();
    final ResultSet result =
        stmt.executeQuery( "SELECT * FROM INFORMATION_SCHEMA.CATALOGS" );
    result.next();
    final DatabaseMetaData dbmd = connection.getMetaData();

    result.close();
    closedResultSet = result;
    stmt.close();
    closedStatement = stmt;
    connection.close();
    closedConnection = connection;
    closedDatabaseMetaData = dbmd;
  }

  ///////////////////////////////////////////////////////////////
  // Connection methods:

  ////////////////////////////////////////
  // - methods that do _not_ throw exception for closed Connection:

  public void testClosedConnection_close_doesNotThrow() throws SQLException {
    closedConnection.close();
  }

  public void testClosedConnection_isClosed_returnsTrue() throws SQLException {
    assertThat( closedConnection.isClosed(), equalTo( true ) );
  }

  public void testClosedConnection_isValid_returnsTrue() throws SQLException {
    assertThat( closedConnection.isValid( 1_000 ), equalTo( true ) );
  }

  ////////////////////////////////////////
  // - methods that do throw exception for closed Connection:

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedConnection_abort_throws() throws SQLException {
    closedConnection.abort( (Executor) null );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedConnection_clearWarnings_throws() throws SQLException {
    closedConnection.clearWarnings();
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedConnection_commit_throws() throws SQLException {
    closedConnection.commit();
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedConnection_createArrayOf_throws() throws SQLException {
    closedConnection.createArrayOf( "typeName" , (Object[]) null );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedConnection_createBlob_throws() throws SQLException {
    closedConnection.createBlob();
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedConnection_createClob_throws() throws SQLException {
    closedConnection.createClob();
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedConnection_createNClob_throws() throws SQLException {
    closedConnection.createNClob();
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedConnection_createSQLXML_throws() throws SQLException {
    closedConnection.createSQLXML();
  }

  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedConnection_createStatement1_throws() throws SQLException {
    closedConnection.createStatement();
  }

  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedConnection_createStatement2_throws() throws SQLException {
    closedConnection.createStatement( -1, -2 );
  }

  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedConnection_createStatement3_throws() throws SQLException {
    closedConnection.createStatement( -1, -2 );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedConnection_createStruct_throws() throws SQLException {
    closedConnection.createStruct( "typeName", (Object[]) null );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedConnection_getAutoCommit_throws() throws SQLException {
    closedConnection.getAutoCommit();
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedConnection_getCatalog_throws() throws SQLException {
    closedConnection.getCatalog();
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedConnection_getClientInfo1_throws() throws SQLException {
    closedConnection.getClientInfo();
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedConnection_getClientInfo2_throws() throws SQLException {
    closedConnection.getClientInfo( " name" );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedConnection_getHoldability_throws() throws SQLException {
    closedConnection.getHoldability();
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedConnection_getMetaData_throws() throws SQLException {
    closedConnection.getMetaData();
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedConnection_getNetworkTimeout_throws() throws SQLException {
    closedConnection.getNetworkTimeout();
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedConnection_getSchema_throws() throws SQLException {
    closedConnection.getSchema();
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedConnection_getTransactionIsolation_throws() throws SQLException {
    closedConnection.getTransactionIsolation();
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedConnection_getTypeMap_throws() throws SQLException {
    closedConnection.getTypeMap();
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedConnection_getWarnings_throws() throws SQLException {
    closedConnection.getWarnings();
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedConnection_isReadOnly_throws() throws SQLException {
    closedConnection.isReadOnly();
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedConnection_nativeSQL_throws() throws SQLException {
    closedConnection.nativeSQL( "sql" );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedConnection_prepareCall1_throws() throws SQLException {
    closedConnection.prepareCall( "sql" );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedConnection_prepareCall2_throws() throws SQLException {
    closedConnection.prepareCall( "sql", -1, -2 );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedConnection_prepareCall3_throws() throws SQLException {
    closedConnection.prepareCall( "sql", -1, -2, -3 );
  }

  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedConnection_prepareStatement1_throws() throws SQLException {
    closedConnection.prepareStatement( "sql" );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedConnection_prepareStatement2_throws() throws SQLException {
    closedConnection.prepareStatement( "sql", (String[]) null );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedConnection_prepareStatement3_throws() throws SQLException {
    closedConnection.prepareStatement( "sql", -4 );
  }

  public void testClosedConnection_prepareStatement4_throws() throws SQLException {
    closedConnection.prepareStatement( "sql", -1, -2 );
  }

  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedConnection_prepareStatement5_throws() throws SQLException {
    closedConnection.prepareStatement( "sql", -1, -2, -3 );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedConnection_prepareStatement6_throws() throws SQLException {
    closedConnection.prepareStatement( "sql", (int[]) null );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedConnection_releaseSavepoint_throws() throws SQLException {
    closedConnection.releaseSavepoint( (Savepoint) null );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedConnection_rollback1_throws() throws SQLException {
    closedConnection.rollback();
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedConnection_rollback2_throws() throws SQLException {
    closedConnection.rollback( (Savepoint) null );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedConnection_setAutoCommit_throws() throws SQLException {
    closedConnection.setAutoCommit( true );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedConnection_setCatalog_throws() throws SQLException {
    closedConnection.setCatalog( "catalog" );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedConnection_setClientInfo1_throws() throws SQLException {
    closedConnection.setClientInfo( (Properties) null );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedConnection_setClientInfo2_throws() throws SQLException {
    closedConnection.setClientInfo( "name", "value" );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedConnection_setHoldability_throws() throws SQLException {
    closedConnection.setHoldability( ResultSet.CLOSE_CURSORS_AT_COMMIT );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedConnection_setNetworkTimeout_throws() throws SQLException {
    closedConnection.setNetworkTimeout( (Executor) null, 1_000 );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedConnection_setReadOnly_throws() throws SQLException {
    closedConnection.setReadOnly( true );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedConnection_setSavepoint1_throws() throws SQLException {
    closedConnection.setSavepoint();
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedConnection_setSavepoint2_throws() throws SQLException {
    closedConnection.setSavepoint( "name" );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedConnection_setSchema_throws() throws SQLException {
    closedConnection.setSchema( "schema" );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedConnection_setTransactionIsolation_throws() throws SQLException {
    closedConnection.setTransactionIsolation( -1 );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedConnection_setTypeMap_throws() throws SQLException {
    closedConnection.setTypeMap( (Map<String,Class<?>>) null );
  }


  ///////////////////////////////////////////////////////////////
  // Statement methods:

  ////////////////////////////////////////
  // - methods that do _not_ throw exception for closed Statement:

  public void testClosedStatement_close_doesNotThrow() throws SQLException {
    closedStatement.close();
  }

  public void testClosedStatement_isClosed_returnsTrue() throws SQLException {
    assertThat( closedStatement.isClosed(), equalTo( true ) );
  }

  ////////////////////////////////////////
  // - methods that do throw exception for closed Statement:

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedStatement_addBatch_throws() throws SQLException {
    closedStatement.addBatch( "USE dfs.tmp" );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedStatement_cancel_throws() throws SQLException {
    closedStatement.cancel();
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedStatement_clearBatch_throws() throws SQLException {
    closedStatement.clearBatch();
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedStatement_clearWarnings_throws() throws SQLException {
    closedStatement.clearWarnings();
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedStatement_execute1_throws() throws SQLException {
    closedStatement.execute( "USE dfs.tmp", Statement.RETURN_GENERATED_KEYS );
  }

  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedStatement_execute2_throws() throws SQLException {
    closedStatement.execute( "USE dfs.tmp" );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedStatement_execute3_throws() throws SQLException {
    closedStatement.execute( "USE dfs.tmp", (int[]) null );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedStatement_execute4_throws() throws SQLException {
    closedStatement.execute( "USE dfs.tmp", (String[]) null );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedStatement_executeBatch_throws() throws SQLException {
    closedStatement.executeBatch();
  }

  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedStatement_executeQuery_throws() throws SQLException {
    closedStatement.executeQuery( "USE dfs.tmp" );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedStatement_executeUpdate1_throws() throws SQLException {
    closedStatement.executeUpdate( "USE dfs.tmp", Statement.RETURN_GENERATED_KEYS );
  }

  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedStatement_executeUpdate2_throws() throws SQLException {
    closedStatement.executeUpdate( "USE dfs.tmp", (int[]) null );
  }

  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedStatement_executeUpdate3_throws() throws SQLException {
    closedStatement.executeUpdate( "USE dfs.tmp", (String[]) null );
  }

  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedStatement_executeUpdate4_throws() throws SQLException {
    closedStatement.executeUpdate( "USE dfs.tmp" );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedStatement_getConnection_throws() throws SQLException {
    closedStatement.getConnection();
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedStatement_getFetchDirection_throws() throws SQLException {
    closedStatement.getFetchDirection();
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedStatement_getFetchSize_throws() throws SQLException {
    closedStatement.getFetchSize();
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedStatement_getGeneratedKeys_throws() throws SQLException {
    closedStatement.getGeneratedKeys();
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedStatement_getMaxFieldSize_throws() throws SQLException {
    closedStatement.getMaxFieldSize();
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedStatement_getMaxRows_throws() throws SQLException {
    closedStatement.getMaxRows();
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedStatement_getMoreResults1_throws() throws SQLException {
    closedStatement.getMoreResults();
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedStatement_getMoreResults2_throws() throws SQLException {
    closedStatement.getMoreResults( 1 );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedStatement_getQueryTimeout_throws() throws SQLException {
    closedStatement.getQueryTimeout();
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedStatement_getResultSet_throws() throws SQLException {
    closedStatement.getResultSet();
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedStatement_getResultSetConcurrency_throws() throws SQLException {
    closedStatement.getResultSetConcurrency();
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedStatement_getResultSetHoldability_throws() throws SQLException {
    closedStatement.getResultSetHoldability();
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedStatement_getResultSetType_throws() throws SQLException {
    closedStatement.getResultSetType();
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedStatement_getUpdateCount_throws() throws SQLException {
    closedStatement.getUpdateCount();
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedStatement_getWarnings_throws() throws SQLException {
    closedStatement.getWarnings();
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedStatement_isCloseOnCompletion_throws() throws SQLException {
    closedStatement.isCloseOnCompletion();
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedStatement_isPoolable_throws() throws SQLException {
    closedStatement.isPoolable();
  }

   @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedStatement_setCursorName_throws() throws SQLException {
    closedStatement.setCursorName( "name" );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedStatement_setEscapeProcessing_throws() throws SQLException {
    closedStatement.setEscapeProcessing( true );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedStatement_setFetchDirection_throws() throws SQLException {
    closedStatement.setFetchDirection( 123 );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedStatement_setFetchSize_throws() throws SQLException {
    closedStatement.setFetchSize( 1 );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedStatement_setMaxFieldSize_throws() throws SQLException {
    closedStatement.setMaxFieldSize( 1 );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedStatement_setMaxRows_throws() throws SQLException {
    closedStatement.setMaxRows(1 );
  }

   @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedStatement_setQueryTimeout_throws() throws SQLException {
    closedStatement.setQueryTimeout( 60 );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedStatement_setPoolable_throws() throws SQLException {
    closedStatement.setPoolable( false );
  }


  ///////////////////////////////////////////////////////////////
  // ResultSet methods:

  ////////////////////////////////////////
  // - methods that do _not_ throw exception for closed ResultSet:

  public void testClosedResultSet_close_doesNotThrow() throws SQLException {
    closedResultSet.close();
  }

  public void testClosedResultSet_isClosed_returnsTrue() throws SQLException {
    assertThat( closedResultSet.isClosed(), equalTo( true ) );
  }

  ///////////////////////////////////////////////////////////////
  // PreparedStatement methods:

  ////////////////////////////////////////
  // - methods that do _not_ throw exception for closed PreparedStatement:

  ////////////////////////////////////////
  // - methods that do throw exception for closed PreparedStatement:

  // TODO


  ///////////////////////////////////////////////////////////////
  // CallableStatement methods:

  ////////////////////////////////////////
  // - methods that do _not_ throw exception for closed CallableStatement:

  ////////////////////////////////////////
  // - methods that do throw exception for closed CallableStatement:

  // TODO


  ////////////////////////////////////////
  // - methods that do throw exception for closed ResultSet:

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_absolute_throws() throws SQLException {
    closedResultSet.absolute( 1 );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_afterLast_throws() throws SQLException {
    closedResultSet.afterLast();
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_beforeFirst_throws() throws SQLException {
    closedResultSet.beforeFirst();
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_cancelRowUpdates_throws() throws SQLException {
    closedResultSet.cancelRowUpdates();
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_clearWarnings_throws() throws SQLException {
    closedResultSet.clearWarnings();
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_deleteRow_throws() throws SQLException {
    closedResultSet.deleteRow();
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_findColumn_throws() throws SQLException {
    closedResultSet.findColumn( "columnLabel" );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_first_throws() throws SQLException {
    closedResultSet.first();
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_getArray1_throws() throws SQLException {
    closedResultSet.getArray( 1 );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_getArray2_throws() throws SQLException {
    closedResultSet.getArray( "columnLabel" );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_getAsciiStream1_throws() throws SQLException {
    closedResultSet.getAsciiStream( 123 );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_getAsciiStream2_throws() throws SQLException {
    closedResultSet.getAsciiStream( "columnLabel" );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_getBigDecimal1_throws() throws SQLException {
    closedResultSet.getBigDecimal( 123 );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  @SuppressWarnings("deprecation")
  public void testClosedResultSet_getBigDecimal2_throws() throws SQLException {
    closedResultSet.getBigDecimal( 123, 2 );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_getBigDecimal3_throws() throws SQLException {
    closedResultSet.getBigDecimal( "columnLabel" );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  @SuppressWarnings("deprecation")
  public void testClosedResultSet_getBigDecimal_throws() throws SQLException {
    closedResultSet.getBigDecimal( "columnLabel", 2 );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_getBinaryStream1_throws() throws SQLException {
    closedResultSet.getBinaryStream( 123 );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_getBinaryStream2_throws() throws SQLException {
    closedResultSet.getBinaryStream( "columnLabel" );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_getBlob1_throws() throws SQLException {
    closedResultSet.getBlob( 123 );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_getBlob2_throws() throws SQLException {
    closedResultSet.getBlob( "columnLabel" );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_getBoolean1_throws() throws SQLException {
    closedResultSet.getBoolean( 123 );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_getBoolean2_throws() throws SQLException {
    closedResultSet.getBoolean( "columnLabel" );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_getByte1_throws() throws SQLException {
    closedResultSet.getByte( 123 );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_getByte2_throws() throws SQLException {
    closedResultSet.getByte( "columnLabel" );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_getBytes1_throws() throws SQLException {
    closedResultSet.getBytes( 123 );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_getBytes2_throws() throws SQLException {
    closedResultSet.getBytes( "columnLabel" );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_getCharacterStream1_throws() throws SQLException {
    closedResultSet.getCharacterStream( 123 );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_getCharacterStream2_throws() throws SQLException {
    closedResultSet.getCharacterStream( "columnLabel" );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_getClob1_throws() throws SQLException {
    closedResultSet.getClob( 123 );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_getClob2_throws() throws SQLException {
    closedResultSet.getClob( "columnLabel" );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_getConcurrency_throws() throws SQLException {
    closedResultSet.getConcurrency();
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_getCursorName_throws() throws SQLException {
    closedResultSet.getCursorName();
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_getDate1_throws() throws SQLException {
    closedResultSet.getDate( 123 );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_getDate2_throws() throws SQLException {
    closedResultSet.getDate( 123, null );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_getDate3_throws() throws SQLException {
    closedResultSet.getDate( "columnLabel" );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_getDate4_throws() throws SQLException {
    closedResultSet.getDate( "columnLabel", null );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_getDouble1_throws() throws SQLException {
    closedResultSet.getDouble( 123 );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_getDouble2_throws() throws SQLException {
    closedResultSet.getDouble( "columnLabel" );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_getFetchDirection_throws() throws SQLException {
    closedResultSet.getFetchDirection();
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_getFetchSize_throws() throws SQLException {
    closedResultSet.getFetchSize();
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_getFloat1_throws() throws SQLException {
    closedResultSet.getFloat( 123 );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_getFloat2_throws() throws SQLException {
    closedResultSet.getFloat( "columnLabel" );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_getHoldability_throws() throws SQLException {
    closedResultSet.getHoldability();
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_getInt1_throws() throws SQLException {
    closedResultSet.getInt( 123 );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_getInt2_throws() throws SQLException {
    closedResultSet.getInt( "columnLabel" );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_getLong1_throws() throws SQLException {
    closedResultSet.getLong( 123 );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_getLong2_throws() throws SQLException {
    closedResultSet.getLong( "columnLabel" );
  }

  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_getMetaData_throws() throws SQLException {
    closedResultSet.getMetaData();
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_getNCharacterStream1_throws() throws SQLException {
    closedResultSet.getNCharacterStream( 123 );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_getNCharacterStream2_throws() throws SQLException {
    closedResultSet.getNCharacterStream( "columnLabel" );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_getNClob1_throws() throws SQLException {
    closedResultSet.getNClob( 123 );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_getNClob2_throws() throws SQLException {
    closedResultSet.getNClob( "columnLabel" );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_getNString1_throws() throws SQLException {
    closedResultSet.getNString( 123 );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_getNString2_throws() throws SQLException {
    closedResultSet.getNString( "columnLabel" );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_getObject1_throws() throws SQLException {
    closedResultSet.getObject( 123 );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_getObject2_throws() throws SQLException {
    closedResultSet.getObject( 123, String.class );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_getObject3_throws() throws SQLException {
    closedResultSet.getObject( 123, (Map<String,Class<?>>) null );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_getObject4_throws() throws SQLException {
    closedResultSet.getObject( "columnLabel" );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_getObject5_throws() throws SQLException {
    closedResultSet.getObject( "columnLabel", String.class );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_getObject6_throws() throws SQLException {
    closedResultSet.getObject( "columnLabel", (Map<String,Class<?>>) null );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_getRef1_throws() throws SQLException {
    closedResultSet.getRef( 123 );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_getRef2_throws() throws SQLException {
    closedResultSet.getRef( "columnLabel" );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_getRow_throws() throws SQLException {
    closedResultSet.getRow();
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_getRowId1_throws() throws SQLException {
    closedResultSet.getRowId( 123 );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_getRowId2_throws() throws SQLException {
    closedResultSet.getRowId( "columnLabel" );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_getShort1_throws() throws SQLException {
    closedResultSet.getShort( 123 );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_getShort2_throws() throws SQLException {
    closedResultSet.getShort( "columnLabel" );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_getSQLXML1_throws() throws SQLException {
    closedResultSet.getSQLXML( 123 );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_getSQLXML2_throws() throws SQLException {
    closedResultSet.getSQLXML( "columnLabel" );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_getStatement_throws() throws SQLException {
    closedResultSet.getStatement();
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_getString1_throws() throws SQLException {
    closedResultSet.getString( 123 );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_getString2_throws() throws SQLException {
    closedResultSet.getString( "columnLabel" );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_getTime1_throws() throws SQLException {
    closedResultSet.getTime( 123 );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_getTime2_throws() throws SQLException {
    closedResultSet.getTime( 123, null );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_getTime3_throws() throws SQLException {
    closedResultSet.getTime( "columnLabel" );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_getTime4_throws() throws SQLException {
    closedResultSet.getTime( "columnLabel", null );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_getTimestamp1_throws() throws SQLException {
    closedResultSet.getTimestamp( 123 );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_getTimestamp2_throws() throws SQLException {
    closedResultSet.getTimestamp( 123, null );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_getTimestamp3_throws() throws SQLException {
    closedResultSet.getTimestamp( "columnLabel" );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_getTimestamp4_throws() throws SQLException {
    closedResultSet.getTimestamp( "columnLabel", null );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_getType_throws() throws SQLException {
    closedResultSet.getType();
  }

  @Test( expected = AlreadyClosedSqlException.class )
  @SuppressWarnings("deprecation")
  @Ignore( "until DRILL-2489 addressed" )
  public void testClosedResultSet_getUnicodeStream1_throws() throws SQLException {
    closedResultSet.getUnicodeStream( 123 );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  @SuppressWarnings("deprecation")
  public void testClosedResultSet_getUnicodeStream2_throws() throws SQLException {
    closedResultSet.getUnicodeStream( "columnLabel" );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_getURL1_throws() throws SQLException {
    closedResultSet.getURL( 123 );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_getURL2_throws() throws SQLException {
    closedResultSet.getURL( "columnLabel" );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_getWarnings_throws() throws SQLException {
    closedResultSet.getWarnings();
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_insertRow_throws() throws SQLException {
    closedResultSet.insertRow();
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_isAfterLast_throws() throws SQLException {
    closedResultSet.isAfterLast();
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_isBeforeFirst_throws() throws SQLException {
    closedResultSet.isBeforeFirst();
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_isFirst_throws() throws SQLException {
    closedResultSet.isFirst();
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_isLast_throws() throws SQLException {
    closedResultSet.isLast();
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_last_throws() throws SQLException {
    closedResultSet.last();
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_moveToCurrentRow_throws() throws SQLException {
    closedResultSet.moveToCurrentRow();
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_moveToInsertRow_throws() throws SQLException {
    closedResultSet.moveToInsertRow();
  }

  public void testClosedResultSet_next_throws() throws SQLException {
    closedResultSet.next();
  }

   @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_previous_throws() throws SQLException {
    closedResultSet.previous();
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_refreshRow_throws() throws SQLException {
    closedResultSet.refreshRow();
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_relative_throws() throws SQLException {
    closedResultSet.relative( 2 );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_rowDeleted_throws() throws SQLException {
    closedResultSet.rowDeleted();
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_rowInserted_throws() throws SQLException {
    closedResultSet.rowInserted();
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_rowUpdated_throws() throws SQLException {
    closedResultSet.rowUpdated();
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_setFetchDirection_throws() throws SQLException {
    closedResultSet.setFetchDirection( -123 );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_setFetchSize_throws() throws SQLException {
    closedResultSet.setFetchSize( 1 );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateArray1_throws() throws SQLException {
    closedResultSet.updateArray( 123, (Array) null );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateArray_throws() throws SQLException {
    closedResultSet.updateArray( "columnLabel2", (Array) null );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateAsciiStream1_throws() throws SQLException {
    closedResultSet.updateAsciiStream( 123, (InputStream) null );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateAsciiStream2_throws() throws SQLException {
    closedResultSet.updateAsciiStream( 123, (InputStream) null, 12 );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateAsciiStream3_throws() throws SQLException {
    closedResultSet.updateAsciiStream( 123, (InputStream) null, 12 );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateAsciiStream4_throws() throws SQLException {
    closedResultSet.updateAsciiStream( "columnLabel", (InputStream) null );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateAsciiStream5_throws() throws SQLException {
    closedResultSet.updateAsciiStream( "columnLabel", (InputStream) null, 12 );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateAsciiStream6_throws() throws SQLException {
    closedResultSet.updateAsciiStream( "columnLabel", (InputStream) null, 12 );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateBigDecimal1_throws() throws SQLException {
    closedResultSet.updateBigDecimal( 123, (BigDecimal) null );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateBigDecimal2_throws() throws SQLException {
    closedResultSet.updateBigDecimal( "columnLabel", (BigDecimal) null );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateBinaryStream3_throws() throws SQLException {
    closedResultSet.updateBinaryStream( 123, (InputStream) null );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateBinaryStream4_throws() throws SQLException {
    closedResultSet.updateBinaryStream( 123, (InputStream) null, 12 );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateBinaryStream5_throws() throws SQLException {
    closedResultSet.updateBinaryStream( 123, (InputStream) null, 12 );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateBinaryStream6_throws() throws SQLException {
    closedResultSet.updateBinaryStream( "columnLabel", (InputStream) null );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateBinaryStream7_throws() throws SQLException {
    closedResultSet.updateBinaryStream( "columnLabel", (InputStream) null, 12 );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateBinaryStream8_throws() throws SQLException {
    closedResultSet.updateBinaryStream( "columnLabel", (InputStream) null, 12 );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateBlob1_throws() throws SQLException {
    closedResultSet.updateBlob( 123, (Blob) null );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateBlob2_throws() throws SQLException {
    closedResultSet.updateBlob( 123, (InputStream) null );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateBlob3_throws() throws SQLException {
    closedResultSet.updateBlob( 123, (InputStream) null, 12 );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateBlob4_throws() throws SQLException {
    closedResultSet.updateBlob( "columnLabel", (Blob) null );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateBlob5_throws() throws SQLException {
    closedResultSet.updateBlob( "columnLabel", (InputStream) null );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateBlob6_throws() throws SQLException {
    closedResultSet.updateBlob( "columnLabel", (InputStream) null, 12 );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateBoolean1_throws() throws SQLException {
    closedResultSet.updateBoolean( 123, true );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateBoolean2_throws() throws SQLException {
    closedResultSet.updateBoolean( "columnLabel", true );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateByte1_throws() throws SQLException {
    closedResultSet.updateByte( 123, (byte) 0 );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateByte2_throws() throws SQLException {
    closedResultSet.updateByte( "columnLabel", (byte) 0 );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateBytes1_throws() throws SQLException {
    closedResultSet.updateBytes( 123, (byte[]) null );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateBytes2_throws() throws SQLException {
    closedResultSet.updateBytes( "columnLabel", (byte[]) null );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateCharacterStream1_throws() throws SQLException {
    closedResultSet.updateCharacterStream( 123, (Reader) null );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateCharacterStream2_throws() throws SQLException {
    closedResultSet.updateCharacterStream( 123, (Reader)  null, 12 );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateCharacterStream3_throws() throws SQLException {
    closedResultSet.updateCharacterStream( 123, (Reader)  null, 12 );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateCharacterStream4_throws() throws SQLException {
    closedResultSet.updateCharacterStream( "columnLabel", (Reader) null );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateCharacterStream5_throws() throws SQLException {
    closedResultSet.updateCharacterStream( "columnLabel", (Reader) null, 12 );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateCharacterStream6_throws() throws SQLException {
    closedResultSet.updateCharacterStream( "columnLabel", (Reader) null, 12 );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateClob1_throws() throws SQLException {
    closedResultSet.updateClob( 123, (Clob) null );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateClob2_throws() throws SQLException {
    closedResultSet.updateClob( 123, (Reader) null );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateClob3_throws() throws SQLException {
    closedResultSet.updateClob( 123, (Reader) null, 12 );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateClob4_throws() throws SQLException {
    closedResultSet.updateClob( "columnLabel", (Clob) null );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateClob5_throws() throws SQLException {
    closedResultSet.updateClob( "columnLabel", (Reader) null );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateClob6_throws() throws SQLException {
    closedResultSet.updateClob( "columnLabel", (Reader) null, 12 );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateDate1() throws SQLException {
    closedResultSet.updateDate( 123, (Date) null );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateDate2_throws() throws SQLException {
    closedResultSet.updateDate( "columnLabel", (Date) null );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateDouble1_throws() throws SQLException {
    closedResultSet.updateDouble( 123, (double) 123 );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateDouble2_throws() throws SQLException {
    closedResultSet.updateDouble( "columnLabel", 456 );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateFloat1_throws() throws SQLException {
    closedResultSet.updateFloat( 123, 1f );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateFloat2_throws() throws SQLException {
    closedResultSet.updateFloat( "columnLabel", 345 );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateInt1_throws() throws SQLException {
    closedResultSet.updateInt( 123, 1 );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateInt2_throws() throws SQLException {
    closedResultSet.updateInt( "columnLabel", 234 );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateLong1_throws() throws SQLException {
    closedResultSet.updateLong( 123, 234 );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateLong2_throws() throws SQLException {
    closedResultSet.updateLong( "columnLabel", 234 );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateNCharacterStream1_throws() throws SQLException {
    closedResultSet.updateNCharacterStream( 123, (Reader) null );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateNCharacterStream2_throws() throws SQLException {
    closedResultSet.updateNCharacterStream( 123, (Reader) null, 12 );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateNCharacterStream3_throws() throws SQLException {
    closedResultSet.updateNCharacterStream( "columnLabel", (Reader) null );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateNCharacterStream4_throws() throws SQLException {
    closedResultSet.updateNCharacterStream( "columnLabel", (Reader) null, 12 );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateNClob1_throws() throws SQLException {
    closedResultSet.updateNClob( 123, (NClob) null );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateNClob2_throws() throws SQLException {
    closedResultSet.updateNClob( 123, (Reader) null );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateNClob3_throws() throws SQLException {
    closedResultSet.updateNClob( 123, (Reader) null, 12 );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateNClob4_throws() throws SQLException {
    closedResultSet.updateNClob( "columnLabel", (NClob) null );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateNClob5_throws() throws SQLException {
    closedResultSet.updateNClob( "columnLabel", (Reader) null );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateNClob6_throws() throws SQLException {
    closedResultSet.updateNClob( "columnLabel", (Reader) null, 12 );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateNString1_throws() throws SQLException {
    closedResultSet.updateNString( 123, (String) null );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateNString2_throws() throws SQLException {
    closedResultSet.updateNString( "columnLabel", (String) null );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateNull1_throws() throws SQLException {
    closedResultSet.updateNull( 123 );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateNull2_throws() throws SQLException {
    closedResultSet.updateNull( "columnLabel" );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateObject1_throws() throws SQLException {
    closedResultSet.updateObject( 123, (Object) null );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateObject2_throws() throws SQLException {
    closedResultSet.updateObject( 123, (Object) null, 7 );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateObject3_throws() throws SQLException {
    closedResultSet.updateObject( "columnLabel", (Object) null );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateObject4_throws() throws SQLException {
    closedResultSet.updateObject( "columnLabel", (Object) null, 17 );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateRef1_throws() throws SQLException {
    closedResultSet.updateRef( 123, (Ref) null );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateRef2_throws() throws SQLException {
    closedResultSet.updateRef( "columnLabel", (Ref) null );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateRow_throws() throws SQLException {
    closedResultSet.updateRow();
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateRowId1_throws() throws SQLException {
    closedResultSet.updateRowId( 123, (RowId) null );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateRowId2_throws() throws SQLException {
    closedResultSet.updateRowId( "columnLabel", (RowId) null );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateShort1_throws() throws SQLException {
    closedResultSet.updateShort( 123, (short) 127 );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateShort2_throws() throws SQLException {
    closedResultSet.updateShort( "columnLabel", (short) 127 );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateSQLXML1_throws() throws SQLException {
    closedResultSet.updateSQLXML( 123, (SQLXML) null );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateSQLXML2_throws() throws SQLException {
    closedResultSet.updateSQLXML( "columnLabel", (SQLXML) null );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateString1_throws() throws SQLException {
    closedResultSet.updateString( 123, (String) null );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateString2_throws() throws SQLException {
    closedResultSet.updateString( "columnLabel", (String) null );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateTime1_throws() throws SQLException {
    closedResultSet.updateTime( 123, (Time) null );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateTime2_throws() throws SQLException {
    closedResultSet.updateTime( "columnLabel", (Time) null );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateTimestamp1_throws() throws SQLException {
    closedResultSet.updateTimestamp( 123, (Timestamp) null );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_updateTimestamp2_throws() throws SQLException {
    closedResultSet.updateTimestamp( "columnLabel", (Timestamp) null );
  }

  @Ignore( "until DRILL-2489 addressed" )
  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedResultSet_wasNull_throws() throws SQLException {
    closedResultSet.wasNull();
  }


  ///////////////////////////////////////////////////////////////
  // ResultSetMetadata methods:

  ////////////////////////////////////////
  // - methods that do _not_ throw exception for closed ResultSetMetadata:

  // TODO

  ////////////////////////////////////////
  // - methods that do throw exception for closed ResultSetMetadata:

  // None?


  ///////////////////////////////////////////////////////////////
  // DatabaseMetaData methods:

  ////////////////////////////////////////
  // - methods that do _not_ throw exception for closed DatabaseMetaData:

  ////////////////////////////////////////
  // - methods that do throw exception for closed DatabaseMetaData:

  // TODO

  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedDatabaseMetaData_getCatalog_throws() throws SQLException {
    closedDatabaseMetaData.getCatalogs();
  }

  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedDatabaseMetaData_getSchemas_throws() throws SQLException {
    closedDatabaseMetaData.getSchemas( null, null);
  }

  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedDatabaseMetaData_getTables_throws() throws SQLException {
    closedDatabaseMetaData.getTables( null, null, null, null );
  }

  @Test( expected = AlreadyClosedSqlException.class )
  public void testClosedDatabaseMetaData_getColumns_throws() throws SQLException {
    closedDatabaseMetaData.getColumns( null, null, null, null );
  }

}
