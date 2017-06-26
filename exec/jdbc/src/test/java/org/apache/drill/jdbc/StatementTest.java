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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test for Drill's implementation of Statement's methods (most).
 */
public class StatementTest extends JdbcTestBase {

  private static final String SYS_VERSION_SQL = "select * from sys.version";
  private static Connection connection;

  @BeforeClass
  public static void setUpStatement() throws SQLException {
    // (Note: Can't use JdbcTest's connect(...) because JdbcTest closes
    // Connection--and other JDBC objects--on test method failure, but this test
    // class uses some objects across methods.)
    connection = new Driver().connect( "jdbc:drill:zk=local", null );
  }

  @AfterClass
  public static void tearDownStatement() throws SQLException {
    connection.close();
  }

  /**
   * Test for reading of default query timeout
   */
  @Test
  public void testDefaultGetQueryTimeout() throws SQLException {
    Statement stmt = connection.createStatement();
    int timeoutValue = stmt.getQueryTimeout();
    assert( 0 == timeoutValue );
  }

  /**
   * Test Invalid parameter by giving negative timeout
   */
  @Test ( expected = InvalidParameterSqlException.class )
  public void testInvalidSetQueryTimeout() throws SQLException {
    Statement stmt = connection.createStatement();
    //Setting negative value
    int valueToSet = -10;
    if (0L == valueToSet) {
      valueToSet--;
    }
    stmt.setQueryTimeout(valueToSet);
  }

  /**
   * Test setting a valid timeout
   */
  @Test
  public void testValidSetQueryTimeout() throws SQLException {
    Statement stmt = connection.createStatement();
    assert(stmt != null);
    //Setting positive value
    int valueToSet = new Random(System.currentTimeMillis()).nextInt(60);
    if (0L == valueToSet) {
      valueToSet++;
    }
    System.out.println("Setting timeout "+ valueToSet);
    stmt.setQueryTimeout(valueToSet);
    assert( valueToSet == stmt.getQueryTimeout() );
  }

  /**
   * Test setting timeout for a query that actually times out
   */
  @Test ( expected = SqlTimeoutException.class )
  public void testTriggeredQueryTimeout() throws SQLException {
    Statement stmt = connection.createStatement();
    //Setting to a very low value (3sec)
    int timeoutDuration = 3;
    try {
      stmt.setQueryTimeout(timeoutDuration);
      System.out.println("Set a timeout of "+ stmt.getQueryTimeout() +" seconds");
      ResultSet rs = stmt.executeQuery(SYS_VERSION_SQL);
      //Pause briefly (a second beyond the timeout) before attempting to fetch rows
      try {
        Thread.sleep( (timeoutDuration + 1) * 1000L );
      } catch (InterruptedException e) {/*DoNothing*/}
      //Fetch rows
      while (rs.next()) {
        rs.getBytes(1);
      }
    } catch (SQLException sqlEx) {
      if (sqlEx instanceof SqlTimeoutException) {
        throw (SqlTimeoutException) sqlEx;
      }
      if (stmt.isClosed()) {
        //The statement is assumed to be closed by timeout, since we didn't call it explicitly
        throw new SqlTimeoutException(timeoutDuration);
      }
    }
    stmt.close();
  }

  /**
   * Test setting timeout that never gets triggered
   */
  @Test
  public void testNonTriggeredQueryTimeout() throws SQLException {
    Statement stmt = connection.createStatement();
    stmt.setQueryTimeout(60);
    stmt.executeQuery(SYS_VERSION_SQL);
    ResultSet rs = stmt.getResultSet();
    int rowCount = 0;
    while (rs.next()) {
      rs.getBytes(1);
      rowCount++;
    }
    stmt.close();
    assert( 1 == rowCount );
  }
}
