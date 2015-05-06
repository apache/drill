/**
 * Licensed to the Apache Software Foundation ( ASF ) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 ( the
 * "License" ); you may not use this file except in compliance
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

import org.apache.drill.jdbc.Driver;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


/**
 * Test for Drill's implementation of Connection's methods (other than
 * main transaction-related methods in {@link ConnectionTransactionMethodsTest}).
 */
public class ConnectionTest extends JdbcTestBase {

  private static Connection connection;

  private static ExecutorService executor;

  @BeforeClass
  public static void setUpConnection() throws SQLException {
    // (Note: Can't use JdbcTest's connect(...) because JdbcTest closes
    // Connection--and other JDBC objects--on test method failure, but this test
    // class uses some objects across methods.)
    connection = new Driver().connect( "jdbc:drill:zk=local", null );
    executor = Executors.newSingleThreadExecutor();
  }

  @AfterClass
  public static void tearDownConnection() throws SQLException {
    connection.close();
    executor.shutdown();
  }


  ////////////////////////////////////////
  // Network timeout methods:

  //////////
  // getNetworkTimeout():

  /** Tests that getNetworkTimeout() indicates no timeout set. */
  @Test
  public void testGetNetworkTimeoutSaysNoTimeout() throws SQLException {
    assertThat( connection.getNetworkTimeout(), equalTo( 0 ) );
  }

  //////////
  // setNetworkTimeout(...):

  /** Tests that setNetworkTimeout(...) accepts (redundantly) setting to
   *  no-timeout mode. */
  @Test
  public void testSetNetworkTimeoutAcceptsNotimeoutRequest() throws SQLException {
    connection.setNetworkTimeout( executor, 0 );
  }

  /** Tests that setNetworkTimeout(...) rejects setting a timeout. */
  @Test( expected = SQLFeatureNotSupportedException.class )
  public void testSetNetworkTimeoutRejectsTimeoutRequest() throws SQLException {
    try {
      connection.setNetworkTimeout( executor, 1_000 );
    }
    catch ( SQLFeatureNotSupportedException e ) {
      // Check exception for some mention of network timeout:
      assertThat( e.getMessage(), anyOf( containsString( "Timeout" ),
                                         containsString( "timeout" ) ) );
      throw e;
    }
  }

  /** Tests that setNetworkTimeout(...) rejects setting a timeout (different
   *  value). */
  @Test( expected = SQLFeatureNotSupportedException.class )
  public void testSetNetworkTimeoutRejectsTimeoutRequest2() throws SQLException {
    connection.setNetworkTimeout( executor, Integer.MAX_VALUE );
  }

  @Test( expected = InvalidParameterSqlException.class )
  public void testSetNetworkTimeoutRejectsBadTimeoutValue() throws SQLException {
    try {
      connection.setNetworkTimeout( executor, -1 );
    }
    catch ( InvalidParameterSqlException e ) {
      // Check exception for some mention of parameter name or semantics:
      assertThat( e.getMessage(), anyOf( containsString( "milliseconds" ),
                                         containsString( "timeout" ),
                                         containsString( "Timeout" ) ) );
      throw e;
    }
  }

  @Test( expected = InvalidParameterSqlException.class )
  public void testSetNetworkTimeoutRejectsBadExecutorValue() throws SQLException {
    try {
      connection.setNetworkTimeout( null, 1 );
    }
    catch ( InvalidParameterSqlException e ) {
      // Check exception for some mention of parameter name or semantics:
      assertThat( e.getMessage(), anyOf( containsString( "executor" ),
                                         containsString( "Executor" ) ) );
      throw e;
    }
  }

}
