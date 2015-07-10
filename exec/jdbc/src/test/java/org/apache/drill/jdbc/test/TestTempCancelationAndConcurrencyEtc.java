/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.drill.jdbc.test;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.drill.common.util.TestTools;
import org.apache.drill.jdbc.Driver;
import org.apache.drill.jdbc.JdbcTestBase;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

public class TestTempCancelationAndConcurrencyEtc extends JdbcTestBase {

  @Rule
  public final TestRule TIMEOUT = TestTools.getTimeoutRule( 900_000 );


  private static Connection connection;

  @BeforeClass
  public static void setUpBeforeClass() throws SQLException {
    connection = new Driver().connect( "jdbc:drill:zk=local", null );
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    connection.close();
  }

  private void executeRepeatedly_simple( final int count ) throws SQLException {
    for ( int i = 1; i <= count ; i++ ) {
      final Statement statement = connection.createStatement();
      ResultSet rs =
        statement.executeQuery( "SELECT * FROM INFORMATION_SCHEMA.CATALOGS" );
      rs.next();
      while ( rs.next() ) {
      }
      statement.close();
    }
  }

  @Test
  public void testExecuteRepeatly_simple_1() throws SQLException {
    executeRepeatedly_simple( 1 );
  }

  @Test
  public void testExecuteRepeatly_simple_10() throws SQLException {
    executeRepeatedly_simple( 10 );
  }

  @Ignore( "???" )
  @Test
  public void testExecuteRepeatly_simple_100() throws SQLException {
    executeRepeatedly_simple( 100 );
  }

  @Ignore( "???" )
  @Test
  public void testExecuteRepeatly_simple_1000() throws SQLException {
    executeRepeatedly_simple( 1000 );
  }


  /**
   * Cancels simple (fast) query at various synchronous points.
   */
  private void cancelQuery_sync_simple( int cancelPoint ) throws SQLException {
    final Statement statement = connection.createStatement();
    if ( --cancelPoint <= 0 ) {
      statement.cancel();
      return;
    }
    ResultSet rs =
        statement.executeQuery( "SELECT * FROM INFORMATION_SCHEMA.CATALOGS" );
    if ( --cancelPoint <= 0 ) {
      statement.cancel();
      return;
    }
    rs.next();
    if ( --cancelPoint <= 0 ) {
      statement.cancel();
      return;
    }
    while ( rs.next() ) {
    }
    if ( --cancelPoint <= 0 ) {
      statement.cancel();
      return;
    }
    statement.close();
    if ( --cancelPoint <= 0 ) {
      statement.cancel();
      return;
    }

  }


  @Test
  public void testSyncSimple_1() throws SQLException {
    cancelQuery_sync_simple( 1 );
  }

  @Test
  public void testSyncSimple_2() throws SQLException {
    cancelQuery_sync_simple( 2 );
  }

  @Test
  public void testSyncSimple_3() throws SQLException {
    cancelQuery_sync_simple( 3 );
  }

  @Test
  public void testSyncSimple_4() throws SQLException {
    cancelQuery_sync_simple( 4 );
  }

  @Test
  public void testSyncSimple_5() throws SQLException {
    cancelQuery_sync_simple( 5 );
  }

}
