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

import static org.junit.Assert.fail;
import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.*;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.exceptions.UserRemoteException;
import org.apache.drill.jdbc.Driver;
import org.apache.drill.jdbc.JdbcTestBase;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


public class TestExecutionExceptionsToClient extends JdbcTestBase {

  private static Connection connection;

  @BeforeClass
  public static void setUpConnection() throws Exception {
    connection = new Driver().connect( "jdbc:drill:zk=local", null );
  }

  @AfterClass
  public static void tearDownConnection() throws SQLException {
    connection.close();
  }

  @Test
  public void testExecuteQueryThrowsRight1() throws Exception {
    final Statement statement = connection.createStatement();
    try {
      statement.executeQuery( "SELECT one case of syntax error" );
    }
    catch ( SQLException e ) {
      assertThat( "Null getCause(); missing expected wrapped exception",
                  e.getCause(), notNullValue() );

      assertThat( "Unexpectedly wrapped another SQLException",
                  e.getCause(), not( instanceOf( SQLException.class ) ) );

      assertThat( "getCause() not UserRemoteException as expected",
                  e.getCause(), instanceOf( UserRemoteException.class ) );

      assertThat( "No expected current \"SYSTEM ERROR\"/eventual \"PARSE ERROR\"",
                  e.getMessage(), anyOf( startsWith( "SYSTEM ERROR" ),
                                         startsWith( "PARSE ERROR" ) ) );
    }
  }

  @Test
  public void testExecuteThrowsRight1() throws Exception {
    final Statement statement = connection.createStatement();
    try {
      statement.execute( "SELECT one case of syntax error" );
    }
    catch ( SQLException e ) {
      assertThat( "Null getCause(); missing expected wrapped exception",
                  e.getCause(), notNullValue() );

      assertThat( "Unexpectedly wrapped another SQLException",
                  e.getCause(), not( instanceOf( SQLException.class ) ) );

      assertThat( "getCause() not UserRemoteException as expected",
                  e.getCause(), instanceOf( UserRemoteException.class ) );

      assertThat( "No expected current \"SYSTEM ERROR\"/eventual \"PARSE ERROR\"",
                  e.getMessage(), anyOf( startsWith( "SYSTEM ERROR" ),
                                         startsWith( "PARSE ERROR" ) ) );
    }
  }

  @Test
  public void testExecuteUpdateThrowsRight1() throws Exception {
    final Statement statement = connection.createStatement();
    try {
      statement.executeUpdate( "SELECT one case of syntax error" );
    }
    catch ( SQLException e ) {
      assertThat( "Null getCause(); missing expected wrapped exception",
                  e.getCause(), notNullValue() );

      assertThat( "Unexpectedly wrapped another SQLException",
                  e.getCause(), not( instanceOf( SQLException.class ) ) );

      assertThat( "getCause() not UserRemoteException as expected",
                  e.getCause(), instanceOf( UserRemoteException.class ) );

      assertThat( "No expected current \"SYSTEM ERROR\"/eventual \"PARSE ERROR\"",
                  e.getMessage(), anyOf( startsWith( "SYSTEM ERROR" ),
                                         startsWith( "PARSE ERROR" ) ) );
    }
  }

  @Test
  public void testExecuteQueryThrowsRight2() throws Exception {
    final Statement statement = connection.createStatement();
    try {
      statement.executeQuery( "BAD QUERY 1" );
    }
    catch ( SQLException e ) {
      assertThat( "Null getCause(); missing expected wrapped exception",
                  e.getCause(), notNullValue() );

      assertThat( "Unexpectedly wrapped another SQLException",
                  e.getCause(), not( instanceOf( SQLException.class ) ) );

      assertThat( "getCause() not UserRemoteException as expected",
                  e.getCause(), instanceOf( UserRemoteException.class ) );

      assertThat( "No expected current \"SYSTEM ERROR\"/eventual \"PARSE ERROR\"",
                  e.getMessage(), anyOf( startsWith( "SYSTEM ERROR" ),
                                         startsWith( "PARSE ERROR" ) ) );
    }
  }

  @Test
  public void testExecuteThrowsRight2() throws Exception {
    final Statement statement = connection.createStatement();
    try {
      statement.execute( "worse query 2" );
    }
    catch ( SQLException e ) {
      assertThat( "Null getCause(); missing expected wrapped exception",
                  e.getCause(), notNullValue() );

      assertThat( "Unexpectedly wrapped another SQLException",
                  e.getCause(), not( instanceOf( SQLException.class ) ) );

      assertThat( "getCause() not UserRemoteException as expected",
                  e.getCause(), instanceOf( UserRemoteException.class ) );

      assertThat( "No expected current \"SYSTEM ERROR\"/eventual \"PARSE ERROR\"",
                  e.getMessage(), anyOf( startsWith( "SYSTEM ERROR" ),
                                         startsWith( "PARSE ERROR" ) ) );
    }
  }

  @Test
  public void testExecuteUpdateThrowsRight2() throws Exception {
    final Statement statement = connection.createStatement();
    try {
      statement.executeUpdate( "naughty, naughty query 3" );
    }
    catch ( SQLException e ) {
      assertThat( "Null getCause(); missing expected wrapped exception",
                  e.getCause(), notNullValue() );

      assertThat( "Unexpectedly wrapped another SQLException",
                  e.getCause(), not( instanceOf( SQLException.class ) ) );

      assertThat( "getCause() not UserRemoteException as expected",
                  e.getCause(), instanceOf( UserRemoteException.class ) );

      assertThat( "No expected current \"SYSTEM ERROR\"/eventual \"PARSE ERROR\"",
                  e.getMessage(), anyOf( startsWith( "SYSTEM ERROR" ),
                                         startsWith( "PARSE ERROR" ) ) );
    }
  }

}
