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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.hamcrest.CoreMatchers.*;

import org.hamcrest.Matcher;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.apache.drill.jdbc.Driver;

import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;


/**
 * Test for Drill's implementation of PreparedStatement's methods.
 */
public class PreparedStatementTest extends JdbcTestBase {

  /** Fuzzy matcher for parameters-not-supported message assertions.  (Based on
   *  current "Prepared-statement dynamic parameters are not supported.") */
  private static final Matcher<String> PARAMETERS_NOT_SUPPORTED_MSG_MATCHER =
      allOf( containsString( "arameter" ),   // allows "Parameter"
             containsString( "not" ),        // (could have false matches)
             containsString( "support" ) );  // allows "supported"

  private static Connection connection;


  @BeforeClass
  public static void setUpConnection() throws SQLException {
    Driver.load();
    connection = DriverManager.getConnection( "jdbc:drill:zk=local" );
  }

  @AfterClass
  public static void tearDownConnection() throws SQLException {
    connection.close();
  }


  //////////
  // Basic querying-works test:

  /** Tests that basic executeQuery() (with query statement) works. */
  @Test
  public void testExecuteQueryBasicCaseWorks() throws SQLException {
    PreparedStatement stmt = connection.prepareStatement( "VALUES 11" );
    ResultSet rs = stmt.executeQuery();
    assertThat( "Unexpected column count",
                rs.getMetaData().getColumnCount(), equalTo( 1 ) );
    assertTrue( "No expected first row", rs.next() );
    assertThat( rs.getInt( 1 ), equalTo( 11 ) );
    assertFalse( "Unexpected second row", rs.next() );
  }


  //////////
  // Parameters-not-implemented tests:

  /** Tests that basic case of trying to set parameter says not supported. */
  @Test( expected = SQLFeatureNotSupportedException.class )
  public void testParamSettingSaysUnsupported() throws SQLException {
    PreparedStatement prepStmt = connection.prepareStatement( "VALUES ?, ?" );
    try {
      prepStmt.setInt( 0, 123456789 );
    }
    catch ( final SQLFeatureNotSupportedException e ) {
      assertThat(
          "Check whether params.-unsupported wording changed or checks changed.",
          e.toString(), PARAMETERS_NOT_SUPPORTED_MSG_MATCHER );
      throw e;
    }
  }

  /** Tests that "not supported" has priority over "bad index" check. */
  @Test( expected = SQLFeatureNotSupportedException.class )
  public void testParamSettingWithImpossibleIndexSaysUnsupported() throws SQLException {
    PreparedStatement prepStmt = connection.prepareStatement( "VALUES ?, ?" );
    try {
      prepStmt.setString( -1, "some value" );
    }
    catch ( final SQLFeatureNotSupportedException e ) {
      assertThat(
          "Check whether params.-unsupported wording changed or checks changed.",
          e.toString(), PARAMETERS_NOT_SUPPORTED_MSG_MATCHER );
      throw e;
    }
  }

  /** Tests that "not supported" has priority over "bad index" check. */
  @Test( expected = SQLFeatureNotSupportedException.class )
  public void testParamSettingWithInconsistentIndexSaysUnsupported() throws SQLException {
    PreparedStatement prepStmt = connection.prepareStatement( "VALUES ?, ?" );
    try {
      prepStmt.setBytes( 4, null );
    }
    catch ( final SQLFeatureNotSupportedException e ) {
      assertThat(
          "Check whether params.-unsupported wording changed or checks changed.",
          e.toString(), PARAMETERS_NOT_SUPPORTED_MSG_MATCHER );
      throw e;
    }
  }

  /** Tests that "not supported" has priority over possible "no parameters"
   *  check. */
  @Test( expected = SQLFeatureNotSupportedException.class )
  public void testParamSettingWhenNoParametersIndexSaysUnsupported() throws SQLException {
    PreparedStatement prepStmt = connection.prepareStatement( "VALUES 1" );
    try {
      prepStmt.setBytes( 4, null );
    }
    catch ( final SQLFeatureNotSupportedException e ) {
      assertThat(
          "Check whether params.-unsupported wording changed or checks changed.",
          e.toString(), PARAMETERS_NOT_SUPPORTED_MSG_MATCHER );
      throw e;
    }
  }

  /** Tests that "not supported" has priority over possible "type not supported"
   *  check. */
  @Test( expected = SQLFeatureNotSupportedException.class )
  public void testParamSettingWhenUnsupportedTypeSaysUnsupported() throws SQLException {
    PreparedStatement prepStmt = connection.prepareStatement( "VALUES 1" );
    try {
      prepStmt.setClob( 2, (Clob) null );
    }
    catch ( final SQLFeatureNotSupportedException e ) {
      assertThat(
          "Check whether params.-unsupported wording changed or checks changed.",
          e.toString(), PARAMETERS_NOT_SUPPORTED_MSG_MATCHER );
      throw e;
    }
  }

}
