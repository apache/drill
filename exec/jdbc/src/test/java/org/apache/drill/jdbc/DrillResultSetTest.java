/*
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

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import net.hydromatic.linq4j.Ord;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.common.logical.data.LogicalOperator;
import org.apache.drill.common.util.Hook;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.test.DrillTest;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.core.StringContains.containsString;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.ImmutableSet.Builder;

public class DrillResultSetTest extends DrillTest {

  // TODO: Move Jetty status server disabling to DrillTest.
  private static final String STATUS_SERVER_PROPERTY_NAME =
      ExecConstants.HTTP_ENABLE;

  private static final String origStatusServerPropValue =
      System.getProperty( STATUS_SERVER_PROPERTY_NAME, "true" );

  // Disable Jetty status server so unit tests run (outside Maven setup).
  // (TODO:  Move this to base test class and/or have Jetty try other ports.)
  @BeforeClass
  public static void setUpClass() {
    System.setProperty( STATUS_SERVER_PROPERTY_NAME, "false" );
  }

  @AfterClass
  public static void tearDownClass() {
    System.setProperty( STATUS_SERVER_PROPERTY_NAME, origStatusServerPropValue );
  }


  @Test
  public void test_next_blocksFurtherAccessAfterEnd()
      throws SQLException
  {
    Connection connection = new Driver().connect( "jdbc:drill:zk=local", null );
    Statement statement = connection.createStatement();
    ResultSet resultSet =
        statement.executeQuery( "SELECT 1 AS x \n" +
                                "FROM cp.`donuts.json` \n" +
                                "LIMIT 2" );

    // Advance to first row; confirm can access data.
    assertThat( resultSet.next(), is( true ) );
    assertThat( resultSet.getInt( 1 ), is ( 1 ) );

    // Advance from first to second (last) row, confirming data access.
    assertThat( resultSet.next(), is( true ) );
    assertThat( resultSet.getInt( 1 ), is ( 1 ) );

    // Now advance past last row.
    assertThat( resultSet.next(), is( false ) );

    // Main check:  That row data access methods now throw SQLException.
    try {
      resultSet.getInt( 1 );
      fail( "Did get expected SQLException." );
    }
    catch ( SQLException e ) {
      // Expect something like current InvalidCursorStateSqlException saying
      // "Result set cursor is already positioned past all rows."
      assertThat( e, instanceOf( InvalidCursorStateSqlException.class ) );
      assertThat( e.toString(), containsString( "past" ) );
    }
    // (Any other exception is unexpected result.)

    assertThat( resultSet.next(), is( false ) );

    // TODO:  Ideally, test all other accessor methods.
  }

  @Test
  public void test_next_blocksFurtherAccessWhenNoRows()
    throws Exception
  {
    Connection connection = new Driver().connect( "jdbc:drill:zk=local", null );
    Statement statement = connection.createStatement();
    ResultSet resultSet =
        statement.executeQuery( "SELECT 'Hi' AS x \n" +
                                "FROM cp.`donuts.json` \n" +
                                "WHERE false" );

    // Do initial next(). (Advance from before results to next possible
    // position (after the set of zero rows).
    assertThat( resultSet.next(), is( false ) );

    // Main check:  That row data access methods throw SQLException.
    try {
      resultSet.getString( 1 );
      fail( "Did get expected SQLException." );
    }
    catch ( SQLException e ) {
      // Expect something like current InvalidRowSQLException saying
      // "Result set cursor is still before all rows.  Call next() first."
      assertThat( e, instanceOf( InvalidCursorStateSqlException.class ) );
      assertThat( e.toString(), containsString( "before" ) );
    }
    // (Any non-SQLException exception is unexpected result.)

    assertThat( resultSet.next(), is( false ) );

    // TODO:  Ideally, test all other accessor methods.
  }


  // TODO:  Ideally, test other methods.

}
