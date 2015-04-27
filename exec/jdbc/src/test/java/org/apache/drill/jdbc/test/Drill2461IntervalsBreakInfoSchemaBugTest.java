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

import org.apache.drill.common.util.TestTools;
import org.apache.drill.jdbc.Driver;
import org.apache.drill.jdbc.JdbcTestBase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;


public class Drill2461IntervalsBreakInfoSchemaBugTest extends JdbcTestBase {

  private static final String VIEW_NAME =
      Drill2461IntervalsBreakInfoSchemaBugTest.class.getSimpleName() + "_View";

  private static Connection connection;


  @BeforeClass
  public static void setUpConnection() throws Exception {
    connection = connect( "jdbc:drill:zk=local" );
  }

  @AfterClass
  public static void tearDownConnection() throws Exception {
    connection.close();
  }


  @Test
  public void testIntervalInViewDoesntCrashInfoSchema() throws Exception {
    final Statement stmt = connection.createStatement();
    ResultSet util;

    // Create a view using an INTERVAL type:
    util = stmt.executeQuery( "USE dfs_test.tmp" );
    assert util.next();
    assert util.getBoolean( 1 )
        : "Error setting schema to dfs_test.tmp: " + util.getString( 2 );
    util = stmt.executeQuery(
        "CREATE OR REPLACE VIEW " + VIEW_NAME + " AS "
      + "\n  SELECT CAST( NULL AS INTERVAL HOUR(4) TO MINUTE ) AS optINTERVAL_HM "
      + "\n  FROM INFORMATION_SCHEMA.CATALOGS "
      + "\n  LIMIT 1 " );
    assert util.next();
    assert util.getBoolean( 1 )
        : "Error creating temporary test-columns view " + VIEW_NAME + ": "
          + util.getString( 2 );

    // Test whether query INFORMATION_SCHEMA.COLUMNS works (doesn't crash):
    util = stmt.executeQuery( "SELECT * FROM INFORMATION_SCHEMA.COLUMNS" );
    assert util.next();

    // Clean up the test view:
    util = connection.createStatement().executeQuery( "DROP VIEW " + VIEW_NAME );
    assert util.next();
    assert util.getBoolean( 1 )
       : "Error dropping temporary test-columns view " + VIEW_NAME + ": "
         + util.getString( 2 );
  }

}
