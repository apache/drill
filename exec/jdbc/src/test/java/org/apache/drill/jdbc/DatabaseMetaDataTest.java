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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.*;
import org.apache.drill.jdbc.Driver;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;


import static java.sql.Connection.*;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Savepoint;
import java.sql.SQLException;

/**
 * Test for Drill's implementation of DatabaseMetaData's methods (other than
 * those tested separately, e.g., {@code getColumn(...)}, tested in
 * {@link DatabaseMetaDataGetColumnsTest})).
 */
public class DatabaseMetaDataTest {

  private static Connection connection;

  @BeforeClass
  public static void setUpConnection() throws SQLException {
    connection = new Driver().connect( "jdbc:drill:zk=local", null );
  }

  @AfterClass
  public static void tearDownConnection() throws SQLException {
    connection.close();
  }

  @Test
  public void testGetDefaultTransactionIsolationSaysNone() throws SQLException {
    final DatabaseMetaData md = connection.getMetaData();
    assertThat( md.getDefaultTransactionIsolation(), equalTo( TRANSACTION_NONE ) );
  }

  @Test
  public void testSupportsTransactionsSaysNo() throws SQLException {
    assertThat( connection.getMetaData().supportsTransactions(), equalTo( false ) );
  }

  @Test
  public void testSupportsTransactionIsolationLevelNoneSaysYes()
      throws SQLException {
    final DatabaseMetaData md = connection.getMetaData();
    assertTrue( md.supportsTransactionIsolationLevel( TRANSACTION_NONE ) );
  }

  @Test
  public void testSupportsTransactionIsolationLevelOthersSayNo()
      throws SQLException {
    final DatabaseMetaData md = connection.getMetaData();
    assertFalse( md.supportsTransactionIsolationLevel( TRANSACTION_READ_UNCOMMITTED ) );
    assertFalse( md.supportsTransactionIsolationLevel( TRANSACTION_READ_COMMITTED ) );
    assertFalse( md.supportsTransactionIsolationLevel( TRANSACTION_REPEATABLE_READ ) );
    assertFalse( md.supportsTransactionIsolationLevel( TRANSACTION_SERIALIZABLE ) );
  }

}
