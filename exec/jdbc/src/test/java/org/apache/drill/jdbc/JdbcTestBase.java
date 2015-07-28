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

import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import com.google.common.base.Strings;

import org.apache.drill.exec.ExecTest;
import org.apache.drill.jdbc.test.JdbcAssert;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

// TODO:  Document this, especially what writers of unit tests need to know
//   (e.g., the reusing of connections, the automatic interception of test
//   failures and resetting of connections, etc.).
public class JdbcTestBase extends ExecTest {
  @SuppressWarnings("unused")
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JdbcTestBase.class);

  @Rule
  public final TestRule watcher = new TestWatcher() {
    @Override
    protected void failed(Throwable e, Description description) {
      reset();
    }
  };

  private static CachingConnectionFactory factory;

  @BeforeClass
  public static void setUpTestCase() {
    factory = new SingleConnectionCachingFactory(new ConnectionFactory() {
      @Override
      public Connection getConnection(ConnectionInfo info) throws Exception {
        Class.forName("org.apache.drill.jdbc.Driver");
        return DriverManager.getConnection(info.getUrl(), info.getParamsAsProperties());
      }
    });
    JdbcAssert.setFactory(factory);
  }

  /**
   * Creates a {@link java.sql.Connection connection} using default parameters.
   * @param url connection URL
   * @throws Exception if connection fails
   */
  protected static Connection connect(String url) throws Exception {
    return connect(url, JdbcAssert.getDefaultProperties());
  }


  /**
   * Creates a {@link java.sql.Connection connection} using the given parameters.
   * @param url connection URL
   * @param info connection info
   * @throws Exception if connection fails
   */
  protected static Connection connect(String url, Properties info) throws Exception {
    final Connection conn = factory.getConnection(new ConnectionInfo(url, info));
    changeSchemaIfSupplied(conn, info);
    return conn;
  }

  /**
   * Changes schema of the given connection if the field "schema" is present in {@link java.util.Properties info}.
   * Does nothing otherwise.
   */
  protected static void changeSchemaIfSupplied(Connection conn, Properties info) {
    final String schema = info.getProperty("schema", null);
    if (!Strings.isNullOrEmpty(schema)) {
      changeSchema(conn, schema);
    }
  }

  // TODO:  Purge nextUntilEnd(...) and calls when remaining fragment race
  // conditions are fixed (not just DRILL-2245 fixes).
  ///**
  // * Calls {@link ResultSet#next} on given {@code ResultSet} until it returns
  // * false.  (For TEMPORARY workaround for query cancelation race condition.)
  // */
  //private static void nextUntilEnd(final ResultSet resultSet) throws SQLException {
  //  while (resultSet.next()) {
  //  }
  //}

  protected static void changeSchema(Connection conn, String schema) {
    final String query = String.format("use %s", schema);
    try ( Statement s = conn.createStatement() ) {
      ResultSet r = s.executeQuery(query);
      // TODO:  Purge nextUntilEnd(...) and calls when remaining fragment
      // race conditions are fixed (not just DRILL-2245 fixes).
      // nextUntilEnd(r);
    } catch (SQLException e) {
      throw new RuntimeException("unable to change schema", e);
    }
  }

  /**
   * Resets the factory closing all of the active connections.
   */
  protected static void reset() {
    try {
      factory.closeConnections();
    } catch (SQLException e) {
      throw new RuntimeException("error while closing connection factory", e);
    }
  }

  @AfterClass
  public static void tearDownTestCase() throws Exception {
    factory.closeConnections();
  }

  /**
   * Test of whether tests that get connection from JdbcTest.connect(...)
   * work with resetting of connections.  If enabling this (failing) test method
   * causes other test methods to fail, something needs to be fixed.
   * (Note:  Not a guaranteed test--depends on order in which test methods are
   * run.)
   */
  @Ignore( "Usually disabled; enable temporarily to check tests" )
  @Test
  public void testJdbcTestConnectionResettingCompatibility() {
    fail( "Intentional failure--did other test methods still run?" );
  }

}
