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

import static java.sql.ResultSetMetaData.columnNoNulls;
import static java.sql.Types.BIGINT;
import static java.sql.Types.DATE;
import static java.sql.Types.DECIMAL;
import static java.sql.Types.INTEGER;
import static java.sql.Types.TIMESTAMP;
import static java.sql.Types.VARCHAR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.hamcrest.CoreMatchers.*;

import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.store.ischema.InfoSchemaConstants;
import org.hamcrest.Matcher;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

import java.math.BigDecimal;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.List;


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
    try(Statement stmt = connection.createStatement()) {
      stmt.execute(String.format("alter session set `%s` = true", PlannerSettings.ENABLE_DECIMAL_DATA_TYPE_KEY));
    }
  }

  @AfterClass
  public static void tearDownConnection() throws SQLException {
    if (connection != null) {
      try (Statement stmt = connection.createStatement()) {
        stmt.execute(String.format("alter session set `%s` = false", PlannerSettings.ENABLE_DECIMAL_DATA_TYPE_KEY));
      }
    }
    connection.close();
  }

  //////////
  // Basic querying-works test:

  /** Tests that basic executeQuery() (with query statement) works. */
  @Test
  public void testExecuteQueryBasicCaseWorks() throws SQLException {
    try (PreparedStatement stmt = connection.prepareStatement( "VALUES 11" )) {
      try(ResultSet rs = stmt.executeQuery()) {
        assertThat("Unexpected column count",
            rs.getMetaData().getColumnCount(), equalTo(1)
        );
        assertTrue("No expected first row", rs.next());
        assertThat(rs.getInt(1), equalTo(11));
        assertFalse("Unexpected second row", rs.next());
      }
    }
  }

  @Test
  public void testQueryMetadataInPreparedStatement() throws SQLException {
    try(PreparedStatement stmt = connection.prepareStatement(
        "SELECT " +
            "cast(1 as INTEGER ) as int_field, " +
            "cast(12384729 as BIGINT ) as bigint_field, " +
            "'varchar_value' as varchar_field, " +
            "timestamp '2008-2-23 10:00:20.123' as ts_field, " +
            "date '2008-2-23' as date_field, " +
            "cast('99999912399.4567' as decimal(18, 5)) as decimal_field" +
            " FROM sys.version")) {

      List<ExpectedColumnResult> exp = ImmutableList.of(
          new ExpectedColumnResult("int_field", INTEGER, columnNoNulls, 0, 0, true, Integer.class.getName()),
          new ExpectedColumnResult("bigint_field", BIGINT, columnNoNulls, 0, 0, true, Long.class.getName()),
          new ExpectedColumnResult("varchar_field", VARCHAR, columnNoNulls, 65536, 0, false, String.class.getName()),
          new ExpectedColumnResult("ts_field", TIMESTAMP, columnNoNulls, 0, 0, false, Timestamp.class.getName()),
          new ExpectedColumnResult("date_field", DATE, columnNoNulls, 0, 0, false, Date.class.getName()),
          new ExpectedColumnResult("decimal_field", DECIMAL, columnNoNulls, 18, 5, true, BigDecimal.class.getName())
      );

      ResultSetMetaData prepareMetadata = stmt.getMetaData();
      verifyMetadata(prepareMetadata, exp);

      try (ResultSet rs = stmt.executeQuery()) {
        ResultSetMetaData executeMetadata = rs.getMetaData();
        verifyMetadata(executeMetadata, exp);

        assertTrue("No expected first row", rs.next());
        assertThat(rs.getInt(1), equalTo(1));
        assertThat(rs.getLong(2), equalTo(12384729L));
        assertThat(rs.getString(3), equalTo("varchar_value"));
        assertThat(rs.getTimestamp(4), equalTo(Timestamp.valueOf("2008-2-23 10:00:20.123")));
        assertThat(rs.getDate(5), equalTo(Date.valueOf("2008-2-23")));
        assertThat(rs.getBigDecimal(6), equalTo(new BigDecimal("99999912399.45670")));
        assertFalse("Unexpected second row", rs.next());
      }
    }
  }

  private static void verifyMetadata(ResultSetMetaData act, List<ExpectedColumnResult> exp) throws SQLException {
    assertEquals(exp.size(), act.getColumnCount());
    for(ExpectedColumnResult e : exp) {
      boolean found = false;
      for(int i=1; i<=act.getColumnCount(); i++) {
        found = e.isEqualsTo(act, i);
        if (found) {
          break;
        }
      }
      assertTrue("Failed to find the expected column metadata: " + e, found);
    }
  }

  private static class ExpectedColumnResult {
    final String columnName;
    final int type;
    final int nullable;
    final int precision;
    final int scale;
    final boolean signed;
    final String className;

    ExpectedColumnResult(String columnName, int type, int nullable, int precision, int scale, boolean signed,
        String className) {
      this.columnName = columnName;
      this.type = type;
      this.nullable = nullable;
      this.precision = precision;
      this.scale = scale;
      this.signed = signed;
      this.className = className;
    }

    boolean isEqualsTo(ResultSetMetaData metadata, int colNum) throws SQLException {
      return
          metadata.getCatalogName(colNum).equals(InfoSchemaConstants.IS_CATALOG_NAME) &&
          metadata.getSchemaName(colNum).isEmpty() &&
          metadata.getTableName(colNum).isEmpty() &&
          metadata.getColumnName(colNum).equals(columnName) &&
          metadata.getColumnLabel(colNum).equals(columnName) &&
          metadata.getColumnType(colNum) == type &&
          metadata.isNullable(colNum) == nullable &&
          // There is an existing bug where query results doesn't contain the precision for VARCHAR field.
          //metadata.getPrecision(colNum) == precision &&
          metadata.getScale(colNum) == scale &&
          metadata.isSigned(colNum) == signed &&
          metadata.getColumnDisplaySize(colNum) == 10 &&
          metadata.getColumnClassName(colNum).equals(className) &&
          metadata.isSearchable(colNum) &&
          metadata.isAutoIncrement(colNum) == false &&
          metadata.isCaseSensitive(colNum) == false &&
          metadata.isReadOnly(colNum) &&
          metadata.isWritable(colNum) == false &&
          metadata.isDefinitelyWritable(colNum) == false &&
          metadata.isCurrency(colNum) == false;
    }

    @Override
    public String toString() {
      return "ExpectedColumnResult[" +
          "columnName='" + columnName + '\'' +
          ", type='" + type + '\'' +
          ", nullable=" + nullable +
          ", precision=" + precision +
          ", scale=" + scale +
          ", signed=" + signed +
          ", className='" + className + '\'' +
          ']';
    }
  }

  //////////
  // Parameters-not-implemented tests:

  /** Tests that basic case of trying to create a prepare statement with parameters. */
  @Test( expected = SQLException.class )
  public void testSqlQueryWithParamNotSupported() throws SQLException {

    try {
      connection.prepareStatement( "VALUES ?, ?" );
    }
    catch ( final SQLException e ) {
      assertThat(
          "Check whether params.-unsupported wording changed or checks changed.",
          e.toString(), containsString("Illegal use of dynamic parameter") );
      throw e;
    }
  }

  /** Tests that "not supported" has priority over possible "no parameters"
   *  check. */
  @Test( expected = SQLFeatureNotSupportedException.class )
  public void testParamSettingWhenNoParametersIndexSaysUnsupported() throws SQLException {
    try(PreparedStatement prepStmt = connection.prepareStatement( "VALUES 1" )) {
      try {
        prepStmt.setBytes(4, null);
      } catch (final SQLFeatureNotSupportedException e) {
        assertThat(
            "Check whether params.-unsupported wording changed or checks changed.",
            e.toString(), PARAMETERS_NOT_SUPPORTED_MSG_MATCHER
        );
        throw e;
      }
    }
  }

  /** Tests that "not supported" has priority over possible "type not supported"
   *  check. */
  @Test( expected = SQLFeatureNotSupportedException.class )
  public void testParamSettingWhenUnsupportedTypeSaysUnsupported() throws SQLException {
    try(PreparedStatement prepStmt = connection.prepareStatement( "VALUES 1" )) {
      try {
        prepStmt.setClob(2, (Clob) null);
      } catch (final SQLFeatureNotSupportedException e) {
        assertThat(
            "Check whether params.-unsupported wording changed or checks changed.",
            e.toString(), PARAMETERS_NOT_SUPPORTED_MSG_MATCHER
        );
        throw e;
      }
    }
  }

}
