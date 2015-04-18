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
import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.*;

import org.apache.drill.jdbc.Driver;
import org.apache.drill.jdbc.test.JdbcAssert;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

import static java.sql.ResultSetMetaData.columnNoNulls;
import static java.sql.ResultSetMetaData.columnNullable;
import static java.sql.ResultSetMetaData.columnNullableUnknown;

import java.sql.SQLException;
import java.sql.Types;

// TODO:  MOVE notes to implementation (have this not (just) in test).

// TODO:  Determine for each numeric type whether its precision is reported in
//   decimal or binary (and NUM_PREC_RADIX is 10 or 2, respectively).
//   The SQL specification for INFORMATION_SCHEMA.COLUMNS seems to specify the
//   radix for each numeric type:
//   - 2 or 10 for SMALLINT, INTEGER, and BIGINT;
//   - only 10 for NUMERIC and DECIMAL; and
//   - only 2  for REAL, DOUBLE PRECISION, and FLOAT.
//   However, it is not clear what the JDBC API intends:
//   - It has NUM_PREC_RADIX, specifying a radix or 10 or 2, but doesn't specify
//     exactly what is applies to.  Apparently, it applies to COLUMN_SIZE abd
//     ResultMetaData.getPrecision() (which are defined in terms of maximum
//     precision for numeric types).
//   - Is has DECIMAL_DIGITS, which is <em>not</em> the number of decimal digits
//     of precision, but which it defines as the number of fractional digits--
//     without actually specifying that it's in decimal.

// TODO:  Review nullability (NULLABLE and IS_NULLABLE columns):
// - It's not clear what JDBC's requirements are.
//   - It does seem obvious that metadata should not contradictorily say that a
//   - column cannot contain nulls when the column currently does contain nulls.
//   - It's not clear whether metadata must say that a column cannot contains
//     nulls if JDBC specifies that the column always has a non-null value.
// - It's not clear why Drill reports that columns that will never contain nulls
//   can contain nulls.
// - It's not clear why Drill sets INFORMATION_SCHEMA.COLUMNS.IS_NULLABLE to
//   'NO' for some columns that contain only null (e.g., for
//   "CREATE VIEW x AS SELECT CAST(NULL AS ...) ..."


/**
 * Test class for Drill's java.sql.DatabaseMetaData.getColumns() implementation.
 * <p>
 *   Based on JDBC 4.1 (Java 7).
 * </p>
 */
public class DatabaseMetaDataGetColumnsTest extends JdbcTest {

  private static final String VIEW_NAME =
      DatabaseMetaDataGetColumnsTest.class.getSimpleName() + "_View";


  /** The one shared JDBC connection to Drill. */
  private static Connection connection;

  /** Overall (connection-level) metadata. */
  private static DatabaseMetaData dbMetadata;


  /** getColumns result metadata.  For checking columns themselves (not cell
   *  values or row order). */
  private static ResultSetMetaData rowsMetadata;


  ////////////////////
  // Results from getColumns for test columns of various types.
  // Each ResultSet is positioned at first row for, and must not be modified by,
  // test methods.

  //////////
  // For columns in temporary test view (types accessible via casting):
  // TODO:  Determine whether any other data types are accessible via CAST
  // and add them.

  private static ResultSet mdrOptBOOLEAN;

  private static ResultSet mdrReqTINYINT;
  private static ResultSet mdrOptSMALLINT;
  private static ResultSet mdrReqINTEGER;
  private static ResultSet mdrOptBIGINT;

  private static ResultSet mdrOptFLOAT;
  private static ResultSet mdrReqDOUBLE;
  private static ResultSet mdrOptREAL;

  private static ResultSet mdrReqDECIMAL_5_3;
  // No NUMERIC while Drill just maps it to DECIMAL.

  private static ResultSet mdrReqVARCHAR_10;
  private static ResultSet mdrOptVARCHAR;
  private static ResultSet mdrReqCHAR_5;
  // No NCHAR, etc., in Drill (?).
  private static ResultSet mdrOptVARBINARY_16;
  private static ResultSet mdrOptBINARY_1048576;

  private static ResultSet mdrReqDATE;
  private static ResultSet mdrOptTIME;
  private static ResultSet mdrOptTIME_7;
  private static ResultSet mdrOptTIMESTAMP;
  // No "... WITH TIME ZONE" in Drill.
  private static ResultSet mdrOptINTERVAL_H_S3;
  private static ResultSet mdrOptINTERVAL_Y4;

  // For columns in schema hive_test.default's infoschematest table:

  // listtype column:      VARCHAR(65535) ARRAY, non-null(?):
  private static ResultSet mdrReqARRAY;
  // maptype column:       (VARCHAR(65535), INTEGER) MAP, non-null(?):
  private static ResultSet mdrReqMAP;
  // structtype column:    STRUCT(INTEGER sint, BOOLEAN sboolean,
  //                              VARCHAR(65535) sstring), non-null(?):
  private static ResultSet testRowSTRUCT;
  // uniontypetype column: OTHER (?), non=nullable(?):
  private static ResultSet testRowUnion;


  private static ResultSet setUpRow( final String schemaName,
                                     final String tableOrViewName,
                                     final String columnName ) throws SQLException
  {
    System.out.println( "(Setting up row for " + tableOrViewName + "." + columnName + ".)");
    assert null != dbMetadata
        : "dbMetadata is null; must be set before calling setUpRow(...)";
    final ResultSet testRow =
        dbMetadata.getColumns( "DRILL", schemaName, tableOrViewName, columnName );
    if ( ! testRow.next() ) {
      assert false
          : "Test setup error:  No row for column DRILL . `" + schemaName + "` . `"
            + tableOrViewName + "` . `" + columnName + "`";
    }
    return testRow;
  }

  @BeforeClass
  public static void setUpConnectionAndMetadataToCheck() throws Exception {

    // Get JDBC connection to Drill:
    connection = new Driver().connect( "jdbc:drill:zk=local", JdbcAssert.getDefaultProperties());
    dbMetadata = connection.getMetaData();
    Statement stmt = connection.createStatement();

    ResultSet util;

    /* TODO(start): Uncomment this block once we have a test plugin which supports all the needed types.
    // Create Hive test data, only if not created already (speed optimization):
    util = stmt.executeQuery( "SELECT * FROM INFORMATION_SCHEMA.COLUMNS "
                              + "WHERE TABLE_SCHEMA = 'hive_test.default' "
                              + "  AND TABLE_NAME = 'infoschematest'" );

    System.out.println( "(Hive infoschematest columns: " );
    int hiveTestColumnRowCount = 0;
    while ( util.next() ) {
      hiveTestColumnRowCount++;
      System.out.println(
          " Hive test column: "
          + util.getString( 1 ) + " - " + util.getString( 2 ) + " - "
          + util.getString( 3 ) + " - " + util.getString( 4 ) );
    }
    System.out.println( " Hive test column count: " + hiveTestColumnRowCount + ")" );
    if ( 0 == hiveTestColumnRowCount ) {
      // No Hive test data--create it.
      new HiveTestDataGenerator().generateTestData();
    } else if ( 17 == hiveTestColumnRowCount ) {
      // Hive data seems to exist already--skip recreating it.
    } else {
      assert false
          : "Expected 17 Hive test columns see " + hiveTestColumnRowCount + "."
            + "  Test code is out of date or Hive data is corrupted.";
    }
    TODO(end) */

    // Note: Assertions must be enabled (as they have been so far in tests).

    // Create temporary test-columns view:
    util = stmt.executeQuery( "USE dfs_test.tmp" );
    assert util.next();
    assert util.getBoolean( 1 )
        : "Error setting schema for test: " + util.getString( 2 );
    util = stmt.executeQuery(
        ""
        +   "CREATE OR REPLACE VIEW " + VIEW_NAME + " AS SELECT  "
        + "\n  CAST( NULL         AS BOOLEAN             ) AS optBOOLEAN,      "
        + "\n"
        + "\n  CAST(    1         AS TINYINT             ) AS reqTINYINT,      "
        + "\n  CAST( NULL         AS SMALLINT            ) AS optSMALLINT,     "
        + "\n  CAST(    2         AS INTEGER             ) AS reqINTEGER,      "
        + "\n  CAST( NULL         AS BIGINT              ) AS optBIGINT,       "
        + "\n"
        + "\n  CAST( NULL         AS FLOAT               ) AS optFLOAT,        "
        + "\n  CAST(  3.3         AS DOUBLE              ) AS reqDOUBLE,       "
        + "\n  CAST( NULL         AS REAL                ) AS optREAL,         "
        + "\n"
        + "\n  CAST(  4.4         AS DECIMAL(5,3)        ) AS reqDECIMAL_5_3,  "
        + "\n"
        + "\n  CAST( 'Hi'         AS VARCHAR(10)         ) AS reqVARCHAR_10,   "
        + "\n  CAST( NULL         AS VARCHAR             ) AS optVARCHAR,      "
        + "\n  CAST( '55'         AS CHAR(5)             ) AS reqCHAR_5,       "
        + "\n  CAST( NULL         AS VARBINARY(16)       ) AS optVARBINARY_16, "
        + "\n  CAST( NULL         AS VARBINARY(1048576)  ) AS optBINARY_1048576, "
        + "\n  CAST( NULL         AS BINARY(8)           ) AS optBINARY_8,     "
        + "\n"
        + "\n  CAST( '2015-01-01' AS DATE                ) AS reqDATE,         "
        + "\n  CAST( NULL         AS TIME                ) AS optTIME,         "
        + "\n  CAST( NULL         AS TIME(7)             ) AS optTIME_7,       "
        + "\n  CAST( NULL         AS TIMESTAMP           ) AS optTIMESTAMP,    "
        + "\n  CAST( NULL  AS INTERVAL HOUR TO SECOND(3) ) AS optINTERVAL_H_S3, "
        + "\n  CAST( NULL  AS INTERVAL YEAR(4)           ) AS optINTERVAL_Y4,  "
        + "\n  '' "
        + "\nFROM INFORMATION_SCHEMA.COLUMNS "
        + "\nLIMIT 1 " );
    assert util.next();
    assert util.getBoolean( 1 )
        : "Error creating temporary test-columns view " + VIEW_NAME + ": "
          + util.getString( 2 );

    // Set up result rows for temporary test view and Hivetest columns:

    mdrOptBOOLEAN        = setUpRow( "dfs_test.tmp", VIEW_NAME, "optBOOLEAN" );

    mdrReqTINYINT        = setUpRow( "dfs_test.tmp", VIEW_NAME, "reqTINYINT" );
    mdrOptSMALLINT       = setUpRow( "dfs_test.tmp", VIEW_NAME, "optSMALLINT" );
    mdrReqINTEGER        = setUpRow( "dfs_test.tmp", VIEW_NAME, "reqINTEGER" );
    mdrOptBIGINT         = setUpRow( "dfs_test.tmp", VIEW_NAME, "optBIGINT" );

    mdrOptFLOAT          = setUpRow( "dfs_test.tmp", VIEW_NAME, "optFLOAT" );
    mdrReqDOUBLE         = setUpRow( "dfs_test.tmp", VIEW_NAME, "reqDOUBLE" );
    mdrOptREAL           = setUpRow( "dfs_test.tmp", VIEW_NAME, "optREAL" );

    mdrReqDECIMAL_5_3    = setUpRow( "dfs_test.tmp", VIEW_NAME, "reqDECIMAL_5_3" );

    mdrReqVARCHAR_10     = setUpRow( "dfs_test.tmp", VIEW_NAME, "reqVARCHAR_10" );
    mdrOptVARCHAR        = setUpRow( "dfs_test.tmp", VIEW_NAME, "optVARCHAR" );
    mdrReqCHAR_5         = setUpRow( "dfs_test.tmp", VIEW_NAME, "reqCHAR_5" );
    mdrOptVARBINARY_16   = setUpRow( "dfs_test.tmp", VIEW_NAME, "optVARBINARY_16" );
    mdrOptBINARY_1048576 = setUpRow( "dfs_test.tmp", VIEW_NAME, "optBINARY_1048576" );

    mdrReqDATE           = setUpRow( "dfs_test.tmp", VIEW_NAME, "reqDATE" );
    mdrOptTIME           = setUpRow( "dfs_test.tmp", VIEW_NAME, "optTIME" );
    mdrOptTIME_7         = setUpRow( "dfs_test.tmp", VIEW_NAME, "optTIME_7" );
    mdrOptTIMESTAMP      = setUpRow( "dfs_test.tmp", VIEW_NAME, "optTIMESTAMP" );
    mdrOptINTERVAL_H_S3  = setUpRow( "dfs_test.tmp", VIEW_NAME, "optINTERVAL_H_S3" );
    mdrOptINTERVAL_Y4    = setUpRow( "dfs_test.tmp", VIEW_NAME, "optINTERVAL_Y4" );

    /* TODO(start): Uncomment this block once we have a test plugin which supports all the needed types.
    mdrReqARRAY   = setUpRow( "hive_test.default", "infoschematest", "listtype" );
    mdrReqMAP     = setUpRow( "hive_test.default", "infoschematest", "maptype" );
    testRowSTRUCT = setUpRow( "hive_test.default", "infoschematest", "structtype" );
    testRowUnion  = setUpRow( "hive_test.default", "infoschematest", "uniontypetype" );
    TODO(end) */

    // Set up getColumns(...)) result set' metadata:

    // Get all columns for more diversity of values (e.g., nulls and non-nulls),
    // in case that affects metadata (e.g., nullability) (in future).
    final ResultSet allColumns = dbMetadata.getColumns( null /* any catalog */,
                                                        null /* any schema */,
                                                        "%"  /* any table */,
                                                        "%"  /* any column */ );
    rowsMetadata = allColumns.getMetaData();
  }

  @AfterClass
  public static void tearDownConnection() throws SQLException {

    ResultSet util =
        connection.createStatement().executeQuery( "DROP VIEW " + VIEW_NAME + "" );
    assert util.next();
    // DRILL-2439:  assert util.getBoolean( 1 ) : ...;
    assert util.getBoolean( 1 )
       : "Error dropping temporary test-columns view " + VIEW_NAME + ": "
         + util.getString( 2 );
    connection.close();
  }


  //////////////////////////////////////////////////////////////////////
  // Tests:

  ////////////////////////////////////////////////////////////
  // Number of columns.

  @Test
  public void testMetadataHasRightNumberOfColumns() throws SQLException {
    // TODO:  Review:  Is this check valid?  (Are extra columns allowed?)
    assertThat( "column count", rowsMetadata.getColumnCount(), equalTo( 24 ) );
  }


  ////////////////////////////////////////////////////////////
  // #1: TABLE_CAT:
  // - JDBC:   "1. ... String => table catalog (may be null)"
  // - Drill:  Apparently chooses always "DRILL".
  // - (Meta): VARCHAR (NVARCHAR?); Non-nullable(?);

  @Test
  public void test_TABLE_CAT_isAtRightPosition() throws SQLException {
    assertThat( rowsMetadata.getColumnLabel( 1 ), equalTo( "TABLE_CAT" ) );
  }

  @Test
  public void test_TABLE_CAT_hasRightValue_optBOOLEAN() throws SQLException {
    assertThat( mdrOptBOOLEAN.getString( "TABLE_CAT" ), equalTo( "DRILL" ) );
  }

  // Not bothering with other test columns for TABLE_CAT.

  @Test
  public void test_TABLE_CAT_hasSameNameAndLabel() throws SQLException {
    assertThat( rowsMetadata.getColumnName( 1 ), equalTo( "TABLE_CAT" ) );
  }

  @Test
  public void test_TABLE_CAT_hasRightTypeString() throws SQLException {
    assertThat( rowsMetadata.getColumnTypeName( 1 ), equalTo( "VARCHAR" ) );
  }

  @Test
  public void test_TABLE_CAT_hasRightTypeCode() throws SQLException {
    assertThat( rowsMetadata.getColumnType( 1 ), equalTo( Types.VARCHAR ) );
  }

  @Ignore( "until fixed (\"none\" -> right class name) (DRILL-2137)" )
  @Test
  public void test_TABLE_CAT_hasRightClass() throws SQLException {
    // TODO:  Confirm that this "java.lang.String" is correct:
    assertThat( rowsMetadata.getColumnClassName( 1 ), equalTo( String.class.getName() ) );
  }

  // (See to-do note near top of file about reviewing nullability.)
  @Test
  public void test_TABLE_CAT_hasRightNullability() throws SQLException {
    assertThat( "ResultSetMetaData.column...Null... nullability code",
                rowsMetadata.isNullable( 1 ), equalTo( columnNullable ) );
  }


  ////////////////////////////////////////////////////////////
  // #2: TABLE_SCHEM:
  // - JDBC:   "2. ... String => table schema (may be null)"
  // - Drill:  Always reports a schema name.
  // - (Meta): VARCHAR (NVARCHAR?); Nullable?;

  @Test
  public void test_TABLE_SCHEM_isAtRightPosition() throws SQLException {
    assertThat( rowsMetadata.getColumnLabel( 2 ), equalTo( "TABLE_SCHEM" ) );
  }

  @Test
  public void test_TABLE_SCHEM_hasRightValue_optBOOLEAN() throws SQLException {
    assertThat( mdrOptBOOLEAN.getString( "TABLE_SCHEM" ), equalTo( "dfs_test.tmp" ) );
  }

  // Not bothering with other _local_view_ test columns for TABLE_SCHEM.

  @Test
  @Ignore("Enable once we have a test plugin which supports all the needed types.")
  public void test_TABLE_SCHEM_hasRightValue_tdbARRAY() throws SQLException {
    assertThat( mdrReqARRAY.getString( "TABLE_SCHEM" ), equalTo( "hive_test.default" ) );
  }

  // Not bothering with other Hive test columns for TABLE_SCHEM.

  @Test
  public void test_TABLE_SCHEM_hasSameNameAndLabel() throws SQLException {
    assertThat( rowsMetadata.getColumnName( 2 ), equalTo( "TABLE_SCHEM" ) );
  }

  @Test
  public void test_TABLE_SCHEM_hasRightTypeString() throws SQLException {
    assertThat( rowsMetadata.getColumnTypeName( 2 ), equalTo( "VARCHAR" ) );
  }

  @Test
  public void test_TABLE_SCHEM_hasRightTypeCode() throws SQLException {
    assertThat( rowsMetadata.getColumnType( 2 ), equalTo( Types.VARCHAR ) );
  }

  @Ignore( "until fixed (\"none\" -> right class name) (DRILL-2137)" )
  @Test
  public void test_TABLE_SCHEM_hasRightClass() throws SQLException {
    // TODO:  Confirm that this "java.lang.String" is correct:
    assertThat( rowsMetadata.getColumnClassName( 2 ), equalTo( String.class.getName() ) );
  }

  @Ignore( "until resolved:  any requirement on nullability (DRILL-2420?)" )
  @Test
  public void test_TABLE_SCHEM_hasRightNullability() throws SQLException {
    // To-do:  CHECK:  Why columnNullable, when seemingly known nullable?
    // (Why not like TABLE_CAT, which does have columnNoNulls?)
    assertThat( "ResultSetMetaData.column...Null... nullability code",
                rowsMetadata.isNullable( 2 ), equalTo( columnNoNulls ) );
  }


  ////////////////////////////////////////////////////////////
  // #3: TABLE_NAME:
  // - JDBC:  "3. ... String => table name"
  // - Drill:
  // - (Meta): VARCHAR (NVARCHAR?); Non-nullable?;

  @Test
  public void test_TABLE_NAME_isAtRightPosition() throws SQLException {
    assertThat( rowsMetadata.getColumnLabel( 3 ), equalTo( "TABLE_NAME" ) );
  }

  @Test
  public void test_TABLE_NAME_hasRightValue_optBOOLEAN() throws SQLException {
    assertThat( mdrOptBOOLEAN.getString( "TABLE_NAME" ), equalTo( VIEW_NAME ) );
  }

  // Not bothering with other _local_view_ test columns for TABLE_NAME.

  @Test
  public void test_TABLE_NAME_hasSameNameAndLabel() throws SQLException {
    assertThat( rowsMetadata.getColumnName( 3 ), equalTo( "TABLE_NAME" ) );
  }

  @Test
  public void test_TABLE_NAME_hasRightTypeString() throws SQLException {
    assertThat( rowsMetadata.getColumnTypeName( 3 ), equalTo( "VARCHAR" ) );
  }

  @Test
  public void test_TABLE_NAME_hasRightTypeCode() throws SQLException {
    assertThat( rowsMetadata.getColumnType( 3 ), equalTo( Types.VARCHAR ) );
  }

  @Ignore( "until fixed (\"none\" -> right class name) (DRILL-2137)" )
  @Test
  public void test_TABLE_NAME_hasRightClass() throws SQLException {
    // TODO:  Confirm that this "java.lang.String" is correct:
    assertThat( rowsMetadata.getColumnClassName( 3 ), equalTo( String.class.getName() ) );
  }

  @Ignore( "until resolved:  any requirement on nullability (DRILL-2420?)" )
  @Test
  public void test_TABLE_NAME_hasRightNullability() throws SQLException {
    // To-do:  CHECK:  Why columnNullable, when seemingly known nullable?
    // (Why not like TABLE_CAT, which does have columnNoNulls?)
    assertThat( "ResultSetMetaData.column...Null... nullability code",
                rowsMetadata.isNullable( 3 ), equalTo( columnNoNulls ) );
  }


  ////////////////////////////////////////////////////////////
  // #4: COLUMN_NAME:
  // - JDBC:  "4. ... String => column name"
  // - Drill:
  // - (Meta): VARCHAR (NVARCHAR?); Non-nullable(?);

  @Test
  public void test_COLUMN_NAME_isAtRightPosition() throws SQLException {
    assertThat( rowsMetadata.getColumnLabel( 4 ), equalTo( "COLUMN_NAME" ) );
  }

  @Test
  public void test_COLUMN_NAME_hasRightValue_optBOOLEAN() throws SQLException {
    assertThat( mdrOptBOOLEAN.getString( "COLUMN_NAME" ), equalTo( "optBOOLEAN" ) );
  }

  // Not bothering with other _local_view_ test columns for TABLE_SCHEM.

  @Test
  @Ignore("Enable once we have a test plugin which supports all the needed types.")
  public void test_COLUMN_NAME_hasRightValue_tdbARRAY() throws SQLException {
    assertThat( mdrReqARRAY.getString( "COLUMN_NAME" ), equalTo( "listtype" ) );
  }

  // Not bothering with other Hive test columns for TABLE_SCHEM.

  @Test
  public void test_COLUMN_NAME_hasSameNameAndLabel() throws SQLException {
    assertThat( rowsMetadata.getColumnName( 4 ), equalTo( "COLUMN_NAME" ) );
  }

  @Test
  public void test_COLUMN_NAME_hasRightTypeString() throws SQLException {
    assertThat( rowsMetadata.getColumnTypeName( 4 ), equalTo( "VARCHAR" ) );
  }

  @Test
  public void test_COLUMN_NAME_hasRightTypeCode() throws SQLException {
    assertThat( rowsMetadata.getColumnType( 4 ), equalTo( Types.VARCHAR ) );
  }

  @Ignore( "until fixed (\"none\" -> right class name) (DRILL-2137)" )
  @Test
  public void test_COLUMN_NAME_hasRightClass() throws SQLException {
    // TODO:  Confirm that this "java.lang.String" is correct:
    assertThat( rowsMetadata.getColumnClassName( 4 ), equalTo( String.class.getName() ) );
  }

  @Ignore( "until resolved:  any requirement on nullability (DRILL-2420?)" )
  @Test
  public void test_COLUMN_NAME_hasRightNullability() throws SQLException {
    // To-do:  CHECK:  Why columnNullable, when seemingly known nullable?
    // (Why not like TABLE_CAT, which does have columnNoNulls?)
    assertThat( "ResultSetMetaData.column...Null... nullability code",
                rowsMetadata.isNullable( 4 ), equalTo( columnNoNulls ) );
  }


  ////////////////////////////////////////////////////////////
  // #5: DATA_TYPE:
  // - JDBC:  "5. ... int => SQL type from java.sql.Types"
  // - Drill:
  // - (Meta): INTEGER(?);  Non-nullable(?);

  @Test
  public void test_DATA_TYPE_isAtRightPosition() throws SQLException {
    assertThat( rowsMetadata.getColumnLabel( 5 ), equalTo( "DATA_TYPE" ) );
  }

  @Test
  public void test_DATA_TYPE_hasRightValue_optBOOLEAN() throws SQLException {
    assertThat( mdrOptBOOLEAN.getInt( "DATA_TYPE" ), equalTo( Types.BOOLEAN ) );
  }

  @Test
  public void test_DATA_TYPE_hasRightValue_reqTINYINT() throws SQLException {
    assertThat( mdrReqTINYINT.getInt( "DATA_TYPE" ), equalTo( Types.TINYINT ) );
  }

  @Test
  public void test_DATA_TYPE_hasRightValue_optSMALLINT() throws SQLException {
    assertThat( mdrOptSMALLINT.getInt( "DATA_TYPE" ), equalTo( Types.SMALLINT ) );
  }

  @Test
  public void test_DATA_TYPE_hasRightValue_reqINTEGER() throws SQLException {
    assertThat( mdrReqINTEGER.getInt( "DATA_TYPE" ), equalTo( Types.INTEGER ) );
  }

  @Test
  public void test_DATA_TYPE_hasRightValue_optBIGINT() throws SQLException {
    assertThat( mdrOptBIGINT.getInt( "DATA_TYPE" ), equalTo( Types.BIGINT ) );
  }

  @Test
  public void test_DATA_TYPE_hasRightValue_optFLOAT() throws SQLException {
    assertThat( mdrOptFLOAT.getInt( "DATA_TYPE" ), equalTo( Types.FLOAT ) );
  }

  @Test
  public void test_DATA_TYPE_hasRightValue_reqDOUBLE() throws SQLException {
    assertThat( mdrReqDOUBLE.getInt( "DATA_TYPE" ), equalTo( Types.DOUBLE ) );
  }

  @Test
  public void test_DATA_TYPE_hasRightValue_optREAL() throws SQLException {
    assertThat( mdrOptREAL.getInt( "DATA_TYPE" ), equalTo( Types.REAL ) );
  }

  @Test
  public void test_DATA_TYPE_hasRightValue_reqDECIMAL_5_3() throws SQLException {
    assertThat( mdrReqDECIMAL_5_3.getInt( "DATA_TYPE" ), equalTo( Types.DECIMAL ) );
  }

  @Test
  public void test_DATA_TYPE_hasRightValue_reqVARCHAR_10() throws SQLException {
    assertThat( mdrReqVARCHAR_10.getInt( "DATA_TYPE" ), equalTo( Types.VARCHAR ) );
  }

  @Test
  public void test_DATA_TYPE_hasRightValue_optVARCHAR() throws SQLException {
    assertThat( mdrOptVARCHAR.getInt( "DATA_TYPE" ), equalTo( Types.VARCHAR ) );
  }

  @Test
  public void test_DATA_TYPE_hasRightValue_reqCHAR_5() throws SQLException {
    assertThat( mdrReqCHAR_5.getInt( "DATA_TYPE" ), equalTo( Types.CHAR ) );
  }

  @Test
  public void test_DATA_TYPE_hasRightValue_optVARBINARY_16() throws SQLException {
    assertThat( mdrOptVARBINARY_16.getInt( "DATA_TYPE" ), equalTo( Types.VARBINARY ) );
  }

  @Test
  public void test_DATA_TYPE_hasRightValue_optBINARY_1048576CHECK() throws SQLException {
    assertThat( mdrOptBINARY_1048576.getInt( "DATA_TYPE" ), equalTo( Types.VARBINARY ) );
  }

  @Test
  public void test_DATA_TYPE_hasRightValue_reqDATE() throws SQLException {
    assertThat( mdrReqDATE.getInt( "DATA_TYPE" ), equalTo( Types.DATE ) );
  }

  @Test
  public void test_DATA_TYPE_hasRightValue_optTIME() throws SQLException {
    assertThat( mdrOptTIME.getInt( "DATA_TYPE" ), equalTo( Types.TIME ) );
  }

  @Test
  public void test_DATA_TYPE_hasRightValue_optTIME_7() throws SQLException {
    assertThat( mdrOptTIME_7.getInt( "DATA_TYPE" ), equalTo( Types.TIME ) );
  }

  @Test
  public void test_DATA_TYPE_hasRightValue_optTIMESTAMP() throws SQLException {
    assertThat( mdrOptTIMESTAMP.getInt( "DATA_TYPE" ), equalTo( Types.TIMESTAMP ) );
  }

  @Ignore( "until resolved:  expected value (DRILL-2420?)" )
  @Test
  public void test_DATA_TYPE_hasRightValue_optINTERVAL_HM() throws SQLException {
    assertThat( mdrOptINTERVAL_H_S3.getInt( "DATA_TYPE" ), equalTo( Types.OTHER ) );
    // To-do:  Determine which.
    assertThat( mdrOptINTERVAL_H_S3.getInt( "DATA_TYPE" ), equalTo( Types.JAVA_OBJECT ) );
  }

  @Ignore( "until resolved:  expected value (DRILL-2420?)" )
  @Test
  public void test_DATA_TYPE_hasRightValue_optINTERVAL_Y3() throws SQLException {
    assertThat( mdrOptINTERVAL_Y4.getInt( "DATA_TYPE" ), equalTo( Types.OTHER ) );
    // To-do:  Determine which.
    assertThat( mdrOptINTERVAL_Y4.getInt( "DATA_TYPE" ), equalTo( Types.JAVA_OBJECT ) );
  }

  @Test
  @Ignore("Enable once we have a test plugin which supports all the needed types.")
  public void test_DATA_TYPE_hasRightValue_tdbARRAY() throws SQLException {
    assertThat( mdrReqARRAY.getInt( "DATA_TYPE" ), equalTo( Types.ARRAY ) );
  }

  @Ignore( "until resolved:  expected value (DRILL-2420?)" )
  @Test
  public void test_DATA_TYPE_hasRightValue_tbdMAP() throws SQLException {
    assertThat( "java.sql.Types.* type code",
                mdrReqMAP.getInt( "DATA_TYPE" ), equalTo( Types.OTHER ) );
    // To-do:  Determine which.
    assertThat( "java.sql.Types.* type code",
                mdrReqMAP.getInt( "DATA_TYPE" ), equalTo( Types.JAVA_OBJECT ) );
  }

  @Test
  @Ignore("Enable once we have a test plugin which supports all the needed types.")
  public void test_DATA_TYPE_hasRightValue_tbdSTRUCT() throws SQLException {
    assertThat( testRowSTRUCT.getInt( "DATA_TYPE" ), equalTo( Types.STRUCT ) );
  }

  @Ignore( "until resolved:  expected value (DRILL-2420?)" )
  @Test
  public void test_DATA_TYPE_hasRightValue_tbdUnion() throws SQLException {
    assertThat( "java.sql.Types.* type code",
                testRowUnion.getInt( "DATA_TYPE" ), equalTo( Types.OTHER ) );
    // To-do:  Determine which.
    assertThat( "java.sql.Types.* type code",
                testRowUnion.getInt( "DATA_TYPE" ), equalTo( Types.JAVA_OBJECT ) );
  }

  @Test
  public void test_DATA_TYPE_hasSameNameAndLabel() throws SQLException {
    assertThat( rowsMetadata.getColumnName( 5 ), equalTo( "DATA_TYPE" ) );
  }

  @Test
  public void test_DATA_TYPE_hasRightTypeString() throws SQLException {
    assertThat( rowsMetadata.getColumnTypeName( 5 ), equalTo( "INTEGER" ) );
  }

  @Test
  public void test_DATA_TYPE_hasRightTypeCode() throws SQLException {
    assertThat( rowsMetadata.getColumnType( 5 ), equalTo( Types.INTEGER ) );
  }

  @Ignore( "until fixed (\"none\" -> right class name) (DRILL-2137)" )
  @Test
  public void test_DATA_TYPE_hasRightClass() throws SQLException {
    // TODO:  Confirm that this "java.lang.Integer" is correct:
    assertThat( rowsMetadata.getColumnClassName( 5 ), equalTo( Integer.class.getName() ) );
  }

  // (See to-do note near top of file about reviewing nullability.)
  @Test
  public void test_DATA_TYPE_hasRightNullability() throws SQLException {
    assertThat( "ResultSetMetaData.column...Null... nullability code",
                rowsMetadata.isNullable( 5 ), equalTo( columnNoNulls ) );
  }


  ////////////////////////////////////////////////////////////
  // #6: TYPE_NAME:
  // - JDBC:  "6. ... String => Data source dependent type name, for a UDT the
  //     type name is fully qualified"
  // - Drill:
  // - (Meta): VARCHAR (NVARCHAR?); Non-nullable?;

  @Test
  public void test_TYPE_NAME_isAtRightPosition() throws SQLException {
    assertThat( rowsMetadata.getColumnLabel( 6 ), equalTo( "TYPE_NAME" ) );
  }

  @Test
  public void test_TYPE_NAME_hasRightValue_optBOOLEAN() throws SQLException {
    assertThat( mdrOptBOOLEAN.getString( "TYPE_NAME" ), equalTo( "BOOLEAN" ) );
  }

  @Test
  public void test_TYPE_NAME_hasRightValue_reqTINYINT() throws SQLException {
    assertThat( mdrReqTINYINT.getString( "TYPE_NAME" ), equalTo( "TINYINT" ) );
  }

  @Test
  public void test_TYPE_NAME_hasRightValue_optSMALLINT() throws SQLException {
    assertThat( mdrOptSMALLINT.getString( "TYPE_NAME" ), equalTo( "SMALLINT" ) );
  }

  @Test
  public void test_TYPE_NAME_hasRightValue_reqINTEGER() throws SQLException {
    assertThat( mdrReqINTEGER.getString( "TYPE_NAME" ), equalTo( "INTEGER" ) );
  }

  @Test
  public void test_TYPE_NAME_hasRightValue_optBIGINT() throws SQLException {
    assertThat( mdrOptBIGINT.getString( "TYPE_NAME" ), equalTo( "BIGINT" ) );
  }

  @Test
  public void test_TYPE_NAME_hasRightValue_optFLOAT() throws SQLException {
    assertThat( mdrOptFLOAT.getString( "TYPE_NAME" ), equalTo( "FLOAT" ) );
  }

  @Test
  public void test_TYPE_NAME_hasRightValue_reqDOUBLE() throws SQLException {
    assertThat( mdrReqDOUBLE.getString( "TYPE_NAME" ), equalTo( "DOUBLE" ) );
  }

  @Test
  public void test_TYPE_NAME_hasRightValue_optREAL() throws SQLException {
    assertThat( mdrOptREAL.getString( "TYPE_NAME" ), equalTo( "REAL" ) );
  }

  @Test
  public void test_TYPE_NAME_hasRightValue_reqDECIMAL_5_3() throws SQLException {
    assertThat( mdrReqDECIMAL_5_3.getString( "TYPE_NAME" ), equalTo( "DECIMAL" ) );
  }

  @Test
  public void test_TYPE_NAME_hasRightValue_reqVARCHAR_10() throws SQLException {
    assertThat( mdrReqVARCHAR_10.getString( "TYPE_NAME" ), equalTo( "VARCHAR" ) );
  }

  @Test
  public void test_TYPE_NAME_hasRightValue_optVARCHAR() throws SQLException {
    assertThat( mdrOptVARCHAR.getString( "TYPE_NAME" ), equalTo( "VARCHAR" ) );
  }

  @Test
  public void test_TYPE_NAME_hasRightValue_reqCHAR_5() throws SQLException {
    assertThat( mdrReqCHAR_5.getString( "TYPE_NAME" ), equalTo( "CHAR" ) );
  }

  @Test
  public void test_TYPE_NAME_hasRightValue_optVARBINARY_16() throws SQLException {
    assertThat( mdrOptVARBINARY_16.getString( "TYPE_NAME" ), equalTo( "VARBINARY" ) );
  }

  @Test
  public void test_TYPE_NAME_hasRightValue_optBINARY_1048576CHECK() throws SQLException {
    assertThat( mdrOptBINARY_1048576.getString( "TYPE_NAME" ), equalTo( "VARBINARY" ) );
  }

  @Test
  public void test_TYPE_NAME_hasRightValue_reqDATE() throws SQLException {
    assertThat( mdrReqDATE.getString( "TYPE_NAME" ), equalTo( "DATE" ) );
  }

  @Test
  public void test_TYPE_NAME_hasRightValue_optTIME() throws SQLException {
    assertThat( mdrOptTIME.getString( "TYPE_NAME" ), equalTo( "TIME" ) );
  }

  @Test
  public void test_TYPE_NAME_hasRightValue_optTIME_7() throws SQLException {
    assertThat( mdrOptTIME_7.getString( "TYPE_NAME" ), equalTo( "TIME" ) );
  }

  @Test
  public void test_TYPE_NAME_hasRightValue_optTIMESTAMP() throws SQLException {
    assertThat( mdrOptTIMESTAMP.getString( "TYPE_NAME" ), equalTo( "TIMESTAMP" ) );
  }

  @Test
  public void test_TYPE_NAME_hasRightValue_optINTERVAL_HM() throws SQLException {
    // (What SQL standard specifies for DATA_TYPE in INFORMATION_SCHEMA.COLUMNS:)
    assertThat( mdrOptINTERVAL_H_S3.getString( "TYPE_NAME" ), equalTo( "INTERVAL" ) );
  }

  @Test
  public void test_TYPE_NAME_hasRightValue_optINTERVAL_Y3() throws SQLException {
    // (What SQL standard specifies for DATA_TYPE in INFORMATION_SCHEMA.COLUMNS:)
    assertThat( mdrOptINTERVAL_Y4.getString( "TYPE_NAME" ), equalTo( "INTERVAL" ) );
  }

  @Ignore( "until resolved:  expected value (DRILL-2420?)" )
  @Test
  public void test_TYPE_NAME_hasRightValue_tdbARRAY() throws SQLException {
    assertThat( mdrReqARRAY.getString( "TYPE_NAME" ), equalTo( "VARCHAR(65535) ARRAY" ) );
    // TODO:  Determine which.
    assertThat( mdrReqARRAY.getString( "TYPE_NAME" ), equalTo( "ARRAY" ) );
  }

  @Ignore( "until resolved:  expected value (DRILL-2420?)" )
  @Test
  public void test_TYPE_NAME_hasRightValue_tbdMAP() throws SQLException {
    assertThat( mdrReqMAP.getString( "TYPE_NAME" ), equalTo( "(VARCHAR(65535), INTEGER) MAP" ) );
    // TODO:  Determine which.
    assertThat( mdrReqMAP.getString( "TYPE_NAME" ), equalTo( "MAP" ) );
  }

  @Ignore( "until resolved:  expected value (DRILL-2420?)" )
  @Test
  public void test_TYPE_NAME_hasRightValue_tbdSTRUCT() throws SQLException {
    assertThat( testRowSTRUCT.getString( "TYPE_NAME" ),
                equalTo( "STRUCT(INTEGER sint, BOOLEAN sboolean, VARCHAR(65535) sstring)" ) ); // TODO:  Confirm.
    // TODO:  Determine which.
    assertThat( testRowSTRUCT.getString( "TYPE_NAME" ), equalTo( "STRUCT" ) );
  }

  @Ignore( "until resolved:  expected value (DRILL-2420?)" )
  @Test
  public void test_TYPE_NAME_hasRightValue_tbdUnion() throws SQLException {
    assertThat( testRowUnion.getString( "TYPE_NAME" ), equalTo( "OTHER" ) );
    fail( "Expected value is not resolved yet." );
  }

  @Test
  public void test_TYPE_NAME_hasSameNameAndLabel() throws SQLException {
    assertThat( rowsMetadata.getColumnName( 6 ), equalTo( "TYPE_NAME" ) );
  }

  @Test
  public void test_TYPE_NAME_hasRightTypeString() throws SQLException {
    assertThat( rowsMetadata.getColumnTypeName( 6 ), equalTo( "VARCHAR" ) );
  }

  @Test
  public void test_TYPE_NAME_hasRightTypeCode() throws SQLException {
    assertThat( rowsMetadata.getColumnType( 6 ), equalTo( Types.VARCHAR ) );
  }

  @Ignore( "until fixed (\"none\" -> right class name) (DRILL-2137)" )
  @Test
  public void test_TYPE_NAME_hasRightClass() throws SQLException {
    // TODO:  Confirm that this "java.lang.String" is correct:
    assertThat( rowsMetadata.getColumnClassName( 6 ), equalTo( String.class.getName() ) );
  }

  @Ignore( "until resolved:  any requirement on nullability (DRILL-2420?)" )
  @Test
  public void test_TYPE_NAME_hasRightNullability() throws SQLException {
    // To-do:  CHECK:  Why columnNullable, when seemingly known nullable?
    assertThat( "ResultSetMetaData.column...Null... nullability code",
                rowsMetadata.isNullable( 6 ), equalTo( columnNoNulls ) );
  }


  ////////////////////////////////////////////////////////////
  // #7: COLUMN_SIZE:
  // - JDBC:  "7. ... int => column size."
  //     "The COLUMN_SIZE column specifies the column size for the given column.
  //      For numeric data, this is the maximum precision.
  //      For character data, this is the length in characters.
  //      For datetime datatypes, this is the length in characters of the String
  //        representation (assuming the maximum allowed precision of the
  //        fractional seconds component).
  //      For binary data, this is the length in bytes.
  //      For the ROWID datatype, this is the length in bytes.
  //      Null is returned for data types where the column size is not applicable."
  //   - "Maximum precision" seem to mean maximum number of digits that can
  //     appear.
  // - (Meta): INTEGER(?); Nullable;

  @Test
  public void test_COLUMN_SIZE_isAtRightPosition() throws SQLException {
    assertThat( rowsMetadata.getColumnLabel( 7 ), equalTo( "COLUMN_SIZE" ) );
  }

  @Test
  public void test_COLUMN_SIZE_hasRightValue_optBOOLEAN() throws SQLException {
    final int value = mdrOptBOOLEAN.getInt( "COLUMN_SIZE" );
    assertThat( "wasNull() [after " + value + "]",
                mdrOptBOOLEAN.wasNull(), equalTo( true )  ); // TODO:  CONFIRM.
    assertThat( value, equalTo( 0 /* NULL */ ) );
  }

  @Test
  public void test_COLUMN_SIZE_hasRightValue_reqTINYINT() throws SQLException {
    assertThat( mdrReqTINYINT.getInt( "COLUMN_SIZE" ), equalTo( 3 ) ); // TODO:  CONFIRM.
  }

  @Test
  public void test_COLUMN_SIZE_hasRightValue_optSMALLINT() throws SQLException {
    assertThat( mdrOptSMALLINT.getInt( "COLUMN_SIZE" ), equalTo( 5 ) );  // TODO:  CONFIRM
  }

  @Test
  public void test_COLUMN_SIZE_hasRightValue_reqINTEGER() throws SQLException {
    assertThat( mdrReqINTEGER.getInt( "COLUMN_SIZE" ), equalTo( 10 ) ); // TODO:  CONFIRM.
  }

  @Test
  public void test_COLUMN_SIZE_hasRightValue_optBIGINT() throws SQLException {
    assertThat( mdrOptBIGINT.getInt( "COLUMN_SIZE" ), equalTo( 19 ) );  // To-do:  CONFIRM.
  }

  @Test
  public void test_COLUMN_SIZE_hasRightValue_optFLOAT() throws SQLException {
    assertThat( mdrOptFLOAT.getInt( "COLUMN_SIZE" ), equalTo( 7 ) );
  }

  @Test
  public void test_COLUMN_SIZE_hasRightValue_reqDOUBLE() throws SQLException {
    assertThat( mdrReqDOUBLE.getInt( "COLUMN_SIZE" ), equalTo( 15 ) );
  }

  @Test
  public void test_COLUMN_SIZE_hasRightValue_optREAL() throws SQLException {
    assertThat( mdrOptREAL.getInt( "COLUMN_SIZE" ), equalTo( 15 ) );
  }

  @Test
  public void test_COLUMN_SIZE_hasRightValue_reqDECIMAL_5_3() throws SQLException {
    assertThat( mdrReqDECIMAL_5_3.getInt( "COLUMN_SIZE" ), equalTo( 5 ) );
  }

  @Test
  public void test_COLUMN_SIZE_hasRightValue_reqVARCHAR_10() throws SQLException {
    assertThat( mdrReqVARCHAR_10.getInt( "COLUMN_SIZE" ), equalTo( 10 ) );
  }

  @Test
  public void test_COLUMN_SIZE_hasRightValue_optVARCHAR() throws SQLException {
    assertThat( mdrOptVARCHAR.getInt( "COLUMN_SIZE" ), equalTo( 1 ) );
  }

  @Test
  public void test_COLUMN_SIZE_hasRightValue_reqCHAR_5() throws SQLException {
    assertThat( mdrReqCHAR_5.getInt( "COLUMN_SIZE" ), equalTo( 5 ) );
  }

  @Test
  public void test_COLUMN_SIZE_hasRightValue_optVARBINARY_16() throws SQLException {
    assertThat( mdrOptVARBINARY_16.getInt( "COLUMN_SIZE" ), equalTo( 16 ) );
  }

  @Test
  public void test_COLUMN_SIZE_hasRightValue_optBINARY_1048576() throws SQLException {
    assertThat( mdrOptBINARY_1048576.getInt( "COLUMN_SIZE" ), equalTo( 1048576 ) );
  }

  @Test
  public void test_COLUMN_SIZE_hasRightValue_reqDATE() throws SQLException {
    assertThat( mdrReqDATE.getInt( "COLUMN_SIZE" ), equalTo( 10 ) );
  }

  @Test
  public void test_COLUMN_SIZE_hasRightValue_optTIME() throws SQLException {
    assertThat( mdrOptTIME.getInt( "COLUMN_SIZE" ),
                equalTo( 8  /* HH:MM:SS */  ) );
  }

  @Ignore( "until resolved:  whether to implement TIME precision or drop test" )
  @Test
  public void test_COLUMN_SIZE_hasRightValue_optTIME_7() throws SQLException {
    assertThat( mdrOptTIME_7.getInt( "COLUMN_SIZE" ),
                equalTo( 8  /* HH:MM:SS */ + 1 /* '.' */ + 7 /* sssssss */ ) );
  }

  @Test
  public void test_COLUMN_SIZE_hasRightValue_optTIMESTAMP() throws SQLException {
    assertThat( mdrOptTIMESTAMP.getInt( "COLUMN_SIZE" ),
                equalTo( 19 /* YYYY-MM-DDTHH:MM:SS */  ) );
  }

  @Ignore( "until fixed:  INTERVAL metadata in INFORMATION_SCHEMA (DRILL-2531)" )
  @Test
  public void test_COLUMN_SIZE_hasRightValue_optINTERVAL_HM() throws SQLException {
    assertThat( mdrOptINTERVAL_H_S3.getInt( "COLUMN_SIZE" ),
                equalTo( 14 ) );  // "P12H12M12.1234S"
  }

  // TODO:  When DRILL-2531 is fixed, remove this:
  @Test
  public void test_COLUMN_SIZE_hasRightINTERIMValue_optINTERVAL_HM() throws SQLException {
    assertThat( mdrOptINTERVAL_H_S3.getInt( "COLUMN_SIZE" ),
                equalTo( 31 ) );  // from max. form "P12..90D12H12M12.12..89S"
  }

  @Ignore( "until fixed:  INTERVAL metadata in INFORMATION_SCHEMA (DRILL-2531)" )
  @Test
  public void test_COLUMN_SIZE_hasRightValue_optINTERVAL_Y3() throws SQLException {
    assertThat( mdrOptINTERVAL_Y4.getInt( "COLUMN_SIZE" ),
                equalTo( 6 ) );  // "P1234Y"
  }

  // TODO:  When DRILL-2531 is fixed, remove this:
  @Test
  public void test_COLUMN_SIZE_hasRightINTERIMValue_optINTERVAL_Y3() throws SQLException {
    assertThat( mdrOptINTERVAL_Y4.getInt( "COLUMN_SIZE" ),
                equalTo( 15 ) );  // from max. form "P12..90Y"
  }

  @Test
  @Ignore("Enable once we have a test plugin which supports all the needed types.")
  public void test_COLUMN_SIZE_hasRightValue_tdbARRAY() throws SQLException {
    final int value = mdrReqARRAY.getInt( "COLUMN_SIZE" );
    assertThat( "wasNull() [after " + value + "]",
                mdrReqARRAY.wasNull(), equalTo( true ) ); // TODO:  Confirm.
    assertThat( value, equalTo( 0 /* NULL */ ) );
  }

  @Test
  @Ignore("Enable once we have a test plugin which supports all the needed types.")
  public void test_COLUMN_SIZE_hasRightValue_tbdMAP() throws SQLException {
    final int value = mdrReqMAP.getInt( "COLUMN_SIZE" );
    assertThat( "wasNull() [after " + value + "]",
                mdrReqMAP.wasNull(), equalTo( true ) ); // TODO:  Confirm.
    assertThat( value, equalTo( 0 /* NULL */ ) );
  }

  @Test
  @Ignore("Enable once we have a test plugin which supports all the needed types.")
  public void test_COLUMN_SIZE_hasRightValue_tbdSTRUCT() throws SQLException {
    final int value = testRowSTRUCT.getInt( "COLUMN_SIZE" );
    assertThat( "wasNull() [after " + value + "]",
                testRowSTRUCT.wasNull(), equalTo( true ) ); // TODO:  Confirm.
    assertThat( value, equalTo( 0 /* NULL */ ) );
  }

  @Test
  @Ignore("Enable once we have a test plugin which supports all the needed types.")
  public void test_COLUMN_SIZE_hasRightValue_tbdUnion() throws SQLException {
    final int value = testRowUnion.getInt( "COLUMN_SIZE" );
    assertThat( "wasNull() [after " + value + "]",
                testRowUnion.wasNull(), equalTo( true ) ); // TODO:  Confirm.
    assertThat( value, equalTo( 0 /* NULL */ ) );
  }

  @Test
  public void test_COLUMN_SIZE_hasSameNameAndLabel() throws SQLException {
    assertThat( rowsMetadata.getColumnName( 7 ), equalTo( "COLUMN_SIZE" ) );
  }

  @Test
  public void test_COLUMN_SIZE_hasRightTypeString() throws SQLException {
    assertThat( rowsMetadata.getColumnTypeName( 7 ), equalTo( "INTEGER" ) );
  }

  @Test
  public void test_COLUMN_SIZE_hasRightTypeCode() throws SQLException {
    assertThat( rowsMetadata.getColumnType( 7 ), equalTo( Types.INTEGER ) );
  }

  @Ignore( "until fixed (\"none\" -> right class name) (DRILL-2137)" )
  @Test
  public void test_COLUMN_SIZE_hasRightClass() throws SQLException {
    // TODO:  Confirm that this "java.lang.Integer" is correct:
    assertThat( rowsMetadata.getColumnClassName( 7 ), equalTo( Integer.class.getName() ) );
  }

  // (See to-do note near top of file about reviewing nullability.)
  @Test
  public void test_COLUMN_SIZE_hasRightNullability() throws SQLException {
    assertThat( "ResultSetMetaData.column...Null... nullability code",
                rowsMetadata.isNullable( 7 ), equalTo( columnNullable ) );
  }


  ////////////////////////////////////////////////////////////
  // #8: BUFFER_LENGTH:
  // - JDBC:   "8. ... is not used"
  // - Drill:
  // - (Meta):

  // Since "unused," check only certain meta-metadata.

  @Test
  public void test_BUFFER_LENGTH_isAtRightPosition() throws SQLException {
    assertThat( rowsMetadata.getColumnLabel( 8 ), equalTo( "BUFFER_LENGTH" ) );
  }

  // No specific value or even type to check for.

  @Test
  public void test_BUFFER_LENGTH_hasSameNameAndLabel() throws SQLException {
    assertThat( rowsMetadata.getColumnName( 8 ), equalTo( "BUFFER_LENGTH" ) );
  }


  ////////////////////////////////////////////////////////////
  // #9: DECIMAL_DIGITS:
  // - JDBC:  "9. ... int => the number of fractional digits. Null is
  //     returned for data types where DECIMAL_DIGITS is not applicable."
  //   - Resolve:  When exactly null?
  // - Drill:
  // - (Meta):  INTEGER(?); Nullable;

  @Test
  public void test_DECIMAL_DIGITS_isAtRightPosition() throws SQLException {
    assertThat( rowsMetadata.getColumnLabel( 9 ), equalTo( "DECIMAL_DIGITS" ) );
  }

  @Test
  public void test_DECIMAL_DIGITS_hasRightValue_optBOOLEAN() throws SQLException {
    final int value = mdrOptBOOLEAN.getInt( "DECIMAL_DIGITS" );
    assertThat( "wasNull() [after " + value + "]",
                mdrOptBOOLEAN.wasNull(), equalTo( true ) );
    assertThat( value, equalTo( 0 /* NULL */ ) );
  }


  @Test
  public void test_DECIMAL_DIGITS_hasRightValue_reqTINYINT() throws SQLException {
    final int value = mdrReqTINYINT.getInt( "DECIMAL_DIGITS" );
    assertThat( "wasNull() [after " + value + "]",
                mdrReqTINYINT.wasNull(), equalTo( false ) );
    assertThat( value, equalTo( 0 /* NULL */ ) );
  }

  @Test
  public void test_DECIMAL_DIGITS_hasRightValue_optSMALLINT() throws SQLException {
    final int value = mdrOptSMALLINT.getInt( "DECIMAL_DIGITS" );
    assertThat( "wasNull() [after " + value + "]",
                mdrOptSMALLINT.wasNull(), equalTo( false ) );
    assertThat( value, equalTo( 0 /* NULL */ ) );
  }

  @Test
  public void test_DECIMAL_DIGITS_hasRightValue_reqINTEGER() throws SQLException {
    final int value = mdrReqINTEGER.getInt( "DECIMAL_DIGITS" );
    assertThat( "wasNull() [after " + value + "]",
                mdrReqINTEGER.wasNull(), equalTo( false ) );
    assertThat( value, equalTo( 0 /* NULL */ ) );
  }

  @Test
  public void test_DECIMAL_DIGITS_hasRightValue_optBIGINT() throws SQLException {
    final int value = mdrOptBIGINT.getInt( "DECIMAL_DIGITS" );
    assertThat( "wasNull() [after " + value + "]",
                mdrOptBIGINT.wasNull(), equalTo( false ) );
    assertThat( value, equalTo( 0 /* NULL */ ) );
  }

  @Test
  public void test_DECIMAL_DIGITS_hasRightValue_optFLOAT() throws SQLException {
    assertThat( mdrOptFLOAT.getInt( "DECIMAL_DIGITS" ), equalTo( 7 ) );
  }

  @Test
  public void test_DECIMAL_DIGITS_hasRightValue_reqDOUBLE() throws SQLException {
    assertThat( mdrReqDOUBLE.getInt( "DECIMAL_DIGITS" ), equalTo( 15 ) );
  }

  @Test
  public void test_DECIMAL_DIGITS_hasRightValue_optREAL() throws SQLException {
    assertThat( mdrOptREAL.getInt( "DECIMAL_DIGITS" ), equalTo( 15 ) );
  }

  @Test
  public void test_DECIMAL_DIGITS_hasRightValue_reqDECIMAL_5_3() throws SQLException {
    assertThat( mdrReqDECIMAL_5_3.getInt( "DECIMAL_DIGITS" ), equalTo( 3 ) );
  }

  @Test
  public void test_DECIMAL_DIGITS_hasRightValue_reqVARCHAR_10() throws SQLException {
    final int value = mdrReqVARCHAR_10.getInt( "DECIMAL_DIGITS" );
    assertThat( "wasNull() [after " + value + "]",
                mdrReqVARCHAR_10.wasNull(), equalTo( true ) );
    assertThat( value, equalTo( 0 /* NULL */ ) );
  }

  @Test
  public void test_DECIMAL_DIGITS_hasRightValue_optVARCHAR() throws SQLException {
    final int value = mdrOptVARCHAR.getInt( "DECIMAL_DIGITS" );
    assertThat( "wasNull() [after " + value + "]",
                mdrOptVARCHAR.wasNull(), equalTo( true ) );
    assertThat( value, equalTo( 0 /* NULL */ ) );
  }

  @Test
  public void test_DECIMAL_DIGITS_hasRightValue_reqCHAR_5() throws SQLException {
    final int value = mdrReqCHAR_5.getInt( "DECIMAL_DIGITS" );
    assertThat( "wasNull() [after " + value + "]",
                mdrReqCHAR_5.wasNull(), equalTo( true ) );
    assertThat( value, equalTo( 0 /* NULL */ ) );
  }

  @Test
  public void test_DECIMAL_DIGITS_hasRightValue_optVARBINARY_16() throws SQLException {
    final int value = mdrOptVARBINARY_16.getInt( "DECIMAL_DIGITS" );
    assertThat( "wasNull() [after " + value + "]",
                mdrOptVARBINARY_16.wasNull(), equalTo( true ) );
    assertThat( value, equalTo( 0 /* NULL */ ) );
  }

  @Test
  public void test_DECIMAL_DIGITS_hasRightValue_optBINARY_1048576CHECK() throws SQLException {
    final int value = mdrOptBINARY_1048576.getInt( "DECIMAL_DIGITS" );
    assertThat( "wasNull() [after " + value + "]",
                mdrOptBINARY_1048576.wasNull(), equalTo( true ) );
    assertThat( value, equalTo( 0 /* NULL */ ) );
  }

  @Test
  public void test_DECIMAL_DIGITS_hasRightValue_reqDATE() throws SQLException {
    final int value = mdrReqDATE.getInt( "DECIMAL_DIGITS" );
    assertThat( "wasNull() [after " + value + "]",
                mdrReqDATE.wasNull(), equalTo( true ) );
    assertThat( value, equalTo( 0 /* NULL */ ) );
  }

  @Test
  public void test_DECIMAL_DIGITS_hasRightValue_optTIME() throws SQLException {
    assertThat( mdrOptTIME.getInt( "DECIMAL_DIGITS" ), equalTo( 0 ) );
  }

  @Ignore( "until resolved:  whether to implement TIME precision or drop test" )
  @Test
  public void test_DECIMAL_DIGITS_hasRightValue_optTIME_7() throws SQLException {
    assertThat( mdrOptTIME_7.getInt( "DECIMAL_DIGITS" ), equalTo( 7 ) );
  }

  @Ignore( "until resolved:  whether to implement TIME precision or drop test" )
  @Test
  public void test_DECIMAL_DIGITS_hasRightValue_optTIMESTAMP() throws SQLException {
    assertThat( mdrOptTIMESTAMP.getInt( "DECIMAL_DIGITS" ), equalTo( 0 ) );
  }

  @Ignore( "until fixed:  INTERVAL metadata in INFORMATION_SCHEMA (DRILL-2531)" )
  @Test
  public void test_DECIMAL_DIGITS_hasRightValue_optINTERVAL_HM() throws SQLException {
    assertThat( mdrOptINTERVAL_H_S3.getInt( "DECIMAL_DIGITS" ), equalTo( 3 ) );
  }

  @Test
  public void test_DECIMAL_DIGITS_hasRightValue_optINTERVAL_Y3() throws SQLException {
    assertThat( mdrOptINTERVAL_Y4.getInt( "DECIMAL_DIGITS" ), equalTo( 0 ) );
  }

  @Test
  @Ignore("Enable once we have a test plugin which supports all the needed types.")
  public void test_DECIMAL_DIGITS_hasRightValue_tdbARRAY() throws SQLException {
    final int value = mdrReqARRAY.getInt( "DECIMAL_DIGITS" );
    assertThat( "wasNull() [after " + value + "]",
                mdrReqARRAY.wasNull(), equalTo( true ) ); // TODO:  Confirm.
    assertThat( value, equalTo( 0 /* NULL */ ) );
  }

  @Test
  @Ignore("Enable once we have a test plugin which supports all the needed types.")
  public void test_DECIMAL_DIGITS_hasRightValue_tbdMAP() throws SQLException {
    final int value = mdrReqMAP.getInt( "DECIMAL_DIGITS" );
    assertThat( "wasNull() [after " + value + "]",
                mdrReqMAP.wasNull(), equalTo( true ) ); // TODO:  Confirm.
    assertThat( value, equalTo( 0 /* NULL */ ) );
  }

  @Test
  @Ignore("Enable once we have a test plugin which supports all the needed types.")
  public void test_DECIMAL_DIGITS_hasRightValue_tbdSTRUCT() throws SQLException {
    final int value = testRowSTRUCT.getInt( "DECIMAL_DIGITS" );
    assertThat( "wasNull() [after " + value + "]",
                testRowSTRUCT.wasNull(), equalTo( true ) ); // TODO:  Confirm.
    assertThat( value, equalTo( 0 /* NULL */ ) );
  }

  @Test
  @Ignore("Enable once we have a test plugin which supports all the needed types.")
  public void test_DECIMAL_DIGITS_hasRightValue_tbdUnion() throws SQLException {
    final int value = testRowUnion.getInt( "DECIMAL_DIGITS" );
    assertThat( "wasNull() [after " + value + "]",
                testRowUnion.wasNull(), equalTo( true ) ); // TODO:  Confirm.
    assertThat( value, equalTo( 0 /* NULL */ ) );
  }

  @Test
  public void test_DECIMAL_DIGITS_hasSameNameAndLabel() throws SQLException {
    assertThat( rowsMetadata.getColumnName( 9 ), equalTo( "DECIMAL_DIGITS" ) );
  }

  @Test
  public void test_DECIMAL_DIGITS_hasRightTypeString() throws SQLException {
    assertThat( rowsMetadata.getColumnTypeName( 9 ), equalTo( "INTEGER" ) );
  }

  @Test
  public void test_DECIMAL_DIGITS_hasRightTypeCode() throws SQLException {
    assertThat( rowsMetadata.getColumnType( 9 ), equalTo( Types.INTEGER ) );
  }

  @Ignore( "until fixed (\"none\" -> right class name) (DRILL-2137)" )
  @Test
  public void test_DECIMAL_DIGITS_hasRightClass() throws SQLException {
    // TODO:  Confirm that this "java.lang.Integer" is correct:
    assertThat( rowsMetadata.getColumnClassName( 9 ), equalTo( Integer.class.getName() ) );
  }

  // (See to-do note near top of file about reviewing nullability.)
  @Test
  public void test_DECIMAL_DIGITS_hasRightNullability() throws SQLException {
    assertThat( "ResultSetMetaData.column...Null... nullability code",
                rowsMetadata.isNullable( 9 ), equalTo( columnNullable ) );
  }


  ////////////////////////////////////////////////////////////
  // #10: NUM_PREC_RADIX:
  // - JDBC:  "10. ... int => Radix (typically either 10 or 2)"
  //   - Seems should be null for non-numeric, but unclear.
  // - Drill:  ?
  // - (Meta): INTEGER?; Nullable?;
  //
  // Note:  Some MS page says NUM_PREC_RADIX specifies the units (decimal digits
  // or binary bits COLUMN_SIZE, and is NULL for non-numeric columns.

  @Test
  public void test_NUM_PREC_RADIX_isAtRightPosition() throws SQLException {
    assertThat( rowsMetadata.getColumnLabel( 10 ), equalTo( "NUM_PREC_RADIX" ) );
  }

  @Test
  public void test_NUM_PREC_RADIX_hasRightValue_optBOOLEAN() throws SQLException {
    final int value = mdrOptBOOLEAN.getInt( "NUM_PREC_RADIX" );
    assertThat( "wasNull() [after " + value + "]",
                mdrOptBOOLEAN.wasNull(), equalTo( true ) );
    assertThat( value, equalTo( 0 /* NULL */ ) );
  }

  @Test
  public void test_NUM_PREC_RADIX_hasRightValue_reqTINYINT() throws SQLException {
    assertThat( mdrReqTINYINT.getInt( "NUM_PREC_RADIX" ), equalTo( 10 ) );
  }

  @Test
  public void test_NUM_PREC_RADIX_hasRightValue_optSMALLINT() throws SQLException {
    assertThat( mdrOptSMALLINT.getInt( "NUM_PREC_RADIX" ), equalTo( 10 ) );
  }

  @Test
  public void test_NUM_PREC_RADIX_hasRightValue_reqINTEGER() throws SQLException {
    assertThat( mdrReqINTEGER.getInt( "NUM_PREC_RADIX" ), equalTo( 10 ) );
  }

  @Test
  public void test_NUM_PREC_RADIX_hasRightValue_optBIGINT() throws SQLException {
    assertThat( mdrOptBIGINT.getInt( "NUM_PREC_RADIX" ), equalTo( 10 ) );
  }

  @Test
  public void test_NUM_PREC_RADIX_hasRightValue_optFLOAT() throws SQLException {
    assertThat( mdrOptFLOAT.getInt( "NUM_PREC_RADIX" ), equalTo( 10 ) );
  }

  @Test
  public void test_NUM_PREC_RADIX_hasRightValue_reqDOUBLE() throws SQLException {
    assertThat( mdrReqDOUBLE.getInt( "NUM_PREC_RADIX" ), equalTo( 10 ) );
  }

  @Test
  public void test_NUM_PREC_RADIX_hasRightValue_optREAL() throws SQLException {
    assertThat( mdrOptREAL.getInt( "NUM_PREC_RADIX" ), equalTo( 10 ) );
  }

  @Test
  public void test_NUM_PREC_RADIX_hasRightValue_reqDECIMAL_5_3() throws SQLException {
    assertThat( mdrReqDECIMAL_5_3.getInt( "NUM_PREC_RADIX" ), equalTo( 10 ) );
  }

  @Test
  public void test_NUM_PREC_RADIX_hasRightValue_reqVARCHAR_10() throws SQLException {
    final int value = mdrReqVARCHAR_10.getInt( "NUM_PREC_RADIX" );
    assertThat( "wasNull() [after " + value + "]",
                mdrReqVARCHAR_10.wasNull(), equalTo( true ) );
    assertThat( value, equalTo( 0 /* NULL */ ) );
  }

  @Test
  public void test_NUM_PREC_RADIX_hasRightValue_optVARCHAR() throws SQLException {
    final int value = mdrOptVARCHAR.getInt( "NUM_PREC_RADIX" );
    assertThat( "wasNull() [after " + value + "]",
                mdrOptVARCHAR.wasNull(), equalTo( true ) );
    assertThat( value, equalTo( 0 /* NULL */ ) );
  }

  @Test
  public void test_NUM_PREC_RADIX_hasRightValue_reqCHAR_5() throws SQLException {
    final int value = mdrReqCHAR_5.getInt( "NUM_PREC_RADIX" );
    assertThat( "wasNull() [after " + value + "]",
                mdrReqCHAR_5.wasNull(), equalTo( true ) );
    assertThat( value, equalTo( 0 /* NULL */ ) );
  }

  @Test
  public void test_NUM_PREC_RADIX_hasRightValue_optVARBINARY_16() throws SQLException {
    final int value = mdrOptVARBINARY_16.getInt( "NUM_PREC_RADIX" );
    assertThat( "wasNull() [after " + value + "]",
                mdrOptVARBINARY_16.wasNull(), equalTo( true ) );
    assertThat( value, equalTo( 0 /* NULL */ ) );
  }

  @Test
  public void test_NUM_PREC_RADIX_hasRightValue_optBINARY_1048576CHECK() throws SQLException {
    final int value = mdrOptBINARY_1048576.getInt( "NUM_PREC_RADIX" );
    assertThat( "wasNull() [after " + value + "]",
                mdrOptBINARY_1048576.wasNull(), equalTo( true ) );
    assertThat( value, equalTo( 0 /* NULL */ ) );
  }

  @Test
  public void test_NUM_PREC_RADIX_hasRightValue_reqDATE() throws SQLException {
    final int value = mdrReqDATE.getInt( "NUM_PREC_RADIX" );
    assertThat( "wasNull() [after " + value + "]",
                mdrReqDATE.wasNull(), equalTo( true ) );
    assertThat( value, equalTo( 0 /* NULL */ ) );
  }

  @Ignore( "until resolved:  expected value" )
  @Test
  public void test_NUM_PREC_RADIX_hasRightValue_optTIME() throws SQLException {
    assertThat( mdrOptTIME.getInt( "NUM_PREC_RADIX" ), equalTo( 10 /* NULL */ ) );
    // To-do:  Determine which.
    final int value = mdrOptTIME.getInt( "NUM_PREC_RADIX" );
    assertThat( "wasNull() [after " + value + "]",
                mdrOptTIME.wasNull(), equalTo( true ) );
    assertThat( value, equalTo( 0 /* NULL */ ) );
  }

  @Ignore( "until resolved:  expected value" )
  @Test
  public void test_NUM_PREC_RADIX_hasRightValue_optTIME_7() throws SQLException {
    assertThat( mdrOptTIME_7.getInt( "NUM_PREC_RADIX" ), equalTo( 10 ) );
    // To-do:  Determine which.
    final int value = mdrOptTIME_7.getInt( "NUM_PREC_RADIX" );
    assertThat( "wasNull() [after " + value + "]",
                mdrOptTIME_7.wasNull(), equalTo( true ) );
    assertThat( value, equalTo( 0 /* NULL */ ) );
  }

  @Ignore( "until resolved:  expected value" )
  @Test
  public void test_NUM_PREC_RADIX_hasRightValue_optTIMESTAMP() throws SQLException {
    assertThat( mdrOptTIMESTAMP.getInt( "NUM_PREC_RADIX" ), equalTo( 10 ) );
    // To-do:  Determine which.
    final int value = mdrOptTIMESTAMP.getInt( "NUM_PREC_RADIX" );
    assertThat( "wasNull() [after " + value + "]",
                 mdrOptTIMESTAMP.wasNull(), equalTo( true ) );
    assertThat( value, equalTo( 0 /* NULL */ ) );
  }

  @Test
  public void test_NUM_PREC_RADIX_hasRightValue_optINTERVAL_HM() throws SQLException {
    assertThat( mdrOptINTERVAL_H_S3.getInt( "NUM_PREC_RADIX" ), equalTo( 10 ) );
  }

  @Test
  public void test_NUM_PREC_RADIX_hasRightValue_optINTERVAL_Y3() throws SQLException {
    assertThat( mdrOptINTERVAL_Y4.getInt( "NUM_PREC_RADIX" ), equalTo( 10 ) );
  }

  @Test
  @Ignore("Enable once we have a test plugin which supports all the needed types.")
  public void test_NUM_PREC_RADIX_hasRightValue_tdbARRAY() throws SQLException {
    final int value = mdrReqARRAY.getInt( "NUM_PREC_RADIX" );
    assertThat( "wasNull() [after " + value + "]",
                mdrReqARRAY.wasNull(), equalTo( true ) ); // TODO:  Confirm.
    assertThat( value, equalTo( 0 /* NULL */ ) );
  }

  @Test
  @Ignore("Enable once we have a test plugin which supports all the needed types.")
  public void test_NUM_PREC_RADIX_hasRightValue_tbdMAP() throws SQLException {
    final int value = mdrReqMAP.getInt( "NUM_PREC_RADIX" );
    assertThat( "wasNull() [after " + value + "]",
                mdrReqMAP.wasNull(), equalTo( true ) ); // TODO:  Confirm.
    assertThat( value, equalTo( 0 /* NULL */ ) );
  }

  @Test
  @Ignore("Enable once we have a test plugin which supports all the needed types.")
  public void test_NUM_PREC_RADIX_hasRightValue_tbdSTRUCT() throws SQLException {
    final int value = testRowSTRUCT.getInt( "NUM_PREC_RADIX" );
    assertThat( "wasNull() [after " + value + "]",
                testRowSTRUCT.wasNull(), equalTo( true ) ); // TODO:  Confirm.
    assertThat( value, equalTo( 0 /* NULL */ ) );
  }

  @Test
  @Ignore("Enable once we have a test plugin which supports all the needed types.")
  public void test_NUM_PREC_RADIX_hasRightValue_tbdUnion() throws SQLException {
    final int value = testRowUnion.getInt( "NUM_PREC_RADIX" );
    assertThat( "wasNull() [after " + value + "]",
                testRowUnion.wasNull(), equalTo( true ) ); // TODO:  Confirm.
    assertThat( value, equalTo( 0 /* NULL */ ) );
  }

  @Test
  public void test_NUM_PREC_RADIX_hasSameNameAndLabel() throws SQLException {
    assertThat( rowsMetadata.getColumnName( 10 ), equalTo( "NUM_PREC_RADIX" ) );
  }

  @Test
  public void test_NUM_PREC_RADIX_hasRightTypeString() throws SQLException {
    assertThat( rowsMetadata.getColumnTypeName( 10 ), equalTo( "INTEGER" ) );
  }

  @Test
  public void test_NUM_PREC_RADIX_hasRightTypeCode() throws SQLException {
    assertThat( rowsMetadata.getColumnType( 10 ), equalTo( Types.INTEGER ) );
  }

  @Ignore( "until fixed (\"none\" -> right class name) (DRILL-2137)" )
  @Test
  public void test_NUM_PREC_RADIX_hasRightClass() throws SQLException {
    // TODO:  Confirm that this "java.lang.Integer" is correct:
    assertThat( rowsMetadata.getColumnClassName( 10 ), equalTo( Integer.class.getName() ) );
  }

  // (See to-do note near top of file about reviewing nullability.)
  @Test
  public void test_NUM_PREC_RADIX_hasRightNullability() throws SQLException {
    assertThat( "ResultSetMetaData.column...Null... nullability code",
                rowsMetadata.isNullable( 10 ), equalTo( columnNullable ) );
  }


  ////////////////////////////////////////////////////////////
  // #11: NULLABLE:
  // - JDBC:  "11. ... int => is NULL allowed.
  //     columnNoNulls - might not allow NULL values
  //     columnNullable - definitely allows NULL values
  //     columnNullableUnknown - nullability unknown"
  // - Drill:
  // - (Meta): INTEGER(?); Non-nullable(?).

  // (See to-do note near top of file about reviewing nullability.)
  @Test
  public void test_NULLABLE_isAtRightPosition() throws SQLException {
    assertThat( rowsMetadata.getColumnLabel( 11 ), equalTo( "NULLABLE" ) );
  }

  // (See to-do note near top of file about reviewing nullability.)
  @Test
  public void test_NULLABLE_hasRightValue_optBOOLEAN() throws SQLException {
    // To-do:  CHECK:  Why columnNullableUnknown, when seemingly known nullable?
    assertThat( "ResultSetMetaData.column...Null... nullability code",
                mdrOptBOOLEAN.getInt( "NULLABLE" ), equalTo( columnNullable ) );
  }

  // (See to-do note near top of file about reviewing nullability.)
  @Test
  public void test_NULLABLE_hasRightValue_reqTINYINT() throws SQLException {
    // To-do:  CHECK:  Why columnNullableUnknown, when seemingly known non-nullable?
    assertThat( "ResultSetMetaData.column...Null... nullability code",
                mdrReqTINYINT.getInt( "NULLABLE" ), equalTo( columnNoNulls ) );
  }

  // (See to-do note near top of file about reviewing nullability.)
  @Test
  public void test_NULLABLE_hasRightValue_optSMALLINT() throws SQLException {
    // To-do:  CHECK:  Why columnNullableUnknown, when seemingly known  nullable?
    assertThat( "ResultSetMetaData.column...Null... nullability code",
                mdrOptSMALLINT.getInt( "NULLABLE" ), equalTo( columnNullable ) );
  }

  // (See to-do note near top of file about reviewing nullability.)
  @Test
  public void test_NULLABLE_hasRightValue_optBIGINT() throws SQLException {
    // To-do:  CHECK:  Why columnNullableUnknown, when seemingly known  nullable?
    assertThat( "ResultSetMetaData.column...Null... nullability code",
                mdrOptBIGINT.getInt( "NULLABLE" ), equalTo( columnNullable ) );
  }

  // (See to-do note near top of file about reviewing nullability.)
  @Test
  public void test_NULLABLE_hasRightValue_optFLOAT() throws SQLException {
    // To-do:  CHECK:  Why columnNullableUnknown, when seemingly known nullable?
    assertThat( "ResultSetMetaData.column...Null... nullability code",
                mdrOptFLOAT.getInt( "NULLABLE" ), equalTo( columnNullable ) );
  }

  // (See to-do note near top of file about reviewing nullability.)
  @Test
  public void test_NULLABLE_hasRightValue_reqDOUBLE() throws SQLException {
    // To-do:  CHECK:  Why columnNullableUnknown, when seemingly known non-nullable?
    assertThat( "ResultSetMetaData.column...Null... nullability code",
                mdrReqDOUBLE.getInt( "NULLABLE" ), equalTo( columnNoNulls ) );
  }

  // (See to-do note near top of file about reviewing nullability.)
  @Test
  public void test_NULLABLE_hasRightValue_optREAL() throws SQLException {
    // To-do:  CHECK:  Why columnNullableUnknown, when seemingly known nullable?
    assertThat( "ResultSetMetaData.column...Null... nullability code",
                mdrOptREAL.getInt( "NULLABLE" ), equalTo( columnNullable ) );
  }

  // (See to-do note near top of file about reviewing nullability.)
  @Test
  public void test_NULLABLE_hasRightValue_reqINTEGER() throws SQLException {
    // To-do:  CHECK:  Why columnNullableUnknown, when seemingly known non-nullable?
    assertThat( "ResultSetMetaData.column...Null... nullability code",
                mdrReqINTEGER.getInt( "NULLABLE" ), equalTo( columnNoNulls ) );
  }

  // (See to-do note near top of file about reviewing nullability.)
  @Test
  public void test_NULLABLE_hasRightValue_reqDECIMAL_5_3() throws SQLException {
    // To-do:  CHECK:  Why columnNullableUnknown, when seemingly known non-nullable?
    assertThat( "ResultSetMetaData.column...Null... nullability code",
                mdrReqDECIMAL_5_3.getInt( "NULLABLE" ), equalTo( columnNoNulls ) );
  }

  // (See to-do note near top of file about reviewing nullability.)
  @Test
  public void test_NULLABLE_hasRightValue_reqVARCHAR_10() throws SQLException {
    // To-do:  CHECK:  Why columnNullableUnknown, when seemingly known non-nullable?
    assertThat( "ResultSetMetaData.column...Null... nullability code",
                mdrReqVARCHAR_10.getInt( "NULLABLE" ), equalTo( columnNoNulls ) );
  }

  // (See to-do note near top of file about reviewing nullability.)
  @Test
  public void test_NULLABLE_hasRightValue_optVARCHAR() throws SQLException {
    // To-do:  CHECK:  Why columnNullableUnknown, when seemingly known nullable?
    assertThat( "ResultSetMetaData.column...Null... nullability code",
                mdrOptVARCHAR.getInt( "NULLABLE" ), equalTo( columnNullable ) );
  }

  // (See to-do note near top of file about reviewing nullability.)
  @Test
  public void test_NULLABLE_hasRightValue_reqCHAR_5() throws SQLException {
    // To-do:  CHECK:  Why columnNullableUnknown, when seemingly known non-nullable?
    assertThat( "ResultSetMetaData.column...Null... nullability code",
                mdrReqCHAR_5.getInt( "NULLABLE" ), equalTo( columnNoNulls ) );
  }

  // (See to-do note near top of file about reviewing nullability.)
  @Test
  public void test_NULLABLE_hasRightValue_optVARBINARY_16() throws SQLException {
    // To-do:  CHECK:  Why columnNullableUnknown, when seemingly known nullable?
    assertThat( "ResultSetMetaData.column...Null... nullability code",
                mdrOptVARBINARY_16.getInt( "NULLABLE" ), equalTo( columnNullable ) );
  }

  // (See to-do note near top of file about reviewing nullability.)
  @Test
  public void test_NULLABLE_hasRightValue_optBINARY_1048576CHECK() throws SQLException {
    // To-do:  CHECK:  Why columnNullableUnknown, when seemingly known nullable?
    assertThat( "ResultSetMetaData.column...Null... nullability code",
                mdrOptBINARY_1048576.getInt( "NULLABLE" ), equalTo( columnNullable ) );
  }

  // (See to-do note near top of file about reviewing nullability.)
  @Test
  public void test_NULLABLE_hasRightValue_reqDATE() throws SQLException {
    // To-do:  CHECK:  Why columnNullableUnknown, when seemingly known non-nullable?
    assertThat( "ResultSetMetaData.column...Null... nullability code",
                mdrReqDATE.getInt( "NULLABLE" ), equalTo( columnNoNulls ) );
  }

  // (See to-do note near top of file about reviewing nullability.)
  @Test
  public void test_NULLABLE_hasRightValue_optTIME() throws SQLException {
    // To-do:  CHECK:  Why columnNullableUnknown, when seemingly known nullable?
    assertThat( "ResultSetMetaData.column...Null... nullability code",
                mdrOptTIME.getInt( "NULLABLE" ), equalTo( columnNullable ) );
  }

  // (See to-do note near top of file about reviewing nullability.)
  @Test
  public void test_NULLABLE_hasRightValue_optTIME_7() throws SQLException {
    // To-do:  CHECK:  Why columnNullableUnknown, when seemingly known nullable?
    assertThat( "ResultSetMetaData.column...Null... nullability code",
                mdrOptTIME_7.getInt( "NULLABLE" ), equalTo( columnNullable ) );
  }

  // (See to-do note near top of file about reviewing nullability.)
  @Test
  public void test_NULLABLE_hasRightValue_optTIMESTAMP() throws SQLException {
    // To-do:  CHECK:  Why columnNullableUnknown, when seemingly known nullable?
    assertThat( "ResultSetMetaData.column...Null... nullability code",
                mdrOptTIMESTAMP.getInt( "NULLABLE" ), equalTo( columnNullable ) );
  }

  // (See to-do note near top of file about reviewing nullability.)
  @Test
  public void test_NULLABLE_hasRightValue_optINTERVAL_HM() throws SQLException {
    // To-do:  CHECK:  Why columnNullableUnknown, when seemingly known nullable?
    assertThat( "ResultSetMetaData.column...Null... nullability code",
                mdrOptINTERVAL_H_S3.getInt( "NULLABLE" ), equalTo( columnNullable ) );
  }

  // (See to-do note near top of file about reviewing nullability.)
  @Test
  public void test_NULLABLE_hasRightValue_optINTERVAL_Y3() throws SQLException {
    // To-do:  CHECK:  Why columnNullableUnknown, when seemingly known nullable?
    assertThat( "ResultSetMetaData.column...Null... nullability code",
                mdrOptINTERVAL_Y4.getInt( "NULLABLE" ), equalTo( columnNullable ) );
  }

  // (See to-do note near top of file about reviewing nullability.)
  @Test
  @Ignore("Enable once we have a test plugin which supports all the needed types.")
  public void test_NULLABLE_hasRightValue_tdbARRAY() throws SQLException {
    assertThat( mdrReqARRAY.getInt( "NULLABLE" ), equalTo( columnNoNulls ) );
  }

  // (See to-do note near top of file about reviewing nullability.)
  @Test
  @Ignore("Enable once we have a test plugin which supports all the needed types.")
  public void test_NULLABLE_hasRightValue_tbdMAP() throws SQLException {
    assertThat( mdrReqMAP.getInt( "NULLABLE" ), equalTo( columnNoNulls ) );
  }

  @Ignore( "until resolved:  any requirement on nullability (DRILL-2420?)" )
  @Test
  public void test_NULLABLE_hasRightValue_tbdSTRUCT() throws SQLException {
    assertThat( testRowSTRUCT.getInt( "NULLABLE" ), equalTo( columnNullable ) );
    // To-do:  Determine which.
    assertThat( testRowSTRUCT.getInt( "NULLABLE" ), equalTo( columnNoNulls ) );
    // To-do:  Determine which.
    assertThat( testRowSTRUCT.getInt( "NULLABLE" ), equalTo( columnNullableUnknown ) );
  }

  @Ignore( "until resolved:  any requirement on nullability (DRILL-2420?)" )
  @Test
  public void test_NULLABLE_hasRightValue_tbdUnion() throws SQLException {
    assertThat( "ResultSetMetaData.column...Null... nullability code",
                testRowUnion.getInt( "NULLABLE" ), equalTo( columnNullable ) );
    // To-do:  Determine which.
    assertThat( "ResultSetMetaData.column...Null... nullability code",
                testRowUnion.getInt( "NULLABLE" ), equalTo( columnNoNulls ) );
    // To-do:  Determine which.
    assertThat( "ResultSetMetaData.column...Null... nullability code",
                testRowUnion.getInt( "NULLABLE" ), equalTo( columnNullableUnknown ) );
  }

  // (See to-do note near top of file about reviewing nullability.)
  @Test
  public void test_NULLABLE_hasSameNameAndLabel() throws SQLException {
    assertThat( rowsMetadata.getColumnName( 11 ), equalTo( "NULLABLE" ) );
  }

  // (See to-do note near top of file about reviewing nullability.)
  @Test
  public void test_NULLABLE_hasRightTypeString() throws SQLException {
    assertThat( rowsMetadata.getColumnTypeName( 11 ), equalTo( "INTEGER" ) );  // TODO:  Confirm.
  }

  // (See to-do note near top of file about reviewing nullability.)
  @Test
  public void test_NULLABLE_hasRightTypeCode() throws SQLException {
    assertThat( rowsMetadata.getColumnType( 11 ), equalTo( Types.INTEGER ) );  // TODO:  Confirm.
  }

  @Ignore( "until fixed (\"none\" -> right class name) (DRILL-2137)" )
  @Test
  public void test_NULLABLE_hasRightClass() throws SQLException {
    // TODO:  Confirm that this "java.lang.Integer" is correct:
    assertThat( rowsMetadata.getColumnClassName( 11 ), equalTo( Integer.class.getName() ) );
  }

  // (See to-do note near top of file about reviewing nullability.)
  @Test
  public void test_NULLABLE_hasRightNullability() throws SQLException {
    assertThat( "ResultSetMetaData.column...Null... nullability code",
                rowsMetadata.isNullable( 11 ), equalTo( columnNoNulls ) );
  }


  ////////////////////////////////////////////////////////////
  // #12: REMARKS:
  // - JDBC:  "12. ... String => comment describing column (may be null)"
  // - Drill: none, so always null
  // - (Meta): VARCHAR (NVARCHAR?); Nullable;

  @Test
  public void test_REMARKS_isAtRightPosition() throws SQLException {
    assertThat( rowsMetadata.getColumnLabel( 12 ), equalTo( "REMARKS" ) );
  }

  @Test
  public void test_REMARKS_hasRightValue_optBOOLEAN() throws SQLException {
    assertThat( mdrOptBOOLEAN.getString( "REMARKS" ), nullValue() );
  }

  @Test
  public void test_REMARKS_hasSameNameAndLabel() throws SQLException {
    assertThat( rowsMetadata.getColumnName( 12 ), equalTo( "REMARKS" ) );
  }

  @Test
  public void test_REMARKS_hasRightTypeString() throws SQLException {
    assertThat( rowsMetadata.getColumnTypeName( 12 ), equalTo( "VARCHAR" ) );
  }

  @Test
  public void test_REMARKS_hasRightTypeCode() throws SQLException {
    assertThat( rowsMetadata.getColumnType( 12 ), equalTo( Types.VARCHAR ) );
  }

  @Ignore( "until fixed (\"none\" -> right class name) (DRILL-2137)" )
  @Test
  public void test_REMARKS_hasRightClass() throws SQLException {
    // TODO:  Confirm that this "java.lang.String" is correct:
    assertThat( rowsMetadata.getColumnClassName( 12 ), equalTo( String.class.getName() ) );
  }

  // (See to-do note near top of file about reviewing nullability.)
  @Test
  public void test_REMARKS_hasRightNullability() throws SQLException {
    assertThat( "ResultSetMetaData.column...Null... nullability code",
                rowsMetadata.isNullable( 12 ), equalTo( columnNullable ) );
  }


  ////////////////////////////////////////////////////////////
  // #13: COLUMN_DEF:
  // - JDBC:  "13. ... String => default value for the column, which should be
  //     interpreted as a string when the value is enclosed in single quotes
  //     (may be null)"
  // - Drill:  no real default values, right?
  // - (Meta): VARCHAR (NVARCHAR?);  Nullable;

  @Test
  public void test_COLUMN_DEF_isAtRightPosition() throws SQLException {
    assertThat( rowsMetadata.getColumnLabel( 13 ), equalTo( "COLUMN_DEF" ) );
  }

  @Test
  public void test_COLUMN_DEF_hasRightValue_optBOOLEAN() throws SQLException {
    assertThat( mdrOptBOOLEAN.getString( "COLUMN_DEF" ), nullValue() );
  }

  @Test
  public void test_COLUMN_DEF_hasSameNameAndLabel() throws SQLException {
    assertThat( rowsMetadata.getColumnName( 13 ), equalTo( "COLUMN_DEF" ) );
  }

  @Test
  public void test_COLUMN_DEF_hasRightTypeString() throws SQLException {
    assertThat( rowsMetadata.getColumnTypeName( 13 ), equalTo( "VARCHAR" ) );
  }

  @Test
  public void test_COLUMN_DEF_hasRightTypeCode() throws SQLException {
    assertThat( rowsMetadata.getColumnType( 13 ), equalTo( Types.VARCHAR ) );
  }

  @Ignore( "until fixed (\"none\" -> right class name) (DRILL-2137)" )
  @Test
  public void test_COLUMN_DEF_hasRightClass() throws SQLException {
    // TODO:  Confirm that this "java.lang.String" is correct:
    assertThat( rowsMetadata.getColumnClassName( 13 ), equalTo( String.class.getName() ) );
  }

  // (See to-do note near top of file about reviewing nullability.)
  @Test
  public void test_COLUMN_DEF_hasRightNullability() throws SQLException {
    assertThat( "ResultSetMetaData.column...Null... nullability code",
                rowsMetadata.isNullable( 13 ), equalTo( columnNullable ) );
  }


  ////////////////////////////////////////////////////////////
  // #14: SQL_DATA_??TYPE:
  // - JDBC:  "14. ... int => unused"
  // - Drill:
  // - (Meta): INTEGER(?);

  // Since "unused," check only certain meta-metadata.

  @Test
  public void test_SQL_DATA_TYPE_isAtRightPosition() throws SQLException {
    assertThat( rowsMetadata.getColumnLabel( 14 ), equalTo( "SQL_DATA_TYPE" ) );
  }

  // No specific value to check for.

  @Test
  public void test_SQL_DATA_TYPE_hasSameNameAndLabel() throws SQLException {
    assertThat( rowsMetadata.getColumnName( 14 ), equalTo( "SQL_DATA_TYPE" ) );
  }

  @Test
  public void test_SQL_DATA_TYPE_hasRightTypeString() throws SQLException {
    assertThat( rowsMetadata.getColumnTypeName( 14 ), equalTo( "INTEGER" ) );
  }

  @Test
  public void test_SQL_DATA_TYPE_hasRightTypeCode() throws SQLException {
    assertThat( rowsMetadata.getColumnType( 14 ), equalTo( Types.INTEGER ) );
  }

  @Ignore( "until fixed (\"none\" -> right class name) (DRILL-2137)" )
  @Test
  public void test_SQL_DATA_TYPE_hasRightClass() throws SQLException {
    // TODO:  Confirm that this "java.lang.Integer" is correct:
    assertThat( rowsMetadata.getColumnClassName( 14 ), equalTo( Integer.class.getName() ) );
  }


  ////////////////////////////////////////////////////////////
  // #15: SQL_DATETIME_SUB:
  // - JDBC:  "15. ... int => unused"
  // - Drill:
  // - (Meta):  INTEGER(?);

  // Since "unused," check only certain meta-metadata.

  @Test
  public void test_SQL_DATETIME_SUB_isAtRightPosition() throws SQLException {
    assertThat( rowsMetadata.getColumnLabel( 15 ), equalTo( "SQL_DATETIME_SUB" ) );
  }

  // No specific value to check for.

  @Test
  public void test_SQL_DATETIME_SUB_hasSameNameAndLabel() throws SQLException {
    assertThat( rowsMetadata.getColumnName( 15 ), equalTo( "SQL_DATETIME_SUB" ) );
  }

  @Test
  public void test_SQL_DATETIME_SUB_hasRightTypeString() throws SQLException {
    assertThat( rowsMetadata.getColumnTypeName( 15 ), equalTo( "INTEGER" ) );  // TODO:  Confirm.
  }

  @Test
  public void test_SQL_DATETIME_SUB_hasRightTypeCode() throws SQLException {
    assertThat( rowsMetadata.getColumnType( 15 ), equalTo( Types.INTEGER ) );  // TODO:  Confirm.
  }

  @Ignore( "until fixed (\"none\" -> right class name) (DRILL-2137)" )
  @Test
  public void test_SQL_DATETIME_SUB_hasRightClass() throws SQLException {
    // TODO:  Confirm that this "java.lang.Integer" is correct:
    assertThat( rowsMetadata.getColumnClassName( 15 ), equalTo( Integer.class.getName() ) );
  }


  ////////////////////////////////////////////////////////////
  // #16: CHAR_OCTET_LENGTH:
  // - JDBC:  "16. ... int => for char types the maximum number of bytes
  //     in the column"
  //   - apparently should be null for non-character types
  // - Drill:
  // - (Meta): INTEGER(?); Nullable(?);

  @Test
  public void test_CHAR_OCTET_LENGTH_isAtRightPosition() throws SQLException {
    assertThat( rowsMetadata.getColumnLabel( 16 ), equalTo( "CHAR_OCTET_LENGTH" ) );
  }

  @Test
  public void test_CHAR_OCTET_LENGTH_hasRightValue_optBOOLEAN() throws SQLException {
    final int value = mdrOptBOOLEAN.getInt( "CHAR_OCTET_LENGTH" );
    assertThat( "wasNull() [after " + value + "]",
                mdrOptBOOLEAN.wasNull(), equalTo( true ) );
    assertThat( value, equalTo( 0 /* NULL */ ) );
  }


  @Test
  public void test_CHAR_OCTET_LENGTH_hasRightValue_reqTINYINT() throws SQLException {
    final int value = mdrReqTINYINT.getInt( "CHAR_OCTET_LENGTH" );
    assertThat( "wasNull() [after " + value + "]",
                mdrReqTINYINT.wasNull(), equalTo( true ) );
    assertThat( value, equalTo( 0 /* NULL */ ) );
  }

  @Test
  public void test_CHAR_OCTET_LENGTH_hasRightValue_optSMALLINT() throws SQLException {
    final int value = mdrOptSMALLINT.getInt( "CHAR_OCTET_LENGTH" );
    assertThat( "wasNull() [after " + value + "]",
                mdrOptSMALLINT.wasNull(), equalTo( true ) );
    assertThat( value, equalTo( 0 /* NULL */ ) );
  }

  @Test
  public void test_CHAR_OCTET_LENGTH_hasRightValue_reqINTEGER() throws SQLException {
    final int value = mdrReqINTEGER.getInt( "CHAR_OCTET_LENGTH" );
    assertThat( "wasNull() [after " + value + "]",
                mdrReqINTEGER.wasNull(), equalTo( true ) );
    assertThat( value, equalTo( 0 /* NULL */ ) );
  }

  @Test
  public void test_CHAR_OCTET_LENGTH_hasRightValue_optBIGINT() throws SQLException {
    final int value = mdrOptREAL.getInt( "CHAR_OCTET_LENGTH" );
    assertThat( "wasNull() [after " + value + "]",
                mdrOptREAL.wasNull(), equalTo( true ) );
    assertThat( value, equalTo( 0 /* NULL */ ) );
  }

  @Test
  public void test_CHAR_OCTET_LENGTH_hasRightValue_optFLOAT() throws SQLException {
    final int value = mdrOptFLOAT.getInt( "CHAR_OCTET_LENGTH" );
    assertThat( "wasNull() [after " + value + "]",
                mdrOptFLOAT.wasNull(), equalTo( true ) );
    assertThat( value, equalTo( 0 /* NULL */ ) );
  }

  @Test
  public void test_CHAR_OCTET_LENGTH_hasRightValue_reqDOUBLE() throws SQLException {
    final int value = mdrReqDOUBLE.getInt( "CHAR_OCTET_LENGTH" );
    assertThat( "wasNull() [after " + value + "]",
                mdrReqDOUBLE.wasNull(), equalTo( true ) );
    assertThat( value, equalTo( 0 /* NULL */ ) );
  }

  @Test
  public void test_CHAR_OCTET_LENGTH_hasRightValue_optREAL() throws SQLException {
    final int value = mdrOptREAL.getInt( "CHAR_OCTET_LENGTH" );
    assertThat( "wasNull() [after " + value + "]",
                mdrOptREAL.wasNull(), equalTo( true ) );
    assertThat( value, equalTo( 0 /* NULL */ ) );
  }

  @Test
  public void test_CHAR_OCTET_LENGTH_hasRightValue_reqDECIMAL_5_3() throws SQLException {
    final int value = mdrReqDECIMAL_5_3.getInt( "CHAR_OCTET_LENGTH" );
    assertThat( "wasNull() [after " + value + "]",
                mdrReqDECIMAL_5_3.wasNull(), equalTo( true ) );
    assertThat( value, equalTo( 0 /* NULL */ ) );
  }

  @Test
  public void test_CHAR_OCTET_LENGTH_hasRightValue_reqVARCHAR_10() throws SQLException {
    assertThat( mdrReqVARCHAR_10.getInt( "CHAR_OCTET_LENGTH" ),
                equalTo( 10   /* chars. */
                         * 4  /* max. UTF-8 bytes per char. */ ) );
  }

  @Test
  public void test_CHAR_OCTET_LENGTH_hasRightValue_optVARCHAR() throws SQLException {
    assertThat( mdrOptVARCHAR.getInt( "CHAR_OCTET_LENGTH" ),
                equalTo( 1    /* chars. (default of 1) */
                         * 4  /* max. UTF-8 bytes per char. */ ) );
  }

  @Test
  public void test_CHAR_OCTET_LENGTH_hasRightValue_reqCHAR_5() throws SQLException {
    assertThat( mdrReqCHAR_5.getInt( "CHAR_OCTET_LENGTH" ),
                equalTo( 5    /* chars. */
                         * 4  /* max. UTF-8 bytes per char. */ ) );
  }

  @Test
  public void test_CHAR_OCTET_LENGTH_hasRightValue_optVARBINARY_16() throws SQLException {
    final int value = mdrOptVARBINARY_16.getInt( "CHAR_OCTET_LENGTH" );
    assertThat( "wasNull() [after " + value + "]",
                mdrOptVARBINARY_16.wasNull(), equalTo( true ) );
    assertThat( value, equalTo( 0 /* NULL */ ) );
  }

  @Test
  public void test_CHAR_OCTET_LENGTH_hasRightValue_optBINARY_1048576CHECK() throws SQLException {
    final int value = mdrOptBINARY_1048576.getInt( "CHAR_OCTET_LENGTH" );
    assertThat( "wasNull() [after " + value + "]",
                mdrOptBINARY_1048576.wasNull(), equalTo( true ) );
    assertThat( value, equalTo( 0 /* NULL */ ) );
  }

  @Test
  public void test_CHAR_OCTET_LENGTH_hasRightValue_reqDATE() throws SQLException {
    final int value = mdrReqDATE.getInt( "CHAR_OCTET_LENGTH" );
    assertThat( "wasNull() [after " + value + "]",
                mdrReqDATE.wasNull(), equalTo( true ) );
    assertThat( value, equalTo( 0 /* NULL */ ) );
  }

  @Test
  public void test_CHAR_OCTET_LENGTH_hasRightValue_optTIME() throws SQLException {
    final int value = mdrOptTIME.getInt( "CHAR_OCTET_LENGTH" );
    assertThat( "wasNull() [after " + value + "]",
                mdrOptTIME.wasNull(), equalTo( true ) );
    assertThat( value, equalTo( 0 /* NULL */ ) );
  }

  @Test
  public void test_CHAR_OCTET_LENGTH_hasRightValue_optTIME_7() throws SQLException {
    final int value = mdrOptTIME_7.getInt( "CHAR_OCTET_LENGTH" );
    assertThat( "wasNull() [after " + value + "]",
                mdrOptTIME_7.wasNull(), equalTo( true ) );
    assertThat( value, equalTo( 0 /* NULL */ ) );
  }

  @Test
  public void test_CHAR_OCTET_LENGTH_hasRightValue_optTIMESTAMP() throws SQLException {
    final int value = mdrOptTIMESTAMP.getInt( "CHAR_OCTET_LENGTH" );
    assertThat( "wasNull() [after " + value + "]",
                mdrOptTIMESTAMP.wasNull(), equalTo( true ) );
    assertThat( value, equalTo( 0 /* NULL */ ) );
  }

  @Test
  public void test_CHAR_OCTET_LENGTH_hasRightValue_optINTERVAL_HM() throws SQLException {
    final int value = mdrOptINTERVAL_H_S3.getInt( "CHAR_OCTET_LENGTH" );
    assertThat( "wasNull() [after " + value + "]",
                mdrOptINTERVAL_H_S3.wasNull(), equalTo( true ) );
    assertThat( value, equalTo( 0 /* NULL */ ) );
  }

  @Test
  public void test_CHAR_OCTET_LENGTH_hasRightValue_optINTERVAL_Y3() throws SQLException {
    final int value = mdrOptINTERVAL_Y4.getInt( "CHAR_OCTET_LENGTH" );
    assertThat( "wasNull() [after " + value + "]",
                mdrOptINTERVAL_Y4.wasNull(), equalTo( true ) );
    assertThat( value, equalTo( 0 /* NULL */ ) );
  }

  @Test
  @Ignore("Enable once we have a test plugin which supports all the needed types.")
  public void test_CHAR_OCTET_LENGTH_hasRightValue_tdbARRAY() throws SQLException {
    final int value = mdrReqARRAY.getInt( "CHAR_OCTET_LENGTH" );
    assertThat( "wasNull() [after " + value + "]",
                mdrReqARRAY.wasNull(), equalTo( true ) );  // TODO:  Confirm.
    assertThat( value, equalTo( 0 /* NULL */ ) );
  }

  @Test
  @Ignore("Enable once we have a test plugin which supports all the needed types.")
  public void test_CHAR_OCTET_LENGTH_hasRightValue_tbdMAP() throws SQLException {
    final int value = mdrReqMAP.getInt( "CHAR_OCTET_LENGTH" );
    assertThat( "wasNull() [after " + value + "]",
                mdrReqMAP.wasNull(), equalTo( true ) );  // TODO:  Confirm.
    assertThat( value, equalTo( 0 /* NULL */ ) );
  }

  @Test
  @Ignore("Enable once we have a test plugin which supports all the needed types.")
  public void test_CHAR_OCTET_LENGTH_hasRightValue_tbdSTRUCT() throws SQLException {
    final int value = testRowSTRUCT.getInt( "CHAR_OCTET_LENGTH" );
    assertThat( "wasNull() [after " + value + "]",
                testRowSTRUCT.wasNull(), equalTo( true ) );  // TODO:  Confirm.
    assertThat( value, equalTo( 0 /* NULL */ ) );
  }

  @Test
  @Ignore("Enable once we have a test plugin which supports all the needed types.")
  public void test_CHAR_OCTET_LENGTH_hasRightValue_tbdUnion() throws SQLException {
    final int value = testRowUnion.getInt( "CHAR_OCTET_LENGTH" );
    assertThat( "wasNull() [after " + value + "]",
                testRowUnion.wasNull(), equalTo( true ) );  // TODO:  Confirm.
    assertThat( value, equalTo( 0 /* NULL */ ) );
  }

  @Test
  public void test_CHAR_OCTET_LENGTH_hasSameNameAndLabel() throws SQLException {
    assertThat( rowsMetadata.getColumnName( 16 ), equalTo( "CHAR_OCTET_LENGTH" ) );
  }

  @Test
  public void test_CHAR_OCTET_LENGTH_hasRightTypeString() throws SQLException {
    assertThat( rowsMetadata.getColumnTypeName( 16 ), equalTo( "INTEGER" ) );
  }

  @Test
  public void test_CHAR_OCTET_LENGTH_hasRightTypeCode() throws SQLException {
    assertThat( rowsMetadata.getColumnType( 16 ), equalTo( Types.INTEGER ) );
  }

  @Ignore( "until fixed (\"none\" -> right class name) (DRILL-2137)" )
  @Test
  public void test_CHAR_OCTET_LENGTH_hasRightClass() throws SQLException {
    // TODO:  Confirm that this "java.lang.Integer" is correct:
    assertThat( rowsMetadata.getColumnClassName( 16 ), equalTo( Integer.class.getName() ) );
  }

  // (See to-do note near top of file about reviewing nullability.)
  @Test
  public void test_CHAR_OCTET_LENGTH_hasRightNullability() throws SQLException {
    assertThat( "ResultSetMetaData.column...Null... nullability code",
                rowsMetadata.isNullable( 16 ), equalTo( columnNullable ) );
  }


  ////////////////////////////////////////////////////////////
  // #17: ORDINAL_POSITION:
  // - JDBC:  "17. ... int => index of column in table (starting at 1)"
  // - Drill:
  // - (Meta):  INTEGER(?); Non-nullable(?).

  @Test
  public void test_ORDINAL_POSITION_isAtRightPosition() throws SQLException {
    assertThat( rowsMetadata.getColumnLabel( 17 ), equalTo( "ORDINAL_POSITION" ) );
  }

  @Test
  public void test_ORDINAL_POSITION_hasRightValue_optBOOLEAN() throws SQLException {
    assertThat( mdrOptBOOLEAN.getInt( "ORDINAL_POSITION" ), equalTo( 1 ) );
  }

  @Test
  public void test_ORDINAL_POSITION_hasRightValue_reqTINYINT() throws SQLException {
    assertThat( mdrReqTINYINT.getInt( "ORDINAL_POSITION" ), equalTo( 2 ) );
  }

  @Test
  public void test_ORDINAL_POSITION_hasRightValue_optSMALLINT() throws SQLException {
    assertThat( mdrOptSMALLINT.getInt( "ORDINAL_POSITION" ), equalTo( 3 ) );
  }

  @Test
  public void test_ORDINAL_POSITION_hasRightValue_reqINTEGER() throws SQLException {
    assertThat( mdrReqINTEGER.getInt( "ORDINAL_POSITION" ), equalTo( 4 ) );
  }

  @Test
  public void test_ORDINAL_POSITION_hasRightValue_optBIGINT() throws SQLException {
    assertThat( mdrOptBIGINT.getInt( "ORDINAL_POSITION" ), equalTo( 5 ) );
  }

  @Test
  public void test_ORDINAL_POSITION_hasRightValue_optFLOAT() throws SQLException {
    assertThat( mdrOptFLOAT.getInt( "ORDINAL_POSITION" ), equalTo( 6 ) );
  }

  @Test
  public void test_ORDINAL_POSITION_hasRightValue_reqDOUBLE() throws SQLException {
    assertThat( mdrReqDOUBLE.getInt( "ORDINAL_POSITION" ), equalTo( 7 ) );
  }

  @Test
  public void test_ORDINAL_POSITION_hasRightValue_optREAL() throws SQLException {
    assertThat( mdrOptREAL.getInt( "ORDINAL_POSITION" ), equalTo( 8 ) );
  }

  @Test
  @Ignore("Enable once we have a test plugin which supports all the needed types.")
  public void test_ORDINAL_POSITION_hasRightValue_tdbARRAY() throws SQLException {
    assertThat( mdrReqARRAY.getInt( "ORDINAL_POSITION" ), equalTo( 14 ) );
  }

  @Test
  public void test_ORDINAL_POSITION_hasSameNameAndLabel() throws SQLException {
    assertThat( rowsMetadata.getColumnName( 17 ), equalTo( "ORDINAL_POSITION" ) );
  }

  @Test
  public void test_ORDINAL_POSITION_hasRightTypeString() throws SQLException {
    assertThat( rowsMetadata.getColumnTypeName( 17 ), equalTo( "INTEGER" ) );  // TODO:  Confirm.
  }

  @Test
  public void test_ORDINAL_POSITION_hasRightTypeCode() throws SQLException {
    assertThat( rowsMetadata.getColumnType( 17 ), equalTo( Types.INTEGER ) );  // TODO:  Confirm.
  }

  @Ignore( "until fixed (\"none\" -> right class name) (DRILL-2137)" )
  @Test
  public void test_ORDINAL_POSITION_hasRightClass() throws SQLException {
    // TODO:  Confirm that this "java.lang.Integer" is correct:
    assertThat( rowsMetadata.getColumnClassName( 17 ), equalTo( Integer.class.getName() ) );
  }

  // (See to-do note near top of file about reviewing nullability.)
  @Test
  public void test_ORDINAL_POSITION_hasRightNullability() throws SQLException {
    assertThat( "ResultSetMetaData.column...Null... nullability code",
                rowsMetadata.isNullable( 17 ), equalTo( columnNoNulls ) );
  }


  ////////////////////////////////////////////////////////////
  // #18: IS_NULLABLE:
  // - JDBC:  "18. ... String => ISO rules are used to determine the nullability for a column.
  //     YES --- if the column can include NULLs
  //     NO --- if the column cannot include NULLs
  //     empty string --- if the nullability for the column is unknown"
  // - Drill:  ?
  // - (Meta): VARCHAR (NVARCHAR?); Not nullable?

  // (See to-do note near top of file about reviewing nullability.)
  @Test
  public void test_IS_NULLABLE_isAtRightPosition() throws SQLException {
    assertThat( rowsMetadata.getColumnLabel( 18 ), equalTo( "IS_NULLABLE" ) );
  }

  // (See to-do note near top of file about reviewing nullability.)
  @Test
  public void test_IS_NULLABLE_hasRightValue_optBOOLEAN() throws SQLException {
    assertThat( mdrOptBOOLEAN.getString( "IS_NULLABLE" ), equalTo( "YES" ) );
  }

  // (See to-do note near top of file about reviewing nullability.)
  @Test
  public void test_IS_NULLABLE_hasRightValue_reqTINYINT() throws SQLException {
    assertThat( mdrReqTINYINT.getString( "IS_NULLABLE" ), equalTo( "NO" ) );
  }

  // (See to-do note near top of file about reviewing nullability.)
  @Test
  public void test_IS_NULLABLE_hasRightValue_optSMALLINT() throws SQLException {
    assertThat( mdrOptSMALLINT.getString( "IS_NULLABLE" ), equalTo( "YES" ) );
  }

  // (See to-do note near top of file about reviewing nullability.)
  @Test
  public void test_IS_NULLABLE_hasRightValue_reqINTEGER() throws SQLException {
    assertThat( mdrReqINTEGER.getString( "IS_NULLABLE" ), equalTo( "NO" ) );
  }

  // (See to-do note near top of file about reviewing nullability.)
  @Test
  public void test_IS_NULLABLE_hasRightValue_optBIGINT() throws SQLException {
    assertThat( mdrOptBIGINT.getString( "IS_NULLABLE" ), equalTo( "YES" ) );
  }

  // (See to-do note near top of file about reviewing nullability.)
  @Test
  public void test_IS_NULLABLE_hasRightValue_optFLOAT() throws SQLException {
    assertThat( mdrOptFLOAT.getString( "IS_NULLABLE" ), equalTo( "YES" ) );
  }

  // (See to-do note near top of file about reviewing nullability.)
  @Test
  public void test_IS_NULLABLE_hasRightValue_reqDOUBLE() throws SQLException {
    assertThat( mdrReqDOUBLE.getString( "IS_NULLABLE" ), equalTo( "NO" ) );
  }

  // (See to-do note near top of file about reviewing nullability.)
  @Test
  public void test_IS_NULLABLE_hasRightValue_optREAL() throws SQLException {
    assertThat( mdrOptREAL.getString( "IS_NULLABLE" ), equalTo( "YES" ) );
  }

  // (See to-do note near top of file about reviewing nullability.)
  @Test
  public void test_IS_NULLABLE_hasRightValue_reqDECIMAL_5_3() throws SQLException {
    assertThat( mdrReqDECIMAL_5_3.getString( "IS_NULLABLE" ), equalTo( "NO" ) );
  }

  // (See to-do note near top of file about reviewing nullability.)
  @Test
  public void test_IS_NULLABLE_hasRightValue_reqVARCHAR_10() throws SQLException {
    assertThat( mdrReqVARCHAR_10.getString( "IS_NULLABLE" ), equalTo( "NO" ) );
  }

  // (See to-do note near top of file about reviewing nullability.)
  @Test
  public void test_IS_NULLABLE_hasRightValue_optVARCHAR() throws SQLException {
    assertThat( mdrOptVARCHAR.getString( "IS_NULLABLE" ), equalTo( "YES" ) );
  }

  // (See to-do note near top of file about reviewing nullability.)
  @Test
  public void test_IS_NULLABLE_hasRightValue_reqCHAR_5() throws SQLException {
    assertThat( mdrReqCHAR_5.getString( "IS_NULLABLE" ), equalTo( "NO" ) );
  }

  // (See to-do note near top of file about reviewing nullability.)
  @Test
  public void test_IS_NULLABLE_hasRightValue_optVARBINARY_16() throws SQLException {
    assertThat( mdrOptVARBINARY_16.getString( "IS_NULLABLE" ), equalTo( "YES" ) );
  }

  // (See to-do note near top of file about reviewing nullability.)
  @Test
  public void test_IS_NULLABLE_hasRightValue_optBINARY_1048576CHECK() throws SQLException {
    assertThat( mdrOptBINARY_1048576.getString( "IS_NULLABLE" ), equalTo( "YES" ) );
  }

  // (See to-do note near top of file about reviewing nullability.)
  @Test
  public void test_IS_NULLABLE_hasRightValue_reqDATE() throws SQLException {
    assertThat( mdrReqDATE.getString( "IS_NULLABLE" ), equalTo( "NO" ) );
  }

  // (See to-do note near top of file about reviewing nullability.)
  @Test
  public void test_IS_NULLABLE_hasRightValue_optTIME() throws SQLException {
    assertThat( mdrOptTIME.getString( "IS_NULLABLE" ), equalTo( "YES" ) );
  }

  // (See to-do note near top of file about reviewing nullability.)
  @Test
  public void test_IS_NULLABLE_hasRightValue_optTIME_7() throws SQLException {
    assertThat( mdrOptTIME_7.getString( "IS_NULLABLE" ), equalTo( "YES" ) );
  }

  // (See to-do note near top of file about reviewing nullability.)
  @Test
  public void test_IS_NULLABLE_hasRightValue_optTIMESTAMP() throws SQLException {
    assertThat( mdrOptTIMESTAMP.getString( "IS_NULLABLE" ), equalTo( "YES" ) );
  }

  // (See to-do note near top of file about reviewing nullability.)
  @Test
  public void test_IS_NULLABLE_hasRightValue_optINTERVAL_HM() throws SQLException {
    assertThat( mdrOptINTERVAL_H_S3.getString( "IS_NULLABLE" ), equalTo( "YES" ) );
  }

  // (See to-do note near top of file about reviewing nullability.)
  @Test
  public void test_IS_NULLABLE_hasRightValue_optINTERVAL_Y3() throws SQLException {
    assertThat( mdrOptINTERVAL_Y4.getString( "IS_NULLABLE" ), equalTo( "YES" ) );
  }

  // (See to-do note near top of file about reviewing nullability.)
  @Test
  @Ignore("Enable once we have a test plugin which supports all the needed types.")
  public void test_IS_NULLABLE_hasRightValue_tdbARRAY() throws SQLException {
    assertThat( mdrReqARRAY.getString( "IS_NULLABLE" ), equalTo( "NO" ) );
  }

  // (See to-do note near top of file about reviewing nullability.)
  @Test
  @Ignore("Enable once we have a test plugin which supports all the needed types.")
  public void test_IS_NULLABLE_hasRightValue_tbdMAP() throws SQLException {
    assertThat( mdrReqMAP.getString( "IS_NULLABLE" ), equalTo( "NO" ) );
  }

  @Ignore( "until resolved:  any requirement on nullability (DRILL-2420?)" )
  @Test
  public void test_IS_NULLABLE_hasRightValue_tbdSTRUCT() throws SQLException {
    assertThat( testRowSTRUCT.getString( "IS_NULLABLE" ), equalTo( "YES" ) );
    // To-do:  Determine which.
    assertThat( testRowSTRUCT.getString( "IS_NULLABLE" ), equalTo( "NO" ) );
    // To-do:  Determine which.
    assertThat( testRowSTRUCT.getString( "IS_NULLABLE" ), equalTo( "" ) );
  }

  @Ignore( "until resolved:  any requirement on nullability (DRILL-2420?)" )
  @Test
  public void test_IS_NULLABLE_hasRightValue_tbdUnion() throws SQLException {
    assertThat( testRowUnion.getString( "IS_NULLABLE" ), equalTo( "YES" ) );
    // To-do:  Determine which.
    assertThat( testRowUnion.getString( "IS_NULLABLE" ), equalTo( "NO" ) );
    // To-do:  Determine which.
    assertThat( testRowUnion.getString( "IS_NULLABLE" ), equalTo( "" ) );
  }

  // (See to-do note near top of file about reviewing nullability.)
  @Test
  public void test_IS_NULLABLE_hasSameNameAndLabel() throws SQLException {
    assertThat( rowsMetadata.getColumnName( 18 ), equalTo( "IS_NULLABLE" ) );
  }

  // (See to-do note near top of file about reviewing nullability.)
  @Test
  public void test_IS_NULLABLE_hasRightTypeString() throws SQLException {
    assertThat( rowsMetadata.getColumnTypeName( 18 ), equalTo( "VARCHAR" ) );
  }

  // (See to-do note near top of file about reviewing nullability.)
  @Test
  public void test_IS_NULLABLE_hasRightTypeCode() throws SQLException {
    assertThat( rowsMetadata.getColumnType( 18 ), equalTo( Types.VARCHAR ) );
  }

  @Ignore( "until fixed (\"none\" -> right class name) (DRILL-2137)" )
  @Test
  public void test_IS_NULLABLE_hasRightClass() throws SQLException {
    // TODO:  Confirm that this "java.lang.String" is correct:
    assertThat( rowsMetadata.getColumnClassName( 18 ), equalTo( String.class.getName() ) );
  }

  @Ignore( "until resolved:  any requirement on nullability (DRILL-2420?)" )
  @Test
  public void test_IS_NULLABLE_hasRightNullability() throws SQLException {
    assertThat( "ResultSetMetaData.column...Null... nullability code",
                rowsMetadata.isNullable( 18 ), equalTo( columnNoNulls ) );
  }


  ////////////////////////////////////////////////////////////
  // #19: SCOPE_CATALOG:
  // - JDBC:  "19. ... String => catalog of table that is the scope of a
  //     reference attribute (null if DATA_TYPE isn't REF)"
  // - Drill:
  // - (Meta): VARCHAR (NVARCHAR?); Nullable;

  @Test
  public void test_SCOPE_CATALOG_isAtRightPosition() throws SQLException {
    assertThat( rowsMetadata.getColumnLabel( 19 ), equalTo( "SCOPE_CATALOG" ) );
  }

  @Test
  public void test_SCOPE_CATALOG_hasRightValue_optBOOLEAN() throws SQLException {
      final String value = mdrOptBOOLEAN.getString( "SCOPE_SCHEMA" );
      assertThat( "wasNull() [after " + value + "]",
                mdrOptBOOLEAN.wasNull(), equalTo( true ) );
      assertThat( value, nullValue() );
    }

  @Test
  public void test_SCOPE_CATALOG_hasSameNameAndLabel() throws SQLException {
    assertThat( rowsMetadata.getColumnName( 19 ), equalTo( "SCOPE_CATALOG" ) );
  }

  @Test
  public void test_SCOPE_CATALOG_hasRightTypeString() throws SQLException {
    assertThat( rowsMetadata.getColumnTypeName( 19 ), equalTo( "VARCHAR" ) );
  }

  @Test
  public void test_SCOPE_CATALOG_hasRightTypeCode() throws SQLException {
    assertThat( rowsMetadata.getColumnType( 19 ), equalTo( Types.VARCHAR ) );
  }

  @Ignore( "until fixed (\"none\" -> right class name) (DRILL-2137)" )
  @Test
  public void test_SCOPE_CATALOG_hasRightClass() throws SQLException {
    // TODO:  Confirm that this "java.lang.String" is correct:
    assertThat( rowsMetadata.getColumnClassName( 19 ), equalTo( String.class.getName() ) );
  }

  // (See to-do note near top of file about reviewing nullability.)
  @Test
  public void test_SCOPE_CATALOG_hasRightNullability() throws SQLException {
    assertThat( "ResultSetMetaData.column...Null... nullability code",
                rowsMetadata.isNullable( 19 ), equalTo( columnNullable ) );
  }


  ////////////////////////////////////////////////////////////
  // #20: SCOPE_SCHEMA:
  // - JDBC:  "20. ... String => schema of table that is the scope of a
  //     reference attribute (null if the DATA_TYPE isn't REF)"
  // - Drill:  no REF, so always null?
  // - (Meta): VARCHAR?; Nullable;

  @Test
  public void test_SCOPE_SCHEMA_isAtRightPosition() throws SQLException {
    assertThat( rowsMetadata.getColumnLabel( 20 ), equalTo( "SCOPE_SCHEMA" ) );
  }

  @Test
  public void test_SCOPE_SCHEMA_hasRightValue_optBOOLEAN() throws SQLException {
    final String value = mdrOptBOOLEAN.getString( "SCOPE_SCHEMA" );
    assertThat( value, nullValue() );
  }

  @Test
  public void test_SCOPE_SCHEMA_hasSameNameAndLabel() throws SQLException {
    assertThat( rowsMetadata.getColumnName( 20 ), equalTo( "SCOPE_SCHEMA" ) );
  }

  @Test
  public void test_SCOPE_SCHEMA_hasRightTypeString() throws SQLException {
    assertThat( rowsMetadata.getColumnTypeName( 20 ), equalTo( "VARCHAR" ) );
  }

  @Test
  public void test_SCOPE_SCHEMA_hasRightTypeCode() throws SQLException {
    assertThat( rowsMetadata.getColumnType( 20 ), equalTo( Types.VARCHAR ) );
  }

  @Ignore( "until fixed (\"none\" -> right class name) (DRILL-2137)" )
  @Test
  public void test_SCOPE_SCHEMA_hasRightClass() throws SQLException {
    // TODO:  Confirm that this "java.lang.String" is correct:
    assertThat( rowsMetadata.getColumnClassName( 20 ), equalTo( String.class.getName() ) );
  }

  // (See to-do note near top of file about reviewing nullability.)
  @Test
  public void test_SCOPE_SCHEMA_hasRightNullability() throws SQLException {
    assertThat( "ResultSetMetaData.column...Null... nullability code",
                rowsMetadata.isNullable( 20 ), equalTo( columnNullable ) );
  }


  ////////////////////////////////////////////////////////////
  // #21: SCOPE_TABLE:
  // - JDBC:  "21. ... String => table name that this the scope of a reference
  //     attribute (null if the DATA_TYPE isn't REF)"
  // - Drill:
  // - (Meta): VARCHAR (NVARCHAR?); Nullable;

  @Test
  public void test_SCOPE_TABLE_isAtRightPosition() throws SQLException {
    assertThat( rowsMetadata.getColumnLabel( 21 ), equalTo( "SCOPE_TABLE" ) );
  }

  @Test
  public void test_SCOPE_TABLE_hasRightValue_optBOOLEAN() throws SQLException {
    final String value = mdrOptBOOLEAN.getString( "SCOPE_TABLE" );
    assertThat( value, nullValue() );
  }

  @Test
  public void test_SCOPE_TABLE_hasSameNameAndLabel() throws SQLException {
    assertThat( rowsMetadata.getColumnName( 21 ), equalTo( "SCOPE_TABLE" ) );
  }

  @Test
  public void test_SCOPE_TABLE_hasRightTypeString() throws SQLException {
    assertThat( rowsMetadata.getColumnTypeName( 21 ), equalTo( "VARCHAR" ) );
  }

  @Test
  public void test_SCOPE_TABLE_hasRightTypeCode() throws SQLException {
    assertThat( rowsMetadata.getColumnType( 21 ), equalTo( Types.VARCHAR ) );
  }

  @Ignore( "until fixed (\"none\" -> right class name) (DRILL-2137)" )
  @Test
  public void test_SCOPE_TABLE_hasRightClass() throws SQLException {
    // TODO:  Confirm that this "java.lang.String" is correct:
    assertThat( rowsMetadata.getColumnClassName( 21 ), equalTo( String.class.getName() ) );
  }

  // (See to-do note near top of file about reviewing nullability.)
  @Test
  public void test_SCOPE_TABLE_hasRightNullability() throws SQLException {
    assertThat( "ResultSetMetaData.column...Null... nullability code",
                rowsMetadata.isNullable( 21 ), equalTo( columnNullable ) );
  }


  ////////////////////////////////////////////////////////////
  // #22: SOURCE_DATA_TYPE:
  // - JDBC:  "22. ... short => source type of a distinct type or user-generated
  //     Ref type, SQL type from java.sql.Types (null if DATA_TYPE isn't
  //     DISTINCT or user-generated REF)"
  // - Drill:  not DISTINCT or REF, so null?
  // - (Meta): SMALLINT(?);  Nullable;

  @Test
  public void test_SOURCE_DATA_TYPE_isAtRightPosition() throws SQLException {
    assertThat( rowsMetadata.getColumnLabel( 22 ), equalTo( "SOURCE_DATA_TYPE" ) );
  }

  @Test
  public void test_SOURCE_DATA_TYPE_hasRightValue_optBOOLEAN() throws SQLException {
    final int value = mdrOptBOOLEAN.getInt( "SOURCE_DATA_TYPE" );
    assertThat( "wasNull() [after " + value + "]",
                mdrOptBOOLEAN.wasNull(), equalTo( true ) );
    assertThat( value, equalTo( 0 /* NULL */ ) );
  }

  @Test
  public void test_SOURCE_DATA_TYPE_hasSameNameAndLabel() throws SQLException {
    assertThat( rowsMetadata.getColumnName( 22 ), equalTo( "SOURCE_DATA_TYPE" ) );
  }

  @Test
  public void test_SOURCE_DATA_TYPE_hasRightTypeString() throws SQLException {
    // TODO:  Resolve:  Bug DRILL-2135 workaround:
    //assertThat( rsMetadata.getColumnTypeName( 22 ), equalTo( "SMALLINT" ) );
    assertThat( rowsMetadata.getColumnTypeName( 22 ), equalTo( "INTEGER" ) );
  }

  @Test
  public void test_SOURCE_DATA_TYPE_hasRightTypeCode() throws SQLException {
    // TODO:  Resolve:  Bug DRILL-2135 workaround:
    //assertThat( rsMetadata.getColumnType( 22 ), equalTo( Types.SMALLINT ) );
    assertThat( rowsMetadata.getColumnType( 22 ), equalTo( Types.INTEGER ) );
  }

  @Ignore( "until fixed (\"none\" -> right class name) (DRILL-2137)" )
  @Test
  public void test_SOURCE_DATA_TYPE_hasRightClass() throws SQLException {
    // TODO:  Confirm that this "java.lang.Integer" is correct:
    assertThat( rowsMetadata.getColumnClassName( 22 ), equalTo( Integer.class.getName() ) );
  }

  // (See to-do note near top of file about reviewing nullability.)
  @Test
  public void test_SOURCE_DATA_TYPE_hasRightNullability() throws SQLException {
    assertThat( "ResultSetMetaData.column...Null... nullability code",
                rowsMetadata.isNullable( 22 ), equalTo( columnNullable ) );
  }


  ////////////////////////////////////////////////////////////
  // #23: IS_AUTOINCREMENT:
  // - JDBC:  "23. ... String => Indicates whether this column is auto incremented
  //     YES --- if the column is auto incremented
  //     NO --- if the column is not auto incremented
  //     empty string --- if it cannot be determined whether the column is auto incremented"
  // - Drill:
  // - (Meta): VARCHAR (NVARCHAR?); Non-nullable(?);

  @Test
  public void test_IS_AUTOINCREMENT_isAtRightPosition() throws SQLException {
    assertThat( rowsMetadata.getColumnLabel( 23 ), equalTo( "IS_AUTOINCREMENT" ) );
  }

  @Test
  public void test_IS_AUTOINCREMENT_hasRightValue_optBOOLEAN() throws SQLException {
    // TODO:  Can is be 'NO' (not auto-increment) rather than '' (unknown)?
    assertThat( mdrOptBOOLEAN.getString( "IS_AUTOINCREMENT" ), equalTo( "" ) );
  }

  // Not bothering with other test columns for IS_AUTOINCREMENT.

  @Test
  public void test_IS_AUTOINCREMENT_hasSameNameAndLabel() throws SQLException {
    assertThat( rowsMetadata.getColumnName( 23 ), equalTo( "IS_AUTOINCREMENT" ) );
  }

  @Test
  public void test_IS_AUTOINCREMENT_hasRightTypeString() throws SQLException {
    assertThat( rowsMetadata.getColumnTypeName( 23 ), equalTo( "VARCHAR" ) );
  }

  @Test
  public void test_IS_AUTOINCREMENT_hasRightTypeCode() throws SQLException {
    assertThat( rowsMetadata.getColumnType( 23 ), equalTo( Types.VARCHAR ) );
  }

  @Ignore( "until fixed (\"none\" -> right class name) (DRILL-2137)" )
  @Test
  public void test_IS_AUTOINCREMENT_hasRightClass() throws SQLException {
    // TODO:  Confirm that this "java.lang.String" is correct:
    assertThat( rowsMetadata.getColumnClassName( 23 ), equalTo( String.class.getName() ) );
  }

  // (See to-do note near top of file about reviewing nullability.)
  @Test
  public void test_IS_AUTOINCREMENT_hasRightNullability() throws SQLException {
    assertThat( "ResultSetMetaData.column...Null... nullability code",
                rowsMetadata.isNullable( 23 ), equalTo( columnNoNulls ) );
  }


  ////////////////////////////////////////////////////////////
  // #24: IS_GENERATEDCOLUMN:
  // - JDBC:  "24. ... String => Indicates whether this is a generated column
  //     YES --- if this a generated column
  //     NO --- if this not a generated column
  //     empty string --- if it cannot be determined whether this is a generated column"
  // - Drill:
  // - (Meta): VARCHAR (NVARCHAR?); Non-nullable(?)

  @Test
  public void test_IS_GENERATEDCOLUMN_isAtRightPosition() throws SQLException {
    assertThat( rowsMetadata.getColumnLabel( 24 ), equalTo( "IS_GENERATEDCOLUMN" ) );
  }

  @Test
  public void test_IS_GENERATEDCOLUMN_hasRightValue_optBOOLEAN() throws SQLException {
    // TODO:  Can is be 'NO' (not auto-increment) rather than '' (unknown)?
    assertThat( mdrOptBOOLEAN.getString( "IS_GENERATEDCOLUMN" ), equalTo( "" ) );
  }

  // Not bothering with other test columns for IS_GENERATEDCOLUMN.

  @Test
  public void test_IS_GENERATEDCOLUMN_hasSameNameAndLabel() throws SQLException {
    assertThat( rowsMetadata.getColumnName( 24 ), equalTo( "IS_GENERATEDCOLUMN" ) );
  }

  @Test
  public void test_IS_GENERATEDCOLUMN_hasRightTypeString() throws SQLException {
    assertThat( rowsMetadata.getColumnTypeName( 24 ), equalTo( "VARCHAR" ) );
  }

  @Test
  public void test_IS_GENERATEDCOLUMN_hasRightTypeCode() throws SQLException {
    assertThat( rowsMetadata.getColumnType( 24 ), equalTo( Types.VARCHAR ) );
  }

  @Ignore( "until fixed (\"none\" -> right class name) (DRILL-2137)" )
  @Test
  public void test_IS_GENERATEDCOLUMN_hasRightClass() throws SQLException {
    // TODO:  Confirm that this "java.lang.String" is correct:
    assertThat( rowsMetadata.getColumnClassName( 24 ), equalTo( String.class.getName() ) );
  }

  // (See to-do note near top of file about reviewing nullability.)
  @Test
  public void test_IS_GENERATEDCOLUMN_hasRightNullability() throws SQLException {
    assertThat( "ResultSetMetaData.column...Null... nullability code",
                rowsMetadata.isNullable( 24 ), equalTo( columnNoNulls ) );
  }

} // class DatabaseMetaGetColumnsDataTest
