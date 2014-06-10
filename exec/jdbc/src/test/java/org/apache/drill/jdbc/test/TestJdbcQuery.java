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

import java.io.File;
import java.lang.Exception;
import java.lang.RuntimeException;
import java.nio.file.Paths;
import java.sql.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.drill.common.util.TestTools;
import org.apache.drill.exec.store.hive.HiveTestDataGenerator;
import org.apache.drill.jdbc.Driver;
import org.apache.drill.jdbc.JdbcTest;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import com.google.common.base.Function;
import com.google.common.base.Stopwatch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestJdbcQuery extends JdbcTest{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestJdbcQuery.class);


  // Set a timeout unless we're debugging.
  @Rule public TestRule TIMEOUT = TestTools.getTimeoutRule(20000);

  protected static final String WORKING_PATH;
  static{
    Driver.load();
    WORKING_PATH = Paths.get("").toAbsolutePath().toString();

  }

  @BeforeClass
  public static void generateHive() throws Exception{
    new HiveTestDataGenerator().generateTestData();

    // delete tmp workspace directory
    File f = new File("/tmp/drilltest");
    if(f.exists()){
      FileUtils.cleanDirectory(f);
      FileUtils.forceDelete(f);
    }
  }

  @Test
  public void testHiveReadWithDb() throws Exception{
    testQuery("select * from hive.`default`.kv");
    testQuery("select key from hive.`default`.kv group by key");
  }

  @Test
  public void testHiveWithDate() throws Exception {
    testQuery("select * from hive.`default`.foodate");
    testQuery("select date_add(a, time '12:23:33'), b from hive.`default`.foodate");
  }

  @Test
  public void testQueryEmptyHiveTable() throws Exception {
    testQuery("SELECT * FROM hive.`default`.empty_table");
  }

  @Test
  @Ignore
  public void testJsonQuery() throws Exception{
    testQuery("select * from cp.`employee.json`");
  }


  @Test
  public void testCast() throws Exception{
    testQuery(String.format("select R_REGIONKEY, cast(R_NAME as varchar(15)) as region, cast(R_COMMENT as varchar(255)) as comment from dfs.`%s/../../sample-data/region.parquet`", WORKING_PATH));
  }

  @Test
  @Ignore
  public void testWorkspace() throws Exception{
    testQuery(String.format("select * from dfs.home.`%s/../../sample-data/region.parquet`", WORKING_PATH));
  }

  @Test
  @Ignore
  public void testWildcard() throws Exception{
    testQuery(String.format("select * from dfs.`%s/../../sample-data/region.parquet`", WORKING_PATH));
  }

  @Test
  public void testCharLiteral() throws Exception {
    testQuery("select 'test literal' from INFORMATION_SCHEMA.`TABLES` LIMIT 1");
  }

  @Test
  public void testVarCharLiteral() throws Exception {
    testQuery("select cast('test literal' as VARCHAR) from INFORMATION_SCHEMA.`TABLES` LIMIT 1");
  }

  @Test
  @Ignore
  public void testLogicalExplain() throws Exception{
    testQuery(String.format("EXPLAIN PLAN WITHOUT IMPLEMENTATION FOR select * from dfs.`%s/../../sample-data/region.parquet`", WORKING_PATH));
  }

  @Test
  @Ignore
  public void testPhysicalExplain() throws Exception{
    testQuery(String.format("EXPLAIN PLAN FOR select * from dfs.`%s/../../sample-data/region.parquet`", WORKING_PATH));
  }

  @Test
  @Ignore
  public void checkUnknownColumn() throws Exception{
    testQuery(String.format("SELECT unknownColumn FROM dfs.`%s/../../sample-data/region.parquet`", WORKING_PATH));
  }

  protected void testQuery(String sql) throws Exception{
    boolean success = false;
    try (Connection c = DriverManager.getConnection("jdbc:drill:zk=local", null);) {
      for (int x = 0; x < 1; x++) {
        Stopwatch watch = new Stopwatch().start();
        Statement s = c.createStatement();
        ResultSet r = s.executeQuery(sql);
        boolean first = true;
        while (r.next()) {
          ResultSetMetaData md = r.getMetaData();
          if (first == true) {
            for (int i = 1; i <= md.getColumnCount(); i++) {
              System.out.print(md.getColumnName(i));
              System.out.print('\t');
            }
            System.out.println();
            first = false;
          }

          for (int i = 1; i <= md.getColumnCount(); i++) {
            System.out.print(r.getObject(i));
            System.out.print('\t');
          }
          System.out.println();
        }

        System.out.println(String.format("Query completed in %d millis.", watch.elapsed(TimeUnit.MILLISECONDS)));
      }

      System.out.println("\n\n\n");
      success = true;
    }finally{
      if(!success) Thread.sleep(2000);
    }
  }

  @Test
  public void testLikeNotLike() throws Exception{
    JdbcAssert.withNoDefaultSchema()
      .sql("SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS " +
        "WHERE TABLE_NAME NOT LIKE 'C%' AND COLUMN_NAME LIKE 'TABLE_%E'")
      .returns(
        "TABLE_NAME=VIEWS; COLUMN_NAME=TABLE_NAME\n" +
        "TABLE_NAME=TABLES; COLUMN_NAME=TABLE_NAME\n" +
        "TABLE_NAME=TABLES; COLUMN_NAME=TABLE_TYPE\n"
      );
  }

  @Test
  public void testSimilarNotSimilar() throws Exception{
    JdbcAssert.withNoDefaultSchema()
      .sql("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.`TABLES` "+
        "WHERE TABLE_NAME SIMILAR TO '%(H|I)E%' AND TABLE_NAME NOT SIMILAR TO 'C%'")
      .returns(
        "TABLE_NAME=VIEWS\n" +
        "TABLE_NAME=SCHEMATA\n"
      );
  }


  @Test
  public void testIntegerLiteral() throws Exception{
    JdbcAssert.withNoDefaultSchema()
      .sql("select substring('asd' from 1 for 2) from INFORMATION_SCHEMA.`TABLES` limit 1")
      .returns("EXPR$0=as\n");
  }

  @Test
  public void testNullOpForNullableType() throws Exception{
    JdbcAssert.withNoDefaultSchema()
        .sql("SELECT * FROM cp.`test_null_op.json` WHERE intType IS NULL AND varCharType IS NOT NULL")
        .returns("intType=null; varCharType=val2");
  }

  @Test
  public void testNullOpForNonNullableType() throws Exception{
    // output of (intType IS NULL) is a non-nullable type
    JdbcAssert.withNoDefaultSchema()
        .sql("SELECT * FROM cp.`test_null_op.json` "+
            "WHERE (intType IS NULL) IS NULL AND (varCharType IS NOT NULL) IS NOT NULL")
        .returns("");
  }

  @Test
  public void testTrueOpForNullableType() throws Exception{
    JdbcAssert.withNoDefaultSchema()
        .sql("SELECT data FROM cp.`test_true_false_op.json` WHERE booleanType IS TRUE")
        .returns("data=set to true");

    JdbcAssert.withNoDefaultSchema()
        .sql("SELECT data FROM cp.`test_true_false_op.json` WHERE booleanType IS FALSE")
        .returns("data=set to false");

    JdbcAssert.withNoDefaultSchema()
        .sql("SELECT data FROM cp.`test_true_false_op.json` WHERE booleanType IS NOT TRUE")
        .returns(
            "data=set to false\n" +
            "data=not set"
        );

    JdbcAssert.withNoDefaultSchema()
        .sql("SELECT data FROM cp.`test_true_false_op.json` WHERE booleanType IS NOT FALSE")
        .returns(
            "data=set to true\n" +
            "data=not set"
        );
  }

  @Test
  public void testTrueOpForNonNullableType() throws Exception{
    // Output of IS TRUE (and others) is a Non-nullable type
    JdbcAssert.withNoDefaultSchema()
        .sql("SELECT data FROM cp.`test_true_false_op.json` WHERE (booleanType IS TRUE) IS TRUE")
        .returns("data=set to true");

    JdbcAssert.withNoDefaultSchema()
        .sql("SELECT data FROM cp.`test_true_false_op.json` WHERE (booleanType IS FALSE) IS FALSE")
        .returns(
            "data=set to true\n" +
            "data=not set"
        );

    JdbcAssert.withNoDefaultSchema()
        .sql("SELECT data FROM cp.`test_true_false_op.json` WHERE (booleanType IS NOT TRUE) IS NOT TRUE")
        .returns("data=set to true");

    JdbcAssert.withNoDefaultSchema()
        .sql("SELECT data FROM cp.`test_true_false_op.json` WHERE (booleanType IS NOT FALSE) IS NOT FALSE")
        .returns(
            "data=set to true\n" +
            "data=not set"
        );
  }

  @Test
  public void testDateTimeAccessors() throws Exception{
    JdbcAssert.withNoDefaultSchema().withConnection(new Function<Connection, Void>() {
      public Void apply(Connection connection) {
        try {
          Statement statement = connection.createStatement();

          // show tables on view
          ResultSet resultSet = statement.executeQuery("select date '2008-2-23', time '12:23:34', timestamp '2008-2-23 12:23:34.456', " +
                                                       "interval '1' year, interval '2' day, " +
                                                       "date_add(date '2008-2-23', interval '1 10:20:30' day to second), " +
                                                       "date_add(date '2010-2-23', 1) " +
                                                       "from cp.`employee.json` limit 1");

          java.sql.Date date = resultSet.getDate(1);
          java.sql.Time time = resultSet.getTime(2);
          java.sql.Timestamp ts = resultSet.getTimestamp(3);
          String intervalYear = resultSet.getString(4);
          String intervalDay  = resultSet.getString(5);
          java.sql.Timestamp ts1 = resultSet.getTimestamp(6);
          java.sql.Date date1 = resultSet.getDate(7);

          java.sql.Timestamp result = java.sql.Timestamp.valueOf("2008-2-24 10:20:30");
          java.sql.Date result1 = java.sql.Date.valueOf("2010-2-24");
          assertEquals(ts1, result);
          assertEquals(date1, result1);

          System.out.println("Date: " + date.toString() + " time: " + time.toString() + " timestamp: " + ts.toString() +
                             "\ninterval year: " + intervalYear + " intervalDay: " + intervalDay +
                             " date_interval_add: " + ts1.toString() + "date_int_add: " + date1.toString());

          statement.close();
          return null;
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    });
  }

  @Test
  public void testVerifyMetadata() throws Exception{
    JdbcAssert.withNoDefaultSchema().withConnection(new Function<Connection, Void>() {
      public Void apply(Connection connection) {
        try {
          Statement statement = connection.createStatement();

          // show files
          ResultSet resultSet = statement.executeQuery("select timestamp '2008-2-23 12:23:23', date '2001-01-01', timestamptztype('2008-2-23 1:20:23 US/Pacific') from cp.`employee.json` limit 1");

          assert (resultSet.getMetaData().getColumnType(1) == Types.TIMESTAMP);
          assert (resultSet.getMetaData().getColumnType(2) == Types.DATE);
          assert (resultSet.getMetaData().getColumnType(3) == Types.TIMESTAMP);

          System.out.println(JdbcAssert.toString(resultSet));
          resultSet.close();
          statement.close();
          return null;
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    });
  }

  @Test
  public void testCaseWithNoElse() throws Exception {
    JdbcAssert.withNoDefaultSchema()
        .sql("SELECT employee_id, CASE WHEN employee_id < 100 THEN first_name END from cp.`employee.json` " +
            "WHERE employee_id = 99 OR employee_id = 100")
        .returns(
            "employee_id=99; EXPR$1=Elizabeth\n" +
            "employee_id=100; EXPR$1=null\n"
        );
  }

  @Test
  public void testCaseWithElse() throws Exception {
    JdbcAssert.withNoDefaultSchema()
        .sql("SELECT employee_id, CASE WHEN employee_id < 100 THEN first_name ELSE 'Test' END from cp.`employee.json` " +
            "WHERE employee_id = 99 OR employee_id = 100")
        .returns(
            "employee_id=99; EXPR$1=Elizabeth\n" +
            "employee_id=100; EXPR$1=Test"
        );
  }

  @Test
  public void testCaseWith2ThensAndNoElse() throws Exception {
    JdbcAssert.withNoDefaultSchema()
        .sql("SELECT employee_id, CASE WHEN employee_id < 100 THEN first_name WHEN employee_id = 100 THEN last_name END " +
            "from cp.`employee.json` " +
            "WHERE employee_id = 99 OR employee_id = 100 OR employee_id = 101")
        .returns(
            "employee_id=99; EXPR$1=Elizabeth\n" +
            "employee_id=100; EXPR$1=Hunt\n" +
            "employee_id=101; EXPR$1=null"
        );
  }

  @Test
  public void testCaseWith2ThensAndElse() throws Exception {
    JdbcAssert.withNoDefaultSchema()
        .sql("SELECT employee_id, CASE WHEN employee_id < 100 THEN first_name WHEN employee_id = 100 THEN last_name ELSE 'Test' END " +
            "from cp.`employee.json` " +
            "WHERE employee_id = 99 OR employee_id = 100 OR employee_id = 101")
        .returns(
            "employee_id=99; EXPR$1=Elizabeth\n" +
            "employee_id=100; EXPR$1=Hunt\n" +
            "employee_id=101; EXPR$1=Test\n"
        );
  }
}
