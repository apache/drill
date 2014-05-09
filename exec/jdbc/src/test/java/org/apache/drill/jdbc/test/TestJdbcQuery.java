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

import java.lang.Exception;
import java.lang.RuntimeException;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestJdbcQuery extends JdbcTest{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestJdbcQuery.class);


  // Set a timeout unless we're debugging.
  @Rule public TestRule TIMEOUT = TestTools.getTimeoutRule(20000);

  private static final String WORKING_PATH;
  static{
    Driver.load();
    WORKING_PATH = Paths.get("").toAbsolutePath().toString();

  }

  @BeforeClass
  public static void generateHive() throws Exception{
    new HiveTestDataGenerator().generateTestData();
  }

  @Test
  @Ignore
  public void testHiveRead() throws Exception{
    testQuery("select * from hive.kv");
  }

  @Test
  public void testHiveReadWithDb() throws Exception{
    testQuery("select * from hive.`default`.kv");
  }

  @Test
  @Ignore
  public void testJsonQuery() throws Exception{
    testQuery("select * from cp.`employee.json`");
  }


  @Test
  public void testInfoSchema() throws Exception{
    testQuery("select * from INFORMATION_SCHEMA.SCHEMATA");
    testQuery("select * from INFORMATION_SCHEMA.CATALOGS");
    testQuery("select * from INFORMATION_SCHEMA.VIEWS");
//    testQuery("select * from INFORMATION_SCHEMA.TABLES");
    testQuery("select * from INFORMATION_SCHEMA.COLUMNS");
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

  private void testQuery(String sql) throws Exception{
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
  public void testShowTables() throws Exception{
    JdbcAssert.withNoDefaultSchema()
      .sql("SHOW TABLES")
      .returns(
        "TABLE_SCHEMA=hive.default; TABLE_NAME=kv\n" +
        "TABLE_SCHEMA=hive.db1; TABLE_NAME=kv_db1\n" +
        "TABLE_SCHEMA=hive; TABLE_NAME=kv\n" +
        "TABLE_SCHEMA=sys; TABLE_NAME=drillbits\n" +
        "TABLE_SCHEMA=sys; TABLE_NAME=options\n" +
        "TABLE_SCHEMA=INFORMATION_SCHEMA; TABLE_NAME=VIEWS\n" +
        "TABLE_SCHEMA=INFORMATION_SCHEMA; TABLE_NAME=COLUMNS\n" +
        "TABLE_SCHEMA=INFORMATION_SCHEMA; TABLE_NAME=TABLES\n" +
        "TABLE_SCHEMA=INFORMATION_SCHEMA; TABLE_NAME=CATALOGS\n" +
        "TABLE_SCHEMA=INFORMATION_SCHEMA; TABLE_NAME=SCHEMATA\n"
      );
  }

  @Test
  public void testShowTablesFromDb() throws Exception{
    JdbcAssert.withNoDefaultSchema()
      .sql("SHOW TABLES FROM INFORMATION_SCHEMA")
      .returns(
        "TABLE_SCHEMA=INFORMATION_SCHEMA; TABLE_NAME=VIEWS\n" +
        "TABLE_SCHEMA=INFORMATION_SCHEMA; TABLE_NAME=COLUMNS\n" +
        "TABLE_SCHEMA=INFORMATION_SCHEMA; TABLE_NAME=TABLES\n" +
        "TABLE_SCHEMA=INFORMATION_SCHEMA; TABLE_NAME=CATALOGS\n" +
        "TABLE_SCHEMA=INFORMATION_SCHEMA; TABLE_NAME=SCHEMATA\n"
      );

    JdbcAssert.withNoDefaultSchema()
      .sql("SHOW TABLES IN hive")
      .returns("TABLE_SCHEMA=hive; TABLE_NAME=kv\n");
  }

  @Test
  public void testShowTablesFromDbWhere() throws Exception{
    JdbcAssert.withNoDefaultSchema()
      .sql("SHOW TABLES FROM INFORMATION_SCHEMA WHERE TABLE_NAME='VIEWS'")
      .returns("TABLE_SCHEMA=INFORMATION_SCHEMA; TABLE_NAME=VIEWS\n");
  }

  @Test
  public void testShowTablesLike() throws Exception{
    JdbcAssert.withNoDefaultSchema()
      .sql("SHOW TABLES LIKE '%CH%'")
      .returns("TABLE_SCHEMA=INFORMATION_SCHEMA; TABLE_NAME=SCHEMATA\n");
  }

  @Test
  public void testShowDatabases() throws Exception{
    String expected =
        "SCHEMA_NAME=hive.default\n" +
        "SCHEMA_NAME=hive.db1\n" +
        "SCHEMA_NAME=hive\n" +
        "SCHEMA_NAME=dfs.home\n" +
        "SCHEMA_NAME=dfs.default\n" +
        "SCHEMA_NAME=dfs\n" +
        "SCHEMA_NAME=cp.default\n" +
        "SCHEMA_NAME=cp\n" +
        "SCHEMA_NAME=sys\n" +
        "SCHEMA_NAME=INFORMATION_SCHEMA\n";

    JdbcAssert.withNoDefaultSchema().sql("SHOW DATABASES").returns(expected);
    JdbcAssert.withNoDefaultSchema().sql("SHOW SCHEMAS").returns(expected);
  }

  @Test
  public void testShowDatabasesWhere() throws Exception{
    JdbcAssert.withNoDefaultSchema()
      .sql("SHOW DATABASES WHERE SCHEMA_NAME='dfs'")
      .returns("SCHEMA_NAME=dfs\n");
  }

  @Test
  public void testShowDatabasesLike() throws Exception{
    JdbcAssert.withNoDefaultSchema()
      .sql("SHOW DATABASES LIKE '%i%'")
      .returns(
        "SCHEMA_NAME=hive.default\n"+
        "SCHEMA_NAME=hive.db1\n"+
        "SCHEMA_NAME=hive\n"
      );
  }

  @Test
  public void testDescribeTable() throws Exception{
    JdbcAssert.withNoDefaultSchema()
      .sql("DESCRIBE CATALOGS")
      .returns(
        "COLUMN_NAME=CATALOG_NAME; DATA_TYPE=VARCHAR; IS_NULLABLE=NO\n"+
        "COLUMN_NAME=CATALOG_DESCRIPTION; DATA_TYPE=VARCHAR; IS_NULLABLE=NO\n"+
        "COLUMN_NAME=CATALOG_CONNECT; DATA_TYPE=VARCHAR; IS_NULLABLE=NO\n"
      );
  }

  @Test
  public void testDescribeTableWithSchema() throws Exception{
    JdbcAssert.withNoDefaultSchema()
      .sql("DESCRIBE INFORMATION_SCHEMA.`TABLES`")
      .returns(
        "COLUMN_NAME=TABLE_CATALOG; DATA_TYPE=VARCHAR; IS_NULLABLE=NO\n"+
        "COLUMN_NAME=TABLE_SCHEMA; DATA_TYPE=VARCHAR; IS_NULLABLE=NO\n"+
        "COLUMN_NAME=TABLE_NAME; DATA_TYPE=VARCHAR; IS_NULLABLE=NO\n"+
        "COLUMN_NAME=TABLE_TYPE; DATA_TYPE=VARCHAR; IS_NULLABLE=NO\n"
      );
  }

  @Test
  @Ignore // DRILL-399 - default schema doesn't work
  public void testDescribeTableWithColumnName() throws Exception{
    JdbcAssert.withFull("INFORMATION_SCHEMA")
        .sql("DESCRIBE `TABLES` TABLE_CATALOG")
        .returns("COLUMN_NAME=TABLE_CATALOG; DATA_TYPE=VARCHAR; IS_NULLABLE=NO\n");
  }

  @Test
  public void testDescribeTableWithSchemaAndColumnName() throws Exception{
    JdbcAssert.withNoDefaultSchema()
      .sql("DESCRIBE INFORMATION_SCHEMA.`TABLES` TABLE_CATALOG")
      .returns("COLUMN_NAME=TABLE_CATALOG; DATA_TYPE=VARCHAR; IS_NULLABLE=NO\n");
  }

  @Test
  @Ignore // DRILL-399 - default schema doesn't work
  public void testDescribeTableWithColQualifier() throws Exception{
    JdbcAssert.withFull("INFORMATION_SCHEMA")
      .sql("DESCRIBE COLUMNS 'TABLE%'")
      .returns(
        "COLUMN_NAME=TABLE_CATALOG; DATA_TYPE=VARCHAR; IS_NULLABLE=NO\n"+
        "COLUMN_NAME=TABLE_SCHEMA; DATA_TYPE=VARCHAR; IS_NULLABLE=NO\n"+
        "COLUMN_NAME=TABLE_NAME; DATA_TYPE=VARCHAR; IS_NULLABLE=NO\n"
      );
  }

  @Test
  public void testDescribeTableWithSchemaAndColQualifier() throws Exception{
    JdbcAssert.withNoDefaultSchema()
      .sql("DESCRIBE INFORMATION_SCHEMA.SCHEMATA 'SCHEMA%'")
      .returns(
        "COLUMN_NAME=SCHEMA_NAME; DATA_TYPE=VARCHAR; IS_NULLABLE=NO\n"+
        "COLUMN_NAME=SCHEMA_OWNER; DATA_TYPE=VARCHAR; IS_NULLABLE=NO\n"
      );
  }

  @Test
  public void testDefaultSchemaDfs() throws Exception{
    JdbcAssert.withFull("dfs")
      .sql(String.format("SELECT R_REGIONKEY FROM `%s/../../sample-data/region.parquet` LIMIT 2", WORKING_PATH))
      .returns(
        "R_REGIONKEY=0\n" +
        "R_REGIONKEY=1\n"
      );
  }

  @Test
  public void testDefaultSchemaClasspath() throws Exception{
    JdbcAssert.withFull("cp")
      .sql("SELECT full_name FROM `employee.json` LIMIT 2")
      .returns(
        "full_name=Sheri Nowmer\n" +
        "full_name=Derrick Whelply\n"
      );
  }

  @Test
  public void testDefaultSchemaHive() throws Exception{
    JdbcAssert.withFull("hive")
      .sql("SELECT * FROM kv LIMIT 2")
      .returns(
        "key=1; value= key_1\n" +
        "key=2; value= key_2\n"
      );
  }

  @Test
  public void testDefaultTwoLevelSchemaHive() throws Exception{
    JdbcAssert.withFull("hive.db1")
      .sql("SELECT * FROM `kv_db1` LIMIT 2")
      .returns(
        "key=1; value= key_1\n" +
        "key=2; value= key_2\n"
      );
  }

  @Test
  public void testQueryFromNonDefaultSchema() throws Exception{
    JdbcAssert.withFull("hive")
      .sql("SELECT full_name FROM cp.`employee.json` LIMIT 2")
      .returns(
        "full_name=Sheri Nowmer\n" +
        "full_name=Derrick Whelply\n"
      );
  }

  @Test
  public void testUseSchema() throws Exception{
    JdbcAssert.withNoDefaultSchema()
      .sql("USE hive.`default`")
      .returns("ok=true; summary=Default schema changed to 'hive.default'");
  }

  @Test
  public void testUseSchemaNegative() throws Exception{
    JdbcAssert.withNoDefaultSchema()
        .sql("USE invalid.schema")
        .returns("ok=false; summary=Failed to change default schema to 'invalid.schema'");
  }

  @Test
  public void testUseSchemaAndQuery() throws Exception{
    JdbcAssert.withNoDefaultSchema().withConnection(new Function<Connection, Void>() {
      public Void apply(Connection connection) {
        try {
          Statement statement = connection.createStatement();
          ResultSet resultSet = statement.executeQuery("USE hive.db1");
          String result = JdbcAssert.toString(resultSet).trim();
          String expected = "ok=true; summary=Default schema changed to 'hive.db1'";
          assertTrue(String.format("Generated string:\n%s\ndoes not match:\n%s", result, expected), expected.equals(result));


          resultSet = statement.executeQuery("SELECT * FROM kv_db1 LIMIT 2");
          result = JdbcAssert.toString(resultSet).trim();
          expected = "key=1; value= key_1\nkey=2; value= key_2";
          assertTrue(String.format("Generated string:\n%s\ndoes not match:\n%s", result, expected), expected.equals(result));
          statement.close();
          return null;
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    });
  }

  /** Helper test method for view tests */
  private void testViewHelper(final String viewCreate, final String viewName,
                              final String viewQuery, final String queryResult) throws Exception{
    JdbcAssert.withNoDefaultSchema().withConnection(new Function<Connection, Void>() {
      public Void apply(Connection connection) {
        try {
          Statement statement = connection.createStatement();

          // change default schema
          statement.executeQuery("USE dfs.`default`");

          // create view
          ResultSet resultSet = statement.executeQuery(viewCreate);
          String result = JdbcAssert.toString(resultSet).trim();
          String viewCreateResult = "ok=true; summary=View '" + viewName + "' created successfully in 'dfs.default' schema";
          assertTrue(String.format("Generated string:\n%s\ndoes not match:\n%s", result, viewCreateResult),
              viewCreateResult.equals(result));

          // query from view
          resultSet = statement.executeQuery(viewQuery);
          result = JdbcAssert.toString(resultSet).trim();
          assertTrue(String.format("Generated string:\n%s\ndoes not match:\n%s", result, queryResult),
              queryResult.equals(result));

          statement.close();
          return null;
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    });
  }

  @Test
  public void testView1() throws Exception{
    testViewHelper(
        "CREATE VIEW testview1 AS SELECT * FROM cp.`region.json`",
        "testview1",
        "SELECT * FROM testview1 LIMIT 1",
        "region_id=0; sales_city=None; sales_state_province=None; sales_district=No District; " +
            "sales_region=No Region; sales_country=No Country; sales_district_id=0");
  }

  @Test
  public void testView2() throws Exception{
    testViewHelper(
        "CREATE VIEW testview2 AS SELECT region_id, sales_city FROM cp.`region.json`",
        "testview2",
        "SELECT * FROM testview2 LIMIT 2",
        "region_id=0; sales_city=None\nregion_id=1; sales_city=San Francisco");
  }

  @Test
  public void testView3() throws Exception{
    testViewHelper(
        "CREATE VIEW testview3(regionid, salescity) AS SELECT region_id, sales_city FROM cp.`region.json`",
        "testview3",
        "SELECT * FROM testview3 LIMIT 2",
        "regionid=0; salescity=None\nregionid=1; salescity=San Francisco");
  }

  @Test
  @Ignore // See DRILL-595 - can't project columns from inner query.
  public void testView4() throws Exception{
    testViewHelper(
        "CREATE VIEW testview1 AS SELECT * FROM cp.`region.json`",
        "testview1",
        "SELECT region_id, sales_city FROM testview1 LIMIT 2",
        "");
  }

  @Test
  public void testView5() throws Exception{
    testViewHelper(
        "CREATE VIEW testview2 AS SELECT region_id, sales_city FROM cp.`region.json`",
        "testview2",
        "SELECT region_id, sales_city FROM testview2 LIMIT 2",
        "region_id=0; sales_city=None\nregion_id=1; sales_city=San Francisco");
  }

  @Test
  public void testView6() throws Exception{
    testViewHelper(
        "CREATE VIEW testview2 AS SELECT region_id, sales_city FROM cp.`region.json`",
        "testview2",
        "SELECT sales_city FROM testview2 LIMIT 2",
        "sales_city=None\nsales_city=San Francisco");
  }

  @Test
  public void testView7() throws Exception{
    testViewHelper(
        "CREATE VIEW testview3(regionid, salescity) AS SELECT region_id, sales_city FROM cp.`region.json` LIMIT 2",
        "testview3",
        "SELECT regionid, salescity FROM testview3",
        "regionid=0; salescity=None\nregionid=1; salescity=San Francisco");
  }

  @Test
  public void testView8() throws Exception{
    testViewHelper(
        "CREATE VIEW testview3(regionid, salescity) AS " +
            "SELECT region_id, sales_city FROM cp.`region.json` ORDER BY region_id DESC",
        "testview3",
        "SELECT regionid FROM testview3 LIMIT 2",
        "regionid=109\nregionid=108");
  }

  @Test
  @Ignore // Query on testview2 fails with CannotPlanException. Seems to be an issue with Union.
  public void testView9() throws Exception{
    testViewHelper(
        "CREATE VIEW testview2 AS " +
            "SELECT region_id FROM cp.`region.json` " +
              "UNION " +
            "SELECT employee_id FROM cp.`employee.json`",
        "testview2",
        "SELECT sales_city FROM testview2 LIMIT 2",
        "sales_city=None\nsales_city=San Francisco");
  }

  @Test
  public void testViewOnHiveTable1() throws Exception{
    testViewHelper(
        "CREATE VIEW hiveview AS SELECT * FROM hive.kv",
        "hiveview",
        "SELECT * FROM hiveview LIMIT 1",
        "key=1; value= key_1");
  }

  @Test
  public void testViewOnHiveTable2() throws Exception{
    testViewHelper(
        "CREATE VIEW hiveview AS SELECT * FROM hive.kv",
        "hiveview",
        "SELECT key, `value` FROM hiveview LIMIT 1",
        "key=1; value= key_1");
  }

  @Test
  public void testViewOnHiveTable3() throws Exception{
    testViewHelper(
        "CREATE VIEW hiveview AS SELECT * FROM hive.kv",
        "hiveview",
        "SELECT `value` FROM hiveview LIMIT 1",
        "value= key_1");
  }

  @Test
  public void testViewOnHiveTable4() throws Exception{
    testViewHelper(
        "CREATE VIEW hiveview AS SELECT key, `value` FROM hive.kv",
        "hiveview",
        "SELECT * FROM hiveview LIMIT 1",
        "key=1; value= key_1");
  }

  @Test
  public void testViewOnHiveTable5() throws Exception{
    testViewHelper(
        "CREATE VIEW hiveview AS SELECT key, `value` FROM hive.kv",
        "hiveview",
        "SELECT key, `value` FROM hiveview LIMIT 1",
        "key=1; value= key_1");
  }

  @Test
  public void testDropView() throws Exception{
    JdbcAssert.withNoDefaultSchema().withConnection(new Function<Connection, Void>() {
      public Void apply(Connection connection) {
        try {
          Statement statement = connection.createStatement();

          // change default schema
          statement.executeQuery("USE dfs.`default`");

          // create view
          statement.executeQuery(
              "CREATE VIEW testview3(regionid) AS SELECT region_id FROM cp.`region.json`");

          // query from view
          ResultSet resultSet = statement.executeQuery("SELECT regionid FROM testview3 LIMIT 1");
          String result = JdbcAssert.toString(resultSet).trim();
          String expected = "regionid=0";
          assertTrue(String.format("Generated string:\n%s\ndoes not match:\n%s", result, expected),
              expected.equals(result));

          resultSet = statement.executeQuery("DROP VIEW testview3");
          result = JdbcAssert.toString(resultSet).trim();
          expected = "ok=true; summary=View 'testview3' deleted successfully from 'dfs.default' schema";
          assertTrue(String.format("Generated string:\n%s\ndoes not match:\n%s", result, expected),
              expected.equals(result));

          statement.close();
          return null;
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    });
  }

  @Test
  public void testShowDescribeTablesWithView() throws Exception{
    JdbcAssert.withNoDefaultSchema().withConnection(new Function<Connection, Void>() {
      public Void apply(Connection connection) {
        try {
          Statement statement = connection.createStatement();

          // change default schema
          statement.executeQuery("USE dfs.`default`");

          // create view
          statement.executeQuery(
              "CREATE VIEW testview3 AS SELECT * FROM hive.kv");

          // show tables on view
          ResultSet resultSet = statement.executeQuery("SHOW TABLES like 'testview3'");
          String result = JdbcAssert.toString(resultSet).trim();
          String expected =
              "TABLE_SCHEMA=dfs.default; TABLE_NAME=testview3\n" +
              "TABLE_SCHEMA=dfs; TABLE_NAME=testview3";
          assertTrue(String.format("Generated string:\n%s\ndoes not match:\n%s", result, expected),
              expected.equals(result));

          // describe a view
          resultSet = statement.executeQuery("DESCRIBE dfs.`default`.testview3");
          result = JdbcAssert.toString(resultSet).trim();
          expected =
              "COLUMN_NAME=key; DATA_TYPE=INTEGER; IS_NULLABLE=NO\n" +
              "COLUMN_NAME=value; DATA_TYPE=VARCHAR; IS_NULLABLE=NO";
          assertTrue(String.format("Generated string:\n%s\ndoes not match:\n%s", result, expected),
              expected.equals(result));

          statement.close();
          return null;
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    });
  }

  @Test
  public void testShowFiles() throws Exception {
    testQuery("show files from dfs.`/tmp`");

  }


  @Test
  public void testShowFilesWithDefaultSchema() throws Exception{
    JdbcAssert.withNoDefaultSchema().withConnection(new Function<Connection, Void>() {
      public Void apply(Connection connection) {
        try {
          Statement statement = connection.createStatement();

          // change default schema
          statement.executeQuery("USE dfs.`default`");

          // show files
          ResultSet resultSet = statement.executeQuery("show files from `/tmp`");

          System.out.println(JdbcAssert.toString(resultSet));

          statement.close();
          return null;
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    });
  }


}