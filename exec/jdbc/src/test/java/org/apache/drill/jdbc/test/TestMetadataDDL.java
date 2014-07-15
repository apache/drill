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

import com.google.common.base.Function;
import org.apache.commons.io.FileUtils;
import org.apache.drill.exec.store.hive.HiveTestDataGenerator;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.Assert.assertTrue;

/**
 * Contains tests for
 * -- InformationSchema
 * -- queries on InformationSchema such as SHOW TABLES, SHOW SCHEMAS or DESCRIBE table
 * -- USE schema
 * -- SHOW FILES
 */
public class TestMetadataDDL extends JdbcTestQueryBase {

  @BeforeClass
  public static void generateHive() throws Exception{
    new HiveTestDataGenerator().generateTestData();
  }

  @Test
  public void testInfoSchema() throws Exception{
    testQuery("select * from INFORMATION_SCHEMA.SCHEMATA");
    testQuery("select * from INFORMATION_SCHEMA.CATALOGS");
    testQuery("select * from INFORMATION_SCHEMA.VIEWS");
    testQuery("select * from INFORMATION_SCHEMA.`TABLES`");
    testQuery("select * from INFORMATION_SCHEMA.COLUMNS");
  }

  @Test
  public void testSchemata() throws Exception{
    JdbcAssert.withNoDefaultSchema()
        .sql("SELECT * FROM INFORMATION_SCHEMA.SCHEMATA")
        .returns(
            "CATALOG_NAME=DRILL; SCHEMA_NAME=dfs.default; SCHEMA_OWNER=<owner>; TYPE=file; IS_MUTABLE=NO\n" +
            "CATALOG_NAME=DRILL; SCHEMA_NAME=dfs.root; SCHEMA_OWNER=<owner>; TYPE=file; IS_MUTABLE=NO\n" +
            "CATALOG_NAME=DRILL; SCHEMA_NAME=dfs.tmp; SCHEMA_OWNER=<owner>; TYPE=file; IS_MUTABLE=YES\n" +
            "CATALOG_NAME=DRILL; SCHEMA_NAME=sys; SCHEMA_OWNER=<owner>; TYPE=system-tables; IS_MUTABLE=NO\n" +
            "CATALOG_NAME=DRILL; SCHEMA_NAME=dfs_test.home; SCHEMA_OWNER=<owner>; TYPE=file; IS_MUTABLE=NO\n" +
            "CATALOG_NAME=DRILL; SCHEMA_NAME=dfs_test.default; SCHEMA_OWNER=<owner>; TYPE=file; IS_MUTABLE=NO\n" +
            "CATALOG_NAME=DRILL; SCHEMA_NAME=dfs_test.tmp; SCHEMA_OWNER=<owner>; TYPE=file; IS_MUTABLE=YES\n" +
            "CATALOG_NAME=DRILL; SCHEMA_NAME=cp.default; SCHEMA_OWNER=<owner>; TYPE=file; IS_MUTABLE=NO\n" +
            "CATALOG_NAME=DRILL; SCHEMA_NAME=hive_test.default; SCHEMA_OWNER=<owner>; TYPE=hive; IS_MUTABLE=NO\n" +
            "CATALOG_NAME=DRILL; SCHEMA_NAME=hive_test.db1; SCHEMA_OWNER=<owner>; TYPE=hive; IS_MUTABLE=NO\n" +
            "CATALOG_NAME=DRILL; SCHEMA_NAME=INFORMATION_SCHEMA; SCHEMA_OWNER=<owner>; TYPE=ischema; IS_MUTABLE=NO\n"
        );
  }

  @Test
  public void testShowTables() throws Exception{
    JdbcAssert.withFull("hive_test.default")
        .sql("SHOW TABLES")
        .returns(
            "TABLE_SCHEMA=hive_test.default; TABLE_NAME=readtest\n" +
            "TABLE_SCHEMA=hive_test.default; TABLE_NAME=empty_table\n" +
            "TABLE_SCHEMA=hive_test.default; TABLE_NAME=infoschematest\n" +
            "TABLE_SCHEMA=hive_test.default; TABLE_NAME=hiveview\n" +
            "TABLE_SCHEMA=hive_test.default; TABLE_NAME=kv\n" +
            "TABLE_SCHEMA=hive_test.default; TABLE_NAME=foodate\n"
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

    JdbcAssert.withFull("dfs_test.tmp")
        .sql("SHOW TABLES IN hive_test.`default`")
        .returns(
            "TABLE_SCHEMA=hive_test.default; TABLE_NAME=readtest\n" +
            "TABLE_SCHEMA=hive_test.default; TABLE_NAME=empty_table\n" +
            "TABLE_SCHEMA=hive_test.default; TABLE_NAME=infoschematest\n" +
            "TABLE_SCHEMA=hive_test.default; TABLE_NAME=hiveview\n" +
            "TABLE_SCHEMA=hive_test.default; TABLE_NAME=kv\n" +
            "TABLE_SCHEMA=hive_test.default; TABLE_NAME=foodate\n");
  }

  @Test
  public void testShowTablesFromDbWhere() throws Exception{
    JdbcAssert.withNoDefaultSchema()
        .sql("SHOW TABLES FROM INFORMATION_SCHEMA WHERE TABLE_NAME='VIEWS'")
        .returns("TABLE_SCHEMA=INFORMATION_SCHEMA; TABLE_NAME=VIEWS\n");
  }

  @Test
  public void testShowTablesLike() throws Exception{
    JdbcAssert.withFull("INFORMATION_SCHEMA")
        .sql("SHOW TABLES LIKE '%CH%'")
        .returns("TABLE_SCHEMA=INFORMATION_SCHEMA; TABLE_NAME=SCHEMATA\n");
  }

  @Test
  public void testShowDatabases() throws Exception{
    String expected =
        "SCHEMA_NAME=dfs.default\n" +
        "SCHEMA_NAME=dfs.root\n" +
        "SCHEMA_NAME=dfs.tmp\n" +
        "SCHEMA_NAME=sys\n" +
        "SCHEMA_NAME=dfs_test.home\n" +
        "SCHEMA_NAME=dfs_test.default\n" +
        "SCHEMA_NAME=dfs_test.tmp\n" +
        "SCHEMA_NAME=cp.default\n" +
        "SCHEMA_NAME=hive_test.default\n" +
        "SCHEMA_NAME=hive_test.db1\n" +
        "SCHEMA_NAME=INFORMATION_SCHEMA\n";

    JdbcAssert.withNoDefaultSchema().sql("SHOW DATABASES").returns(expected);
    JdbcAssert.withNoDefaultSchema().sql("SHOW SCHEMAS").returns(expected);
  }

  @Test
  public void testShowDatabasesWhere() throws Exception{
    JdbcAssert.withNoDefaultSchema()
        .sql("SHOW DATABASES WHERE SCHEMA_NAME='dfs_test.tmp'")
        .returns("SCHEMA_NAME=dfs_test.tmp\n");
  }

  @Test
  public void testShowDatabasesLike() throws Exception{
    JdbcAssert.withNoDefaultSchema()
        .sql("SHOW DATABASES LIKE '%i%'")
        .returns(
            "SCHEMA_NAME=hive_test.default\n"+
            "SCHEMA_NAME=hive_test.db1");
  }

  @Test
  public void testDescribeTable() throws Exception{
    JdbcAssert.withFull("INFORMATION_SCHEMA")
        .sql("DESCRIBE CATALOGS")
        .returns(
            "COLUMN_NAME=CATALOG_NAME; DATA_TYPE=VARCHAR; IS_NULLABLE=NO\n"+
            "COLUMN_NAME=CATALOG_DESCRIPTION; DATA_TYPE=VARCHAR; IS_NULLABLE=NO\n"+
            "COLUMN_NAME=CATALOG_CONNECT; DATA_TYPE=VARCHAR; IS_NULLABLE=NO\n");
  }


  @Test
  public void testDescribeTableNullableColumns() throws Exception{
    JdbcAssert.withNoDefaultSchema()
        .sql("DESCRIBE hive_test.`default`.kv")
        .returns(
            "COLUMN_NAME=key; DATA_TYPE=INTEGER; IS_NULLABLE=YES\n" +
            "COLUMN_NAME=value; DATA_TYPE=VARCHAR; IS_NULLABLE=YES\n"
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
            "COLUMN_NAME=TABLE_TYPE; DATA_TYPE=VARCHAR; IS_NULLABLE=NO\n");
  }

  @Test
  public void testDescribeSameTableInMultipleSchemas() throws Exception{
    JdbcAssert.withNoDefaultSchema().withConnection(new Function<Connection, Void>() {
      public Void apply(Connection connection) {
        try {
          Statement statement = connection.createStatement();
          statement.executeQuery("USE dfs_test.tmp").close();

          // INFORMATION_SCHEMA already has a table named "TABLES". Now create a table with same name in "dfs_test.tmp" schema
          statement.executeQuery("CREATE OR REPLACE VIEW `TABLES` AS SELECT key FROM hive_test.kv").close();

          // Test describe of `TABLES` with no schema qualifier
          ResultSet resultSet = statement.executeQuery("DESCRIBE `TABLES`");
          String result = JdbcAssert.toString(resultSet).trim();
          resultSet.close();
          String expected = "COLUMN_NAME=key; DATA_TYPE=INTEGER; IS_NULLABLE=NO";
          assertTrue(String.format("Generated string:\n%s\ndoes not match:\n%s", result, expected), expected.equals(result));

          // Test describe of `TABLES` with a schema qualifier which is not in default schema
          resultSet = statement.executeQuery("DESCRIBE INFORMATION_SCHEMA.`TABLES`");
          result = JdbcAssert.toString(resultSet).trim();
          resultSet.close();
          expected =
              "COLUMN_NAME=TABLE_CATALOG; DATA_TYPE=VARCHAR; IS_NULLABLE=NO\n" +
              "COLUMN_NAME=TABLE_SCHEMA; DATA_TYPE=VARCHAR; IS_NULLABLE=NO\n" +
              "COLUMN_NAME=TABLE_NAME; DATA_TYPE=VARCHAR; IS_NULLABLE=NO\n" +
              "COLUMN_NAME=TABLE_TYPE; DATA_TYPE=VARCHAR; IS_NULLABLE=NO";
          assertTrue(String.format("Generated string:\n%s\ndoes not match:\n%s", result, expected), expected.equals(result));

          // drop created view
          statement.executeQuery("DROP VIEW `TABLES`").close();

          statement.close();
          return null;
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    });
  }

  @Test
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
  public void testDescribeTableWithColQualifier() throws Exception{
    JdbcAssert.withFull("INFORMATION_SCHEMA")
        .sql("DESCRIBE COLUMNS 'TABLE%'")
        .returns(
            "COLUMN_NAME=TABLE_CATALOG; DATA_TYPE=VARCHAR; IS_NULLABLE=NO\n"+
            "COLUMN_NAME=TABLE_SCHEMA; DATA_TYPE=VARCHAR; IS_NULLABLE=NO\n"+
            "COLUMN_NAME=TABLE_NAME; DATA_TYPE=VARCHAR; IS_NULLABLE=NO\n");
  }

  @Test
  public void testDescribeTableWithSchemaAndColQualifier() throws Exception{
    JdbcAssert.withNoDefaultSchema()
        .sql("DESCRIBE INFORMATION_SCHEMA.SCHEMATA 'SCHEMA%'")
        .returns(
            "COLUMN_NAME=SCHEMA_NAME; DATA_TYPE=VARCHAR; IS_NULLABLE=NO\n"+
            "COLUMN_NAME=SCHEMA_OWNER; DATA_TYPE=VARCHAR; IS_NULLABLE=NO\n");
  }

  @Test
  public void testVarCharMaxLengthAndDecimalPrecisionInInfoSchema() throws Exception{
    JdbcAssert.withNoDefaultSchema()
        .sql("SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION " +
            "FROM INFORMATION_SCHEMA.`COLUMNS` " +
            "WHERE TABLE_SCHEMA = 'hive_test.default' AND TABLE_NAME = 'infoschematest' AND " +
            "(COLUMN_NAME = 'stringtype' OR COLUMN_NAME = 'varchartype' OR " +
            "COLUMN_NAME = 'inttype' OR COLUMN_NAME = 'decimaltype')")
        .returns(
            "COLUMN_NAME=inttype; DATA_TYPE=INTEGER; CHARACTER_MAXIMUM_LENGTH=-1; NUMERIC_PRECISION=-1\n" +
            "COLUMN_NAME=decimaltype; DATA_TYPE=DECIMAL; CHARACTER_MAXIMUM_LENGTH=-1; NUMERIC_PRECISION=38\n" +
            "COLUMN_NAME=stringtype; DATA_TYPE=VARCHAR; CHARACTER_MAXIMUM_LENGTH=65535; NUMERIC_PRECISION=-1\n" +
            "COLUMN_NAME=varchartype; DATA_TYPE=VARCHAR; CHARACTER_MAXIMUM_LENGTH=20; NUMERIC_PRECISION=-1");
  }

  @Test
  public void testDefaultSchemaDfs() throws Exception{
    JdbcAssert.withFull("dfs_test")
        .sql(String.format("SELECT R_REGIONKEY FROM `%s/../../sample-data/region.parquet` LIMIT 2", WORKING_PATH))
        .returns(
            "R_REGIONKEY=0\n" +
            "R_REGIONKEY=1\n");
  }

  @Test
  public void testDefaultSchemaClasspath() throws Exception{
    JdbcAssert.withFull("cp")
        .sql("SELECT full_name FROM `employee.json` LIMIT 2")
        .returns(
            "full_name=Sheri Nowmer\n" +
            "full_name=Derrick Whelply\n");
  }

  @Test
  public void testDefaultSchemaHive() throws Exception{
    JdbcAssert.withFull("hive_test")
        .sql("SELECT * FROM kv LIMIT 2")
        .returns(
            "key=1; value= key_1\n" +
            "key=2; value= key_2\n");
  }

  @Test
  public void testDefaultTwoLevelSchemaHive() throws Exception{
    JdbcAssert.withFull("hive_test.db1")
        .sql("SELECT * FROM `kv_db1` LIMIT 2")
        .returns(
            "key=1; value= key_1\n" +
            "key=2; value= key_2\n");
  }

  @Test
  public void testQueryFromNonDefaultSchema() throws Exception{
    JdbcAssert.withFull("hive_test")
        .sql("SELECT full_name FROM cp.`employee.json` LIMIT 2")
        .returns(
            "full_name=Sheri Nowmer\n" +
            "full_name=Derrick Whelply\n");
  }

  @Test
  public void testUseSchema() throws Exception{
    JdbcAssert.withNoDefaultSchema()
        .sql("USE hive_test.`default`")
        .returns("ok=true; summary=Default schema changed to 'hive_test.default'");
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
          ResultSet resultSet = statement.executeQuery("USE hive_test.db1");
          String result = JdbcAssert.toString(resultSet).trim();
          resultSet.close();
          String expected = "ok=true; summary=Default schema changed to 'hive_test.db1'";
          assertTrue(String.format("Generated string:\n%s\ndoes not match:\n%s", result, expected), expected.equals(result));


          resultSet = statement.executeQuery("SELECT * FROM kv_db1 LIMIT 2");
          result = JdbcAssert.toString(resultSet).trim();
          resultSet.close();
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

  // Tests using backticks around the complete schema path
  // select * from `dfs_test.tmp`.`/tmp/nation.parquet`;
  @Test
  public void testCompleteSchemaRef1() throws Exception {
    testQuery("select * from `cp.default`.`employee.json` limit 2");
  }

  @Test
  public void testCompleteSchemaRef2() throws Exception {
    JdbcAssert.withNoDefaultSchema().withConnection(new Function<Connection, Void>() {
      public Void apply(Connection connection) {
        try {
          Statement statement = connection.createStatement();

          // change default schema
          ResultSet resultSet = statement.executeQuery("USE `dfs_test.default`");
          String result = JdbcAssert.toString(resultSet).trim();
          resultSet.close();
          String expected = "ok=true; summary=Default schema changed to 'dfs_test.default'";
          assertTrue(String.format("Generated string:\n%s\ndoes not match:\n%s", result, expected), expected.equals(result));

          resultSet =  statement.executeQuery(
              String.format("select R_REGIONKEY from `%s/../../sample-data/region.parquet` LIMIT 1", WORKING_PATH));
          result = JdbcAssert.toString(resultSet).trim();
          resultSet.close();
          expected = "R_REGIONKEY=0";
          assertTrue(String.format("Generated string:\n%s\ndoes not match:\n%s", result, expected),
              expected.equals(result));

          return null;
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    });
  }

  @Test
  public void testShowFiles() throws Exception {
    testQuery("show files from dfs_test.`/tmp`");
    testQuery("show files from `dfs_test.default`.`/tmp`");

  }

  @Test
  public void testShowFilesWithDefaultSchema() throws Exception{
    JdbcAssert.withNoDefaultSchema().withConnection(new Function<Connection, Void>() {
      public Void apply(Connection connection) {
        try {
          Statement statement = connection.createStatement();

          // change default schema
          statement.executeQuery("USE dfs_test.`default`");

          // show files
          ResultSet resultSet = statement.executeQuery("show files from `/tmp`");

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
}
