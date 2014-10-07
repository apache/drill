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

import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Set;

import org.apache.drill.exec.store.hive.HiveTestDataGenerator;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;

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
        .returnsSet(ImmutableSet.of(
            "CATALOG_NAME=DRILL; SCHEMA_NAME=dfs.default; SCHEMA_OWNER=<owner>; TYPE=file; IS_MUTABLE=NO",
            "CATALOG_NAME=DRILL; SCHEMA_NAME=dfs.root; SCHEMA_OWNER=<owner>; TYPE=file; IS_MUTABLE=NO",
            "CATALOG_NAME=DRILL; SCHEMA_NAME=dfs.tmp; SCHEMA_OWNER=<owner>; TYPE=file; IS_MUTABLE=YES",
            "CATALOG_NAME=DRILL; SCHEMA_NAME=sys; SCHEMA_OWNER=<owner>; TYPE=system-tables; IS_MUTABLE=NO",
            "CATALOG_NAME=DRILL; SCHEMA_NAME=dfs_test.home; SCHEMA_OWNER=<owner>; TYPE=file; IS_MUTABLE=NO",
            "CATALOG_NAME=DRILL; SCHEMA_NAME=dfs_test.default; SCHEMA_OWNER=<owner>; TYPE=file; IS_MUTABLE=NO",
            "CATALOG_NAME=DRILL; SCHEMA_NAME=dfs_test.tmp; SCHEMA_OWNER=<owner>; TYPE=file; IS_MUTABLE=YES",
            "CATALOG_NAME=DRILL; SCHEMA_NAME=cp.default; SCHEMA_OWNER=<owner>; TYPE=file; IS_MUTABLE=NO",
            "CATALOG_NAME=DRILL; SCHEMA_NAME=hive_test.default; SCHEMA_OWNER=<owner>; TYPE=hive; IS_MUTABLE=NO",
            "CATALOG_NAME=DRILL; SCHEMA_NAME=hive_test.db1; SCHEMA_OWNER=<owner>; TYPE=hive; IS_MUTABLE=NO",
            "CATALOG_NAME=DRILL; SCHEMA_NAME=INFORMATION_SCHEMA; SCHEMA_OWNER=<owner>; TYPE=ischema; IS_MUTABLE=NO")
        );
  }

  @Test
  public void testShowTables() throws Exception{
    JdbcAssert.withFull("hive_test.default")
        .sql("SHOW TABLES")
        .returnsSet(ImmutableSet.of(
            "TABLE_SCHEMA=hive_test.default; TABLE_NAME=readtest",
            "TABLE_SCHEMA=hive_test.default; TABLE_NAME=empty_table",
            "TABLE_SCHEMA=hive_test.default; TABLE_NAME=infoschematest",
            "TABLE_SCHEMA=hive_test.default; TABLE_NAME=hiveview",
            "TABLE_SCHEMA=hive_test.default; TABLE_NAME=kv",
            "TABLE_SCHEMA=hive_test.default; TABLE_NAME=foodate")
        );
  }

  @Test
  public void testShowTablesFromDb() throws Exception{
    JdbcAssert.withNoDefaultSchema()
        .sql("SHOW TABLES FROM INFORMATION_SCHEMA")
        .returnsSet(ImmutableSet.of(
            "TABLE_SCHEMA=INFORMATION_SCHEMA; TABLE_NAME=VIEWS",
            "TABLE_SCHEMA=INFORMATION_SCHEMA; TABLE_NAME=COLUMNS",
            "TABLE_SCHEMA=INFORMATION_SCHEMA; TABLE_NAME=TABLES",
            "TABLE_SCHEMA=INFORMATION_SCHEMA; TABLE_NAME=CATALOGS",
            "TABLE_SCHEMA=INFORMATION_SCHEMA; TABLE_NAME=SCHEMATA"));

    JdbcAssert.withFull("dfs_test.tmp")
        .sql("SHOW TABLES IN hive_test.`default`")
        .returnsSet(ImmutableSet.of(
            "TABLE_SCHEMA=hive_test.default; TABLE_NAME=readtest",
            "TABLE_SCHEMA=hive_test.default; TABLE_NAME=empty_table",
            "TABLE_SCHEMA=hive_test.default; TABLE_NAME=infoschematest",
            "TABLE_SCHEMA=hive_test.default; TABLE_NAME=hiveview",
            "TABLE_SCHEMA=hive_test.default; TABLE_NAME=kv",
            "TABLE_SCHEMA=hive_test.default; TABLE_NAME=foodate"));
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
    Set<String> expected = ImmutableSet.of(
        "SCHEMA_NAME=dfs.default",
        "SCHEMA_NAME=dfs.root",
        "SCHEMA_NAME=dfs.tmp",
        "SCHEMA_NAME=sys",
        "SCHEMA_NAME=dfs_test.home",
        "SCHEMA_NAME=dfs_test.default",
        "SCHEMA_NAME=dfs_test.tmp",
        "SCHEMA_NAME=cp.default",
        "SCHEMA_NAME=hive_test.default",
        "SCHEMA_NAME=hive_test.db1",
        "SCHEMA_NAME=INFORMATION_SCHEMA");

    JdbcAssert.withNoDefaultSchema().sql("SHOW DATABASES").returnsSet(expected);
    JdbcAssert.withNoDefaultSchema().sql("SHOW SCHEMAS").returnsSet(expected);
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
        .returnsSet(ImmutableSet.of(
            "SCHEMA_NAME=hive_test.default",
            "SCHEMA_NAME=hive_test.db1"));
  }

  @Test
  public void testDescribeTable() throws Exception{
    JdbcAssert.withFull("INFORMATION_SCHEMA")
        .sql("DESCRIBE CATALOGS")
        .returnsSet(ImmutableSet.of(
            "COLUMN_NAME=CATALOG_NAME; DATA_TYPE=VARCHAR; IS_NULLABLE=NO",
            "COLUMN_NAME=CATALOG_DESCRIPTION; DATA_TYPE=VARCHAR; IS_NULLABLE=NO",
            "COLUMN_NAME=CATALOG_CONNECT; DATA_TYPE=VARCHAR; IS_NULLABLE=NO"));
  }


  @Test
  public void testDescribeTableNullableColumns() throws Exception{
    JdbcAssert.withNoDefaultSchema()
        .sql("DESCRIBE hive_test.`default`.kv")
        .returnsSet(ImmutableSet.of(
            "COLUMN_NAME=key; DATA_TYPE=INTEGER; IS_NULLABLE=YES",
            "COLUMN_NAME=value; DATA_TYPE=VARCHAR; IS_NULLABLE=YES"));
  }

  @Test
  public void testDescribeTableWithSchema() throws Exception{
    JdbcAssert.withNoDefaultSchema()
        .sql("DESCRIBE INFORMATION_SCHEMA.`TABLES`")
        .returnsSet(ImmutableSet.of(
            "COLUMN_NAME=TABLE_CATALOG; DATA_TYPE=VARCHAR; IS_NULLABLE=NO",
            "COLUMN_NAME=TABLE_SCHEMA; DATA_TYPE=VARCHAR; IS_NULLABLE=NO",
            "COLUMN_NAME=TABLE_NAME; DATA_TYPE=VARCHAR; IS_NULLABLE=NO",
            "COLUMN_NAME=TABLE_TYPE; DATA_TYPE=VARCHAR; IS_NULLABLE=NO"));
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
          Set<String> result = JdbcAssert.toStringSet(resultSet);
          resultSet.close();
          ImmutableSet<String> expected = ImmutableSet.of("COLUMN_NAME=key; DATA_TYPE=INTEGER; IS_NULLABLE=NO");
          assertTrue(String.format("Generated string:\n%s\ndoes not match:\n%s", result, expected), expected.equals(result));

          // Test describe of `TABLES` with a schema qualifier which is not in default schema
          resultSet = statement.executeQuery("DESCRIBE INFORMATION_SCHEMA.`TABLES`");
          result = JdbcAssert.toStringSet(resultSet);
          resultSet.close();
          expected = ImmutableSet.of(
              "COLUMN_NAME=TABLE_CATALOG; DATA_TYPE=VARCHAR; IS_NULLABLE=NO",
              "COLUMN_NAME=TABLE_SCHEMA; DATA_TYPE=VARCHAR; IS_NULLABLE=NO",
              "COLUMN_NAME=TABLE_NAME; DATA_TYPE=VARCHAR; IS_NULLABLE=NO",
              "COLUMN_NAME=TABLE_TYPE; DATA_TYPE=VARCHAR; IS_NULLABLE=NO");
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
        .returnsSet(ImmutableSet.of(
            "COLUMN_NAME=TABLE_CATALOG; DATA_TYPE=VARCHAR; IS_NULLABLE=NO",
            "COLUMN_NAME=TABLE_SCHEMA; DATA_TYPE=VARCHAR; IS_NULLABLE=NO",
            "COLUMN_NAME=TABLE_NAME; DATA_TYPE=VARCHAR; IS_NULLABLE=NO"));
  }

  @Test
  public void testDescribeTableWithSchemaAndColQualifier() throws Exception{
    JdbcAssert.withNoDefaultSchema()
        .sql("DESCRIBE INFORMATION_SCHEMA.SCHEMATA 'SCHEMA%'")
        .returnsSet(ImmutableSet.of(
            "COLUMN_NAME=SCHEMA_NAME; DATA_TYPE=VARCHAR; IS_NULLABLE=NO",
            "COLUMN_NAME=SCHEMA_OWNER; DATA_TYPE=VARCHAR; IS_NULLABLE=NO"));
  }

  @Test
  public void testVarCharMaxLengthAndDecimalPrecisionInInfoSchema() throws Exception{
    JdbcAssert.withNoDefaultSchema()
        .sql("SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION " +
            "FROM INFORMATION_SCHEMA.`COLUMNS` " +
            "WHERE TABLE_SCHEMA = 'hive_test.default' AND TABLE_NAME = 'infoschematest' AND " +
            "(COLUMN_NAME = 'stringtype' OR COLUMN_NAME = 'varchartype' OR " +
            "COLUMN_NAME = 'inttype' OR COLUMN_NAME = 'decimaltype')")
        .returnsSet(ImmutableSet.of(
            "COLUMN_NAME=inttype; DATA_TYPE=INTEGER; CHARACTER_MAXIMUM_LENGTH=-1; NUMERIC_PRECISION=-1",
            "COLUMN_NAME=decimaltype; DATA_TYPE=DECIMAL; CHARACTER_MAXIMUM_LENGTH=-1; NUMERIC_PRECISION=38",
            "COLUMN_NAME=stringtype; DATA_TYPE=VARCHAR; CHARACTER_MAXIMUM_LENGTH=65535; NUMERIC_PRECISION=-1",
            "COLUMN_NAME=varchartype; DATA_TYPE=VARCHAR; CHARACTER_MAXIMUM_LENGTH=20; NUMERIC_PRECISION=-1"));
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
