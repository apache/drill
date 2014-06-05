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
import org.junit.Test;

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
public class TestMetadataDDL extends TestJdbcQuery {

  @Test
  public void testInfoSchema() throws Exception{
    testQuery("select * from INFORMATION_SCHEMA.SCHEMATA");
    testQuery("select * from INFORMATION_SCHEMA.CATALOGS");
    testQuery("select * from INFORMATION_SCHEMA.VIEWS");
    testQuery("select * from INFORMATION_SCHEMA.`TABLES`");
    testQuery("select * from INFORMATION_SCHEMA.COLUMNS");
  }

  @Test
  public void testShowTables() throws Exception{
    JdbcAssert.withNoDefaultSchema()
        .sql("SHOW TABLES")
        .returns(
            "TABLE_SCHEMA=hive.default; TABLE_NAME=kv\n" +
            "TABLE_SCHEMA=hive.default; TABLE_NAME=foodate\n" +
            "TABLE_SCHEMA=hive.db1; TABLE_NAME=kv_db1\n" +
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
        .sql("SHOW TABLES IN hive.`default`")
        .returns(
            "TABLE_SCHEMA=hive.default; TABLE_NAME=kv\n" +
            "TABLE_SCHEMA=hive.default; TABLE_NAME=foodate\n");
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
        "SCHEMA_NAME=dfs.home\n" +
        "SCHEMA_NAME=dfs.default\n" +
        "SCHEMA_NAME=dfs.tmp\n" +
        "SCHEMA_NAME=cp.default\n" +
        "SCHEMA_NAME=sys\n" +
        "SCHEMA_NAME=INFORMATION_SCHEMA\n";

    JdbcAssert.withNoDefaultSchema().sql("SHOW DATABASES").returns(expected);
    JdbcAssert.withNoDefaultSchema().sql("SHOW SCHEMAS").returns(expected);
  }

  @Test
  public void testShowDatabasesWhere() throws Exception{
    JdbcAssert.withNoDefaultSchema()
        .sql("SHOW DATABASES WHERE SCHEMA_NAME='dfs.tmp'")
        .returns("SCHEMA_NAME=dfs.tmp\n");
  }

  @Test
  public void testShowDatabasesLike() throws Exception{
    JdbcAssert.withNoDefaultSchema()
        .sql("SHOW DATABASES LIKE '%i%'")
        .returns(
            "SCHEMA_NAME=hive.default\n"+
            "SCHEMA_NAME=hive.db1");
  }

  @Test
  public void testDescribeTable() throws Exception{
    JdbcAssert.withNoDefaultSchema()
        .sql("DESCRIBE CATALOGS")
        .returns(
            "COLUMN_NAME=CATALOG_NAME; DATA_TYPE=VARCHAR; IS_NULLABLE=NO\n"+
            "COLUMN_NAME=CATALOG_DESCRIPTION; DATA_TYPE=VARCHAR; IS_NULLABLE=NO\n"+
            "COLUMN_NAME=CATALOG_CONNECT; DATA_TYPE=VARCHAR; IS_NULLABLE=NO\n");
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
  public void testDefaultSchemaDfs() throws Exception{
    JdbcAssert.withFull("dfs")
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
    JdbcAssert.withFull("hive")
        .sql("SELECT * FROM kv LIMIT 2")
        .returns(
            "key=1; value= key_1\n" +
            "key=2; value= key_2\n");
  }

  @Test
  public void testDefaultTwoLevelSchemaHive() throws Exception{
    JdbcAssert.withFull("hive.db1")
        .sql("SELECT * FROM `kv_db1` LIMIT 2")
        .returns(
            "key=1; value= key_1\n" +
            "key=2; value= key_2\n");
  }

  @Test
  public void testQueryFromNonDefaultSchema() throws Exception{
    JdbcAssert.withFull("hive")
        .sql("SELECT full_name FROM cp.`employee.json` LIMIT 2")
        .returns(
            "full_name=Sheri Nowmer\n" +
            "full_name=Derrick Whelply\n");
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
          resultSet.close();
          String expected = "ok=true; summary=Default schema changed to 'hive.db1'";
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
  // select * from `dfs.tmp`.`/tmp/nation.parquet`;
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
          ResultSet resultSet = statement.executeQuery("USE `dfs.default`");
          String result = JdbcAssert.toString(resultSet).trim();
          resultSet.close();
          String expected = "ok=true; summary=Default schema changed to 'dfs.default'";
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
    testQuery("show files from dfs.`/tmp`");
    testQuery("show files from `dfs.default`.`/tmp`");

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
