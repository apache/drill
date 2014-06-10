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
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.Assert.assertTrue;

/** Contains tests for creating/droping and using views in Drill. */
public class TestViews extends TestJdbcQuery {

  private final static String NEW_LINE = System.getProperty("line.separator");

  /** Helper test method for view tests */
  private void testViewHelper(final String viewCreate, final String viewName,
                              final String viewQuery, final String queryResult) throws Exception{
    JdbcAssert.withNoDefaultSchema().withConnection(new Function<Connection, Void>() {
      public Void apply(Connection connection) {
        try {
          Statement statement = connection.createStatement();

          // change default schema
          statement.executeQuery("USE dfs.tmp");

          // create view
          ResultSet resultSet = statement.executeQuery(viewCreate);
          String result = JdbcAssert.toString(resultSet).trim();
          resultSet.close();
          String viewCreateResult = "ok=true; summary=View '" + viewName + "' created successfully in 'dfs.tmp' schema";
          assertTrue(String.format("Generated string:\n%s\ndoes not match:\n%s", result, viewCreateResult),
              viewCreateResult.equals(result));

          // query from view
          resultSet = statement.executeQuery(viewQuery);
          result = JdbcAssert.toString(resultSet).trim();
          resultSet.close();
          assertTrue(String.format("Generated string:\n%s\ndoes not match:\n%s", result, queryResult),
              queryResult.equals(result));

          statement.executeQuery("drop view " + viewName).close();

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
          statement.executeQuery("USE dfs.tmp");

          // create view
          statement.executeQuery(
              "CREATE VIEW testview3(regionid) AS SELECT region_id FROM cp.`region.json`");

          // query from view
          ResultSet resultSet = statement.executeQuery("SELECT regionid FROM testview3 LIMIT 1");
          String result = JdbcAssert.toString(resultSet).trim();
          resultSet.close();
          String expected = "regionid=0";
          assertTrue(String.format("Generated string:\n%s\ndoes not match:\n%s", result, expected),
              expected.equals(result));

          resultSet = statement.executeQuery("DROP VIEW testview3");
          result = JdbcAssert.toString(resultSet).trim();
          resultSet.close();
          expected = "ok=true; summary=View 'testview3' deleted successfully from 'dfs.tmp' schema";
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
  public void testInfoSchemaWithView() throws Exception{
    JdbcAssert.withNoDefaultSchema().withConnection(new Function<Connection, Void>() {
      public Void apply(Connection connection) {
        try {
          Statement statement = connection.createStatement();

          // change default schema
          statement.executeQuery("USE dfs.tmp");

          // create view
          statement.executeQuery(
              "CREATE VIEW testview3 AS SELECT * FROM hive.kv");

          // show tables on view
          ResultSet resultSet = statement.executeQuery("SHOW TABLES like 'testview3'");
          String result = JdbcAssert.toString(resultSet).trim();
          resultSet.close();
          String expected = "TABLE_SCHEMA=dfs.tmp; TABLE_NAME=testview3";
          assertTrue(String.format("Generated string:\n%s\ndoes not match:\n%s", result, expected),
              expected.equals(result));

          // test record in INFORMATION_SCHEMA.VIEWS
          resultSet = statement.executeQuery("SELECT * FROM INFORMATION_SCHEMA.VIEWS " +
              "WHERE TABLE_NAME = 'testview3'");
          result = JdbcAssert.toString(resultSet).trim();
          resultSet.close();
          expected = "TABLE_CATALOG=DRILL; TABLE_SCHEMA=dfs.tmp; TABLE_NAME=testview3; VIEW_DEFINITION=SELECT *"+NEW_LINE+"FROM `hive`.`kv`";
          assertTrue(String.format("Generated string:\n%s\ndoes not match:\n%s", result, expected),
              expected.equals(result));

          // test record in INFORMATION_SCHEMA.TABLES
          resultSet = statement.executeQuery("SELECT * FROM INFORMATION_SCHEMA.`TABLES` " +
              "WHERE TABLE_NAME = 'testview3'");
          result = JdbcAssert.toString(resultSet).trim();
          resultSet.close();
          expected = "TABLE_CATALOG=DRILL; TABLE_SCHEMA=dfs.tmp; TABLE_NAME=testview3; TABLE_TYPE=VIEW";
          assertTrue(String.format("Generated string:\n%s\ndoes not match:\n%s", result, expected),
              expected.equals(result));

          // describe a view
          resultSet = statement.executeQuery("DESCRIBE dfs.tmp.testview3");
          result = JdbcAssert.toString(resultSet).trim();
          resultSet.close();
          expected =
              "COLUMN_NAME=key; DATA_TYPE=INTEGER; IS_NULLABLE=NO\n" +
              "COLUMN_NAME=value; DATA_TYPE=VARCHAR; IS_NULLABLE=NO";
          assertTrue(String.format("Generated string:\n%s\ndoes not match:\n%s", result, expected),
              expected.equals(result));

          statement.executeQuery("drop view testview3").close();

          statement.close();
          return null;
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    });
  }

  @Test
  public void testViewWithFullSchemaIdentifier() throws Exception{
    JdbcAssert.withNoDefaultSchema().withConnection(new Function<Connection, Void>() {
      public Void apply(Connection connection) {
        try {
          Statement statement = connection.createStatement();

          // change default schema
          statement.executeQuery("USE cp");

          // create a view with full schema identifier
          ResultSet resultSet =  statement.executeQuery("CREATE VIEW dfs.tmp.testview AS SELECT * FROM hive.kv");
          String result = JdbcAssert.toString(resultSet).trim();
          resultSet.close();
          String expected = "ok=true; summary=View 'testview' created successfully in 'dfs.tmp' schema";
          assertTrue(String.format("Generated string:\n%s\ndoes not match:\n%s", result, expected),
              expected.equals(result));

          // query from view
          resultSet = statement.executeQuery("SELECT key FROM dfs.tmp.testview LIMIT 1");
          result = JdbcAssert.toString(resultSet).trim();
          resultSet.close();
          expected = "key=1";
          assertTrue(String.format("Generated string:\n%s\ndoes not match:\n%s", result, expected),
              expected.equals(result));

          statement.executeQuery("drop view dfs.tmp.testview").close();

          statement.executeQuery("drop view dfs.tmp.testview").close();

          statement.close();
          return null;
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    });
  }

  @Test
  public void testViewWithPartialSchemaIdentifier() throws Exception{
    JdbcAssert.withNoDefaultSchema().withConnection(new Function<Connection, Void>() {
      public Void apply(Connection connection) {
        try {
          Statement statement = connection.createStatement();

          // change default schema
          statement.executeQuery("USE dfs");

          // create a view with partial schema identifier
          ResultSet resultSet =  statement.executeQuery("CREATE VIEW tmp.testview AS SELECT * FROM hive.kv");
          String result = JdbcAssert.toString(resultSet).trim();
          resultSet.close();
          String expected = "ok=true; summary=View 'testview' created successfully in 'dfs.tmp' schema";
          assertTrue(String.format("Generated string:\n%s\ndoes not match:\n%s", result, expected),
              expected.equals(result));

          // query from view
          resultSet = statement.executeQuery("SELECT key FROM tmp.testview LIMIT 1");
          result = JdbcAssert.toString(resultSet).trim();
          resultSet.close();
          expected = "key=1";
          assertTrue(String.format("Generated string:\n%s\ndoes not match:\n%s", result, expected),
              expected.equals(result));

          // change the default schema and query
          statement.executeQuery("USE dfs.tmp");
          resultSet = statement.executeQuery("SELECT key FROM testview LIMIT 1");
          result = JdbcAssert.toString(resultSet).trim();
          resultSet.close();
          assertTrue(String.format("Generated string:\n%s\ndoes not match:\n%s", result, expected),
              expected.equals(result));

          statement.executeQuery("drop view tmp.testview").close();

          statement.close();
          return null;
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    });
  }
}
