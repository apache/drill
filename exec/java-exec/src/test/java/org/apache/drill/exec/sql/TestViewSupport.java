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
package org.apache.drill.exec.sql;

import com.google.common.collect.ImmutableList;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;

public class TestViewSupport extends TestBaseViewSupport {
  private static final String TEMP_SCHEMA = "dfs_test.tmp";

  @Test
  public void referToSchemaInsideAndOutsideView() throws Exception {
    String use = "use dfs_test.tmp;";
    String selectInto = "create table monkey as select c_custkey, c_nationkey from cp.`tpch/customer.parquet`";
    String createView = "create or replace view myMonkeyView as select c_custkey, c_nationkey from monkey";
    String selectInside = "select * from myMonkeyView;";
    String use2 = "use cp;";
    String selectOutside = "select * from dfs_test.tmp.myMonkeyView;";

    test(use);
    test(selectInto);
    test(createView);
    test(selectInside);
    test(use2);
    test(selectOutside);
  }

  /**
   * DRILL-2342 This test is for case where output columns are nullable. Existing tests already cover the case
   * where columns are required.
   */
  @Test
  public void nullabilityPropertyInViewPersistence() throws Exception {
    final String viewName = "testNullabilityPropertyInViewPersistence";
    try {

      test("USE dfs_test.tmp");
      test(String.format("CREATE OR REPLACE VIEW %s AS SELECT " +
          "CAST(customer_id AS BIGINT) as cust_id, " +
          "CAST(fname AS VARCHAR(25)) as fname, " +
          "CAST(country AS VARCHAR(20)) as country " +
          "FROM cp.`customer.json` " +
          "ORDER BY customer_id " +
          "LIMIT 1;", viewName));

      testBuilder()
          .sqlQuery(String.format("DESCRIBE %s", viewName))
          .unOrdered()
          .baselineColumns("COLUMN_NAME", "DATA_TYPE", "IS_NULLABLE")
          .baselineValues("cust_id", "BIGINT", "YES")
          .baselineValues("fname", "VARCHAR", "YES")
          .baselineValues("country", "VARCHAR", "YES")
          .go();

      testBuilder()
          .sqlQuery(String.format("SELECT * FROM %s", viewName))
          .ordered()
          .baselineColumns("cust_id", "fname", "country")
          .baselineValues(1L, "Sheri", "Mexico")
          .go();
    } finally {
      test("drop view " + viewName + ";");
    }
  }

  @Test
  public void viewWithStarInDef_StarInQuery() throws Exception {
    testViewHelper(
        TEMP_SCHEMA,
        null,
        "SELECT * FROM cp.`region.json` ORDER BY `region_id`",
        "SELECT * FROM TEST_SCHEMA.TEST_VIEW_NAME LIMIT 1",
        new String[] { "region_id", "sales_city", "sales_state_province", "sales_district", "sales_region",
            "sales_country", "sales_district_id" },
        ImmutableList.of(new Object[] { 0L, "None", "None", "No District", "No Region", "No Country", 0L })
    );
  }

  @Test
  public void viewWithSelectFieldsInDef_StarInQuery() throws Exception {
    testViewHelper(
        TEMP_SCHEMA,
        null,
        "SELECT region_id, sales_city FROM cp.`region.json` ORDER BY `region_id`",
        "SELECT * FROM TEST_SCHEMA.TEST_VIEW_NAME LIMIT 2",
        new String[] { "region_id", "sales_city" },
        ImmutableList.of(
            new Object[] { 0L, "None" },
            new Object[] { 1L, "San Francisco" }
        )
    );
  }

  @Test
  public void viewWithSelectFieldsInDef_SelectFieldsInView_StarInQuery() throws Exception {
    testViewHelper(
        TEMP_SCHEMA,
        "(regionid, salescity)",
        "SELECT region_id, sales_city FROM cp.`region.json` ORDER BY `region_id`",
        "SELECT * FROM TEST_SCHEMA.TEST_VIEW_NAME LIMIT 2",
        new String[] { "regionid", "salescity" },
        ImmutableList.of(
            new Object[] { 0L, "None" },
            new Object[] { 1L, "San Francisco" }
        )
    );
  }

  @Test
  public void viewWithStarInDef_SelectFieldsInQuery() throws Exception{
    testViewHelper(
        TEMP_SCHEMA,
        null,
        "SELECT * FROM cp.`region.json` ORDER BY `region_id`",
        "SELECT region_id, sales_city FROM TEST_SCHEMA.TEST_VIEW_NAME LIMIT 2",
        new String[] { "region_id", "sales_city" },
        ImmutableList.of(
            new Object[] { 0L, "None" },
            new Object[] { 1L, "San Francisco" }
        )
    );
  }

  @Test
  public void viewWithSelectFieldsInDef_SelectFieldsInQuery1() throws Exception {
    testViewHelper(
        TEMP_SCHEMA,
        null,
        "SELECT region_id, sales_city FROM cp.`region.json` ORDER BY `region_id`",
        "SELECT region_id, sales_city FROM TEST_SCHEMA.TEST_VIEW_NAME LIMIT 2",
        new String[] { "region_id", "sales_city" },
        ImmutableList.of(
            new Object[] { 0L, "None" },
            new Object[] { 1L, "San Francisco" }
        )
    );
  }

  @Test
  public void viewWithSelectFieldsInDef_SelectFieldsInQuery2() throws Exception {
    testViewHelper(
        TEMP_SCHEMA,
        null,
        "SELECT region_id, sales_city FROM cp.`region.json` ORDER BY `region_id`",
        "SELECT sales_city FROM TEST_SCHEMA.TEST_VIEW_NAME LIMIT 2",
        new String[] { "sales_city" },
        ImmutableList.of(
            new Object[] { "None" },
            new Object[] { "San Francisco" }
        )
    );
  }

  @Test
  public void viewWithSelectFieldsInDef_SelectFieldsInView_SelectFieldsInQuery1() throws Exception {
    testViewHelper(
        TEMP_SCHEMA,
        "(regionid, salescity)",
        "SELECT region_id, sales_city FROM cp.`region.json` ORDER BY `region_id` LIMIT 2",
        "SELECT regionid, salescity FROM TEST_SCHEMA.TEST_VIEW_NAME LIMIT 2",
        new String[] { "regionid", "salescity" },
        ImmutableList.of(
            new Object[] { 0L, "None" },
            new Object[] { 1L, "San Francisco" }
        )
    );
  }

  @Test
  public void viewWithSelectFieldsInDef_SelectFieldsInView_SelectFieldsInQuery2() throws Exception {
    testViewHelper(
        TEMP_SCHEMA,
        "(regionid, salescity)",
        "SELECT region_id, sales_city FROM cp.`region.json` ORDER BY `region_id` DESC",
        "SELECT regionid FROM TEST_SCHEMA.TEST_VIEW_NAME LIMIT 2",
        new String[] { "regionid" },
        ImmutableList.of(
            new Object[] { 109L },
            new Object[] { 108L }
        )
    );
  }

  @Test
  @Ignore("DRILL-1921")
  public void viewWithUnionWithSelectFieldsInDef_StarInQuery() throws Exception{
    testViewHelper(
        TEMP_SCHEMA,
        null,
        "SELECT region_id FROM cp.`region.json` UNION SELECT employee_id FROM cp.`employee.json`",
        "SELECT regionid FROM TEST_SCHEMA.TEST_VIEW_NAME LIMIT 2",
        new String[] { "regionid" },
        ImmutableList.of(
            new Object[] { 110L },
            new Object[] { 108L }
        )
    );
  }

  @Test
  public void viewCreatedFromAnotherView() throws Exception {
    final String innerView = generateViewName();
    final String outerView = generateViewName();

    try {
      createViewHelper(TEMP_SCHEMA, innerView, TEMP_SCHEMA, null,
          "SELECT region_id, sales_city FROM cp.`region.json` ORDER BY `region_id`");

      createViewHelper(TEMP_SCHEMA, outerView, TEMP_SCHEMA, null,
          String.format("SELECT region_id FROM %s.`%s`", TEMP_SCHEMA, innerView));

      queryViewHelper(
          String.format("SELECT region_id FROM %s.`%s` LIMIT 1", TEMP_SCHEMA, outerView),
          new String[] { "region_id" },
          ImmutableList.of(new Object[] { 0L })
      );
    } finally {
      dropViewHelper(TEMP_SCHEMA, outerView, TEMP_SCHEMA);
      dropViewHelper(TEMP_SCHEMA, innerView, TEMP_SCHEMA);
    }
  }

  @Test // DRILL-1015
  public void viewWithCompoundIdentifiersInDef() throws Exception{
    final String viewDef = "SELECT " +
        "cast(columns[0] AS int) n_nationkey, " +
        "cast(columns[1] AS CHAR(25)) n_name, " +
        "cast(columns[2] AS INT) n_regionkey, " +
        "cast(columns[3] AS VARCHAR(152)) n_comment " +
        "FROM dfs_test.`[WORKING_PATH]/src/test/resources/nation`";

    testViewHelper(
        TEMP_SCHEMA,
        null,
        viewDef,
        "SELECT * FROM TEST_SCHEMA.TEST_VIEW_NAME LIMIT 1",
        new String[] { "n_nationkey", "n_name", "n_regionkey", "n_comment" },
        ImmutableList.of(
            new Object[] { 0, "ALGERIA", 0, " haggle. carefully final deposits detect slyly agai" }
        )
    );
  }

  @Test
  public void createViewWhenViewAlreadyExists() throws Exception {
    final String viewName = generateViewName();

    try {
      final String viewDef1 = "SELECT region_id, sales_city FROM cp.`region.json`";

      // Create the view
      createViewHelper(TEMP_SCHEMA, viewName, TEMP_SCHEMA, null, viewDef1);

      // Try to create the view with same name in same schema.
      final String createViewSql = String.format("CREATE VIEW %s.`%s` AS %s", TEMP_SCHEMA, viewName, viewDef1);
      testBuilder()
          .sqlQuery(createViewSql)
          .unOrdered()
          .baselineColumns("ok", "summary")
          .baselineValues(false, "View with given name already exists in current schema")
          .go();

      // Try creating the view with same name in same schema, but with CREATE OR REPLACE VIEW clause
      final String viewDef2 = "SELECT sales_state_province FROM cp.`region.json` ORDER BY `region_id`";
      final String createOrReplaceViewSql = String.format("CREATE OR REPLACE VIEW %s.`%s` AS %s", TEMP_SCHEMA,
          viewName, viewDef2);

      testBuilder()
          .sqlQuery(createOrReplaceViewSql)
          .unOrdered()
          .baselineColumns("ok", "summary")
          .baselineValues(true,
              String.format("View '%s' replaced successfully in '%s' schema", viewName, TEMP_SCHEMA))
          .go();

      // Make sure the new view created returns the data expected.
      queryViewHelper(String.format("SELECT * FROM %s.`%s` LIMIT 1", TEMP_SCHEMA, viewName),
          new String[] { "sales_state_province" },
          ImmutableList.of(new Object[] { "None" })
      );
    } finally {
      dropViewHelper(TEMP_SCHEMA, viewName, TEMP_SCHEMA);
    }
  }

  @Test
  public void infoSchemaWithView() throws Exception {
    final String viewName = generateViewName();

    try {
      test("USE " + TEMP_SCHEMA);
      createViewHelper(null /*pass no schema*/, viewName, TEMP_SCHEMA, null,
          "SELECT cast(`employee_id` as integer) employeeid FROM cp.`employee.json`");

      // Test SHOW TABLES on view
      testBuilder()
          .sqlQuery(String.format("SHOW TABLES like '%s'", viewName))
          .unOrdered()
          .baselineColumns("TABLE_SCHEMA", "TABLE_NAME")
          .baselineValues(TEMP_SCHEMA, viewName)
          .go();

      // Test record in INFORMATION_SCHEMA.VIEWS
      testBuilder()
          .sqlQuery(String.format("SELECT * FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_NAME = '%s'", viewName))
          .unOrdered()
          .baselineColumns("TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "VIEW_DEFINITION")
          .baselineValues("DRILL", TEMP_SCHEMA, viewName,
              "SELECT CAST(`employee_id` AS INTEGER) AS `employeeid`\n" + "FROM `cp`.`employee.json`")
          .go();

      // Test record in INFORMATION_SCHEMA.TABLES
      testBuilder()
          .sqlQuery(String.format("SELECT * FROM INFORMATION_SCHEMA.`TABLES` WHERE TABLE_NAME = '%s'", viewName))
          .unOrdered()
          .baselineColumns("TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "TABLE_TYPE")
          .baselineValues("DRILL", TEMP_SCHEMA, viewName, "VIEW")
          .go();

      // Test DESCRIBE view
      testBuilder()
          .sqlQuery(String.format("DESCRIBE `%s`", viewName))
          .unOrdered()
          .baselineColumns("COLUMN_NAME", "DATA_TYPE", "IS_NULLABLE")
          .baselineValues("employeeid", "INTEGER", "YES")
          .go();
    } finally {
      dropViewHelper(TEMP_SCHEMA, viewName, TEMP_SCHEMA);
    }
  }

  @Test
  public void viewWithPartialSchemaIdentifier() throws Exception {
    final String viewName = generateViewName();

    try {
      // Change default schema to just "dfs_test". View is actually created in "dfs_test.tmp" schema.
      test("USE dfs_test");

      // Create a view with with "tmp" schema identifier
      createViewHelper("tmp", viewName, TEMP_SCHEMA, null,
          "SELECT CAST(`employee_id` AS INTEGER) AS `employeeid`\n" + "FROM `cp`.`employee.json`");

      final String[] baselineColumns = new String[] { "employeeid" };
      final List<Object[]> baselineValues = ImmutableList.of(new Object[] { 1156 });

      // Query view from current schema "dfs_test" by referring to the view using "tmp.viewName"
      queryViewHelper(
          String.format("SELECT * FROM %s.`%s` ORDER BY `employeeid` DESC LIMIT 1", "tmp", viewName),
          baselineColumns, baselineValues);

      // Change the default schema to "dfs_test.tmp" and query view by referring to it using "viewName"
      test("USE dfs_test.tmp");
      queryViewHelper(
          String.format("SELECT * FROM `%s` ORDER BY `employeeid` DESC LIMIT 1", viewName),
          baselineColumns, baselineValues);

      // Change the default schema to "cp" and query view by referring to it using "dfs_test.tmp.viewName";
      test("USE cp");
      queryViewHelper(
          String.format("SELECT * FROM %s.`%s` ORDER BY `employeeid` DESC LIMIT 1", "dfs_test.tmp", viewName),
          baselineColumns, baselineValues);

    } finally {
      dropViewHelper(TEMP_SCHEMA, viewName, TEMP_SCHEMA);
    }
  }

  @Test // DRILL-1114
  public void viewResolvingTablesInWorkspaceSchema() throws Exception {
    final String viewName = generateViewName();

    try {
      // Change default schema to "cp"
      test("USE cp");

      // Create a view with full schema identifier and refer the "region.json" as without schema.
      createViewHelper(TEMP_SCHEMA, viewName, TEMP_SCHEMA, null, "SELECT region_id, sales_city FROM `region.json`");

      final String[] baselineColumns = new String[] { "region_id", "sales_city" };
      final List<Object[]> baselineValues = ImmutableList.of(new Object[] { 109L, "Santa Fe"});

      // Query the view
      queryViewHelper(
          String.format("SELECT * FROM %s.`%s` ORDER BY region_id DESC LIMIT 1", "dfs_test.tmp", viewName),
          baselineColumns, baselineValues);

      // Change default schema to "dfs_test" and query by referring to the view using "tmp.viewName"
      test("USE dfs_test");
      queryViewHelper(
          String.format("SELECT * FROM %s.`%s` ORDER BY region_id DESC LIMIT 1", "tmp", viewName),
          baselineColumns,baselineValues);

    } finally {
      dropViewHelper(TEMP_SCHEMA, viewName, TEMP_SCHEMA);
    }
  }
}
