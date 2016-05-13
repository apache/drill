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
import org.apache.commons.io.FileUtils;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.util.List;

public class TestViewSupport extends TestBaseViewSupport {
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
          .baselineValues("fname", "CHARACTER VARYING", "YES")
          .baselineValues("country", "CHARACTER VARYING", "YES")
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
        new String[]{"regionid"},
        ImmutableList.of(
            new Object[]{109L},
            new Object[]{108L}
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
        new String[]{"regionid"},
        ImmutableList.of(
            new Object[]{110L},
            new Object[]{108L}
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
        new String[]{"n_nationkey", "n_name", "n_regionkey", "n_comment"},
        ImmutableList.of(
            new Object[]{0, "ALGERIA", 0, " haggle. carefully final deposits detect slyly agai"}
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
      errorMsgTestHelper(createViewSql,
          String.format("A view with given name [%s] already exists in schema [%s]", viewName, TEMP_SCHEMA));

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
          new String[]{"sales_state_province"},
          ImmutableList.of(new Object[]{"None"})
      );
    } finally {
      dropViewHelper(TEMP_SCHEMA, viewName, TEMP_SCHEMA);
    }
  }

  @Test // DRILL-2422
  public void createViewWhenATableWithSameNameAlreadyExists() throws Exception {
    final String tableName = generateViewName();

    try {
      final String tableDef1 = "SELECT region_id, sales_city FROM cp.`region.json`";

      test(String.format("CREATE TABLE %s.%s as %s", TEMP_SCHEMA, tableName, tableDef1));

      // Try to create the view with same name in same schema.
      final String createViewSql = String.format("CREATE VIEW %s.`%s` AS %s", TEMP_SCHEMA, tableName, tableDef1);
      errorMsgTestHelper(createViewSql,
          String.format("A non-view table with given name [%s] already exists in schema [%s]", tableName, TEMP_SCHEMA));

      // Try creating the view with same name in same schema, but with CREATE OR REPLACE VIEW clause
      final String viewDef2 = "SELECT sales_state_province FROM cp.`region.json` ORDER BY `region_id`";
      errorMsgTestHelper(String.format("CREATE OR REPLACE VIEW %s.`%s` AS %s", TEMP_SCHEMA, tableName, viewDef2),
          String.format("A non-view table with given name [%s] already exists in schema [%s]", tableName, TEMP_SCHEMA));
    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tableName));
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
      final List<Object[]> baselineValues = ImmutableList.of(new Object[]{109L, "Santa Fe"});

      // Query the view
      queryViewHelper(
          String.format("SELECT * FROM %s.`%s` ORDER BY region_id DESC LIMIT 1", "dfs_test.tmp", viewName),
          baselineColumns, baselineValues);

      // Change default schema to "dfs_test" and query by referring to the view using "tmp.viewName"
      test("USE dfs_test");
      queryViewHelper(
          String.format("SELECT * FROM %s.`%s` ORDER BY region_id DESC LIMIT 1", "tmp", viewName),
          baselineColumns, baselineValues);

    } finally {
      dropViewHelper(TEMP_SCHEMA, viewName, TEMP_SCHEMA);
    }
  }

  // DRILL-2341, View schema verification where view's field is not specified is already tested in
  // TestViewSupport.infoSchemaWithView.
  @Test
  public void viewSchemaWhenSelectFieldsInDef_SelectFieldsInView() throws Exception {
    final String viewName = generateViewName();

    try {
      test("USE " + TEMP_SCHEMA);
      createViewHelper(null, viewName, TEMP_SCHEMA, "(id, name, bday)",
          "SELECT " +
              "cast(`region_id` as integer), " +
              "cast(`full_name` as varchar(100)), " +
              "cast(`birth_date` as date) " +
              "FROM cp.`employee.json`");

      // Test DESCRIBE view
      testBuilder()
          .sqlQuery(String.format("DESCRIBE `%s`", viewName))
          .unOrdered()
          .baselineColumns("COLUMN_NAME", "DATA_TYPE", "IS_NULLABLE")
          .baselineValues("id", "INTEGER", "YES")
          .baselineValues("name", "CHARACTER VARYING", "YES")
          .baselineValues("bday", "DATE", "YES")
          .go();
    } finally {
      dropViewHelper(TEMP_SCHEMA, viewName, TEMP_SCHEMA);
    }
  }

  @Test // DRILL-2589
  public void createViewWithDuplicateColumnsInDef1() throws Exception {
    createViewErrorTestHelper(
        "CREATE VIEW %s.%s AS SELECT region_id, region_id FROM cp.`region.json`",
        String.format("Duplicate column name [%s]", "region_id")
    );
  }

  @Test // DRILL-2589
  public void createViewWithDuplicateColumnsInDef2() throws Exception {
    createViewErrorTestHelper("CREATE VIEW %s.%s AS SELECT region_id, sales_city, sales_city FROM cp.`region.json`",
        String.format("Duplicate column name [%s]", "sales_city")
    );
  }

  @Test // DRILL-2589
  public void createViewWithDuplicateColumnsInDef3() throws Exception {
    createViewErrorTestHelper(
        "CREATE VIEW %s.%s(regionid, regionid) AS SELECT region_id, sales_city FROM cp.`region.json`",
        String.format("Duplicate column name [%s]", "regionid")
    );
  }

  @Test // DRILL-2589
  public void createViewWithDuplicateColumnsInDef4() throws Exception {
    createViewErrorTestHelper(
        "CREATE VIEW %s.%s(regionid, salescity, salescity) " +
            "AS SELECT region_id, sales_city, sales_city FROM cp.`region.json`",
        String.format("Duplicate column name [%s]", "salescity")
    );
  }

  @Test // DRILL-2589
  public void createViewWithDuplicateColumnsInDef5() throws Exception {
    createViewErrorTestHelper(
        "CREATE VIEW %s.%s(regionid, salescity, SalesCity) " +
            "AS SELECT region_id, sales_city, sales_city FROM cp.`region.json`",
        String.format("Duplicate column name [%s]", "SalesCity")
    );
  }

  @Test // DRILL-2589
  public void createViewWithDuplicateColumnsInDef6() throws Exception {
    createViewErrorTestHelper(
        "CREATE VIEW %s.%s " +
            "AS SELECT t1.region_id, t2.region_id FROM cp.`region.json` t1 JOIN cp.`region.json` t2 " +
            "ON t1.region_id = t2.region_id LIMIT 1",
        String.format("Duplicate column name [%s]", "region_id")
    );
  }

  @Test // DRILL-2589
  public void createViewWithUniqueColsInFieldListDuplicateColsInQuery1() throws Exception {
    testViewHelper(
        TEMP_SCHEMA,
        "(regionid1, regionid2)",
        "SELECT region_id, region_id FROM cp.`region.json` LIMIT 1",
        "SELECT * FROM TEST_SCHEMA.TEST_VIEW_NAME",
        new String[]{"regionid1", "regionid2"},
        ImmutableList.of(
            new Object[]{0L, 0L}
        )
    );
  }

  @Test // DRILL-2589
  public void createViewWithUniqueColsInFieldListDuplicateColsInQuery2() throws Exception {
    testViewHelper(
        TEMP_SCHEMA,
        "(regionid1, regionid2)",
        "SELECT t1.region_id, t2.region_id FROM cp.`region.json` t1 JOIN cp.`region.json` t2 " +
            "ON t1.region_id = t2.region_id LIMIT 1",
        "SELECT * FROM TEST_SCHEMA.TEST_VIEW_NAME",
        new String[]{"regionid1", "regionid2"},
        ImmutableList.of(
            new Object[]{0L, 0L}
        )
    );
  }

  @Test // DRILL-2589
  public void createViewWhenInEqualColumnCountInViewDefVsInViewQuery() throws Exception {
    createViewErrorTestHelper(
        "CREATE VIEW %s.%s(regionid, salescity) " +
            "AS SELECT region_id, sales_city, sales_region FROM cp.`region.json`",
        "view's field list and the view's query field list have different counts."
    );
  }

  @Test // DRILL-2589
  public void createViewWhenViewQueryColumnHasStarAndViewFiledListIsSpecified() throws Exception {
    createViewErrorTestHelper(
        "CREATE VIEW %s.%s(regionid, salescity) " +
            "AS SELECT region_id, * FROM cp.`region.json`",
        "view's query field list has a '*', which is invalid when view's field list is specified."
    );
  }

  private static void createViewErrorTestHelper(final String viewSql, final String expErrorMsg) throws Exception {
    final String createViewSql = String.format(viewSql, TEMP_SCHEMA, "duplicateColumnsInViewDef");
    errorMsgTestHelper(createViewSql, expErrorMsg);
  }

  @Test // DRILL-2423
  public void showProperMsgWhenDroppingNonExistentView() throws Exception{
    errorMsgTestHelper("DROP VIEW dfs_test.tmp.nonExistentView",
        "Unknown view [nonExistentView] in schema [dfs_test.tmp].");
  }

  @Test // DRILL-2423
  public void showProperMsgWhenTryingToDropAViewInImmutableSchema() throws Exception{
    errorMsgTestHelper("DROP VIEW cp.nonExistentView",
        "Unable to create or drop tables/views. Schema [cp] is immutable.");
  }

  @Test // DRILL-2423
  public void showProperMsgWhenTryingToDropANonViewTable() throws Exception{
    final String testTableName = "testTableShowErrorMsg";
    try {
      test(String.format("CREATE TABLE %s.%s AS SELECT c_custkey, c_nationkey from cp.`tpch/customer.parquet`",
          TEMP_SCHEMA, testTableName));

      errorMsgTestHelper(String.format("DROP VIEW %s.%s", TEMP_SCHEMA, testTableName),
          "[testTableShowErrorMsg] is not a VIEW in schema [dfs_test.tmp]");
    } finally {
      File tblPath = new File(getDfsTestTmpSchemaLocation(), testTableName);
      FileUtils.deleteQuietly(tblPath);
    }
  }

  @Test // DRILL-4673
  public void dropViewIfExistsWhenViewExists() throws Exception {
    final String existentViewName = generateViewName();

    // successful dropping of existent view
    createViewHelper(TEMP_SCHEMA, existentViewName, TEMP_SCHEMA, null,
        "SELECT c_custkey, c_nationkey from cp.`tpch/customer.parquet`");
    dropViewIfExistsHelper(TEMP_SCHEMA, existentViewName, TEMP_SCHEMA, true);
  }

  @Test // DRILL-4673
  public void dropViewIfExistsWhenViewDoesNotExist() throws Exception {
    final String nonExistentViewName = generateViewName();

    // dropping of non existent view without error
    dropViewIfExistsHelper(TEMP_SCHEMA, nonExistentViewName, TEMP_SCHEMA, false);
  }

  @Test // DRILL-4673
  public void dropViewIfExistsWhenItIsATable() throws Exception {
    final String tableName = "table_name";
    try{
      // dropping of non existent view without error if the table with such name is existed
      test(String.format("CREATE TABLE %s.%s as SELECT region_id, sales_city FROM cp.`region.json`",
          TEMP_SCHEMA, tableName));
      dropViewIfExistsHelper(TEMP_SCHEMA, tableName, TEMP_SCHEMA, false);
    } finally {
      test(String.format("DROP TABLE IF EXISTS %s.%s ", TEMP_SCHEMA, tableName));
    }
  }
}
