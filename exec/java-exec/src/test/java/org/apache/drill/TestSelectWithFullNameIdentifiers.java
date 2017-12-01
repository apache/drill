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
package org.apache.drill;

import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;

public class TestSelectWithFullNameIdentifiers extends BaseTestQuery {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestSelectWithFullNameIdentifiers.class);

  // create table in dfs_test
  @BeforeClass
  public static void createTableForTest() throws Exception {
    test("USE dfs_test");
    test("CREATE TABLE tmp.`department` (department_id, department_description) as select department_id, department_description from cp.`department.json`");
  }

  // delete table from dfs_test
  @AfterClass
  public static void dropCreatedTable() throws Exception {
    test("DROP TABLE IF EXISTS dfs_test.tmp.`department`");
  }

  @Test
  public void testSimpleQuery() throws Exception {
      // Query with using scalar function, WHERE statement, ORDER BY statement
    testBuilder()
        .sqlQuery("SELECT cp.`employee.json`.employee_id, UPPER(cp.`employee.json`.full_name) full_name " +
            "FROM cp.`employee.json` " +
            "WHERE cp.`employee.json`.department_id = 4 " +
            "ORDER BY cp.`employee.json`.full_name")
        .unOrdered()
        .baselineColumns("employee_id", "full_name")
        .baselineValues(43L, "JUANITA SHARP")
        .baselineValues(44L, "SANDRA BRUNNER")
        .build()
        .run();
  }

  @Test
  @Ignore
  public void testQueryWithStar() throws Exception {
    // Query with using full schema name for star identifier
    // TODO: Can be used after resolving "CALCITE-1323: Wrong prefix number in DelegatingScope.fullyQualify()"
    testBuilder()
        .sqlQuery("SELECT cp.`department.json`.* FROM cp.`department.json` LIMIT 1")
        .unOrdered()
        .baselineColumns("department_id", "department_description")
        .baselineValues(1L, "HQ General Management")
        .build()
        .run();
  }

  @Test
  @Ignore
  public void testQueryWithAggregation() throws Exception {
    // Query with aggregation
    // TODO: Can be used after resolving "DRILL-3993: Rebase Drill on Calcite master branch"
    // Fixed in "CALCITE-881: Allow schema.table.column references in GROUP BY".
    testBuilder()
        .sqlQuery("SELECT cp.`employee.json`.position_title, " +
            "COUNT(cp.`employee.json`.employee_id) employee_number " +
            "FROM cp.`employee.json` " +
            "WHERE cp.`employee.json`.position_title = 'Store Permanent Stocker' " +
            "GROUP BY cp.`employee.json`.position_title")
        .unOrdered()
        .baselineColumns("position_title", "employee_number")
        .baselineValues("Store Permanent Stocker", 222L)
        .build()
        .run();

  }

  @Test
  public void testLeftJoin() throws Exception {
    // Query with left join (with different schema-qualified tables)
    testBuilder()
        .sqlQuery("SELECT cp.`employee.json`.employee_id, cp.`employee.json`.full_name, dfs_test.tmp.`department`.department_id " +
            "FROM cp.`employee.json` LEFT JOIN dfs_test.tmp.`department` " +
            "ON cp.`employee.json`.department_id = dfs_test.tmp.`department`.department_id " +
            "WHERE dfs_test.tmp.`department`.department_description = 'HQ Marketing' " +
            "ORDER BY cp.`employee.json`.full_name")
        .unOrdered()
        .baselineColumns("employee_id", "full_name", "department_id")
        .baselineValues(36L, "Donna Arnold", 3L)
        .baselineValues(42L, "Doris Carter", 3L)
        .baselineValues(41L, "Howard Bechard", 3L)
        .baselineValues(7L, "Rebecca Kanagaki", 3L)
        .build()
        .run();
  }

  @Test
  @Ignore
  public void testNestedQueryInWhereStatement() throws Exception {
    // Query with sub-query in where statement (with different schema-qualified tables)
    // TODO: Calcite issue: AssertionError: must call validate first
    testBuilder()
        .sqlQuery("SELECT cp.`employee.json`.employee_id, cp.`employee.json`.department_id " +
            "FROM cp.`employee.json` " +
            "WHERE cp.`employee.json`.employee_id < 5 " +
            "AND cp.`employee.json`.department_id IN " +
            "(SELECT dfs_test.tmp.`department`.department_id " +
            "FROM dfs_test.tmp.`department` " +
            "WHERE dfs_test.tmp.`department`.department_id > 0)")
        .unOrdered()
        .baselineColumns("employee_id", "department_id")
        .baselineValues(1L, 1L)
        .baselineValues(2L, 1L)
        .baselineValues(4L, 1L)
        .build()
        .run();
  }

  @Test
  @Ignore
  public void testCorrelatedSubQuery() throws Exception {
    // Correlated sub-query (nested query references the enclosing query with another schema-qualified table)
    // TODO: Calcite issue: AssertionError
    testBuilder()
        .sqlQuery("SELECT dfs_test.tmp.`department`.department_id, " +
            "(SELECT COUNT(cp.`employee.json`.employee_id) FROM cp.`employee.json` " +
            "WHERE cp.`employee.json`.department_id = dfs_test.tmp.`department`.department_id) count_employee " +
            "FROM dfs_test.tmp.`department` " +
            "WHERE dfs_test.tmp.`department`.department_id = 1")
        .unOrdered()
        .baselineColumns("department_id", "count_employee")
        .baselineValues(1L, 7L)
        .build()
        .run();
  }

  @Test
  public void testNestedQueryInFromStatement() throws Exception {
    testBuilder()
        .sqlQuery("SELECT * FROM (SELECT cp.`employee.json`.employee_id, cp.`employee.json`.full_name " +
            "FROM cp.`employee.json`) t " +
            "WHERE t.employee_id = 1")
        .unOrdered()
        .baselineColumns("employee_id", "full_name")
        .baselineValues(1L, "Sheri Nowmer")
        .build()
        .run();
  }

  @Test
  public void testNestedQueryInLeftJoinStatement() throws Exception {
    // Query with sub-query in left join statement (with different schema-qualified tables)
    testBuilder()
        .sqlQuery("SELECT cp.`employee.json`.employee_id, cp.`employee.json`.full_name, t.store_id " +
            "FROM cp.`employee.json` LEFT JOIN (select cp.`store.json`.store_id from cp.`store.json`) t " +
            "ON cp.`employee.json`.store_id = t.store_id " +
            "WHERE cp.`employee.json`.employee_id = 1")
        .unOrdered()
        .baselineColumns("employee_id", "full_name", "store_id")
        .baselineValues(1L, "Sheri Nowmer", 0L)
        .build()
        .run();
  }
}
