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

import org.apache.drill.BaseTestQuery;
import org.junit.Test;

public class TestViewSupport extends BaseTestQuery{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestViewSupport.class);

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
  public void testNullabilityPropertyInViewPersistence() throws Exception {
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
}
