/*
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
package org.apache.drill.exec.physical.impl.lateraljoin;

import org.apache.drill.test.BaseTestQuery;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.file.Paths;

import static junit.framework.TestCase.fail;

public class TestE2EUnnestAndLateral extends BaseTestQuery {

  private static final String regularTestFile_1 = "cust_order_10_1.json";
  private static final String regularTestFile_2 = "cust_order_10_2.json";
  private static final String schemaChangeFile_1 = "cust_order_10_2_stringNationKey.json";
  private static final String schemaChangeFile_2 = "cust_order_10_2_stringOrderShipPriority.json";
  private static final String schemaChangeFile_3 = "cust_order_10_2_stringNationKey_ShipPriority.json";

  @BeforeClass
  public static void setupTestFiles() throws Exception {
    dirTestWatcher.copyResourceToRoot(Paths.get("lateraljoin", "multipleFiles", regularTestFile_1));
    dirTestWatcher.copyResourceToRoot(Paths.get("lateraljoin", "multipleFiles", regularTestFile_2));
    test("alter session set `planner.enable_unnest_lateral`=true");
  }

  /***********************************************************************************************
   Test with single batch but using different keyword for Lateral and it has limit/filter operator
   within the subquery of Lateral and Unnest
   **********************************************************************************************/

  @Test
  public void testLateral_WithLimitInSubQuery() throws Exception {
    String Sql = "SELECT customer.c_name, customer.c_address, orders.o_id, orders.o_amount " +
      "FROM cp.`lateraljoin/nested-customer.parquet` customer, LATERAL " +
      "(SELECT t.ord.o_id as o_id, t.ord.o_amount as o_amount FROM UNNEST(customer.orders) t(ord) LIMIT 1) orders";
    test(Sql);
  }

  @Test
  public void testLateral_WithFilterInSubQuery() throws Exception {
    String Sql = "SELECT customer.c_name, customer.c_address, orders.o_id, orders.o_amount " +
      "FROM cp.`lateraljoin/nested-customer.parquet` customer, LATERAL " +
      "(SELECT t.ord.o_id as o_id, t.ord.o_amount as o_amount FROM UNNEST(customer.orders) t(ord) WHERE t.ord.o_amount > 10) orders";
    test(Sql);
  }

  @Test
  public void testLateral_WithFilterAndLimitInSubQuery() throws Exception {
    String Sql = "SELECT customer.c_name, customer.c_address, orders.o_id, orders.o_amount " +
      "FROM cp.`lateraljoin/nested-customer.parquet` customer, LATERAL " +
      "(SELECT t.ord.o_id as o_id, t.ord.o_amount as o_amount FROM UNNEST(customer.orders) t(ord) WHERE t.ord.o_amount > 10 LIMIT 1) orders";
    test(Sql);
  }

  @Test
  public void testOuterApply_WithFilterAndLimitInSubQuery() throws Exception {
    String Sql = "SELECT customer.c_name, customer.c_address, orders.o_id, orders.o_amount " +
      "FROM cp.`lateraljoin/nested-customer.parquet` customer OUTER APPLY " +
      "(SELECT t.ord.o_id as o_id , t.ord.o_amount as o_amount FROM UNNEST(customer.orders) t(ord) WHERE t.ord.o_amount > 10 LIMIT 1) orders";
    test(Sql);
  }

  @Test
  public void testLeftLateral_WithFilterAndLimitInSubQuery() throws Exception {
    String Sql = "SELECT customer.c_name, customer.c_address, orders.o_id, orders.o_amount " +
      "FROM cp.`lateraljoin/nested-customer.parquet` customer LEFT JOIN LATERAL " +
      "(SELECT t.ord.o_id as o_id, t.ord.o_amount as o_amount FROM UNNEST(customer.orders) t(ord) WHERE t.ord.o_amount > 10 LIMIT 1) orders ON TRUE";
    test(Sql);
  }

  @Test
  public void testMultiUnnestAtSameLevel() throws Exception {
    String Sql = "EXPLAIN PLAN FOR SELECT customer.c_name, customer.c_address, U1.order_id, U1.order_amt," +
      " U1.itemName, U1.itemNum" + " FROM cp.`lateraljoin/nested-customer.parquet` customer, LATERAL" +
      " (SELECT t.ord.o_id AS order_id, t.ord.o_amount AS order_amt, U2.item_name AS itemName, U2.item_num AS " +
        "itemNum FROM UNNEST(customer.orders) t(ord) , LATERAL" +
      " (SELECT t1.ord.i_name AS item_name, t1.ord.i_number AS item_num FROM UNNEST(t.ord) AS t1(ord)) AS U2) AS U1";
    test(Sql);
  }

  @Test
  public void testNestedUnnest() throws Exception {
    String Sql = "select * from (select customer.orders as orders from cp.`lateraljoin/nested-customer.parquet` customer ) t1," +
        " lateral ( select t.ord.items as items from unnest(t1.orders) t(ord) ) t2, unnest(t2.items) t3(item) ";
    test(Sql);
  }

  /***********************************************************************************************
   Test where multiple files are used to trigger multiple batch scenario. And it has limit/filter
   within the subquery of Lateral and Unnest
   **********************************************************************************************/
  @Test
  public void testMultipleBatchesLateralQuery() throws Exception {
    String sql = "SELECT customer.c_name, customer.c_address, orders.o_orderkey, orders.o_totalprice " +
      "FROM dfs.`lateraljoin/multipleFiles` customer, LATERAL " +
      "(SELECT t.ord.o_orderkey as o_orderkey, t.ord.o_totalprice as o_totalprice FROM UNNEST(customer.c_orders) t(ord)) orders";
    test(sql);
  }

  @Test
  public void testMultipleBatchesLateral_WithLimitInSubQuery() throws Exception {
    String sql = "SELECT customer.c_name, customer.c_address, orders.o_orderkey, orders.o_totalprice " +
      "FROM dfs.`lateraljoin/multipleFiles` customer, LATERAL " +
      "(SELECT t.ord.o_orderkey as o_orderkey, t.ord.o_totalprice as o_totalprice FROM UNNEST(customer.c_orders) t(ord) LIMIT 10) orders";
    test(sql);
  }

  @Test
  public void testMultipleBatchesLateral_WithLimitFilterInSubQuery() throws Exception {
    String sql = "SELECT customer.c_name, customer.c_address, orders.o_orderkey, orders.o_totalprice " +
      "FROM dfs.`lateraljoin/multipleFiles` customer, LATERAL " +
      "(SELECT t.ord.o_orderkey as o_orderkey, t.ord.o_totalprice as o_totalprice FROM UNNEST(customer.c_orders) t(ord) WHERE t.ord.o_totalprice > 100000 LIMIT 2) " +
      "orders";
    test(sql);
  }

  /***********************************************************************************************
   Test where multiple files are used to trigger schema change for Lateral and Unnest
   **********************************************************************************************/

  @Test
  public void testSchemaChangeOnNonUnnestColumn() throws Exception {

    try {
      dirTestWatcher.copyResourceToRoot(Paths.get("lateraljoin", "multipleFiles", schemaChangeFile_1));

      String sql = "SELECT customer.c_name, customer.c_address, orders.o_orderkey, orders.o_totalprice " +
        "FROM dfs.`lateraljoin/multipleFiles` customer, LATERAL " +
        "(SELECT t.ord.o_orderkey as o_orderkey, t.ord.o_totalprice as o_totalprice FROM UNNEST (customer.c_orders) t(ord)) orders";
      test(sql);
    } catch (Exception ex) {
      fail();
    } finally {
      dirTestWatcher.removeFileFromRoot(Paths.get("lateraljoin", "multipleFiles", schemaChangeFile_1));
    }
  }

  @Test
  public void testSchemaChangeOnUnnestColumn() throws Exception {
    try {
      dirTestWatcher.copyResourceToRoot(Paths.get("lateraljoin", "multipleFiles", schemaChangeFile_2));

      String sql = "SELECT customer.c_name, customer.c_address, orders.o_orderkey, orders.o_totalprice " +
        "FROM dfs.`lateraljoin/multipleFiles` customer, LATERAL " +
        "(SELECT t.ord.o_orderkey as o_orderkey, t.ord.o_totalprice as o_totalprice FROM UNNEST(customer.c_orders) t(ord)) orders";
      test(sql);
    } catch (Exception ex) {
      fail();
    } finally {
      dirTestWatcher.removeFileFromRoot(Paths.get("lateraljoin", "multipleFiles", schemaChangeFile_2));
    }
  }

  @Test
  public void testSchemaChangeOnMultipleColumns() throws Exception {
    try {
      dirTestWatcher.copyResourceToRoot(Paths.get("lateraljoin", "multipleFiles", schemaChangeFile_3));

      String sql = "SELECT customer.c_name, customer.c_address, customer.c_nationkey, orders.o_orderkey, " +
        "orders.o_totalprice FROM dfs.`lateraljoin/multipleFiles` customer, LATERAL " +
        "(SELECT t.ord.o_orderkey as o_orderkey, t.ord.o_totalprice as o_totalprice, t.ord.o_shippriority o_shippriority FROM UNNEST(customer.c_orders) t(ord)) orders";

      test(sql);
    } catch (Exception ex) {
      fail();
    } finally {
      dirTestWatcher.removeFileFromRoot(Paths.get("lateraljoin", "multipleFiles", schemaChangeFile_3));
    }
  }

  /*****************************************************************************************
   Test where Limit/Filter/Agg/Sort operator are used in parent query
   *****************************************************************************************/

  @Test
  public void testMultipleBatchesLateral_WithLimitInParent() throws Exception {
    String sql = "SELECT customer.c_name, customer.c_address, orders.o_orderkey, orders.o_totalprice " +
      "FROM dfs.`lateraljoin/multipleFiles` customer, LATERAL " +
      "(SELECT t.ord.o_orderkey as o_orderkey, t.ord.o_totalprice  as o_totalprice FROM UNNEST(customer.c_orders) t(ord) WHERE t.ord.o_totalprice > 100000 LIMIT 2) " +
      "orders LIMIT 1";
    test(sql);
  }

  @Test
  public void testMultipleBatchesLateral_WithFilterInParent() throws Exception {
    String sql = "SELECT customer.c_name, customer.c_address, orders.o_orderkey, orders.o_totalprice " +
      "FROM dfs.`lateraljoin/multipleFiles` customer, LATERAL " +
      "(SELECT t.ord.o_orderkey as o_orderkey, t.ord.o_totalprice as o_totalprice FROM UNNEST(customer.c_orders) t(ord) WHERE t.ord.o_totalprice > 100000 LIMIT 2) " +
      "orders WHERE orders.o_totalprice > 240000";
    test(sql);
  }

  @Test
  public void testMultipleBatchesLateral_WithGroupByInParent() throws Exception {
    String sql = "SELECT customer.c_name, avg(orders.o_totalprice) AS avgPrice " +
      "FROM dfs.`lateraljoin/multipleFiles` customer, LATERAL " +
      "(SELECT t.ord.o_totalprice as o_totalprice FROM UNNEST(customer.c_orders) t(ord) WHERE t.ord.o_totalprice > 100000 LIMIT 2) " +
      "orders GROUP BY customer.c_name";
    test(sql);
  }

  @Test
  public void testMultipleBatchesLateral_WithOrderByInParent() throws Exception {
    String sql = "SELECT customer.c_name, customer.c_address, orders.o_orderkey, orders.o_totalprice " +
      "FROM dfs.`lateraljoin/multipleFiles` customer, LATERAL " +
      "(SELECT t.ord.o_orderkey as o_orderkey, t.ord.o_totalprice as o_totalprice FROM UNNEST(customer.c_orders) t(ord)) orders " +
      "ORDER BY orders.o_orderkey";
    test(sql);
  }
}
