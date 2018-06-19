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

import org.apache.drill.categories.OperatorTest;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.test.TestBuilder;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.nio.file.Paths;

import static junit.framework.TestCase.fail;

@Category(OperatorTest.class)
public class TestE2EUnnestAndLateral extends ClusterTest {

  private static final String regularTestFile_1 = "cust_order_10_1.json";
  private static final String regularTestFile_2 = "cust_order_10_2.json";
  private static final String schemaChangeFile_1 = "cust_order_10_2_stringNationKey.json";
  private static final String schemaChangeFile_2 = "cust_order_10_2_stringOrderShipPriority.json";
  private static final String schemaChangeFile_3 = "cust_order_10_2_stringNationKey_ShipPriority.json";

  @BeforeClass
  public static void setupTestFiles() throws Exception {
    dirTestWatcher.copyResourceToRoot(Paths.get("lateraljoin", "multipleFiles", regularTestFile_1));
    dirTestWatcher.copyResourceToRoot(Paths.get("lateraljoin", "multipleFiles", regularTestFile_2));
    ClusterFixtureBuilder builder = ClusterFixture.builder(dirTestWatcher)
        .sessionOption(ExecConstants.ENABLE_UNNEST_LATERAL_KEY, true)
        .maxParallelization(1);
    startCluster(builder);
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
  public void testLateral_WithTopNInSubQuery() throws Exception {
    String Sql = "SELECT customer.c_name, orders.o_id, orders.o_amount " +
      "FROM cp.`lateraljoin/nested-customer.parquet` customer, LATERAL " +
      "(SELECT t.ord.o_id as o_id, t.ord.o_amount as o_amount FROM UNNEST(customer.orders) t(ord) ORDER BY " +
      "o_amount DESC LIMIT 1) orders";

    testBuilder()
      .sqlQuery(Sql)
      .unOrdered()
      .baselineColumns("c_name", "o_id", "o_amount")
      .baselineValues("customer1", 3.0,  294.5)
      .baselineValues("customer2", 10.0,  724.5)
      .baselineValues("customer3", 23.0,  772.2)
      .baselineValues("customer4", 32.0,  1030.1)
      .go();
  }

  /**
   * Test which disables the TopN operator from planner settings before running query using SORT and LIMIT in
   * subquery. The same query as in above test is executed and same result is expected.
   */
  @Test
  public void testLateral_WithSortAndLimitInSubQuery() throws Exception {

    test("alter session set `planner.enable_topn`=false");

    String Sql = "SELECT customer.c_name, orders.o_id, orders.o_amount " +
      "FROM cp.`lateraljoin/nested-customer.parquet` customer, LATERAL " +
      "(SELECT t.ord.o_id as o_id, t.ord.o_amount as o_amount FROM UNNEST(customer.orders) t(ord) ORDER BY " +
      "o_amount DESC LIMIT 1) orders";

    try {
      testBuilder()
        .sqlQuery(Sql)
        .unOrdered()
        .baselineColumns("c_name", "o_id", "o_amount")
        .baselineValues("customer1", 3.0,  294.5)
        .baselineValues("customer2", 10.0,  724.5)
        .baselineValues("customer3", 23.0,  772.2)
        .baselineValues("customer4", 32.0,  1030.1)
        .go();
    } finally {
      test("alter session set `planner.enable_topn`=true");
    }
  }

  @Test
  public void testLateral_WithSortInSubQuery() throws Exception {
    String Sql = "SELECT customer.c_name, orders.o_id, orders.o_amount " +
      "FROM cp.`lateraljoin/nested-customer.parquet` customer, LATERAL " +
      "(SELECT t.ord.o_id as o_id, t.ord.o_amount as o_amount FROM UNNEST(customer.orders) t(ord) ORDER BY " +
      "o_amount DESC) orders WHERE customer.c_id = 1.0";

    testBuilder()
      .sqlQuery(Sql)
      .ordered()
      .baselineColumns("c_name", "o_id", "o_amount")
      .baselineValues("customer1", 3.0,  294.5)
      .baselineValues("customer1", 2.0,  104.5)
      .baselineValues("customer1", 1.0,  4.5)
      .go();
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
  public void testUnnestWithItem() throws Exception {
    String sql = "select u.item from\n" +
        "cp.`lateraljoin/nested-customer.parquet` c," +
        "unnest(c.orders['items']) as u(item)\n" +
        "limit 1";

    testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineColumns("item")
        .baselineValues(
            TestBuilder.mapOf("i_name", "paper towel",
                "i_number", 2.0,
                "i_supplier", "oregan"))
        .go();
  }

  @Test
  public void testUnnestWithFunctionCall() throws Exception {
    String sql = "select u.ord.o_amount o_amount from\n" +
        "cp.`lateraljoin/nested-customer.parquet` c," +
        "unnest(convert_fromjson(convert_tojson(c.orders))) as u(ord)\n" +
        "limit 1";

    testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineColumns("o_amount")
        .baselineValues(4.5)
        .go();
  }

  @Test
  public void testUnnestWithMap() throws Exception {
    String sql = "select u.item from\n" +
        "cp.`lateraljoin/nested-customer.parquet` c," +
        "unnest(c.orders.items) as u(item)\n" +
        "limit 1";

    testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineColumns("item")
        .baselineValues(
            TestBuilder.mapOf("i_name", "paper towel",
                "i_number", 2.0,
                "i_supplier", "oregan"))
        .go();
  }

  @Test
  public void testMultiUnnestWithMap() throws Exception {
    String sql = "select u.item from\n" +
        "cp.`lateraljoin/nested-customer.parquet` c," +
        "unnest(c.orders.items) as u(item)," +
        "unnest(c.orders.items) as u1(item1)\n" +
        "limit 1";

    testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineColumns("item")
        .baselineValues(
            TestBuilder.mapOf("i_name", "paper towel",
                "i_number", 2.0,
                "i_supplier", "oregan"))
        .go();
  }

  @Test
  public void testSingleUnnestCol() throws Exception {
    String sql =
      "select t.orders.o_id as id " +
      "from (select u.orders from\n" +
            "cp.`lateraljoin/nested-customer.parquet` c," +
            "unnest(c.orders) as u(orders)\n" +
            "limit 1) t";

    testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineColumns("id")
        .baselineValues(1.0)
        .go();
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
  public void testMultipleBatchesLateral_WithTopNInSubQuery() throws Exception {
    String sql = "SELECT customer.c_name, orders.o_orderkey, orders.o_totalprice " +
      "FROM dfs.`lateraljoin/multipleFiles` customer, LATERAL " +
      "(SELECT t.ord.o_orderkey as o_orderkey, t.ord.o_totalprice as o_totalprice FROM UNNEST(customer.c_orders) t(ord)" +
      " ORDER BY o_totalprice DESC LIMIT 1) orders";

    testBuilder()
      .sqlQuery(sql)
      .unOrdered()
      .baselineColumns("c_name", "o_orderkey", "o_totalprice")
      .baselineValues("Customer#000951313", (long)47035683, 306996.2)
      .baselineValues("Customer#000007180", (long)54646821, 367189.55)
      .go();
  }

  @Test
  public void testMultipleBatchesLateral_WithSortAndLimitInSubQuery() throws Exception {

    test("alter session set `planner.enable_topn`=false");

    String sql = "SELECT customer.c_name, orders.o_orderkey, orders.o_totalprice " +
      "FROM dfs.`lateraljoin/multipleFiles` customer, LATERAL " +
      "(SELECT t.ord.o_orderkey as o_orderkey, t.ord.o_totalprice as o_totalprice FROM UNNEST(customer.c_orders) t(ord)" +
      " ORDER BY o_totalprice DESC LIMIT 1) orders";

    try {
      testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineColumns("c_name", "o_orderkey", "o_totalprice")
        .baselineValues("Customer#000951313", (long)47035683, 306996.2)
        .baselineValues("Customer#000007180", (long)54646821, 367189.55)
        .go();
    } finally {
      test("alter session set `planner.enable_topn`=true");
    }
  }

  @Test
  public void testMultipleBatchesLateral_WithSortInSubQuery() throws Exception {

    String sql = "SELECT customer.c_name, customer.c_custkey, orders.o_orderkey, orders.o_totalprice " +
      "FROM dfs.`lateraljoin/multipleFiles` customer, LATERAL " +
      "(SELECT t.ord.o_orderkey as o_orderkey, t.ord.o_totalprice as o_totalprice FROM UNNEST(customer.c_orders) t(ord)" +
      " ORDER BY o_totalprice DESC) orders WHERE customer.c_custkey = '7180' LIMIT 1";

    testBuilder()
      .sqlQuery(sql)
      .ordered()
      .baselineColumns("c_name", "c_custkey", "o_orderkey", "o_totalprice")
      .baselineValues("Customer#000007180", "7180", (long) 54646821, 367189.55)
      .go();

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

  @Test
  public void testMultipleBatchesLateral_WithHashAgg() throws Exception {
    String sql = "SELECT t2.maxprice FROM (SELECT customer.c_orders AS c_orders FROM "
      + "dfs.`lateraljoin/multipleFiles/` customer) t1, LATERAL (SELECT CAST(MAX(t.ord.o_totalprice)"
      + " AS int) AS maxprice FROM UNNEST(t1.c_orders) t(ord) GROUP BY t.ord.o_orderstatus) t2";

    try {
    testBuilder()
      .optionSettingQueriesForTestQuery("alter session set `%s` = false",
        PlannerSettings.STREAMAGG.getOptionName())
      .sqlQuery(sql)
      .unOrdered()
      .baselineColumns("maxprice")
      .baselineValues(367190)
      .baselineValues(316347)
      .baselineValues(146610)
      .baselineValues(306996)
      .baselineValues(235695)
      .baselineValues(177819)
      .build().run();
    } finally {
      test("alter session set `" + PlannerSettings.STREAMAGG.getOptionName() + "` = true");
    }
  }

  @Test
  public void testLateral_HashAgg_with_nulls() throws Exception {
    String sql = "SELECT key, t3.dsls FROM cp.`lateraljoin/with_nulls.json` t LEFT OUTER "
    + "JOIN LATERAL (SELECT DISTINCT t2.sls AS dsls FROM UNNEST(t.sales) t2(sls)) t3 ON TRUE";

    try {
    testBuilder()
      .optionSettingQueriesForTestQuery("alter session set `%s` = false",
        PlannerSettings.STREAMAGG.getOptionName())
      .sqlQuery(sql)
      .unOrdered()
      .baselineColumns("key","dsls")
      .baselineValues("aa",null)
      .baselineValues("bb",100L)
      .baselineValues("bb",200L)
      .baselineValues("bb",300L)
      .baselineValues("bb",400L)
      .baselineValues("cc",null)
      .baselineValues("dd",111L)
      .baselineValues("dd",222L)
      .build().run();
    } finally {
      test("alter session set `" + PlannerSettings.STREAMAGG.getOptionName() + "` = true");
    }
  }

  @Test
  public void testMultipleBatchesLateral_WithStreamingAgg() throws Exception {
    String sql = "SELECT t2.maxprice FROM (SELECT customer.c_orders AS c_orders FROM "
        + "dfs.`lateraljoin/multipleFiles/` customer) t1, LATERAL (SELECT CAST(MAX(t.ord.o_totalprice)"
        + " AS int) AS maxprice FROM UNNEST(t1.c_orders) t(ord) GROUP BY t.ord.o_orderstatus) t2";

    testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineColumns("maxprice")
        .baselineValues(367190)
        .baselineValues(316347)
        .baselineValues(146610)
        .baselineValues(306996)
        .baselineValues(235695)
        .baselineValues(177819)
        .build().run();
  }

  @Test
  public void testLateral_StreamingAgg_with_nulls() throws Exception {
    String sql = "SELECT key, t3.dsls FROM cp.`lateraljoin/with_nulls.json` t LEFT OUTER "
        + "JOIN LATERAL (SELECT DISTINCT t2.sls AS dsls FROM UNNEST(t.sales) t2(sls)) t3 ON TRUE";

    testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineColumns("key","dsls")
        .baselineValues("aa",null)
        .baselineValues("bb",100L)
        .baselineValues("bb",200L)
        .baselineValues("bb",300L)
        .baselineValues("bb",400L)
        .baselineValues("cc",null)
        .baselineValues("dd",111L)
        .baselineValues("dd",222L)
        .build().run();
  }

  @Test
  public void testMultipleBatchesLateral_WithStreamingAggNoGroup() throws Exception {
    String sql = "SELECT t2.maxprice FROM (SELECT customer.c_orders AS c_orders FROM "
        + "dfs.`lateraljoin/multipleFiles/` customer) t1, LATERAL (SELECT CAST(MAX(t.ord.o_totalprice)"
        + " AS int) AS maxprice FROM UNNEST(t1.c_orders) t(ord) ) t2";

    testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineColumns("maxprice")
        .baselineValues(367190)
        .baselineValues(306996)
        .build().run();
  }

}
