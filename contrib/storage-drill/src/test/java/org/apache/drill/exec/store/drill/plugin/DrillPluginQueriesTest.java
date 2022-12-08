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
package org.apache.drill.exec.store.drill.plugin;

import org.apache.drill.common.AutoCloseables;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.physical.rowSet.RowSetBuilder;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.test.ClientFixture;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class DrillPluginQueriesTest extends ClusterTest {

  private static final String TABLE_NAME = "dfs.tmp.test_table";

  private static ClusterFixture drill;
  private static ClientFixture drillClient;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    initPlugin();
  }

  @AfterClass
  public static void shutdown() throws Exception {
    AutoCloseables.close(drill, drillClient);
  }

  private static void initPlugin() throws Exception {
    startCluster(ClusterFixture.builder(dirTestWatcher));
    drill = ClusterFixture.builder(dirTestWatcher).build();

    DrillStoragePluginConfig config = new DrillStoragePluginConfig(
      "jdbc:drill:drillbit=localhost:" + drill.drillbit().getUserPort(),
      new Properties(), null, null);
    config.setEnabled(true);
    cluster.defineStoragePlugin("drill", config);
    cluster.defineStoragePlugin("drill2", config);
    drillClient = drill.clientFixture();

    drillClient.queryBuilder()
      .sql("create table %s as select * from cp.`tpch/nation.parquet`", TABLE_NAME)
      .run();
  }

  @Test
  public void testSerDe() throws Exception {
    String plan = queryBuilder().sql("select * from drill.%s", TABLE_NAME).explainJson();
    long count = queryBuilder().physical(plan).run().recordCount();
    assertEquals(25, count);
  }

  @Test
  public void testShowDatabases() throws Exception {
    testBuilder()
      .sqlQuery("show databases where SCHEMA_NAME='drill.dfs.tmp'")
      .unOrdered()
      .baselineColumns("SCHEMA_NAME")
      .baselineValues("drill.dfs.tmp")
      .go();
  }

  @Test
  public void testShowTables() throws Exception {
    testBuilder()
      .sqlQuery("show tables IN drill.INFORMATION_SCHEMA")
      .unOrdered()
      .baselineColumns("TABLE_SCHEMA", "TABLE_NAME")
      .baselineValues("drill.information_schema", "VIEWS")
      .baselineValues("drill.information_schema", "CATALOGS")
      .baselineValues("drill.information_schema", "COLUMNS")
      .baselineValues("drill.information_schema", "PARTITIONS")
      .baselineValues("drill.information_schema", "FILES")
      .baselineValues("drill.information_schema", "SCHEMATA")
      .baselineValues("drill.information_schema", "TABLES")
      .go();
  }

  @Test
  public void testProjectPushDown() throws Exception {
    String query = "select n_nationkey, n_regionkey, n_name from drill.%s";

    queryBuilder()
        .sql(query, TABLE_NAME)
        .planMatcher()
        .include("query=\"SELECT `n_nationkey`, `n_regionkey`, `n_name`")
        .exclude("\\*")
        .match();

    RowSet sets = queryBuilder()
      .sql(query, TABLE_NAME)
      .rowSet();

    TupleMetadata schema = new SchemaBuilder()
      .add("n_nationkey", TypeProtos.MinorType.INT)
      .add("n_regionkey", TypeProtos.MinorType.INT)
      .add("n_name", TypeProtos.MinorType.VARCHAR)
      .build();

    RowSet expected = new RowSetBuilder(client.allocator(), schema)
      .addRow(0, 0, "ALGERIA")
      .addRow(1, 1, "ARGENTINA")
      .addRow(2, 1, "BRAZIL")
      .addRow(3, 1, "CANADA")
      .addRow(4, 4, "EGYPT")
      .addRow(5, 0, "ETHIOPIA")
      .addRow(6, 3, "FRANCE")
      .addRow(7, 3, "GERMANY")
      .addRow(8, 2, "INDIA")
      .addRow(9, 2, "INDONESIA")
      .addRow(10, 4, "IRAN")
      .addRow(11, 4, "IRAQ")
      .addRow(12, 2, "JAPAN")
      .addRow(13, 4, "JORDAN")
      .addRow(14, 0, "KENYA")
      .addRow(15, 0, "MOROCCO")
      .addRow(16, 0, "MOZAMBIQUE")
      .addRow(17, 1, "PERU")
      .addRow(18, 2, "CHINA")
      .addRow(19, 3, "ROMANIA")
      .addRow(20, 4, "SAUDI ARABIA")
      .addRow(21, 2, "VIETNAM")
      .addRow(22, 3, "RUSSIA")
      .addRow(23, 3, "UNITED KINGDOM")
      .addRow(24, 1, "UNITED STATES")
      .build();

    new RowSetComparison(expected).verifyAndClearAll(sets);
  }

  @Test
  public void testFilterPushDown() throws Exception {
    String query = "select n_name, n_nationkey from drill.%s where n_nationkey = 0";
    queryBuilder()
        .sql(query, TABLE_NAME)
        .planMatcher()
        .include("WHERE")
        .exclude("Filter")
        .match();

    testBuilder()
      .unOrdered()
      .sqlQuery(query, TABLE_NAME)
      .baselineColumns("n_name", "n_nationkey")
      .baselineValues("ALGERIA", 0)
      .go();
  }

  @Test
  public void testFilterPushDownWithJoin() throws Exception {
    String query = "select * from drill.%s e\n" +
        "join drill.%s s on e.n_nationkey = s.n_nationkey where e.n_name = 'BRAZIL'";

    queryBuilder()
        .sql(query, TABLE_NAME, TABLE_NAME)
        .planMatcher()
        .include("INNER JOIN")
        .match();

    testBuilder()
      .ordered()
      .sqlQuery(query, TABLE_NAME, TABLE_NAME)
      .baselineColumns("n_nationkey", "n_name", "n_regionkey", "n_comment", "n_nationkey0",
        "n_name0", "n_regionkey0", "n_comment0")
      .baselineValues(2, "BRAZIL", 1, "y alongside of the pending deposits. carefully special " +
        "packages are about the ironic forges. slyly special ", 2, "BRAZIL", 1, "y alongside of " +
        "the pending deposits. carefully special packages are about the ironic forges. slyly special ")
      .go();
  }

  @Test
  public void testJoinDifferentDrillPlugins() throws Exception {
    String query = "select * from drill.%s e\n" +
      "join drill2.cp.`tpch/nation.parquet` s on e.n_nationkey = s.n_nationkey where e.n_name = 'BRAZIL'";

    queryBuilder()
      .sql(query, TABLE_NAME, TABLE_NAME)
      .planMatcher()
      .include("HashJoin")
      .exclude("INNER JOIN")
      .match();

    testBuilder()
      .unOrdered()
      .sqlQuery(query, TABLE_NAME, TABLE_NAME)
      .baselineColumns("n_nationkey", "n_name", "n_regionkey", "n_comment", "n_nationkey0",
        "n_name0", "n_regionkey0", "n_comment0")
      .baselineValues(2, "BRAZIL", 1, "y alongside of the pending deposits. carefully special " +
        "packages are about the ironic forges. slyly special ", 2, "BRAZIL", 1, "y alongside of " +
        "the pending deposits. carefully special packages are about the ironic forges. slyly special ")
      .go();
  }

  @Test
  public void testAggregationPushDown() throws Exception {
    String query = "select count(*) c from drill.%s";
    queryBuilder()
        .sql(query, TABLE_NAME)
        .planMatcher()
        .include("query=\"SELECT COUNT\\(\\*\\)")
        .match();

    testBuilder()
      .unOrdered()
      .sqlQuery(query, TABLE_NAME)
      .baselineColumns("c")
      .baselineValues(25L)
      .go();
  }

  @Test
  public void testLimitPushDown() throws Exception {
    String query = "select n_name from drill.%s FETCH NEXT 1 ROWS ONLY";
    queryBuilder()
        .sql(query, TABLE_NAME)
        .planMatcher()
        .include("FETCH NEXT 1 ROWS ONLY")
        .match();

    testBuilder()
      .unOrdered()
      .sqlQuery(query, TABLE_NAME)
      .baselineColumns("n_name")
      .baselineValues("ALGERIA")
      .go();
  }

  @Test
  public void testLimitWithSortPushDown() throws Exception {
    String query = "select n_nationkey from drill.%s order by n_name limit 3";
    queryBuilder()
        .sql(query, TABLE_NAME)
        .planMatcher()
        .include("ORDER BY `n_name`", "FETCH NEXT 3 ROWS ONLY")
        .match();

    testBuilder()
      .unOrdered()
      .sqlQuery(query, TABLE_NAME)
      .baselineColumns("n_nationkey")
      .baselineValues(0)
      .baselineValues(1)
      .baselineValues(2)
      .go();
  }

  @Test
  public void testAggregationWithGroupByPushDown() throws Exception {
    String query = "select sum(n_nationkey) s from drill.%s group by n_regionkey";
    queryBuilder()
        .sql(query, TABLE_NAME)
        .planMatcher()
        .include("query=\"SELECT SUM\\(`n_nationkey`\\)", "GROUP BY `n_regionkey`")
        .match();

    testBuilder()
      .unOrdered()
      .sqlQuery(query, TABLE_NAME)
      .baselineColumns("s")
      .baselineValues(47L)
      .baselineValues(50L)
      .baselineValues(58L)
      .baselineValues(68L)
      .baselineValues(77L)
      .go();
  }

  @Test
  public void testUnionAllPushDown() throws Exception {
    String query = "select col1, col2 from drill.%s " +
      "union all " +
      "select col1, col2 from drill.%s";
    queryBuilder()
      .sql(query, TABLE_NAME, TABLE_NAME)
      .planMatcher()
      .include("UNION ALL")
      .match();

    long recordCount = queryBuilder()
      .sql(query, TABLE_NAME, TABLE_NAME)
      .run()
      .recordCount();

    assertEquals(50L, recordCount);
  }
}
