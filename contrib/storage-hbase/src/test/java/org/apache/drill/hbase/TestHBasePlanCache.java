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
package org.apache.drill.hbase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.drill.categories.HbaseStorageTest;
import org.apache.drill.categories.SlowTest;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.planner.PhysicalPlanReader;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.planner.sql.DrillSqlWorker;
import org.apache.drill.exec.planner.sql.PlanCache;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.UserBitShared.UserCredentials;
import org.apache.drill.exec.rpc.user.UserSession;
import org.apache.drill.exec.store.hbase.HBaseGroupScan;
import org.apache.drill.exec.store.hbase.HBaseStoragePluginConfig;
import org.apache.drill.exec.util.Pointer;
import org.apache.drill.test.TestBuilder;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@Category({SlowTest.class, HbaseStorageTest.class})
public class TestHBasePlanCache extends BaseHBaseTest {
  private TableName tableName;

  @Before
  public void createTable() throws Exception {
    test("ALTER SESSION SET `planner.enable_plan_cache` = false");
    cache().awaitWrites();
    cache().clear();
    tableName = TableName.valueOf("plan_cache_" + UUID.randomUUID().toString().replace("-", ""));
    HBaseTestsSuite.getAdmin().createTable(TableDescriptorBuilder.newBuilder(tableName)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of("f")).build());
    put("a", "one");
    put("b", "two");
    put("c", "three");
  }

  @After
  public void dropTable() throws Exception {
    cache().awaitWrites();
    getDrillbitContext().getStorage().put(HBASE_STORAGE_PLUGIN_NAME, storagePluginConfig);
    test("ALTER SESSION SET `planner.enable_plan_cache` = false");
    if (HBaseTestsSuite.getAdmin().tableExists(tableName)) {
      HBaseTestsSuite.getAdmin().disableTable(tableName);
      HBaseTestsSuite.getAdmin().deleteTable(tableName);
    }
  }

  @Test
  public void testPointBoundsReboundAndEmptyHit() throws Exception {
    rows(pointSql("a"), "a", "one");
    enableCache();
    long hits = cache().getHitCount();
    rows(pointSql("a"), "a", "one");
    cache().awaitWrites();
    assertEquals(hits, cache().getHitCount());
    rows(pointSql("b"), "b", "two");
    rows(pointSql("z"));
    assertEquals(hits + 2, cache().getHitCount());
  }

  @Test
  public void testRangeBoundsRebuiltForNewValues() throws Exception {
    rows(rangeSql("a", "b"), "a", "one", "b", "two");
    enableCache();
    rows(rangeSql("a", "b"), "a", "one", "b", "two");
    cache().awaitWrites();
    long hits = cache().getHitCount();
    rows(rangeSql("b", "c"), "b", "two", "c", "three");
    assertEquals(hits + 1, cache().getHitCount());
  }

  @Test
  public void testDataUpdatesVisibleOnHit() throws Exception {
    warmPoint();
    put("a", "updated");
    put("d", "four");
    long hits = cache().getHitCount();
    rows(pointSql("a"), "a", "updated");
    rows(pointSql("d"), "d", "four");
    assertEquals(hits + 2, cache().getHitCount());
  }

  @Test
  public void testSchemaChangeInvalidatesAndReplans() throws Exception {
    warmPoint();
    HBaseTestsSuite.getAdmin().addColumnFamily(tableName, ColumnFamilyDescriptorBuilder.of("extra"));
    long hits = cache().getHitCount();
    rows(pointSql("a"), "a", "one");
    cache().awaitWrites();
    assertEquals(hits, cache().getHitCount());
    rows(pointSql("b"), "b", "two");
    assertEquals(hits + 1, cache().getHitCount());
  }

  @Test
  public void testPluginConfigurationsKeepSeparateEntries() throws Exception {
    warmPoint();
    Map<String, String> properties = new HashMap<>(storagePluginConfig.getConfig());
    properties.put("hbase.client.operation.timeout", "90000");
    HBaseStoragePluginConfig changed = new HBaseStoragePluginConfig(properties, false);
    changed.setEnabled(true);
    getDrillbitContext().getStorage().put(HBASE_STORAGE_PLUGIN_NAME, changed);
    long hits = cache().getHitCount();
    rows(pointSql("b"), "b", "two");
    cache().awaitWrites();
    assertEquals(hits, cache().getHitCount());
    rows(pointSql("c"), "c", "three");
    assertEquals(hits + 1, cache().getHitCount());
    getDrillbitContext().getStorage().put(HBASE_STORAGE_PLUGIN_NAME, storagePluginConfig);
    rows(pointSql("a"), "a", "one");
    assertEquals(hits + 2, cache().getHitCount());
  }

  @Test
  public void testJoinWithUnsupportedJsonBypassesCache() throws Exception {
    String json = tableName.getNameAsString() + ".json";
    Files.write(dirTestWatcher.getDfsTestTmpDir().toPath().resolve(json),
        Arrays.asList("{\"k\":\"a\"}", "{\"k\":\"b\"}"), StandardCharsets.UTF_8);
    String sql = projection() + " JOIN dfs.tmp.`" + json
        + "` j ON CONVERT_FROM(t.row_key, 'UTF8') = j.k WHERE t.row_key = '%s'";
    rows(String.format(sql, "a"), "a", "one");
    enableCache();
    long hits = cache().getHitCount();
    rows(String.format(sql, "a"), "a", "one");
    cache().awaitWrites();
    rows(String.format(sql, "a"), "a", "one");
    rows(String.format(sql, "b"), "b", "two");
    cache().awaitWrites();
    assertEquals(hits, cache().getHitCount());
  }

  @Test
  public void testPhysicalJsonUsesSerializedScanWithoutCacheReconstruction() throws Exception {
    PhysicalPlanReader reader = getDrillbitContext().getPlanReader();
    PhysicalPlan planned = planSql(pointSql("a"));
    assertTrue(hbaseScan(planned).isPushedFilterFullyConverted());
    assertNotNull(hbaseScan(planned).getPushedFilter());
    ObjectMapper mapper = new ObjectMapper();
    for (String scenario : Arrays.asList("missing-base", "partial", "malformed")) {
      JsonNode tree = mapper.readTree(reader.writeJson(planned));
      for (JsonNode operator : tree.get("graph")) {
        if ("hbase-scan".equals(operator.path("pop").asText())) {
          ObjectNode scan = (ObjectNode) operator;
          if ("missing-base".equals(scenario)) {
            scan.putNull("baseScanSpec");
          } else {
            scan.put("pushedFilter", "malformed".equals(scenario)
                ? "not a valid expression(" : "unsupported_predicate(`row_key`)");
          }
        }
      }
      String json = mapper.writeValueAsString(tree);
      // Cache reads must reject missing inputs or weaker conversion guarantees.
      assertThrows(IOException.class, () -> reader.readPhysicalPlanForCache(json));
      // Ordinary JSON/profile replay needs only the already serialized native scan.
      assertNotNull(reader.readPhysicalPlan(json));
      testBuilder().sqlQuery(pointSql("a")).unOrdered().physicalPlanBaseline(json).go();
    }
  }

  @Test
  public void testPartialFilterKeepsResidualOnCacheHit() throws Exception {
    try (Table table = HBaseTestsSuite.getConnection().getTable(tableName)) {
      table.put(new Put(Bytes.toBytes("d"))
          .addColumn(Bytes.toBytes("f"), Bytes.toBytes("other"), Bytes.toBytes("missing-v")));
    }
    String sql = projection() + " WHERE t.row_key = '%s' AND t.f.v IS NOT NULL";
    PhysicalPlan planned = planSql(String.format(sql, "a"), true);
    HBaseGroupScan scan = hbaseScan(planned);
    assertNotNull(getDrillbitContext().getPlanReader().writeJson(planned), scan.getPushedFilter());
    assertFalse(scan.isPushedFilterFullyConverted());
    rows(String.format(sql, "a"), "a", "one");
    enableCache();
    rows(String.format(sql, "a"), "a", "one");
    cache().awaitWrites();
    long hits = cache().getHitCount();
    rows(String.format(sql, "b"), "b", "two");
    rows(String.format(sql, "d"));
    assertEquals(hits + 2, cache().getHitCount());
  }

  @Test
  public void testFullToPartialConversionReplansForNewParameter() throws Exception {
    try (Table table = HBaseTestsSuite.getConnection().getTable(tableName)) {
      for (int i = 1; i <= 3; i++) {
        byte[] row = Bytes.add(Bytes.toBytes((long) i), new byte[] {0});
        table.put(new Put(row).addColumn(Bytes.toBytes("f"), Bytes.toBytes("v"), Bytes.toBytes("v" + i)));
      }
    }
    String sql = "SELECT CONVERT_FROM(t.f.v, 'UTF8') AS val FROM hbase.`" + tableName.getNameAsString()
        + "` t WHERE t.row_key < 'a' AND CONVERT_FROM(BYTE_SUBSTR(t.row_key, %d, 8), "
        + "'TIMESTAMP_EPOCH_BE') >= TIMESTAMP '1970-01-01 00:00:00.002'";
    assertTrue(hbaseScan(planSql(String.format(sql, 1))).isPushedFilterFullyConverted());
    assertFalse(hbaseScan(planSql(String.format(sql, 2))).isPushedFilterFullyConverted());
    valueRows(String.format(sql, 1), "v2", "v3");
    valueRows(String.format(sql, 2), "v1", "v2", "v3");
    enableCache();
    valueRows(String.format(sql, 1), "v2", "v3");
    cache().awaitWrites();
    long hits = cache().getHitCount();
    // Offset 1 is a row-key prefix, but offset 2 requires a residual filter.
    valueRows(String.format(sql, 2), "v1", "v2", "v3");
    cache().awaitWrites();
    assertEquals(hits, cache().getHitCount());
    valueRows(String.format(sql, 2), "v1", "v2", "v3");
    assertEquals(hits + 1, cache().getHitCount());
  }

  @Test
  public void testRegionSplitBetweenWarmupAndHit() throws Exception {
    String sql = rangeSql("a", "z");
    PhysicalPlanReader reader = getDrillbitContext().getPlanReader();
    PhysicalPlan original = planSql(sql);
    assertEquals(1, hbaseScan(original).getMaxParallelizationWidth());
    String json = reader.writeJson(original);
    rows(sql, "a", "one", "b", "two", "c", "three");
    enableCache();
    rows(sql, "a", "one", "b", "two", "c", "three");
    cache().awaitWrites();
    long hits = cache().getHitCount();
    HBaseTestsSuite.getAdmin().flush(tableName);
    HBaseTestsSuite.getAdmin().split(tableName, Bytes.toBytes("b"));
    Waiter.waitFor(HBaseTestsSuite.getConf(), 30_000,
        () -> HBaseTestsSuite.getAdmin().getRegions(tableName).size() == 2);
    try (RegionLocator locator = HBaseTestsSuite.getConnection().getRegionLocator(tableName)) {
      Waiter.waitFor(HBaseTestsSuite.getConf(), 30_000, () -> {
        locator.clearRegionLocationCache();
        return locator.getAllRegionLocations().size() == 2
            && locator.getAllRegionLocations().stream().allMatch(region -> region.getServerName() != null);
      });
    }
    assertEquals(2, hbaseScan(reader.readPhysicalPlanForCache(json)).getMaxParallelizationWidth());
    rows(rangeSql("a", "y"), "a", "one", "b", "two", "c", "three");
    assertEquals(hits + 1, cache().getHitCount());
  }

  private PhysicalPlan planSql(String sql) throws Exception {
    return planSql(sql, false);
  }

  private PhysicalPlan planSql(String sql, boolean enableCache) throws Exception {
    UserSession session = UserSession.Builder.newBuilder()
        .withCredentials(UserCredentials.newBuilder().setUserName("hbase-plan-test").build())
        .withOptionManager(getDrillbitContext().getOptionManager()).setSupportComplexTypes(true).build();
    session.getOptions().setLocalOption(PlannerSettings.ENABLE_PLAN_CACHE_OPTION, enableCache);
    try (QueryContext context = new QueryContext(session, getDrillbitContext(), QueryId.getDefaultInstance())) {
      return DrillSqlWorker.getPlan(context, sql, new Pointer<>());
    }
  }

  private HBaseGroupScan hbaseScan(PhysicalPlan plan) {
    return plan.getSortedOperators().stream().filter(HBaseGroupScan.class::isInstance)
        .map(HBaseGroupScan.class::cast).findFirst().orElseThrow(AssertionError::new);
  }

  private void valueRows(String sql, String... values) throws Exception {
    TestBuilder builder = testBuilder().sqlQuery(sql).unOrdered().baselineColumns("val");
    for (String value : values) {
      builder.baselineValues(value);
    }
    builder.go();
  }

  private void warmPoint() throws Exception {
    rows(pointSql("a"), "a", "one");
    enableCache();
    rows(pointSql("a"), "a", "one");
    cache().awaitWrites();
    long hits = cache().getHitCount();
    rows(pointSql("b"), "b", "two");
    assertEquals(hits + 1, cache().getHitCount());
  }

  private void enableCache() throws Exception {
    test("ALTER SESSION SET `planner.enable_plan_cache` = true");
  }

  private String projection() {
    return "SELECT CONVERT_FROM(t.row_key, 'UTF8') AS k, CONVERT_FROM(t.f.v, 'UTF8') AS val FROM hbase.`"
        + tableName.getNameAsString() + "` t";
  }

  private String pointSql(String key) {
    return projection() + " WHERE t.row_key = '" + key + "'";
  }

  private String rangeSql(String start, String end) {
    return projection() + " WHERE t.row_key >= '" + start + "' AND t.row_key <= '" + end + "'";
  }

  private void rows(String sql, Object... values) throws Exception {
    TestBuilder builder = testBuilder().sqlQuery(sql).unOrdered();
    if (values.length == 0) {
      builder.expectsEmptyResultSet();
    } else {
      builder.baselineColumns("k", "val");
      for (int i = 0; i < values.length; i += 2) {
        builder.baselineValues(values[i], values[i + 1]);
      }
    }
    builder.go();
  }

  private void put(String key, String value) throws Exception {
    try (Table table = HBaseTestsSuite.getConnection().getTable(tableName)) {
      table.put(new Put(Bytes.toBytes(key)).addColumn(Bytes.toBytes("f"), Bytes.toBytes("v"), Bytes.toBytes(value)));
    }
  }

  private PlanCache cache() {
    return getDrillbitContext().getPlanCache();
  }
}
