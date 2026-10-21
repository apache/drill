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
package org.apache.drill.exec.store.iceberg;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.drill.common.config.DrillProperties;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.exec.metrics.DrillMetrics;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.planner.sql.PlanCache;
import org.apache.drill.exec.store.dfs.FileSystemConfig;
import org.apache.drill.exec.store.iceberg.format.IcebergFormatPluginConfig;
import org.apache.drill.test.ClientFixture;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.test.TestBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Types;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class IcebergPlanCacheTest extends ClusterTest {
  private static FileSystemConfig config;
  private final HadoopTables tables = new HadoopTables(new Configuration());
  private Table table;
  private String name;

  @BeforeClass
  public static void setupCluster() throws Exception {
    startCluster(ClusterFixture.builder(dirTestWatcher));
    FileSystemConfig original = (FileSystemConfig) cluster.drillbit().getContext()
        .getStorage().getPlugin("dfs").getConfig();
    Map<String, FormatPluginConfig> formats = new HashMap<>(original.getFormats());
    formats.put("iceberg", IcebergFormatPluginConfig.builder().build());
    config = original.copyWithFormats(formats);
  }

  @Before
  public void setupTable() throws Exception {
    cluster.drillbit().getContext().getStorage().put("dfs", config);
    client.alterSession(PlannerSettings.ENABLE_PLAN_CACHE_OPTION, false);
    cache().awaitWrites();
    cache().clear();
    name = "cache_" + UUID.randomUUID().toString().replace("-", "");
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()),
        Types.NestedField.optional(2, "label", Types.StringType.get()));
    table = tables.create(schema, dirTestWatcher.getDfsTestTmpDir().toPath().resolve(name).toString());
    append(1, "one", 2, "two", 3, "three");
  }

  @Test
  public void testDifferentValuesAcrossConnectionsAndEmptyHit() throws Exception {
    try (ClientFixture first = cluster.clientBuilder().property(DrillProperties.USER, "iceberg-cache").build();
         ClientFixture second = cluster.clientBuilder().property(DrillProperties.USER, "iceberg-cache").build()) {
      String sql = pointSql(1);
      first.alterSession(PlannerSettings.ENABLE_PLAN_CACHE_OPTION, false);
      rows(first, sql, 1L, "one");
      first.alterSession(PlannerSettings.ENABLE_PLAN_CACHE_OPTION, true);
      second.alterSession(PlannerSettings.ENABLE_PLAN_CACHE_OPTION, true);
      long hits = cache().getHitCount();
      rows(first, sql, 1L, "one");
      cache().awaitWrites();
      assertEquals(hits, cache().getHitCount());
      rows(second, pointSql(2), 2L, "two");
      assertEquals(hits + 1, cache().getHitCount());
      // Keep the literal precision unchanged so this tests an empty cache hit.
      rows(second, pointSql(9));
      assertEquals(hits + 2, cache().getHitCount());
    }
  }

  @Test
  public void testRangePruningRebuiltForNewValues() throws Exception {
    rows(client, rangeSql(1, 2), 1L, "one", 2L, "two");
    client.alterSession(PlannerSettings.ENABLE_PLAN_CACHE_OPTION, true);
    rows(client, rangeSql(1, 2), 1L, "one", 2L, "two");
    cache().awaitWrites();
    long hits = cache().getHitCount();
    rows(client, rangeSql(2, 3), 2L, "two", 3L, "three");
    assertEquals(hits + 1, cache().getHitCount());
  }

  @Test
  public void testCompatibleAppendVisibleOnHit() throws Exception {
    warmPoint();
    append(4, "four");
    long hits = cache().getHitCount();
    rows(client, pointSql(4), 4L, "four");
    assertEquals(hits + 1, cache().getHitCount());
  }

  @Test
  public void testSchemaChangeInvalidatesAndReplans() throws Exception {
    warmPoint();
    table.updateSchema().addColumn("extra", Types.StringType.get()).commit();
    append(4, "four");
    long hits = cache().getHitCount();
    long invalidations = invalidations();
    rows(client, pointSql(4), 4L, "four");
    cache().awaitWrites();
    assertEquals(hits, cache().getHitCount());
    assertEquals(invalidations + 1, invalidations());
    rows(client, pointSql(2), 2L, "two");
    assertEquals(hits + 1, cache().getHitCount());
  }

  @Test
  public void testReplacementAtSamePathInvalidatesAndReadsNewTable() throws Exception {
    warmPoint();
    Schema schema = table.schema();
    String location = table.location();
    tables.dropTable(location, true);
    table = tables.create(schema, location);
    append(1, "replacement");
    long hits = cache().getHitCount();
    long invalidations = invalidations();
    rows(client, pointSql(1), 1L, "replacement");
    cache().awaitWrites();
    assertEquals(hits, cache().getHitCount());
    assertEquals(invalidations + 1, invalidations());
    rows(client, pointSql(1), 1L, "replacement");
    assertEquals(hits + 1, cache().getHitCount());
  }

  @Test
  public void testPluginConfigurationsKeepSeparateEntries() throws Exception {
    warmPoint();
    Map<String, FormatPluginConfig> formats = new HashMap<>(config.getFormats());
    formats.put("iceberg", IcebergFormatPluginConfig.builder().includeColumnStats(true).build());
    cluster.drillbit().getContext().getStorage().put("dfs", config.copyWithFormats(formats));
    long hits = cache().getHitCount();
    rows(client, pointSql(2), 2L, "two");
    cache().awaitWrites();
    assertEquals(hits, cache().getHitCount());
    rows(client, pointSql(3), 3L, "three");
    assertEquals(hits + 1, cache().getHitCount());
    cluster.drillbit().getContext().getStorage().put("dfs", config);
    rows(client, pointSql(1), 1L, "one");
    assertEquals(hits + 2, cache().getHitCount());
  }

  @Test
  public void testJoinWithUnsupportedJsonBypassesCache() throws Exception {
    String json = name + ".json";
    Files.write(dirTestWatcher.getDfsTestTmpDir().toPath().resolve(json),
        Arrays.asList("{\"id\":1}", "{\"id\":2}"), StandardCharsets.UTF_8);
    String sql = "SELECT t.id, t.label FROM dfs.tmp.`" + name + "` t JOIN dfs.tmp.`" + json
        + "` j ON t.id = j.id WHERE t.id = %d";
    rows(client, String.format(sql, 1), 1L, "one");
    client.alterSession(PlannerSettings.ENABLE_PLAN_CACHE_OPTION, true);
    long hits = cache().getHitCount();
    rows(client, String.format(sql, 1), 1L, "one");
    rows(client, String.format(sql, 2), 2L, "two");
    cache().awaitWrites();
    assertEquals(hits, cache().getHitCount());
    assertEquals(0L, (long) DrillMetrics.getRegistry().getGauges()
        .get("drill.plan_cache.entries").getValue());
  }

  private void warmPoint() throws Exception {
    rows(client, pointSql(1), 1L, "one");
    client.alterSession(PlannerSettings.ENABLE_PLAN_CACHE_OPTION, true);
    rows(client, pointSql(1), 1L, "one");
    cache().awaitWrites();
    long hits = cache().getHitCount();
    rows(client, pointSql(2), 2L, "two");
    assertEquals(hits + 1, cache().getHitCount());
  }

  private String pointSql(long id) {
    return "SELECT id, label FROM dfs.tmp.`" + name + "` WHERE id = " + id;
  }

  private String rangeSql(long start, long end) {
    return "SELECT id, label FROM dfs.tmp.`" + name + "` WHERE id >= " + start + " AND id <= " + end;
  }

  private void rows(ClientFixture target, String sql, Object... values) throws Exception {
    TestBuilder builder = target.testBuilder().sqlQuery(sql).unOrdered();
    if (values.length == 0) {
      builder.expectsEmptyResultSet();
    } else {
      builder.baselineColumns("id", "label");
      for (int i = 0; i < values.length; i += 2) {
        builder.baselineValues(values[i], values[i + 1]);
      }
    }
    builder.go();
  }

  private void append(Object... values) throws IOException {
    OutputFile output = table.io().newOutputFile(table.location() + "/"
        + FileFormat.PARQUET.addExtension(UUID.randomUUID().toString()));
    FileAppender<Record> appender = new GenericAppenderFactory(table.schema()).newAppender(output, FileFormat.PARQUET);
    try (FileAppender<Record> closeable = appender) {
      for (int i = 0; i < values.length; i += 2) {
        Record record = GenericRecord.create(table.schema());
        record.setField("id", ((Number) values[i]).longValue());
        record.setField("label", values[i + 1]);
        closeable.add(record);
      }
    }
    table.newAppend().appendFile(DataFiles.builder(table.spec()).withInputFile(output.toInputFile())
        .withMetrics(appender.metrics()).build()).commit();
  }

  private PlanCache cache() {
    return cluster.drillbit().getContext().getPlanCache();
  }

  private long invalidations() {
    return (Long) DrillMetrics.getRegistry().getGauges().get("drill.plan_cache.invalidations").getValue();
  }
}
