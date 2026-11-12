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
package org.apache.drill.exec.planner.sql;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileTime;
import java.util.Arrays;
import java.util.UUID;

import org.apache.drill.common.config.DrillProperties;
import org.apache.drill.common.exceptions.UserRemoteException;
import org.apache.drill.exec.metrics.DrillMetrics;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.test.ClientFixture;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.test.TestBuilder;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/** End-to-end coverage using text scans with no predicate pushdown. */
public class TestTextPlanCache extends ClusterTest {
  private Path file;
  private String table;

  @BeforeClass
  public static void setupCluster() throws Exception {
    startCluster(ClusterFixture.builder(dirTestWatcher));
  }

  @Before
  public void setupFile() throws Exception {
    client.alterSession(PlannerSettings.ENABLE_PLAN_CACHE_OPTION, false);
    cache().awaitWrites();
    cache().clear();
    String name = "cache_" + UUID.randomUUID().toString().replace("-", "") + ".csvh";
    file = dirTestWatcher.getDfsTestTmpDir().toPath().resolve(name);
    Files.write(file, Arrays.asList("id,label", "1,one", "2,two", "3,three"), StandardCharsets.UTF_8);
    table = "dfs.tmp.`" + name + "`";
  }

  @Test
  public void testDifferentValuesAcrossConnectionsAndEmptyHit() throws Exception {
    try (ClientFixture first = cluster.clientBuilder().property(DrillProperties.USER, "text-cache").build();
         ClientFixture second = cluster.clientBuilder().property(DrillProperties.USER, "text-cache").build()) {
      rows(first, pointSql(1), "1", "one");
      first.alterSession(PlannerSettings.ENABLE_PLAN_CACHE_OPTION, true);
      second.alterSession(PlannerSettings.ENABLE_PLAN_CACHE_OPTION, true);
      long hits = cache().getHitCount();
      rows(first, pointSql(1), "1", "one");
      cache().awaitWrites();
      assertEquals(hits, cache().getHitCount());
      rows(second, pointSql(2), "2", "two");
      assertEquals(hits + 1, cache().getHitCount());
      rows(second, pointSql(9));
      assertEquals(hits + 2, cache().getHitCount());
    }
  }

  @Test
  public void testRangePredicateStaysInDrillAndRebinds() throws Exception {
    String sql = "SELECT id, label FROM " + table + " WHERE id >= '%d' AND id <= '%d'";
    rows(client, String.format(sql, 1, 2), "1", "one", "2", "two");
    String plan = client.queryBuilder().sql(sql, 1, 2).explainText();
    assertTrue(plan, plan.contains("Filter("));
    client.alterSession(PlannerSettings.ENABLE_PLAN_CACHE_OPTION, true);
    rows(client, String.format(sql, 1, 2), "1", "one", "2", "two");
    cache().awaitWrites();
    long hits = cache().getHitCount();
    rows(client, String.format(sql, 2, 3), "2", "two", "3", "three");
    assertEquals(hits + 1, cache().getHitCount());
  }

  @Test
  public void testStringToNumericCastsOnMissAndHit() throws Exception {
    for (String type : Arrays.asList("INTEGER", "BIGINT", "DOUBLE", "DECIMAL(10, 2)")) {
      String sql = "SELECT id, label FROM " + table
          + " WHERE CAST(id AS INTEGER) > CAST('%d' AS " + type + ")";
      client.alterSession(PlannerSettings.ENABLE_PLAN_CACHE_OPTION, false);
      rows(client, String.format(sql, 0), "1", "one", "2", "two", "3", "three");
      rows(client, String.format(sql, 1), "2", "two", "3", "three");
      client.alterSession(PlannerSettings.ENABLE_PLAN_CACHE_OPTION, true);
      long hits = cache().getHitCount();
      long misses = metric("misses");
      rows(client, String.format(sql, 0), "1", "one", "2", "two", "3", "three");
      cache().awaitWrites();
      assertEquals(hits, cache().getHitCount());
      assertEquals(misses + 1, metric("misses"));
      rows(client, String.format(sql, 1), "2", "two", "3", "three");
      rows(client, String.format(sql, 3));
      assertEquals(hits + 2, cache().getHitCount());
      assertEquals(misses + 1, metric("misses"));
    }
  }

  @Test
  public void testStringToTemporalCastsOnMissAndHit() throws Exception {
    String[][] cases = {
        {"DATE", "2020-01-01", "2021-01-01", "2022-01-01"},
        {"TIME", "10:00:00", "12:00:00", "14:00:00"},
        {"TIMESTAMP", "2020-01-01 00:00:00", "2021-01-01 00:00:00", "2022-01-01 00:00:00"}
    };
    for (String[] testCase : cases) {
      String sql = "SELECT id, label FROM " + table + " WHERE CAST('%s' AS " + testCase[0]
          + ") < " + testCase[0] + " '" + testCase[2] + "'";
      client.alterSession(PlannerSettings.ENABLE_PLAN_CACHE_OPTION, false);
      rows(client, String.format(sql, testCase[1]), "1", "one", "2", "two", "3", "three");
      rows(client, String.format(sql, testCase[3]));
      client.alterSession(PlannerSettings.ENABLE_PLAN_CACHE_OPTION, true);
      long hits = cache().getHitCount();
      long misses = metric("misses");
      rows(client, String.format(sql, testCase[1]), "1", "one", "2", "two", "3", "three");
      cache().awaitWrites();
      assertEquals(hits, cache().getHitCount());
      assertEquals(misses + 1, metric("misses"));
      rows(client, String.format(sql, testCase[3]));
      rows(client, String.format(sql, testCase[1]), "1", "one", "2", "two", "3", "three");
      assertEquals(hits + 2, cache().getHitCount());
      assertEquals(misses + 1, metric("misses"));
    }
  }

  @Test
  public void testNumericToStringCastOnMissAndHit() throws Exception {
    String sql = "SELECT id, label FROM " + table + " WHERE id = CAST(%d AS VARCHAR(20))";
    rows(client, String.format(sql, 1), "1", "one");
    rows(client, String.format(sql, 2), "2", "two");
    client.alterSession(PlannerSettings.ENABLE_PLAN_CACHE_OPTION, true);
    long hits = cache().getHitCount();
    long misses = metric("misses");
    rows(client, String.format(sql, 1), "1", "one");
    cache().awaitWrites();
    assertEquals(hits, cache().getHitCount());
    assertEquals(misses + 1, metric("misses"));
    rows(client, String.format(sql, 2), "2", "two");
    rows(client, String.format(sql, 9));
    assertEquals(hits + 2, cache().getHitCount());
    assertEquals(misses + 1, metric("misses"));
  }

  @Test
  public void testHeaderlessCsvAndTsv() throws Exception {
    for (String extension : Arrays.asList("csv", "tsv")) {
      Path data = file.resolveSibling(file.getFileName() + "." + extension);
      String delimiter = "csv".equals(extension) ? "," : "\t";
      Files.write(data, Arrays.asList("1" + delimiter + "one", "2" + delimiter + "two"),
          StandardCharsets.UTF_8);
      String sql = "SELECT columns[0] AS id, columns[1] AS label FROM dfs.tmp.`"
          + data.getFileName() + "` WHERE columns[0] = '%d'";
      client.alterSession(PlannerSettings.ENABLE_PLAN_CACHE_OPTION, false);
      rows(client, String.format(sql, 1), "1", "one");
      client.alterSession(PlannerSettings.ENABLE_PLAN_CACHE_OPTION, true);
      rows(client, String.format(sql, 1), "1", "one");
      cache().awaitWrites();
      long hits = cache().getHitCount();
      rows(client, String.format(sql, 2), "2", "two");
      assertEquals(hits + 1, cache().getHitCount());
    }
  }

  @Test
  public void testAppendInvalidatesAndReadsNewRows() throws Exception {
    warmPoint();
    Files.write(file, Arrays.asList("4,four"), StandardCharsets.UTF_8, StandardOpenOption.APPEND);
    long hits = cache().getHitCount();
    long invalidations = metric("invalidations");
    rows(client, pointSql(4), "4", "four");
    cache().awaitWrites();
    assertEquals(hits, cache().getHitCount());
    assertEquals(invalidations + 1, metric("invalidations"));
    rows(client, pointSql(2), "2", "two");
    assertEquals(hits + 1, cache().getHitCount());
  }

  @Test
  public void testSameLengthReplacementInvalidatesOnModificationTime() throws Exception {
    warmPoint();
    long modified = Files.getLastModifiedTime(file).toMillis();
    long size = Files.size(file);
    Files.write(file, Arrays.asList("id,label", "1,new", "2,two", "3,three"), StandardCharsets.UTF_8);
    Files.setLastModifiedTime(file, FileTime.fromMillis(modified + 2000));
    assertEquals(size, Files.size(file));
    long hits = cache().getHitCount();
    long invalidations = metric("invalidations");
    rows(client, pointSql(1), "1", "new");
    cache().awaitWrites();
    assertEquals(hits, cache().getHitCount());
    assertEquals(invalidations + 1, metric("invalidations"));
    rows(client, pointSql(2), "2", "two");
    assertEquals(hits + 1, cache().getHitCount());
  }

  @Test
  public void testHeaderChangeInvalidatesAndReplansProjection() throws Exception {
    String sql = "SELECT * FROM " + table + " WHERE id = '%d'";
    client.alterSession(PlannerSettings.ENABLE_PLAN_CACHE_OPTION, true);
    rows(client, String.format(sql, 1), "1", "one");
    cache().awaitWrites();
    long hits = cache().getHitCount();
    long invalidations = metric("invalidations");
    Files.write(file, Arrays.asList("id,description", "1,replaced", "2,two"), StandardCharsets.UTF_8);
    client.testBuilder().sqlQuery(sql, 1).unOrdered()
        .baselineColumns("id", "description").baselineValues("1", "replaced").go();
    cache().awaitWrites();
    assertEquals(hits, cache().getHitCount());
    assertEquals(invalidations + 1, metric("invalidations"));
    client.testBuilder().sqlQuery(sql, 2).unOrdered()
        .baselineColumns("id", "description").baselineValues("2", "two").go();
    assertEquals(hits + 1, cache().getHitCount());
  }

  @Test
  public void testJoinChecksBothFileVersions() throws Exception {
    Path other = file.resolveSibling("other_" + file.getFileName());
    Files.write(other, Arrays.asList("id,label", "1,first", "2,second"), StandardCharsets.UTF_8);
    String sql = "SELECT t.id, u.label FROM " + table + " t JOIN dfs.tmp.`" + other.getFileName()
        + "` u ON t.id = u.id WHERE t.id = '%d'";
    rows(client, String.format(sql, 1), "1", "first");
    client.alterSession(PlannerSettings.ENABLE_PLAN_CACHE_OPTION, true);
    rows(client, String.format(sql, 1), "1", "first");
    cache().awaitWrites();
    long hits = cache().getHitCount();
    rows(client, String.format(sql, 2), "2", "second");
    assertEquals(hits + 1, cache().getHitCount());
    long invalidations = metric("invalidations");
    Files.write(other, Arrays.asList("id,label", "1,replacement", "2,second"), StandardCharsets.UTF_8);
    rows(client, String.format(sql, 1), "1", "replacement");
    cache().awaitWrites();
    assertEquals(hits + 1, cache().getHitCount());
    assertEquals(invalidations + 1, metric("invalidations"));
  }

  @Test
  public void testDirectoryAndWildcardSelectionsBypassCache() throws Exception {
    Path directory = Files.createDirectory(file.resolveSibling(
        "dir_" + file.getFileName().toString().replace(".csvh", "")));
    Files.copy(file, directory.resolve("part.csvh"));
    client.alterSession(PlannerSettings.ENABLE_PLAN_CACHE_OPTION, true);
    long hits = cache().getHitCount();
    for (String selection : Arrays.asList(directory.getFileName().toString(),
        directory.getFileName() + "/*.csvh")) {
      String sql = "SELECT id, label FROM dfs.tmp.`" + selection + "` WHERE id = '%d'";
      rows(client, String.format(sql, 1), "1", "one");
      rows(client, String.format(sql, 2), "2", "two");
    }
    cache().awaitWrites();
    assertEquals(hits, cache().getHitCount());
    assertEquals(0L, metric("entries"));
  }

  @Test
  public void testJoinWithUnsupportedJsonBypassesCache() throws Exception {
    Path json = file.resolveSibling(file.getFileName() + ".json");
    Files.write(json, Arrays.asList("{\"id\":\"1\"}", "{\"id\":\"2\"}"), StandardCharsets.UTF_8);
    String sql = "SELECT t.id, t.label FROM " + table + " t JOIN dfs.tmp.`" + json.getFileName()
        + "` j ON t.id = j.id WHERE t.id = '%d'";
    client.alterSession(PlannerSettings.ENABLE_PLAN_CACHE_OPTION, true);
    long hits = cache().getHitCount();
    rows(client, String.format(sql, 1), "1", "one");
    rows(client, String.format(sql, 2), "2", "two");
    cache().awaitWrites();
    assertEquals(hits, cache().getHitCount());
    assertEquals(0L, metric("entries"));
  }

  @Test
  public void testDeletedFileCannotUseCachedPlan() throws Exception {
    warmPoint();
    long hits = cache().getHitCount();
    Files.delete(file);
    assertThrows(UserRemoteException.class, () -> client.queryBuilder().sql(pointSql(1)).run());
    assertEquals(hits, cache().getHitCount());
  }

  private void warmPoint() throws Exception {
    rows(client, pointSql(1), "1", "one");
    client.alterSession(PlannerSettings.ENABLE_PLAN_CACHE_OPTION, true);
    rows(client, pointSql(1), "1", "one");
    cache().awaitWrites();
    long hits = cache().getHitCount();
    rows(client, pointSql(2), "2", "two");
    assertEquals(hits + 1, cache().getHitCount());
  }

  private String pointSql(int id) {
    return "SELECT id, label FROM " + table + " WHERE id = '" + id + "'";
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

  private PlanCache cache() {
    return cluster.drillbit().getContext().getPlanCache();
  }

  private long metric(String name) {
    return (Long) DrillMetrics.getRegistry().getGauges().get("drill.plan_cache." + name).getValue();
  }
}
