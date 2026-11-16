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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import org.apache.drill.exec.metrics.DrillMetrics;
import org.apache.drill.exec.physical.rowSet.RowSetReader;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.test.QueryRowSetReader;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class TestPlanCacheDifferential extends ClusterTest {
  private static final String SEED_PROPERTY = "drill.test.planCache.seed";
  private static final String FILE_NAME = "plan_cache_differential.csvh";
  private static final String TABLE = "dfs.tmp.`" + FILE_NAME + "`";
  private static final List<String> CSV = Arrays.asList(
      "id,label,day_text", "1,one,2019-12-31", "2,two,2020-01-02", "3,one,2020-01-03",
      "4,six,2020-01-04", "5,two,2020-01-05", "6,six,2019-12-31");

  private final long seed;

  public TestPlanCacheDifferential(long seed) {
    this.seed = seed;
  }

  @Parameterized.Parameters(name = "seed={0,number,#}")
  public static List<Long> seeds() {
    String seeds = System.getProperty(SEED_PROPERTY);
    if (seeds == null) {
      return Collections.singletonList(ThreadLocalRandom.current().nextLong());
    }
    return Arrays.stream(seeds.split(",", -1))
        .map(String::trim)
        .map(Long::decode)
        .collect(Collectors.toList());
  }

  @BeforeClass
  public static void setupCluster() throws Exception {
    startCluster(ClusterFixture.builder(dirTestWatcher));
    Path file = dirTestWatcher.getDfsTestTmpDir().toPath().resolve(FILE_NAME);
    Files.write(file, CSV, StandardCharsets.UTF_8);
  }

  @Test
  public void testGeneratedVariantsMatchCacheDisabledResults() throws Exception {
    System.out.println("TestPlanCacheDifferential: replay with -D" + SEED_PROPERTY + "=" + seed);
    Random random = new Random(seed);

    List<Integer> bounds = Arrays.asList(random.nextInt(2), 2 + random.nextInt(2), 4 + random.nextInt(2));
    List<Integer> upperBounds = Arrays.asList(3 + random.nextInt(2), 5 + random.nextInt(2), 7 + random.nextInt(3));
    List<String> labels = Arrays.asList("one", "two", "six");
    Collections.shuffle(bounds, random);
    Collections.shuffle(upperBounds, random);
    Collections.shuffle(labels, random);

    String filter = "CAST(id AS INTEGER) > CAST('%1$d' AS INTEGER)";
    List<String> templates = Arrays.asList(
        "SELECT label FROM " + TABLE + " WHERE " + filter + " AND CAST(id AS INTEGER) <= %2$d",
        "SELECT id, label FROM " + TABLE + " WHERE label = '%3$s' AND CAST(day_text AS DATE) > DATE '2020-01-01'",
        "SELECT l.id AS left_id, r.id AS right_id FROM " + TABLE + " l LEFT JOIN " + TABLE
            + " r ON l.label = r.label AND CAST(r.id AS INTEGER) > %1$d",
        "SELECT label || '!' AS k, COUNT(*) AS n FROM " + TABLE + " WHERE " + filter
            + " GROUP BY label || '!' HAVING label || '!' = 'one!'",
        "WITH filtered AS (SELECT id, label FROM " + TABLE + " WHERE " + filter + ")"
            + " SELECT id, label, ROW_NUMBER() OVER (PARTITION BY label ORDER BY id) AS rn FROM filtered"
            + " QUALIFY ROW_NUMBER() OVER (PARTITION BY label ORDER BY id) = 1",
        "SELECT label FROM " + TABLE + " WHERE " + filter
            + " UNION ALL SELECT label FROM " + TABLE + " WHERE label = '%3$s'");

    client.alterSession(PlannerSettings.ENABLE_PLAN_CACHE_FALLBACK_OPTION, false);
    try {
      for (String template : templates) {
        List<String> variants = new ArrayList<>();
        for (int i = 0; i < bounds.size(); i++) {
          variants.add(String.format(Locale.ROOT, template, bounds.get(i), upperBounds.get(i), labels.get(i)));
        }
        compareCacheModes(variants, "seed=" + seed + ", CSV=" + CSV);
      }
    } finally {
      client.alterSession(PlannerSettings.ENABLE_PLAN_CACHE_OPTION, false);
      client.resetSession(PlannerSettings.ENABLE_PLAN_CACHE_FALLBACK_OPTION);
    }
  }

  private void compareCacheModes(List<String> variants, String context) throws Exception {
    client.alterSession(PlannerSettings.ENABLE_PLAN_CACHE_OPTION, false);
    List<Map<List<Object>, Integer>> expected = new ArrayList<>();
    for (String sql : variants) {
      expected.add(readRows(sql, context + ", cache disabled, SQL=" + sql));
    }

    client.alterSession(PlannerSettings.ENABLE_PLAN_CACHE_OPTION, true);
    // Rotate the query that fills the cache so every binding also runs through planning.
    for (int first = 0; first < variants.size(); first++) {
      cache().awaitWrites();
      cache().clear();
      String attempt = context + ", cache fill SQL=" + variants.get(first);
      long hits = cache().getHitCount();
      long misses = metric("misses");
      for (int offset = 0; offset < variants.size(); offset++) {
        int index = (first + offset) % variants.size();
        String sql = variants.get(index);
        String phase = attempt + (offset == 0 ? ", expected miss" : ", expected hit") + ", SQL=" + sql;
        assertEquals(phase, expected.get(index), readRows(sql, phase));
        assertEquals(phase, hits + offset, cache().getHitCount());
        assertEquals(phase, misses + 1, metric("misses"));
        if (offset == 0) {
          cache().awaitWrites();
        }
      }
    }
  }

  private Map<List<Object>, Integer> readRows(String sql, String context) {
    Map<List<Object>, Integer> rows = new HashMap<>();
    QueryRowSetReader batches = client.queryBuilder().sql(sql).rowSetReader();
    try {
      while (batches.next()) {
        RowSetReader reader = batches.reader();
        while (reader.next()) {
          List<Object> row = new ArrayList<>();
          for (int i = 0; i < reader.tupleSchema().size(); i++) {
            row.add(reader.column(i).getObject());
          }
          rows.merge(row, 1, Integer::sum);
        }
      }
    } catch (Exception e) {
      throw new AssertionError(context, e);
    } finally {
      batches.close();
    }
    return rows;
  }

  private PlanCache cache() {
    return cluster.drillbit().getContext().getPlanCache();
  }

  private long metric(String name) {
    return (Long) DrillMetrics.getRegistry().getGauges().get("drill.plan_cache." + name).getValue();
  }
}
