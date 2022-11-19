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
package org.apache.drill.exec.store.delta;

import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.security.PlainCredentialsProvider;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.delta.format.DeltaFormatPluginConfig;
import org.apache.drill.exec.store.dfs.FileSystemConfig;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.junit.BeforeClass;
import org.junit.Test;

import java.math.BigDecimal;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import static org.apache.drill.exec.util.StoragePluginTestUtils.DFS_PLUGIN_NAME;
import static org.junit.Assert.assertEquals;

public class DeltaQueriesTest extends ClusterTest {

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    startCluster(ClusterFixture.builder(dirTestWatcher));

    StoragePluginRegistry pluginRegistry = cluster.drillbit().getContext().getStorage();
    FileSystemConfig pluginConfig = (FileSystemConfig) pluginRegistry.getPlugin(DFS_PLUGIN_NAME).getConfig();
    Map<String, FormatPluginConfig> formats = new HashMap<>(pluginConfig.getFormats());
    formats.put("delta", new DeltaFormatPluginConfig());
    FileSystemConfig newPluginConfig = new FileSystemConfig(
      pluginConfig.getConnection(),
      pluginConfig.getConfig(),
      pluginConfig.getWorkspaces(),
      formats,
      PlainCredentialsProvider.EMPTY_CREDENTIALS_PROVIDER);
    newPluginConfig.setEnabled(pluginConfig.isEnabled());
    pluginRegistry.put(DFS_PLUGIN_NAME, newPluginConfig);

    dirTestWatcher.copyResourceToRoot(Paths.get("data-reader-primitives"));
    dirTestWatcher.copyResourceToRoot(Paths.get("data-reader-partition-values"));
    dirTestWatcher.copyResourceToRoot(Paths.get("data-reader-nested-struct"));
  }

  @Test
  public void testSerDe() throws Exception {
    String plan = queryBuilder().sql("select * from dfs.`data-reader-partition-values`").explainJson();
    long count = queryBuilder().physical(plan).run().recordCount();
    assertEquals(3, count);
  }

  @Test
  public void testAllPrimitives() throws Exception {
    testBuilder()
      .sqlQuery("select * from dfs.`data-reader-primitives`")
      .ordered()
      .baselineColumns("as_int", "as_long", "as_byte", "as_short", "as_boolean", "as_float",
        "as_double", "as_string", "as_binary", "as_big_decimal")
      .baselineValues(null, null, null, null, null, null, null, null, null, null)
      .baselineValues(0, 0L, 0, 0, true, 0.0f, 0.0, "0", new byte[]{0, 0}, BigDecimal.valueOf(0))
      .baselineValues(1, 1L, 1, 1, false, 1.0f, 1.0, "1", new byte[]{1, 1}, BigDecimal.valueOf(1))
      .baselineValues(2, 2L, 2, 2, true, 2.0f, 2.0, "2", new byte[]{2, 2}, BigDecimal.valueOf(2))
      .baselineValues(3, 3L, 3, 3, false, 3.0f, 3.0, "3", new byte[]{3, 3}, BigDecimal.valueOf(3))
      .baselineValues(4, 4L, 4, 4, true, 4.0f, 4.0, "4", new byte[]{4, 4}, BigDecimal.valueOf(4))
      .baselineValues(5, 5L, 5, 5, false, 5.0f, 5.0, "5", new byte[]{5, 5}, BigDecimal.valueOf(5))
      .baselineValues(6, 6L, 6, 6, true, 6.0f, 6.0, "6", new byte[]{6, 6}, BigDecimal.valueOf(6))
      .baselineValues(7, 7L, 7, 7, false, 7.0f, 7.0, "7", new byte[]{7, 7}, BigDecimal.valueOf(7))
      .baselineValues(8, 8L, 8, 8, true, 8.0f, 8.0, "8", new byte[]{8, 8}, BigDecimal.valueOf(8))
      .baselineValues(9, 9L, 9, 9, false, 9.0f, 9.0, "9", new byte[]{9, 9}, BigDecimal.valueOf(9))
      .go();
  }

  @Test
  public void testProjectingColumns() throws Exception {

    String query = "select as_int, as_string from dfs.`data-reader-primitives`";

    queryBuilder()
      .sql(query)
      .planMatcher()
      .include("columns=\\[`as_int`, `as_string`\\]")
      .match();

    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("as_int", "as_string")
      .baselineValues(null, null)
      .baselineValues(0, "0")
      .baselineValues(1, "1")
      .baselineValues(2, "2")
      .baselineValues(3, "3")
      .baselineValues(4, "4")
      .baselineValues(5, "5")
      .baselineValues(6, "6")
      .baselineValues(7, "7")
      .baselineValues(8, "8")
      .baselineValues(9, "9")
      .go();
  }

  @Test
  public void testProjectNestedColumn() throws Exception {
    String query = "select t.a.ac.acb as acb, b from dfs.`data-reader-nested-struct` t";

    queryBuilder()
      .sql(query)
      .planMatcher()
      .include("columns=\\[`a`.`ac`.`acb`, `b`\\]")
      .match();

    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("acb", "b")
      .baselineValues(0L, 0)
      .baselineValues(1L, 1)
      .baselineValues(2L, 2)
      .baselineValues(3L, 3)
      .baselineValues(4L, 4)
      .baselineValues(5L, 5)
      .baselineValues(6L, 6)
      .baselineValues(7L, 7)
      .baselineValues(8L, 8)
      .baselineValues(9L, 9)
      .go();
  }

  @Test
  public void testPartitionPruning() throws Exception {
    String query = "select as_int, as_string from dfs.`data-reader-partition-values` where as_long = 1";

    queryBuilder()
      .sql(query)
      .planMatcher()
      .include("numFiles\\=1")
      .match();

    testBuilder()
      .sqlQuery(query)
      .ordered()
      .baselineColumns("as_int", "as_string")
      .baselineValues("1", "1")
      .go();
  }

  @Test
  public void testEmptyResults() throws Exception {
    String query = "select as_int, as_string from dfs.`data-reader-partition-values` where as_long = 101";

    queryBuilder()
      .sql(query)
      .planMatcher()
      .include("numFiles\\=1")
      .match();

    testBuilder()
      .sqlQuery(query)
      .ordered()
      .expectsEmptyResultSet()
      .go();
  }

  @Test
  public void testLimit() throws Exception {
    String query = "select as_int, as_string from dfs.`data-reader-partition-values` limit 1";

    // Note that both of the following two limits are expected because this format plugin supports an "artificial" limit.
    queryBuilder()
      .sql(query)
      .planMatcher()
      .include("Limit\\(fetch\\=\\[1\\]\\)")
      .include("limit\\=1")
      .match();

    long count = queryBuilder().sql(query).run().recordCount();
    assertEquals(1, count);
  }
}
