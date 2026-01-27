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
package org.apache.drill.exec.store.paimon;

import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.security.PlainCredentialsProvider;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.dfs.FileSystemConfig;
import org.apache.drill.exec.store.paimon.format.PaimonFormatPluginConfig;
import org.apache.drill.common.exceptions.UserRemoteException;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.types.DataTypes;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.StringContains.containsString;
import static org.apache.drill.exec.util.StoragePluginTestUtils.DFS_PLUGIN_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class PaimonQueriesTest extends ClusterTest {

  private static final String DB_NAME = "default";
  private static final String TABLE_NAME = "append_table";
  private static final String PK_TABLE_NAME = "pk_table";
  private static String tableRelativePath;
  private static String pkTableRelativePath;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    startCluster(ClusterFixture.builder(dirTestWatcher));

    StoragePluginRegistry pluginRegistry = cluster.drillbit().getContext().getStorage();
    FileSystemConfig pluginConfig = (FileSystemConfig) pluginRegistry.getPlugin(DFS_PLUGIN_NAME).getConfig();
    Map<String, FormatPluginConfig> formats = new HashMap<>(pluginConfig.getFormats());
    formats.put("paimon", PaimonFormatPluginConfig.builder().build());
    FileSystemConfig newPluginConfig = new FileSystemConfig(
      pluginConfig.getConnection(),
      pluginConfig.getConfig(),
      pluginConfig.getWorkspaces(),
      formats,
      PlainCredentialsProvider.EMPTY_CREDENTIALS_PROVIDER);
    newPluginConfig.setEnabled(pluginConfig.isEnabled());
    pluginRegistry.put(DFS_PLUGIN_NAME, newPluginConfig);

    tableRelativePath = createAppendTable();
    pkTableRelativePath = createPrimaryKeyTable();
  }

  @Test
  public void testReadAppendTable() throws Exception {
    String query = String.format("select id, name from dfs.tmp.`%s`", tableRelativePath);
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("id", "name")
      .baselineValues(1, "alice")
      .baselineValues(2, "bob")
      .baselineValues(3, "carol")
      .go();
  }

  @Test
  public void testReadPrimaryKeyTable() throws Exception {
    String query = String.format("select id, name from dfs.tmp.`%s`", pkTableRelativePath);
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("id", "name")
      .baselineValues(1, "dave")
      .baselineValues(2, "erin")
      .go();
  }

  @Test
  public void testProjectionPushdown() throws Exception {
    String query = String.format("select name from dfs.tmp.`%s`", tableRelativePath);

    queryBuilder()
      .sql(query)
      .planMatcher()
      .include("columns=\\[.*name.*\\]")
      .match();

    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("name")
      .baselineValues("alice")
      .baselineValues("bob")
      .baselineValues("carol")
      .go();
  }

  @Test
  public void testMultiColumnProjection() throws Exception {
    String query = String.format("select id, name from dfs.tmp.`%s`", tableRelativePath);

    testBuilder()
      .sqlQuery(query)
      .ordered()
      .baselineColumns("id", "name")
      .baselineValues(1, "alice")
      .baselineValues(2, "bob")
      .baselineValues(3, "carol")
      .go();
  }

  @Test
  public void testFilterPushdown() throws Exception {
    String query = String.format("select id, name from dfs.tmp.`%s` where id = 2", tableRelativePath);

    queryBuilder()
      .sql(query)
      .planMatcher()
      .include("condition=.*equal.*id.*2")
      .match();

    testBuilder()
      .sqlQuery(query)
      .ordered()
      .baselineColumns("id", "name")
      .baselineValues(2, "bob")
      .go();
  }

  @Test
  public void testFilterPushdownGT() throws Exception {
    String query = String.format("select id, name from dfs.tmp.`%s` where id > 1", tableRelativePath);

    testBuilder()
      .sqlQuery(query)
      .ordered()
      .baselineColumns("id", "name")
      .baselineValues(2, "bob")
      .baselineValues(3, "carol")
      .go();
  }

  @Test
  public void testFilterPushdownLT() throws Exception {
    String query = String.format("select id, name from dfs.tmp.`%s` where id < 3", tableRelativePath);

    testBuilder()
      .sqlQuery(query)
      .ordered()
      .baselineColumns("id", "name")
      .baselineValues(1, "alice")
      .baselineValues(2, "bob")
      .go();
  }

  @Test
  public void testFilterPushdownGE() throws Exception {
    String query = String.format("select id, name from dfs.tmp.`%s` where id >= 2", tableRelativePath);

    testBuilder()
      .sqlQuery(query)
      .ordered()
      .baselineColumns("id", "name")
      .baselineValues(2, "bob")
      .baselineValues(3, "carol")
      .go();
  }

  @Test
  public void testFilterPushdownLE() throws Exception {
    String query = String.format("select id, name from dfs.tmp.`%s` where id <= 2", tableRelativePath);

    testBuilder()
      .sqlQuery(query)
      .ordered()
      .baselineColumns("id", "name")
      .baselineValues(1, "alice")
      .baselineValues(2, "bob")
      .go();
  }

  @Test
  public void testFilterPushdownNE() throws Exception {
    String query = String.format("select id, name from dfs.tmp.`%s` where id <> 2", tableRelativePath);

    testBuilder()
      .sqlQuery(query)
      .ordered()
      .baselineColumns("id", "name")
      .baselineValues(1, "alice")
      .baselineValues(3, "carol")
      .go();
  }

  @Test
  public void testFilterPushdownAnd() throws Exception {
    String query = String.format("select id, name from dfs.tmp.`%s` where id > 1 and id < 3", tableRelativePath);

    testBuilder()
      .sqlQuery(query)
      .ordered()
      .baselineColumns("id", "name")
      .baselineValues(2, "bob")
      .go();
  }

  @Test
  public void testFilterPushdownOr() throws Exception {
    String query = String.format("select id, name from dfs.tmp.`%s` where id = 1 or id = 3", tableRelativePath);

    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("id", "name")
      .baselineValues(1, "alice")
      .baselineValues(3, "carol")
      .go();
  }

  @Test
  public void testFilterPushdownNot() throws Exception {
    String query = String.format("select id, name from dfs.tmp.`%s` where not (id = 2)", tableRelativePath);

    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("id", "name")
      .baselineValues(1, "alice")
      .baselineValues(3, "carol")
      .go();
  }

  @Test
  public void testLimitPushdown() throws Exception {
    String query = String.format("select id from dfs.tmp.`%s` limit 2", tableRelativePath);

    queryBuilder()
      .sql(query)
      .planMatcher()
      .include("maxRecords=2")
      .match();

    assertEquals(2, queryBuilder().sql(query).run().recordCount());
  }

  @Test
  public void testCombinedPushdownFilterProjectionLimit() throws Exception {
    String query = String.format("select id, name from dfs.tmp.`%s` where id > 1 limit 1", tableRelativePath);

    testBuilder()
      .sqlQuery(query)
      .ordered()
      .baselineColumns("id", "name")
      .baselineValues(2, "bob")
      .go();
  }

  @Test
  public void testSelectWildcard() throws Exception {
    String query = String.format("select * from dfs.tmp.`%s`", tableRelativePath);

    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("id", "name")
      .baselineValues(1, "alice")
      .baselineValues(2, "bob")
      .baselineValues(3, "carol")
      .go();
  }

  @Test
  public void testSelectWithOrderBy() throws Exception {
    String query = String.format("select id, name from dfs.tmp.`%s` order by id desc", tableRelativePath);

    testBuilder()
      .sqlQuery(query)
      .ordered()
      .baselineColumns("id", "name")
      .baselineValues(3, "carol")
      .baselineValues(2, "bob")
      .baselineValues(1, "alice")
      .go();
  }

  @Test
  public void testSelectWithCount() throws Exception {
    String query = String.format("select count(*) from dfs.tmp.`%s`", tableRelativePath);

    assertEquals(3, queryBuilder().sql(query).singletonLong());
  }

  @Test
  public void testInvalidColumnName() throws Exception {
    String query = String.format("select id, invalid_column from dfs.tmp.`%s`", tableRelativePath);
    try {
      queryBuilder().sql(query).run();
      fail("Expected UserRemoteException for invalid column name");
    } catch (UserRemoteException e) {
      assertThat(e.getVerboseMessage(), containsString("invalid_column"));
    }
  }

  @Test
  public void testSelectWithSnapshotId() throws Exception {
    String snapshotQuery = String.format(
      "select snapshot_id from dfs.tmp.`%s#snapshots` order by commit_time limit 1", tableRelativePath);

    long snapshotId = queryBuilder().sql(snapshotQuery).singletonLong();
    String query = String.format(
      "select id, name from table(dfs.tmp.`%s`(type => 'paimon', snapshotId => %d))",
      tableRelativePath, snapshotId);
    long count = queryBuilder().sql(query).run().recordCount();
    assertEquals(2, count);
  }

  @Test
  public void testSelectWithSnapshotAsOfTime() throws Exception {
    String snapshotQuery = String.format(
      "select commit_time from dfs.tmp.`%s#snapshots` order by commit_time limit 1", tableRelativePath);

    long snapshotTime = queryBuilder().sql(snapshotQuery).singletonLong();
    String query = String.format(
      "select id, name from table(dfs.tmp.`%s`(type => 'paimon', snapshotAsOfTime => %d))",
      tableRelativePath, snapshotTime);
    long count = queryBuilder().sql(query).run().recordCount();
    assertEquals(2, count);
  }

  @Test
  public void testSelectWithSnapshotIdAndSnapshotAsOfTime() throws Exception {
    String query = String.format(
      "select * from table(dfs.tmp.`%s`(type => 'paimon', snapshotId => %d, snapshotAsOfTime => %d))",
      tableRelativePath, 123, 456);
    try {
      queryBuilder().sql(query).run();
      fail();
    } catch (UserRemoteException e) {
      assertThat(e.getVerboseMessage(),
        containsString("Both 'snapshotId' and 'snapshotAsOfTime' cannot be specified"));
    }
  }

  @Test
  public void testSelectSnapshotsMetadata() throws Exception {
    String query = String.format("select * from dfs.tmp.`%s#snapshots`", tableRelativePath);

    long count = queryBuilder().sql(query).run().recordCount();
    assertEquals(2, count);
  }

  @Test
  public void testSelectSchemasMetadata() throws Exception {
    String query = String.format("select * from dfs.tmp.`%s#schemas`", tableRelativePath);

    long count = queryBuilder().sql(query).run().recordCount();
    assertEquals(1, count);
  }

  @Test
  public void testSelectFilesMetadata() throws Exception {
    String query = String.format("select * from dfs.tmp.`%s#files`", tableRelativePath);

    long count = queryBuilder().sql(query).run().recordCount();
    assertEquals(2, count);
  }

  @Test
  public void testSelectManifestsMetadata() throws Exception {
    String query = String.format("select * from dfs.tmp.`%s#manifests`", tableRelativePath);

    long count = queryBuilder().sql(query).run().recordCount();
    assertEquals(2, count);
  }

  private static String createAppendTable() throws Exception {
    Path dfsRoot = Paths.get(dirTestWatcher.getDfsTestTmpDir().toURI().getPath());
    Path warehouseDir = dfsRoot.resolve("paimon_warehouse");

    Options options = new Options();
    options.set("warehouse", warehouseDir.toUri().toString());
    options.set("metastore", "filesystem");

    CatalogContext context = CatalogContext.create(options, new Configuration());
    try (Catalog catalog = CatalogFactory.createCatalog(context)) {
      catalog.createDatabase(DB_NAME, true);

      Schema schema = Schema.newBuilder()
        .column("id", DataTypes.INT())
        .column("name", DataTypes.STRING())
        .build();
      Identifier identifier = Identifier.create(DB_NAME, TABLE_NAME);
      catalog.createTable(identifier, schema, false);

      Table table = catalog.getTable(identifier);
      writeRows(table, Arrays.asList(
        GenericRow.of(1, BinaryString.fromString("alice")),
        GenericRow.of(2, BinaryString.fromString("bob"))
      ));
      writeRows(table, Arrays.asList(
        GenericRow.of(3, BinaryString.fromString("carol"))
      ));
    }

    Path tablePath = warehouseDir.resolve(DB_NAME + ".db").resolve(TABLE_NAME);
    Path relativePath = dfsRoot.relativize(tablePath);
    return relativePath.toString().replace('\\', '/');
  }

  private static String createPrimaryKeyTable() throws Exception {
    Path dfsRoot = Paths.get(dirTestWatcher.getDfsTestTmpDir().toURI().getPath());
    Path warehouseDir = dfsRoot.resolve("paimon_warehouse");

    Options options = new Options();
    options.set("warehouse", warehouseDir.toUri().toString());
    options.set("metastore", "filesystem");

    CatalogContext context = CatalogContext.create(options, new Configuration());
    try (Catalog catalog = CatalogFactory.createCatalog(context)) {
      catalog.createDatabase(DB_NAME, true);

      Schema schema = Schema.newBuilder()
        .column("id", DataTypes.INT())
        .column("name", DataTypes.STRING())
        .option(CoreOptions.BUCKET.key(), "1")
        .primaryKey("id")
        .build();
      Identifier identifier = Identifier.create(DB_NAME, PK_TABLE_NAME);
      catalog.createTable(identifier, schema, false);

      Table table = catalog.getTable(identifier);
      writeRows(table, Arrays.asList(
        GenericRow.of(1, BinaryString.fromString("dave")),
        GenericRow.of(2, BinaryString.fromString("erin"))
      ));
    }

    Path tablePath = warehouseDir.resolve(DB_NAME + ".db").resolve(PK_TABLE_NAME);
    Path relativePath = dfsRoot.relativize(tablePath);
    return relativePath.toString().replace('\\', '/');
  }

  private static void writeRows(Table table, List<GenericRow> rows) throws Exception {
    BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
    List<CommitMessage> messages;
    try (BatchTableWrite write = writeBuilder.newWrite()) {
      for (GenericRow row : rows) {
        write.write(row);
      }
      messages = write.prepareCommit();
    }
    try (BatchTableCommit commit = writeBuilder.newCommit()) {
      commit.commit(messages);
    }
  }
}
