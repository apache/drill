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
package org.apache.drill.exec.store.parquet;

import com.google.common.collect.Lists;
import org.apache.commons.io.filefilter.FalseFileFilter;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.drill.PlanTestBase;
import org.apache.drill.categories.UnlikelyTest;
import org.apache.commons.io.FileUtils;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.store.parquet.metadata.Metadata;
import org.apache.drill.exec.store.parquet.metadata.MetadataVersion;
import org.apache.drill.test.rowSet.schema.SchemaBuilder;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestParquetMetadataCache extends PlanTestBase {
  private static final String TABLE_NAME_1 = "parquetTable1";
  private static final String TABLE_NAME_2 = "parquetTable2";

  @BeforeClass
  public static void copyData() throws Exception {
    dirTestWatcher.copyResourceToRoot(Paths.get("multilevel"));
    dirTestWatcher.copyResourceToRoot(Paths.get("multilevel/parquet"), Paths.get(TABLE_NAME_1));
    dirTestWatcher.copyResourceToRoot(Paths.get("multilevel/parquet2"), Paths.get(TABLE_NAME_2));
    dirTestWatcher.copyResourceToRoot(Paths.get("parquet"));
  }

  @Test
  public void testPartitionPruningWithMetadataCache_1() throws Exception {
    test("refresh table metadata dfs.`%s`", TABLE_NAME_1);
    checkForMetadataFile(TABLE_NAME_1);
    String query = String.format("select dir0, dir1, o_custkey, o_orderdate from dfs.`%s` " +
            " where dir0=1994 and dir1 in ('Q1', 'Q2')", TABLE_NAME_1);
    int expectedRowCount = 20;
    int expectedNumFiles = 2;

    int actualRowCount = testSql(query);
    assertEquals(expectedRowCount, actualRowCount);
    String numFilesPattern = "numFiles=" + expectedNumFiles;
    String usedMetaPattern = "usedMetadataFile=true";
    String cacheFileRootPattern = String.format("cacheFileRoot=%s/%s/1994", dirTestWatcher.getRootDir().getCanonicalPath(), TABLE_NAME_1);
    PlanTestBase.testPlanMatchingPatterns(query, new String[]{numFilesPattern, usedMetaPattern, cacheFileRootPattern},
        new String[] {});
  }

  @Test // DRILL-3917, positive test case for DRILL-4530
  @Category(UnlikelyTest.class)
  public void testPartitionPruningWithMetadataCache_2() throws Exception {
    test("refresh table metadata dfs.`%s`", TABLE_NAME_1);
    checkForMetadataFile(TABLE_NAME_1);
    String query = String.format("select dir0, dir1, o_custkey, o_orderdate from dfs.`%s` " +
            " where dir0=1994", TABLE_NAME_1);
    int expectedRowCount = 40;
    int expectedNumFiles = 4;

    int actualRowCount = testSql(query);
    assertEquals(expectedRowCount, actualRowCount);
    String numFilesPattern = "numFiles=" + expectedNumFiles;
    String usedMetaPattern = "usedMetadataFile=true";
    String cacheFileRootPattern = String.format("cacheFileRoot=%s/%s/1994", dirTestWatcher.getRootDir().getCanonicalPath(), TABLE_NAME_1);
    PlanTestBase.testPlanMatchingPatterns(query, new String[]{numFilesPattern, usedMetaPattern, cacheFileRootPattern},
        new String[] {"Filter"});
  }

  @Test // DRILL-3937 (partitioning column is varchar)
  @Category(UnlikelyTest.class)
  public void testPartitionPruningWithMetadataCache_3() throws Exception {
    String tableName = "orders_ctas_varchar";
    test("use dfs");
    test("create table %s (o_orderdate, o_orderpriority) partition by (o_orderpriority) "
        + "as select o_orderdate, o_orderpriority from dfs.`multilevel/parquet/1994/Q1`", tableName);
    test("refresh table metadata %s", tableName);
    checkForMetadataFile(tableName);
    String query = String.format("select * from %s where o_orderpriority = '1-URGENT'", tableName);
    int expectedRowCount = 3;
    int expectedNumFiles = 1;

    int actualRowCount = testSql(query);
    assertEquals(expectedRowCount, actualRowCount);
    String numFilesPattern = "numFiles=" + expectedNumFiles;
    String usedMetaPattern = "usedMetadataFile=true";
    testPlanMatchingPatterns(query, new String[]{numFilesPattern, usedMetaPattern},
        new String[] {});
  }

  @Test // DRILL-3937 (partitioning column is binary using convert_to)
  @Category(UnlikelyTest.class)
  public void testPartitionPruningWithMetadataCache_4() throws Exception {
    String tableName = "orders_ctas_binary";
    test("use dfs");

    test("create table %s (o_orderdate, o_orderpriority) partition by (o_orderpriority) "
        + "as select o_orderdate, convert_to(o_orderpriority, 'UTF8') as o_orderpriority "
        + "from dfs.`multilevel/parquet/1994/Q1`", tableName);
    test("refresh table metadata %s", tableName);
    checkForMetadataFile(tableName);
    String query = String.format("select * from %s where o_orderpriority = '1-URGENT'", tableName);
    int expectedRowCount = 3;
    int expectedNumFiles = 1;

    int actualRowCount = testSql(query);
    assertEquals(expectedRowCount, actualRowCount);
    String numFilesPattern = "numFiles=" + expectedNumFiles;
    String usedMetaPattern = "usedMetadataFile=true";

    testPlanMatchingPatterns(query, new String[]{numFilesPattern, usedMetaPattern}, new String[] {});
  }

  @Test
  public void testCache() throws Exception {
    String tableName = "nation_ctas";
    test("use dfs");
    test("create table `%s/t1` as select * from cp.`tpch/nation.parquet`", tableName);
    test("create table `%s/t2` as select * from cp.`tpch/nation.parquet`", tableName);
    test("refresh table metadata %s", tableName);
    checkForMetadataFile(tableName);
    String query = String.format("select * from %s", tableName);
    int rowCount = testSql(query);
    Assert.assertEquals(50, rowCount);
    testPlanMatchingPatterns(query, new String[] { "usedMetadataFile=true" }, new String[]{});
  }

  @Test
  public void testUpdate() throws Exception {
    String tableName = "nation_ctas_update";
    test("use dfs");
    test("create table `%s/t1` as select * from cp.`tpch/nation.parquet`", tableName);
    test("refresh table metadata %s", tableName);
    checkForMetadataFile(tableName);
    Thread.sleep(1000);
    test("create table `%s/t2` as select * from cp.`tpch/nation.parquet`", tableName);
    int rowCount = testSql(String.format("select * from %s", tableName));
    Assert.assertEquals(50, rowCount);
  }

  @Test
  public void testCacheWithSubschema() throws Exception {
    String tableName = "nation_ctas_subschema";
    test("create table dfs.`%s/t1` as select * from cp.`tpch/nation.parquet`", tableName);
    test("refresh table metadata dfs.%s", tableName);
    checkForMetadataFile(tableName);
    int rowCount = testSql(String.format("select * from dfs.%s", tableName));
    Assert.assertEquals(25, rowCount);
  }

  @Test
  public void testFix4449() throws Exception {
    runSQL("CREATE TABLE dfs.tmp.`4449` PARTITION BY(l_discount) AS SELECT l_orderkey, l_discount FROM cp.`tpch/lineitem.parquet`");
    runSQL("REFRESH TABLE METADATA dfs.tmp.`4449`");

    testBuilder()
      .sqlQuery("SELECT COUNT(*) cnt FROM (" +
        "SELECT l_orderkey FROM dfs.tmp.`4449` WHERE l_discount < 0.05" +
        " UNION ALL" +
        " SELECT l_orderkey FROM dfs.tmp.`4449` WHERE l_discount > 0.02)")
      .unOrdered()
      .baselineColumns("cnt")
      .baselineValues(71159L)
      .go();
  }

  @Test
  public void testAbsentPluginOrWorkspaceError() throws Exception {
    testBuilder()
        .sqlQuery("refresh table metadata dfs.incorrect.table_name")
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(false, "Storage plugin or workspace does not exist [dfs.incorrect]")
        .go();

    testBuilder()
        .sqlQuery("refresh table metadata incorrect.table_name")
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(false, "Storage plugin or workspace does not exist [incorrect]")
        .go();
  }

  @Test //DRILL-4511
  @Category(UnlikelyTest.class)
  public void testTableDoesNotExistWithEmptyDirectory() throws Exception {
    final String emptyDirName = "empty_directory";
    dirTestWatcher.makeTestTmpSubDir(Paths.get(emptyDirName));

    testBuilder()
        .sqlQuery("refresh table metadata dfs.tmp.`%s`", emptyDirName)
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(false, String.format("Table %s is empty and doesn't contain any parquet files.", emptyDirName))
        .go();
  }

  @Test //DRILL-4511
  @Category(UnlikelyTest.class)
  public void testTableDoesNotExistWithIncorrectTableName() throws Exception {
    String tableName = "incorrect_table";
    testBuilder()
        .sqlQuery("refresh table metadata dfs.`%s`", tableName)
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(false, String.format("Table %s does not exist.", tableName))
        .go();
  }

  @Test
  public void testNoSupportedError() throws Exception {
    testBuilder()
        .sqlQuery("refresh table metadata cp.`tpch/nation.parquet`")
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(false, "Table tpch/nation.parquet does not support metadata refresh. " +
            "Support is currently limited to directory-based Parquet tables.")
        .go();
  }

  @Test // DRILL-4530  // single leaf level partition
  @Category(UnlikelyTest.class)
  public void testDrill4530_1() throws Exception {
    // create metadata cache
    test("refresh table metadata dfs.`%s`", TABLE_NAME_2);
    checkForMetadataFile(TABLE_NAME_2);

    // run query and check correctness
    String query1 = String.format("select dir0, dir1, o_custkey, o_orderdate from dfs.`%s` " +
            " where dir0=1995 and dir1='Q3'", TABLE_NAME_2);
    int expectedRowCount = 20;
    int expectedNumFiles = 2;

    int actualRowCount = testSql(query1);
    assertEquals(expectedRowCount, actualRowCount);
    String numFilesPattern = "numFiles=" + expectedNumFiles;
    String usedMetaPattern = "usedMetadataFile=true";
    String cacheFileRootPattern = String.format("cacheFileRoot=%s/%s/1995/Q3", dirTestWatcher.getRootDir().getCanonicalPath(), TABLE_NAME_2);
    PlanTestBase.testPlanMatchingPatterns(query1, new String[]{numFilesPattern, usedMetaPattern, cacheFileRootPattern},
        new String[] {"Filter"});
  }

  @Test // DRILL-4530  // single non-leaf level partition
  @Category(UnlikelyTest.class)
  public void testDrill4530_2() throws Exception {
    // create metadata cache
    test("refresh table metadata dfs.`%s`", TABLE_NAME_2);
    checkForMetadataFile(TABLE_NAME_2);

    // run query and check correctness
    String query1 = String.format("select dir0, dir1, o_custkey, o_orderdate from dfs.`%s` " +
            " where dir0=1995", TABLE_NAME_2);
    int expectedRowCount = 80;
    int expectedNumFiles = 8;

    int actualRowCount = testSql(query1);
    assertEquals(expectedRowCount, actualRowCount);
    String numFilesPattern = "numFiles=" + expectedNumFiles;
    String usedMetaPattern = "usedMetadataFile=true";
    String cacheFileRootPattern = String.format("cacheFileRoot=%s/%s/1995", dirTestWatcher.getRootDir().getCanonicalPath(), TABLE_NAME_2);
    PlanTestBase.testPlanMatchingPatterns(query1, new String[]{numFilesPattern, usedMetaPattern, cacheFileRootPattern},
        new String[] {"Filter"});
  }

  @Test // DRILL-4530  // only dir1 filter is present, no dir0, hence this maps to multiple partitions
  @Category(UnlikelyTest.class)
  public void testDrill4530_3() throws Exception {
    // create metadata cache
    test("refresh table metadata dfs.`%s`", TABLE_NAME_2);
    checkForMetadataFile(TABLE_NAME_2);

    // run query and check correctness
    String query1 = String.format("select dir0, dir1, o_custkey, o_orderdate from dfs.`%s` " +
            " where dir1='Q3'", TABLE_NAME_2);
    int expectedRowCount = 40;
    int expectedNumFiles = 4;

    int actualRowCount = testSql(query1);
    assertEquals(expectedRowCount, actualRowCount);
    String numFilesPattern = "numFiles=" + expectedNumFiles;
    String usedMetaPattern = "usedMetadataFile=true";
    String cacheFileRootPattern = String.format("cacheFileRoot=%s/%s", dirTestWatcher.getRootDir().getCanonicalPath(), TABLE_NAME_2);
    PlanTestBase.testPlanMatchingPatterns(query1, new String[]{numFilesPattern, usedMetaPattern, cacheFileRootPattern},
        new String[] {});
  }

  @Test // DRILL-4530  // non-existent partition (1 subdirectory's cache file will still be read for schema)
  @Category(UnlikelyTest.class)
  public void testDrill4530_4() throws Exception {
    // create metadata cache
    test("refresh table metadata dfs.`%s`", TABLE_NAME_2);
    checkForMetadataFile(TABLE_NAME_2);

    // run query and check correctness
    String query1 = String.format("select dir0, dir1, o_custkey, o_orderdate from dfs.`%s` " +
            " where dir0=1995 and dir1='Q6'", TABLE_NAME_2);
    int expectedRowCount = 0;
    int expectedNumFiles = 1;

    int actualRowCount = testSql(query1);
    assertEquals(expectedRowCount, actualRowCount);
    String numFilesPattern = "numFiles=" + expectedNumFiles;
    String usedMetaPattern = "usedMetadataFile=true";
    String cacheFileRootPattern = String.format("cacheFileRoot=%s/%s/*/*", dirTestWatcher.getRootDir().getCanonicalPath(), TABLE_NAME_2);
    PlanTestBase.testPlanMatchingPatterns(query1, new String[]{numFilesPattern, usedMetaPattern, cacheFileRootPattern},
        new String[] {});
  }

  @Test // DRILL-4794
  @Category(UnlikelyTest.class)
  public void testDrill4794() throws Exception {
    test("refresh table metadata dfs.`%s`", TABLE_NAME_1);
    checkForMetadataFile(TABLE_NAME_1);
    String query = String.format("select dir0, dir1, o_custkey, o_orderdate from dfs.`%s` " +
            " where dir0=1994 or dir1='Q3'", TABLE_NAME_1);

    int expectedRowCount = 60;
    int expectedNumFiles = 6;

    int actualRowCount = testSql(query);
    assertEquals(expectedRowCount, actualRowCount);
    String numFilesPattern = "numFiles=" + expectedNumFiles;
    String usedMetaPattern = "usedMetadataFile=true";
    String cacheFileRootPattern = String.format("cacheFileRoot=%s/%s", dirTestWatcher.getRootDir().getCanonicalPath(), TABLE_NAME_1);
    PlanTestBase.testPlanMatchingPatterns(query, new String[]{numFilesPattern, usedMetaPattern, cacheFileRootPattern},
        new String[] {});
  }

  @Test // DRILL-4786
  @Category(UnlikelyTest.class)
  public void testDrill4786_1() throws Exception {
    // create metadata cache
    test("refresh table metadata dfs.`%s`", TABLE_NAME_2);
    checkForMetadataFile(TABLE_NAME_2);

    // run query and check correctness
    String query1 = String.format("select dir0, dir1, o_custkey, o_orderdate from dfs.`%s` " +
            " where dir0=1995 and dir1 in ('Q1', 'Q2')", TABLE_NAME_2);

    int expectedRowCount = 40;
    int expectedNumFiles = 4;

    int actualRowCount = testSql(query1);
    assertEquals(expectedRowCount, actualRowCount);
    String numFilesPattern = "numFiles=" + expectedNumFiles;
    String usedMetaPattern = "usedMetadataFile=true";
    String cacheFileRootPattern = String.format("cacheFileRoot=%s/%s/1995", dirTestWatcher.getRootDir().getCanonicalPath(), TABLE_NAME_2);
    PlanTestBase.testPlanMatchingPatterns(query1, new String[]{numFilesPattern, usedMetaPattern, cacheFileRootPattern},
        new String[] {});

  }

  @Test // DRILL-4786
  @Category(UnlikelyTest.class)
  public void testDrill4786_2() throws Exception {
    // create metadata cache
    test("refresh table metadata dfs.`%s`", TABLE_NAME_2);
    checkForMetadataFile(TABLE_NAME_2);

    // run query and check correctness
    String query1 = String.format("select dir0, dir1, o_custkey, o_orderdate from dfs.`%s` " +
            " where dir0 in (1994, 1995) and dir1 = 'Q3'", TABLE_NAME_2);

    int expectedRowCount = 40;
    int expectedNumFiles = 4;

    int actualRowCount = testSql(query1);
    assertEquals(expectedRowCount, actualRowCount);
    String numFilesPattern = "numFiles=" + expectedNumFiles;
    String usedMetaPattern = "usedMetadataFile=true";
    String cacheFileRootPattern = String.format("cacheFileRoot=%s/%s", dirTestWatcher.getRootDir().getCanonicalPath(), TABLE_NAME_2);
    PlanTestBase.testPlanMatchingPatterns(query1, new String[]{numFilesPattern, usedMetaPattern, cacheFileRootPattern},
        new String[] {});
  }

  @Test // DRILL-4877
  @Category(UnlikelyTest.class)
  public void testDrill4877() throws Exception {
    // create metadata cache
    test("refresh table metadata dfs.`%s`", TABLE_NAME_2);
    checkForMetadataFile(TABLE_NAME_2);

    // run query and check correctness
    String query1 = String.format("select max(dir0) as max0, max(dir1) as max1 from dfs.`%s` ", TABLE_NAME_2);

    testBuilder()
    .sqlQuery(query1)
      .unOrdered()
      .baselineColumns("max0", "max1")
      .baselineValues("1995", "Q4")
      .go();

    int expectedNumFiles = 1; // point to selectionRoot since no pruning is done in this query

    String numFilesPattern = "numFiles=" + expectedNumFiles;
    String usedMetaPattern = "usedMetadataFile=true";
    String cacheFileRootPattern = String.format("cacheFileRoot=%s/%s", dirTestWatcher.getRootDir().getCanonicalPath(), TABLE_NAME_2);
    PlanTestBase.testPlanMatchingPatterns(query1, new String[]{numFilesPattern, usedMetaPattern, cacheFileRootPattern},
        new String[] {});

  }

  @Test // DRILL-3867
  public void testMoveCache() throws Exception {
    final String tableName = "nation_move";
    final String newTableName = "nation_moved";
    try {
      test("use dfs");
      test("create table `%s/t1` as select * from cp.`tpch/nation.parquet`", tableName);
      test("create table `%s/t2` as select * from cp.`tpch/nation.parquet`", tableName);
      test("refresh table metadata %s", tableName);
      checkForMetadataFile(tableName);
      File srcFile = new File(dirTestWatcher.getRootDir(), tableName);
      File dstFile = new File(dirTestWatcher.getRootDir(), newTableName);
      FileUtils.moveDirectory(srcFile, dstFile);
      assertFalse("Cache file was not moved successfully", srcFile.exists());
      int rowCount = testSql(String.format("select * from %s", newTableName));
      assertEquals("An incorrect result was obtained while querying a table with metadata cache files", 50, rowCount);
    } finally {
      test("drop table if exists %s", newTableName);
    }
  }

  @Test
  public void testOldMetadataVersions() throws Exception {
    final Path tablePath = Paths.get("absolute_paths_metadata");
    final Path rootMetadataPath = Paths.get("parquet", "metadata_files_with_old_versions");
    // gets folders with different metadata cache versions
    String[] metadataPaths = dirTestWatcher.getRootDir()
      .toPath()
      .resolve(rootMetadataPath)
      .toFile()
      .list();

    for (String metadataPath : metadataPaths) {
      try {
        test("use dfs.tmp");
        // creating two inner directories to leverage METADATA_DIRECTORIES_FILENAME metadata file as well
        final Path absolutePathsMetadataT1 = tablePath.resolve("t1");
        final Path absolutePathsMetadataT2 = tablePath.resolve("t2");

        String createQuery = "create table `%s` as select * from cp.`tpch/nation.parquet`";

        test(createQuery, absolutePathsMetadataT1);
        test(createQuery, absolutePathsMetadataT2);

        Path relativePath = rootMetadataPath.resolve(metadataPath);

        File metaFile = dirTestWatcher.copyResourceToTestTmp(
          relativePath.resolve("metadata_directories.requires_replace.txt"),
          tablePath.resolve(Metadata.METADATA_DIRECTORIES_FILENAME));
        dirTestWatcher.replaceMetaDataContents(metaFile, dirTestWatcher.getDfsTestTmpDir(),
          null);
        metaFile = dirTestWatcher.copyResourceToTestTmp(
          relativePath.resolve("metadata_table.requires_replace.txt"),
          tablePath.resolve(Metadata.METADATA_FILENAME));
        dirTestWatcher.replaceMetaDataContents(metaFile, dirTestWatcher.getDfsTestTmpDir(),
          null);
        metaFile = dirTestWatcher.copyResourceToTestTmp(
          relativePath.resolve("metadata_table_t1.requires_replace.txt"),
          absolutePathsMetadataT1.resolve(Metadata.METADATA_FILENAME));
        dirTestWatcher.replaceMetaDataContents(metaFile, dirTestWatcher.getDfsTestTmpDir(),
          null);
        metaFile = dirTestWatcher.copyResourceToTestTmp(
          relativePath.resolve("metadata_table_t2.requires_replace.txt"),
          absolutePathsMetadataT2.resolve(Metadata.METADATA_FILENAME));
        dirTestWatcher.replaceMetaDataContents(metaFile, dirTestWatcher.getDfsTestTmpDir(),
          null);
        String query = String.format("select * from %s", tablePath);
        int expectedRowCount = 50;
        int expectedNumFiles = 1; // point to selectionRoot since no pruning is done in this query
        int actualRowCount = testSql(query);
        assertEquals("An incorrect result was obtained while querying a table with metadata cache files",
                      expectedRowCount, actualRowCount);
        String numFilesPattern = "numFiles=" + expectedNumFiles;
        String usedMetaPattern = "usedMetadataFile=true";
        String cacheFileRootPattern = String.format("cacheFileRoot=%s/%s", dirTestWatcher.getDfsTestTmpDir().getCanonicalPath(), tablePath);
        PlanTestBase.testPlanMatchingPatterns(query, new String[]{numFilesPattern, usedMetaPattern, cacheFileRootPattern},
                                              new String[]{"Filter"});
      } finally {
        test("drop table if exists %s", tablePath);
      }
    }
  }

  @Test
  public void testSpacesInMetadataCachePath() throws Exception {
    final String pathWithSpaces = "path with spaces";
    try {
      test("use dfs");
      // creating multilevel table to store path with spaces in both metadata files (METADATA and METADATA_DIRECTORIES)
      test("create table `%s` as select * from cp.`tpch/nation.parquet`", pathWithSpaces);
      test("create table `%1$s/%1$s` as select * from cp.`tpch/nation.parquet`", pathWithSpaces);
      test("refresh table metadata `%s`", pathWithSpaces);
      checkForMetadataFile(pathWithSpaces);
      String query = String.format("select * from `%s`", pathWithSpaces);
      int expectedRowCount = 50;
      int expectedNumFiles = 1; // point to selectionRoot since no pruning is done in this query
      int actualRowCount = testSql(query);
      assertEquals("An incorrect result was obtained while querying a table with metadata cache files",
          expectedRowCount, actualRowCount);
      String numFilesPattern = "numFiles=" + expectedNumFiles;
      String usedMetaPattern = "usedMetadataFile=true";
      String cacheFileRootPattern = String.format("cacheFileRoot=%s/%s", dirTestWatcher.getRootDir().getCanonicalPath(), pathWithSpaces);
      PlanTestBase.testPlanMatchingPatterns(query, new String[]{numFilesPattern, usedMetaPattern, cacheFileRootPattern},
          new String[] {"Filter"});
    } finally {
      test("drop table if exists `%s`", pathWithSpaces);
    }
  }

  @Test
  public void testFutureUnsupportedMetadataVersion() throws Exception {
    final String unsupportedMetadataVersion = "unsupported_metadata_version";
    try {
      test("use dfs.tmp");
      test("create table `%s` as select * from cp.`tpch/nation.parquet`", unsupportedMetadataVersion);
      MetadataVersion lastVersion = MetadataVersion.Constants.SUPPORTED_VERSIONS.last();
      // Get the future version, which is absent in MetadataVersions.SUPPORTED_VERSIONS set
      String futureVersion = new MetadataVersion(lastVersion.getMajor() + 1, 0).toString();
      File metaDataFile = dirTestWatcher.copyResourceToTestTmp(
        Paths.get("parquet", "unsupported_metadata", "unsupported_metadata_version.requires_replace.txt"),
        Paths.get(unsupportedMetadataVersion, Metadata.METADATA_FILENAME));
      dirTestWatcher.replaceMetaDataContents(metaDataFile, dirTestWatcher.getDfsTestTmpDir(), futureVersion);
      String query = String.format("select * from %s", unsupportedMetadataVersion);
      int expectedRowCount = 25;
      int expectedNumFiles = 1;
      int actualRowCount = testSql(query);
      assertEquals("An incorrect result was obtained while querying a table with metadata cache files",
          expectedRowCount, actualRowCount);
      String numFilesPattern = "numFiles=" + expectedNumFiles;
      String usedMetaPattern = "usedMetadataFile=false"; // ignoring metadata cache file
      PlanTestBase.testPlanMatchingPatterns(query, new String[]{numFilesPattern, usedMetaPattern},
          new String[] {"Filter"});
    } finally {
      test("drop table if exists %s", unsupportedMetadataVersion);
    }
  }

  @Test
  public void testCorruptedMetadataFile() throws Exception {
    final String corruptedMetadata = "corrupted_metadata";
    try {
      test("use dfs.tmp");
      test("create table `%s` as select * from cp.`tpch/nation.parquet`", corruptedMetadata);
      dirTestWatcher.copyResourceToTestTmp(
        Paths.get("parquet", "unsupported_metadata", "corrupted_metadata.requires_replace.txt"),
        Paths.get(corruptedMetadata, Metadata.METADATA_FILENAME));
      String query = String.format("select * from %s", corruptedMetadata);
      int expectedRowCount = 25;
      int expectedNumFiles = 1;
      int actualRowCount = testSql(query);
      assertEquals("An incorrect result was obtained while querying a table with metadata cache files",
          expectedRowCount, actualRowCount);
      String numFilesPattern = "numFiles=" + expectedNumFiles;
      String usedMetaPattern = "usedMetadataFile=false"; // ignoring metadata cache file
      PlanTestBase.testPlanMatchingPatterns(query, new String[]{numFilesPattern, usedMetaPattern},
          new String[] {"Filter"});
    } finally {
      test("drop table if exists %s", corruptedMetadata);
    }
  }

  @Test
  public void testEmptyMetadataFile() throws Exception {
    final String emptyMetadataFile = "empty_metadata_file";
    try {
      test("use dfs.tmp");
      test("create table `%s` as select * from cp.`tpch/nation.parquet`", emptyMetadataFile);
      dirTestWatcher.copyResourceToTestTmp(
        Paths.get("parquet", "unsupported_metadata", "empty_metadata_file.requires_replace.txt"),
        Paths.get(emptyMetadataFile, Metadata.METADATA_FILENAME));
      String query = String.format("select * from %s", emptyMetadataFile);
      int expectedRowCount = 25;
      int expectedNumFiles = 1;
      int actualRowCount = testSql(query);
      assertEquals("An incorrect result was obtained while querying a table with metadata cache files",
          expectedRowCount, actualRowCount);
      String numFilesPattern = "numFiles=" + expectedNumFiles;
      String usedMetaPattern = "usedMetadataFile=false"; // ignoring metadata cache file
      PlanTestBase.testPlanMatchingPatterns(query, new String[]{numFilesPattern, usedMetaPattern},
          new String[] {"Filter"});
    } finally {
      test("drop table if exists %s", emptyMetadataFile);
    }
  }

  @Test
  public void testRootMetadataFileIsAbsent() throws Exception {
    final String rootMetaCorruptedTable = "root_meta_corrupted_table";
    File dataDir = dirTestWatcher.copyResourceToRoot(
      Paths.get("multilevel", "parquet"),
      Paths.get(rootMetaCorruptedTable));

    try {
      test("use dfs");
      test("refresh table metadata `%s`", rootMetaCorruptedTable);
      checkForMetadataFile(rootMetaCorruptedTable);
      File rootMetadataFile = FileUtils.getFile(dataDir, Metadata.METADATA_FILENAME);
      assertTrue(String.format("Metadata cache file '%s' isn't deleted", rootMetadataFile.getPath()), rootMetadataFile.delete());

      setTimestampToZero(dataDir);

      String query = String.format("select dir0, dir1, o_custkey, o_orderdate from `%s` " + " where dir0=1994 or dir1='Q3'", rootMetaCorruptedTable);
      int expectedRowCount = 60;
      int expectedNumFiles = 6;
      int actualRowCount = testSql(query);
      assertEquals("An incorrect result was obtained while querying a table with metadata cache files", expectedRowCount, actualRowCount);
      String numFilesPattern = "numFiles=" + expectedNumFiles;
      String usedMetaPattern = "usedMetadataFile=false";
      PlanTestBase.testPlanMatchingPatterns(query, new String[]{numFilesPattern, usedMetaPattern}, new String[]{"cacheFileRoot", "Filter"});
    } finally {
      FileUtils.deleteQuietly(dataDir);
    }
  }

  @Test
  public void testInnerMetadataFilesAreAbsent() throws Exception {
    final String innerMetaCorruptedTable = "inner_meta_corrupted_table";
    File dataDir = dirTestWatcher.copyResourceToRoot(
      Paths.get("multilevel", "parquet"),
      Paths.get(innerMetaCorruptedTable));

    try {
      test("use dfs");
      test("refresh table metadata `%s`", innerMetaCorruptedTable);
      checkForMetadataFile(innerMetaCorruptedTable);
      File firstInnerMetadataFile = FileUtils.getFile(dataDir, "1994", Metadata.METADATA_FILENAME);
      File secondInnerMetadataFile = FileUtils.getFile(dataDir, "1994", "Q3", Metadata.METADATA_FILENAME);
      assertTrue(String.format("Metadata cache file '%s' isn't deleted", firstInnerMetadataFile.getPath()),
          firstInnerMetadataFile.delete());
      assertTrue(String.format("Metadata cache file '%s' isn't deleted", secondInnerMetadataFile.getPath()),
          secondInnerMetadataFile.delete());

      setTimestampToZero(dataDir);

      String query = String.format("select dir0, dir1, o_custkey, o_orderdate from `%s` " +
          " where dir0=1994 or dir1='Q3'", innerMetaCorruptedTable);
      int expectedRowCount = 60;
      int expectedNumFiles = 6;
      int actualRowCount = testSql(query);
      assertEquals("An incorrect result was obtained while querying a table with metadata cache files",
          expectedRowCount, actualRowCount);
      String numFilesPattern = "numFiles=" + expectedNumFiles;
      String usedMetaPattern = "usedMetadataFile=false";
      PlanTestBase.testPlanMatchingPatterns(query, new String[]{numFilesPattern, usedMetaPattern},
          new String[] {"cacheFileRoot", "Filter"});
    } finally {
      FileUtils.deleteQuietly(dataDir);
    }
  }

  private void setTimestampToZero(File dataDir) throws IOException {
    Collection<File> dirs = FileUtils.listFilesAndDirs(dataDir, FalseFileFilter.INSTANCE, TrueFileFilter.INSTANCE);

    for (File dir: dirs) {
      Files.setLastModifiedTime(dir.toPath(), FileTime.from(0, TimeUnit.MILLISECONDS));
    }
  }

  @Test // DRILL-4264
  @Category(UnlikelyTest.class)
  public void testMetadataCacheFieldWithDots() throws Exception {
    final String tableWithDots = "dfs.tmp.`complex_table`";
    try {
      test("create table %s as\n" +
        "select cast(1 as int) as `column.with.dots`, t.`column`.`with.dots`\n" +
        "from cp.`store/parquet/complex/complex.parquet` t limit 1", tableWithDots);

      String query = String.format("select * from %s", tableWithDots);
      int expectedRowCount = 1;

      int actualRowCount = testSql(query);
      assertEquals("Row count does not match the expected value", expectedRowCount, actualRowCount);
      PlanTestBase.testPlanMatchingPatterns(query, new String[]{"usedMetadataFile=false"}, null);

      test("refresh table metadata %s", tableWithDots);

      actualRowCount = testSql(query);
      assertEquals("Row count does not match the expected value", expectedRowCount, actualRowCount);
      PlanTestBase.testPlanMatchingPatterns(query, new String[]{"usedMetadataFile=true"}, null);
    } finally {
      test(String.format("drop table if exists %s", tableWithDots));
    }
  }

  @Test // DRILL-4139
  public void testBooleanPartitionPruning() throws Exception {
    final String boolPartitionTable = "dfs.tmp.`interval_bool_partition`";
    try {
      test("create table %s partition by (col_bln) as " +
        "select * from cp.`parquet/alltypes_required.parquet`", boolPartitionTable);

      String query = String.format("select * from %s where col_bln = true", boolPartitionTable);
      int expectedRowCount = 2;

      int actualRowCount = testSql(query);
      assertEquals("Row count does not match the expected value", expectedRowCount, actualRowCount);
      PlanTestBase.testPlanMatchingPatterns(query, new String[]{"usedMetadataFile=false"}, new String[]{"Filter"});

      test("refresh table metadata %s", boolPartitionTable);

      actualRowCount = testSql(query);
      assertEquals("Row count does not match the expected value", expectedRowCount, actualRowCount);
      PlanTestBase.testPlanMatchingPatterns(query, new String[]{"usedMetadataFile=true"}, new String[]{"Filter"});
    } finally {
      test("drop table if exists %s", boolPartitionTable);
    }
  }

  @Ignore // Statistics for INTERVAL is not available (see PARQUET-1064)
  @Test // DRILL-4139
  public void testIntervalDayPartitionPruning() throws Exception {
    final String intervalDayPartitionTable = "dfs.tmp.`interval_day_partition`";
    try {
      test("create table %s partition by (col_intrvl_day) as " +
        "select * from cp.`parquet/alltypes_optional.parquet`", intervalDayPartitionTable);

      String query = String.format("select * from %s " +
        "where col_intrvl_day = cast('P26DT27386S' as interval day)", intervalDayPartitionTable);
      int expectedRowCount = 1;

      int actualRowCount = testSql(query);
      assertEquals("Row count does not match the expected value", expectedRowCount, actualRowCount);
      PlanTestBase.testPlanMatchingPatterns(query, new String[]{"usedMetadataFile=false"}, new String[]{"Filter"});

      test("refresh table metadata %s", intervalDayPartitionTable);

      actualRowCount = testSql(query);
      assertEquals("Row count does not match the expected value", expectedRowCount, actualRowCount);
      PlanTestBase.testPlanMatchingPatterns(query, new String[]{"usedMetadataFile=true"}, new String[]{"Filter"});
    } finally {
      test(String.format("drop table if exists %s", intervalDayPartitionTable));
    }
  }

  @Ignore // Statistics for INTERVAL is not available (see PARQUET-1064)
  @Test // DRILL-4139
  public void testIntervalYearPartitionPruning() throws Exception {
    final String intervalYearPartitionTable = "dfs.tmp.`interval_yr_partition`";
    try {
      test("create table %s partition by (col_intrvl_yr) as " +
        "select * from cp.`parquet/alltypes_optional.parquet`", intervalYearPartitionTable);

      String query = String.format("select * from %s where col_intrvl_yr = cast('P314M' as interval year)",
        intervalYearPartitionTable);
      int expectedRowCount = 1;

      int actualRowCount = testSql(query);
      assertEquals("Row count does not match the expected value", expectedRowCount, actualRowCount);
      PlanTestBase.testPlanMatchingPatterns(query, new String[]{"usedMetadataFile=false"}, new String[]{"Filter"});

      test("refresh table metadata %s", intervalYearPartitionTable);

      actualRowCount = testSql(query);
      assertEquals("Row count does not match the expected value", expectedRowCount, actualRowCount);
      PlanTestBase.testPlanMatchingPatterns(query, new String[]{"usedMetadataFile=true"}, new String[]{"Filter"});
    } finally {
      test("drop table if exists %s", intervalYearPartitionTable);
    }
  }

  @Test // DRILL-4139
  public void testVarCharWithNullsPartitionPruning() throws Exception {
    final String intervalYearPartitionTable = "dfs.tmp.`varchar_optional_partition`";
    try {
      test("create table %s partition by (col_vrchr) as " +
        "select * from cp.`parquet/alltypes_optional.parquet`", intervalYearPartitionTable);

      String query = String.format("select * from %s where col_vrchr = 'Nancy Cloke'",
        intervalYearPartitionTable);
      int expectedRowCount = 1;

      int actualRowCount = testSql(query);
      assertEquals("Row count does not match the expected value", expectedRowCount, actualRowCount);
      PlanTestBase.testPlanMatchingPatterns(query, new String[]{"usedMetadataFile=false"}, new String[]{"Filter"});

      test("refresh table metadata %s", intervalYearPartitionTable);

      actualRowCount = testSql(query);
      assertEquals("Row count does not match the expected value", expectedRowCount, actualRowCount);
      PlanTestBase.testPlanMatchingPatterns(query, new String[]{"usedMetadataFile=true"}, new String[]{"Filter"});
    } finally {
      test("drop table if exists %s", intervalYearPartitionTable);
    }
  }

  @Ignore // Statistics for DECIMAL is not available (see PARQUET-1322).
  @Test // DRILL-4139
  public void testDecimalPartitionPruning() throws Exception {
    List<String> ctasQueries = Lists.newArrayList();
    // decimal stores as fixed_len_byte_array
    ctasQueries.add("create table %s partition by (manager_id) as " +
      "select * from cp.`parquet/fixedlenDecimal.parquet`");
    // decimal stores as int32
    ctasQueries.add("create table %s partition by (manager_id) as " +
      "select cast(manager_id as decimal(6, 0)) as manager_id, EMPLOYEE_ID, FIRST_NAME, LAST_NAME " +
      "from cp.`parquet/fixedlenDecimal.parquet`");
    // decimal stores as int64
    ctasQueries.add("create table %s partition by (manager_id) as " +
      "select cast(manager_id as decimal(18, 6)) as manager_id, EMPLOYEE_ID, FIRST_NAME, LAST_NAME " +
      "from cp.`parquet/fixedlenDecimal.parquet`");
    final String decimalPartitionTable = "dfs.tmp.`decimal_optional_partition`";
    for (String ctasQuery : ctasQueries) {
      try {
        test("alter session set `%s` = true", PlannerSettings.ENABLE_DECIMAL_DATA_TYPE_KEY);
        test(ctasQuery, decimalPartitionTable);

        String query = String.format("select * from %s where manager_id = 148", decimalPartitionTable);
        int expectedRowCount = 6;

        int actualRowCount = testSql(query);
        assertEquals("Row count does not match the expected value", expectedRowCount, actualRowCount);
        PlanTestBase.testPlanMatchingPatterns(query, new String[]{"usedMetadataFile=false"}, new String[]{"Filter"});

        test("refresh table metadata %s", decimalPartitionTable);

        actualRowCount = testSql(query);
        assertEquals("Row count does not match the expected value", expectedRowCount, actualRowCount);
        PlanTestBase.testPlanMatchingPatterns(query, new String[]{"usedMetadataFile=true"}, new String[]{"Filter"});
      } finally {
        test("drop table if exists %s", decimalPartitionTable);
        test("alter session set `%s` = false", PlannerSettings.ENABLE_DECIMAL_DATA_TYPE_KEY);
      }
    }
  }

  @Test // DRILL-4139
  public void testIntWithNullsPartitionPruning() throws Exception {
    try {
      test("create table dfs.tmp.`t5/a` as\n" +
        "select 100 as mykey from cp.`tpch/nation.parquet`\n" +
        "union all\n" +
        "select col_notexist from cp.`tpch/region.parquet`");

      test("create table dfs.tmp.`t5/b` as\n" +
        "select 200 as mykey from cp.`tpch/nation.parquet`\n" +
        "union all\n" +
        "select col_notexist from cp.`tpch/region.parquet`");

      String query = "select mykey from dfs.tmp.`t5` where mykey = 100";
      int actualRowCount = testSql(query);
      assertEquals("Row count does not match the expected value", 25, actualRowCount);

      test("refresh table metadata dfs.tmp.`t5`");

      actualRowCount = testSql(query);
      assertEquals("Row count does not match the expected value", 25, actualRowCount);
    } finally {
      test("drop table if exists dfs.tmp.`t5`");
    }
  }

  @Test // DRILL-4139
  public void testPartitionPruningWithIsNull() throws Exception {
    try {
      test("create table dfs.tmp.`t6/a` as\n" +
        "select col_notexist as mykey from cp.`tpch/region.parquet`");

      test("create table dfs.tmp.`t6/b` as\n" +
        "select 100 as mykey from cp.`tpch/region.parquet`");

      String query = "select mykey from dfs.tmp.t6 where mykey is null";

      int actualRowCount = testSql(query);
      assertEquals("Row count does not match the expected value", 5, actualRowCount);
      PlanTestBase.testPlanMatchingPatterns(query, new String[]{"usedMetadataFile=false"}, new String[]{"Filter"});

      test("refresh table metadata dfs.tmp.`t6`");

      actualRowCount = testSql(query);
      assertEquals("Row count does not match the expected value", 5, actualRowCount);
      PlanTestBase.testPlanMatchingPatterns(query, new String[]{"usedMetadataFile=true"}, new String[]{"Filter"});
    } finally {
      test("drop table if exists dfs.tmp.`t6`");
    }
  }

  @Test // DRILL-4139
  public void testPartitionPruningWithIsNotNull() throws Exception {
    try {
      test("create table dfs.tmp.`t7/a` as\n" +
        "select col_notexist as mykey from cp.`tpch/region.parquet`");

      test("create table dfs.tmp.`t7/b` as\n" +
        "select 100 as mykey from cp.`tpch/region.parquet`");

      String query = "select mykey from dfs.tmp.t7 where mykey is null";

      int actualRowCount = testSql(query);
      assertEquals("Row count does not match the expected value", 5, actualRowCount);
      PlanTestBase.testPlanMatchingPatterns(query, new String[]{"usedMetadataFile=false"}, new String[]{"Filter"});

      test("refresh table metadata dfs.tmp.`t7`");

      actualRowCount = testSql(query);
      assertEquals("Row count does not match the expected value", 5, actualRowCount);
      PlanTestBase.testPlanMatchingPatterns(query, new String[]{"usedMetadataFile=true"}, new String[]{"Filter"});
    } finally {
      test("drop table if exists dfs.tmp.`t7`");
    }
  }

  @Test
  public void testEmptyDirectoryWithMetadataFile() throws Exception {
    final String emptyDirNameWithMetadataFile = "empty_directory";
    dirTestWatcher.makeTestTmpSubDir(Paths.get(emptyDirNameWithMetadataFile));
    dirTestWatcher.copyResourceToTestTmp(
        Paths.get("parquet", "metadata_files_with_old_versions", "v3_1", "metadata_table.requires_replace.txt"),
        Paths.get(emptyDirNameWithMetadataFile, Metadata.METADATA_FILENAME));

    final BatchSchema expectedSchema = new SchemaBuilder().build();

    testBuilder()
        .sqlQuery("select * from dfs.tmp.`%s`", emptyDirNameWithMetadataFile)
        .schemaBaseLine(expectedSchema)
        .build()
        .run();
  }

  @Test
  public void testEmptyDirectoryWithMetadataDirFile() throws Exception {
    final String emptyDirNameWithMetadataFile = "empty_directory";
    dirTestWatcher.makeTestTmpSubDir(Paths.get(emptyDirNameWithMetadataFile));
    dirTestWatcher.makeTestTmpSubDir(Paths.get(emptyDirNameWithMetadataFile, "t2"));
    dirTestWatcher.makeTestTmpSubDir(Paths.get(emptyDirNameWithMetadataFile, "t1"));
    dirTestWatcher.copyResourceToTestTmp(
            Paths.get("parquet", "metadata_files_with_old_versions", "v3_1", "metadata_directories.requires_replace.txt"),
            Paths.get(emptyDirNameWithMetadataFile, Metadata.METADATA_DIRECTORIES_FILENAME));

    final BatchSchema expectedSchema = new SchemaBuilder().build();

    testBuilder()
            .sqlQuery("select * from dfs.tmp.`%s`", emptyDirNameWithMetadataFile)
            .schemaBaseLine(expectedSchema)
            .build()
            .run();
  }

  /**
   * Helper method for checking the metadata file existence
   *
   * @param table table name or table path
   */
  private void checkForMetadataFile(String table) {
    final String tmpDir;

    try {
      tmpDir = dirTestWatcher.getRootDir().getCanonicalPath();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    File metaFile = table.startsWith(tmpDir) ? FileUtils.getFile(table, Metadata.METADATA_FILENAME)
        : FileUtils.getFile(tmpDir, table, Metadata.METADATA_FILENAME);
    assertTrue(String.format("There is no metadata cache file for the %s table", table),
        Files.exists(metaFile.toPath()));
  }
}
