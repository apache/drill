/**
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

import com.google.common.base.Joiner;
import org.apache.drill.PlanTestBase;
import org.apache.drill.common.util.TestTools;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;

import static org.junit.Assert.assertEquals;

public class TestParquetMetadataCache extends PlanTestBase {
  private static final String WORKING_PATH = TestTools.getWorkingPath();
  private static final String TEST_RES_PATH = WORKING_PATH + "/src/test/resources";
  private static final String tableName1 = "parquetTable1";
  private static final String tableName2 = "parquetTable2";


  @BeforeClass
  public static void copyData() throws Exception {
    // copy the data into the temporary location
    String tmpLocation = getDfsTestTmpSchemaLocation();
    File dataDir1 = new File(tmpLocation + Path.SEPARATOR + tableName1);
    dataDir1.mkdir();
    FileUtils.copyDirectory(new File(String.format(String.format("%s/multilevel/parquet", TEST_RES_PATH))),
        dataDir1);

    File dataDir2 = new File(tmpLocation + Path.SEPARATOR + tableName2);
    dataDir2.mkdir();
    FileUtils.copyDirectory(new File(String.format(String.format("%s/multilevel/parquet2", TEST_RES_PATH))),
        dataDir2);
  }

  @Test
  public void testPartitionPruningWithMetadataCache_1() throws Exception {
    test(String.format("refresh table metadata dfs_test.`%s/%s`", getDfsTestTmpSchemaLocation(), tableName1));
    checkForMetadataFile(tableName1);
    String query = String.format("select dir0, dir1, o_custkey, o_orderdate from dfs_test.`%s/%s` " +
            " where dir0=1994 and dir1 in ('Q1', 'Q2')",
        getDfsTestTmpSchemaLocation(), tableName1);
    int expectedRowCount = 20;
    int expectedNumFiles = 2;

    int actualRowCount = testSql(query);
    assertEquals(expectedRowCount, actualRowCount);
    String numFilesPattern = "numFiles=" + expectedNumFiles;
    String usedMetaPattern = "usedMetadataFile=true";
    String cacheFileRootPattern = String.format("cacheFileRoot=%s/%s/1994", getDfsTestTmpSchemaLocation(), tableName1);
    PlanTestBase.testPlanMatchingPatterns(query, new String[]{numFilesPattern, usedMetaPattern, cacheFileRootPattern},
        new String[] {});
  }

  @Test // DRILL-3917, positive test case for DRILL-4530
  public void testPartitionPruningWithMetadataCache_2() throws Exception {
    test(String.format("refresh table metadata dfs_test.`%s/%s`", getDfsTestTmpSchemaLocation(), tableName1));
    checkForMetadataFile(tableName1);
    String query = String.format("select dir0, dir1, o_custkey, o_orderdate from dfs_test.`%s/%s` " +
            " where dir0=1994",
        getDfsTestTmpSchemaLocation(), tableName1);
    int expectedRowCount = 40;
    int expectedNumFiles = 4;

    int actualRowCount = testSql(query);
    assertEquals(expectedRowCount, actualRowCount);
    String numFilesPattern = "numFiles=" + expectedNumFiles;
    String usedMetaPattern = "usedMetadataFile=true";
    String cacheFileRootPattern = String.format("cacheFileRoot=%s/%s/1994", getDfsTestTmpSchemaLocation(), tableName1);
    PlanTestBase.testPlanMatchingPatterns(query, new String[]{numFilesPattern, usedMetaPattern, cacheFileRootPattern},
        new String[] {"Filter"});
  }

  @Test // DRILL-3937 (partitioning column is varchar)
  public void testPartitionPruningWithMetadataCache_3() throws Exception {
    String tableName = "orders_ctas_varchar";
    test("use dfs_test.tmp");

    test(String.format("create table %s (o_orderdate, o_orderpriority) partition by (o_orderpriority) "
        + "as select o_orderdate, o_orderpriority from dfs_test.`%s/multilevel/parquet/1994/Q1`", tableName, TEST_RES_PATH));
    test(String.format("refresh table metadata %s", tableName));
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
  public void testPartitionPruningWithMetadataCache_4() throws Exception {
    String tableName = "orders_ctas_binary";
    test("use dfs_test.tmp");

    test(String.format("create table %s (o_orderdate, o_orderpriority) partition by (o_orderpriority) "
        + "as select o_orderdate, convert_to(o_orderpriority, 'UTF8') as o_orderpriority "
        + "from dfs_test.`%s/multilevel/parquet/1994/Q1`", tableName, TEST_RES_PATH));
    test(String.format("refresh table metadata %s", tableName));
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
    test("use dfs_test.tmp");
    test(String.format("create table `%s/t1` as select * from cp.`tpch/nation.parquet`", tableName));
    test(String.format("create table `%s/t2` as select * from cp.`tpch/nation.parquet`", tableName));
    test(String.format("refresh table metadata %s", tableName));
    checkForMetadataFile(tableName);
    String query = String.format("select * from %s", tableName);
    int rowCount = testSql(query);
    Assert.assertEquals(50, rowCount);
    testPlanMatchingPatterns(query, new String[] { "usedMetadataFile=true" }, new String[]{});
  }

  @Test
  public void testUpdate() throws Exception {
    String tableName = "nation_ctas_update";
    test("use dfs_test.tmp");
    test(String.format("create table `%s/t1` as select * from cp.`tpch/nation.parquet`", tableName));
    test(String.format("refresh table metadata %s", tableName));
    checkForMetadataFile(tableName);
    Thread.sleep(1000);
    test(String.format("create table `%s/t2` as select * from cp.`tpch/nation.parquet`", tableName));
    int rowCount = testSql(String.format("select * from %s", tableName));
    Assert.assertEquals(50, rowCount);
  }

  @Test
  public void testCacheWithSubschema() throws Exception {
    String tableName = "nation_ctas_subschema";
    test(String.format("create table dfs_test.tmp.`%s/t1` as select * from cp.`tpch/nation.parquet`", tableName));
    test(String.format("refresh table metadata dfs_test.tmp.%s", tableName));
    checkForMetadataFile(tableName);
    int rowCount = testSql(String.format("select * from dfs_test.tmp.%s", tableName));
    Assert.assertEquals(25, rowCount);
  }

  @Test
  public void testFix4449() throws Exception {
    runSQL("CREATE TABLE dfs_test.tmp.`4449` PARTITION BY(l_discount) AS SELECT l_orderkey, l_discount FROM cp.`tpch/lineitem.parquet`");
    runSQL("REFRESH TABLE METADATA dfs_test.tmp.`4449`");

    testBuilder()
      .sqlQuery("SELECT COUNT(*) cnt FROM (" +
        "SELECT l_orderkey FROM dfs_test.tmp.`4449` WHERE l_discount < 0.05" +
        " UNION ALL" +
        " SELECT l_orderkey FROM dfs_test.tmp.`4449` WHERE l_discount > 0.02)")
      .unOrdered()
      .baselineColumns("cnt")
      .baselineValues(71159L)
      .go();
  }

  @Test
  public void testAbsentPluginOrWorkspaceError() throws Exception {
    testBuilder()
        .sqlQuery("refresh table metadata dfs_test.incorrect.table_name")
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(false, "Storage plugin or workspace does not exist [dfs_test.incorrect]")
        .go();

    testBuilder()
        .sqlQuery("refresh table metadata incorrect.table_name")
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(false, "Storage plugin or workspace does not exist [incorrect]")
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
  public void testDrill4530_1() throws Exception {
    // create metadata cache
    test(String.format("refresh table metadata dfs_test.`%s/%s`", getDfsTestTmpSchemaLocation(), tableName2));
    checkForMetadataFile(tableName2);

    // run query and check correctness
    String query1 = String.format("select dir0, dir1, o_custkey, o_orderdate from dfs_test.`%s/%s` " +
            " where dir0=1995 and dir1='Q3'",
        getDfsTestTmpSchemaLocation(), tableName2);
    int expectedRowCount = 20;
    int expectedNumFiles = 2;

    int actualRowCount = testSql(query1);
    assertEquals(expectedRowCount, actualRowCount);
    String numFilesPattern = "numFiles=" + expectedNumFiles;
    String usedMetaPattern = "usedMetadataFile=true";
    String cacheFileRootPattern = String.format("cacheFileRoot=%s/%s/1995/Q3", getDfsTestTmpSchemaLocation(), tableName2);
    PlanTestBase.testPlanMatchingPatterns(query1, new String[]{numFilesPattern, usedMetaPattern, cacheFileRootPattern},
        new String[] {"Filter"});
  }

  @Test // DRILL-4530  // single non-leaf level partition
  public void testDrill4530_2() throws Exception {
    // create metadata cache
    test(String.format("refresh table metadata dfs_test.`%s/%s`", getDfsTestTmpSchemaLocation(), tableName2));
    checkForMetadataFile(tableName2);

    // run query and check correctness
    String query1 = String.format("select dir0, dir1, o_custkey, o_orderdate from dfs_test.`%s/%s` " +
            " where dir0=1995",
        getDfsTestTmpSchemaLocation(), tableName2);
    int expectedRowCount = 80;
    int expectedNumFiles = 8;

    int actualRowCount = testSql(query1);
    assertEquals(expectedRowCount, actualRowCount);
    String numFilesPattern = "numFiles=" + expectedNumFiles;
    String usedMetaPattern = "usedMetadataFile=true";
    String cacheFileRootPattern = String.format("cacheFileRoot=%s/%s/1995", getDfsTestTmpSchemaLocation(), tableName2);
    PlanTestBase.testPlanMatchingPatterns(query1, new String[]{numFilesPattern, usedMetaPattern, cacheFileRootPattern},
        new String[] {"Filter"});
  }

  @Test // DRILL-4530  // only dir1 filter is present, no dir0, hence this maps to multiple partitions
  public void testDrill4530_3() throws Exception {
    // create metadata cache
    test(String.format("refresh table metadata dfs_test.`%s/%s`", getDfsTestTmpSchemaLocation(), tableName2));
    checkForMetadataFile(tableName2);

    // run query and check correctness
    String query1 = String.format("select dir0, dir1, o_custkey, o_orderdate from dfs_test.`%s/%s` " +
            " where dir1='Q3'",
        getDfsTestTmpSchemaLocation(), tableName2);
    int expectedRowCount = 40;
    int expectedNumFiles = 4;

    int actualRowCount = testSql(query1);
    assertEquals(expectedRowCount, actualRowCount);
    String numFilesPattern = "numFiles=" + expectedNumFiles;
    String usedMetaPattern = "usedMetadataFile=true";
    String cacheFileRootPattern = String.format("cacheFileRoot=%s/%s", getDfsTestTmpSchemaLocation(), tableName2);
    PlanTestBase.testPlanMatchingPatterns(query1, new String[]{numFilesPattern, usedMetaPattern, cacheFileRootPattern},
        new String[] {});
  }

  @Test // DRILL-4530  // non-existent partition (1 subdirectory's cache file will still be read for schema)
  public void testDrill4530_4() throws Exception {
    // create metadata cache
    test(String.format("refresh table metadata dfs_test.`%s/%s`", getDfsTestTmpSchemaLocation(), tableName2));
    checkForMetadataFile(tableName2);

    // run query and check correctness
    String query1 = String.format("select dir0, dir1, o_custkey, o_orderdate from dfs_test.`%s/%s` " +
            " where dir0=1995 and dir1='Q6'",
        getDfsTestTmpSchemaLocation(), tableName2);
    int expectedRowCount = 0;
    int expectedNumFiles = 1;

    int actualRowCount = testSql(query1);
    assertEquals(expectedRowCount, actualRowCount);
    String numFilesPattern = "numFiles=" + expectedNumFiles;
    String usedMetaPattern = "usedMetadataFile=true";
    String cacheFileRootPattern = String.format("cacheFileRoot=%s/%s/*/*", getDfsTestTmpSchemaLocation(), tableName2);
    PlanTestBase.testPlanMatchingPatterns(query1, new String[]{numFilesPattern, usedMetaPattern, cacheFileRootPattern},
        new String[] {});
  }

  @Test // DRILL-4794
  public void testDrill4794() throws Exception {
    test(String.format("refresh table metadata dfs_test.`%s/%s`", getDfsTestTmpSchemaLocation(), tableName1));
    checkForMetadataFile(tableName1);
    String query = String.format("select dir0, dir1, o_custkey, o_orderdate from dfs_test.`%s/%s` " +
            " where dir0=1994 or dir1='Q3'",
        getDfsTestTmpSchemaLocation(), tableName1);

    int expectedRowCount = 60;
    int expectedNumFiles = 6;

    int actualRowCount = testSql(query);
    assertEquals(expectedRowCount, actualRowCount);
    String numFilesPattern = "numFiles=" + expectedNumFiles;
    String usedMetaPattern = "usedMetadataFile=true";
    String cacheFileRootPattern = String.format("cacheFileRoot=%s/%s", getDfsTestTmpSchemaLocation(), tableName1);
    PlanTestBase.testPlanMatchingPatterns(query, new String[]{numFilesPattern, usedMetaPattern, cacheFileRootPattern},
        new String[] {});
  }

  @Test // DRILL-4786
  public void testDrill4786_1() throws Exception {
    // create metadata cache
    test(String.format("refresh table metadata dfs_test.`%s/%s`", getDfsTestTmpSchemaLocation(), tableName2));
    checkForMetadataFile(tableName2);

    // run query and check correctness
    String query1 = String.format("select dir0, dir1, o_custkey, o_orderdate from dfs_test.`%s/%s` " +
            " where dir0=1995 and dir1 in ('Q1', 'Q2')",
        getDfsTestTmpSchemaLocation(), tableName2);

    int expectedRowCount = 40;
    int expectedNumFiles = 4;

    int actualRowCount = testSql(query1);
    assertEquals(expectedRowCount, actualRowCount);
    String numFilesPattern = "numFiles=" + expectedNumFiles;
    String usedMetaPattern = "usedMetadataFile=true";
    String cacheFileRootPattern = String.format("cacheFileRoot=%s/%s/1995", getDfsTestTmpSchemaLocation(), tableName2);
    PlanTestBase.testPlanMatchingPatterns(query1, new String[]{numFilesPattern, usedMetaPattern, cacheFileRootPattern},
        new String[] {});

  }

  @Test // DRILL-4786
  public void testDrill4786_2() throws Exception {
    // create metadata cache
    test(String.format("refresh table metadata dfs_test.`%s/%s`", getDfsTestTmpSchemaLocation(), tableName2));
    checkForMetadataFile(tableName2);

    // run query and check correctness
    String query1 = String.format("select dir0, dir1, o_custkey, o_orderdate from dfs_test.`%s/%s` " +
            " where dir0 in (1994, 1995) and dir1 = 'Q3'",
        getDfsTestTmpSchemaLocation(), tableName2);

    int expectedRowCount = 40;
    int expectedNumFiles = 4;

    int actualRowCount = testSql(query1);
    assertEquals(expectedRowCount, actualRowCount);
    String numFilesPattern = "numFiles=" + expectedNumFiles;
    String usedMetaPattern = "usedMetadataFile=true";
    String cacheFileRootPattern = String.format("cacheFileRoot=%s/%s", getDfsTestTmpSchemaLocation(), tableName2);
    PlanTestBase.testPlanMatchingPatterns(query1, new String[]{numFilesPattern, usedMetaPattern, cacheFileRootPattern},
        new String[] {});

  }

  @Test // DRILL-4877
  public void testDrill4877() throws Exception {
    // create metadata cache
    test(String.format("refresh table metadata dfs_test.`%s/%s`", getDfsTestTmpSchemaLocation(), tableName2));
    checkForMetadataFile(tableName2);

    // run query and check correctness
    String query1 = String.format("select max(dir0) as max0, max(dir1) as max1 from dfs_test.`%s/%s` ",
        getDfsTestTmpSchemaLocation(), tableName2);

    testBuilder()
    .sqlQuery(query1)
      .unOrdered()
      .baselineColumns("max0", "max1")
      .baselineValues("1995", "Q4")
      .go();

    int expectedNumFiles = 1; // point to selectionRoot since no pruning is done in this query

    String numFilesPattern = "numFiles=" + expectedNumFiles;
    String usedMetaPattern = "usedMetadataFile=true";
    String cacheFileRootPattern = String.format("cacheFileRoot=%s/%s", getDfsTestTmpSchemaLocation(), tableName2);
    PlanTestBase.testPlanMatchingPatterns(query1, new String[]{numFilesPattern, usedMetaPattern, cacheFileRootPattern},
        new String[] {});

  }

  private void checkForMetadataFile(String table) throws Exception {
    String tmpDir = getDfsTestTmpSchemaLocation();
    String metaFile = Joiner.on("/").join(tmpDir, table, Metadata.METADATA_FILENAME);
    Assert.assertTrue(Files.exists(new File(metaFile).toPath()));
  }

}
