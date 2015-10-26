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
  private static final String tableName = "parquetTable";


  @BeforeClass
  public static void copyData() throws Exception {
    // copy the data into the temporary location
    String tmpLocation = getDfsTestTmpSchemaLocation();
    File dataDir = new File(tmpLocation + Path.SEPARATOR + tableName);
    dataDir.mkdir();
    FileUtils.copyDirectory(new File(String.format(String.format("%s/multilevel/parquet", TEST_RES_PATH))),
        dataDir);
  }

  @Test
  public void testPartitionPruningWithMetadataCache_1() throws Exception {
    test(String.format("refresh table metadata dfs_test.`%s/%s`", getDfsTestTmpSchemaLocation(), tableName));
    checkForMetadataFile(tableName);
    String query = String.format("select dir0, dir1, o_custkey, o_orderdate from dfs_test.`%s/%s` " +
            " where dir0=1994 and dir1='Q1'",
        getDfsTestTmpSchemaLocation(), tableName);
    int expectedRowCount = 10;
    int expectedNumFiles = 1;

    int actualRowCount = testSql(query);
    assertEquals(expectedRowCount, actualRowCount);
    String numFilesPattern = "numFiles=" + expectedNumFiles;
    String usedMetaPattern = "usedMetadataFile=true";
    PlanTestBase.testPlanMatchingPatterns(query, new String[]{numFilesPattern, usedMetaPattern}, new String[] {"Filter"});
  }

  @Test // DRILL-3917
  public void testPartitionPruningWithMetadataCache_2() throws Exception {
    test(String.format("refresh table metadata dfs_test.`%s/%s`", getDfsTestTmpSchemaLocation(), tableName));
    checkForMetadataFile(tableName);
    String query = String.format("select dir0, dir1, o_custkey, o_orderdate from dfs_test.`%s/%s` " +
            " where dir0=1994",
        getDfsTestTmpSchemaLocation(), tableName);
    int expectedRowCount = 40;
    int expectedNumFiles = 4;

    int actualRowCount = testSql(query);
    assertEquals(expectedRowCount, actualRowCount);
    String numFilesPattern = "numFiles=" + expectedNumFiles;
    String usedMetaPattern = "usedMetadataFile=true";
    PlanTestBase.testPlanMatchingPatterns(query, new String[]{numFilesPattern, usedMetaPattern}, new String[] {"Filter"});
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

    testPlanMatchingPatterns(query, new String[]{numFilesPattern, usedMetaPattern}, new String[] {});
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

  private void checkForMetadataFile(String table) throws Exception {
    String tmpDir = getDfsTestTmpSchemaLocation();
    String metaFile = Joiner.on("/").join(tmpDir, table, Metadata.METADATA_FILENAME);
    Assert.assertTrue(Files.exists(new File(metaFile).toPath()));
  }
}
