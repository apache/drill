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
import org.apache.drill.BaseTestQuery;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;

public class TestParquetMetadataCache extends BaseTestQuery {

  @Test
  public void testCache() throws Exception {
    String tableName = "nation_ctas";
    test("use dfs_test.tmp");
    test(String.format("create table `%s/t1` as select * from cp.`tpch/nation.parquet`", tableName));
    test(String.format("refresh table metadata %s", tableName));
    checkForMetadataFile(tableName);
    int rowCount = testSql(String.format("select * from %s", tableName));
    Assert.assertEquals(25, rowCount);
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
