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
package org.apache.drill.exec.physical.impl.writer;

import org.apache.drill.BaseTestQuery;
import org.apache.drill.exec.ExecConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestParquetWriterEmptyFiles extends BaseTestQuery {

  private static FileSystem fs;

  @BeforeClass
  public static void initFs() throws Exception {
    Configuration conf = new Configuration();
    conf.set(FileSystem.FS_DEFAULT_NAME_KEY, "local");

    fs = FileSystem.get(conf);

    updateTestCluster(3, null);
  }

  @Test // see DRILL-2408
  public void testWriteEmptyFile() throws Exception {
    final String outputFile = "testparquetwriteremptyfiles_testwriteemptyfile";

    try {
      test("CREATE TABLE dfs_test.tmp.%s AS SELECT * FROM cp.`employee.json` WHERE 1=0", outputFile);

      final Path path = new Path(getDfsTestTmpSchemaLocation(), outputFile);
      Assert.assertFalse(fs.exists(path));
    } finally {
      deleteTableIfExists(outputFile);
    }
  }

  @Test
  public void testMultipleWriters() throws Exception {
    final String outputFile = "testparquetwriteremptyfiles_testmultiplewriters";

    runSQL("alter session set `planner.slice_target` = 1");

    try {
      final String query = "SELECT position_id FROM cp.`employee.json` WHERE position_id IN (15, 16) GROUP BY position_id";
      test("CREATE TABLE dfs_test.tmp.%s AS %s", outputFile, query);

      // this query will fail if an "empty" file was created
      testBuilder()
        .unOrdered()
        .sqlQuery("SELECT * FROM dfs_test.tmp.%s", outputFile)
        .sqlBaselineQuery(query)
        .go();
    } finally {
      runSQL("alter session set `planner.slice_target` = " + ExecConstants.SLICE_TARGET_DEFAULT);
      deleteTableIfExists(outputFile);
    }
  }

  @Test // see DRILL-2408
  public void testWriteEmptyFileAfterFlush() throws Exception {
    final String outputFile = "testparquetwriteremptyfiles_test_write_empty_file_after_flush";
    deleteTableIfExists(outputFile);

    try {
      // this specific value will force a flush just after the final row is written
      // this may cause the creation of a new "empty" parquet file
      test("ALTER SESSION SET `store.parquet.block-size` = 19926");

      final String query = "SELECT * FROM cp.`employee.json` LIMIT 100";
      test("CREATE TABLE dfs_test.tmp.%s AS %s", outputFile, query);

      // this query will fail if an "empty" file was created
      testBuilder()
        .unOrdered()
        .sqlQuery("SELECT * FROM dfs_test.tmp.%s", outputFile)
        .sqlBaselineQuery(query)
        .go();
    } finally {
      // restore the session option
      test("ALTER SESSION SET `store.parquet.block-size` = %d", ExecConstants.PARQUET_BLOCK_SIZE_VALIDATOR.getDefault().num_val);
      deleteTableIfExists(outputFile);
    }
  }

  private static boolean deleteTableIfExists(String tableName) {
    try {
      Path path = new Path(getDfsTestTmpSchemaLocation(), tableName);
      if (fs.exists(path)) {
        return fs.delete(path, true);
      }
    } catch (Exception e) {
      // ignore exceptions.
      return false;
    }

    return true;
  }
}
