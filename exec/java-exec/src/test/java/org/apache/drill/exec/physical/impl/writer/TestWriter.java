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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.drill.BaseTestQuery;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.apache.drill.exec.vector.BigIntVector;
import org.apache.drill.exec.vector.VarCharVector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

public class TestWriter extends BaseTestQuery {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestWriter.class);

  static FileSystem fs;
  static String ALTER_SESSION = String.format("ALTER SESSION SET `%s` = 'csv'", ExecConstants.OUTPUT_FORMAT_OPTION);

  @BeforeClass
  public static void initFs() throws Exception {
    Configuration conf = new Configuration();
    conf.set(FileSystem.FS_DEFAULT_NAME_KEY, "local");

    fs = FileSystem.get(conf);
  }

  @Test
  public void simpleCsv() throws Exception {
    // before executing the test deleting the existing CSV files in /tmp/csvtest
    Path path = new Path("/tmp/csvtest");
    if (fs.exists(path)) {
      fs.delete(path, true);
    }

    String plan = Files.toString(FileUtils.getResourceAsFile("/writer/simple_csv_writer.json"), Charsets.UTF_8);

    List<QueryDataBatch> results = testPhysicalWithResults(plan);

    RecordBatchLoader batchLoader = new RecordBatchLoader(getAllocator());

    QueryDataBatch batch = results.get(0);
    assertTrue(batchLoader.load(batch.getHeader().getDef(), batch.getData()));

    VarCharVector fragmentIdV = (VarCharVector) batchLoader.getValueAccessorById(VarCharVector.class, 0).getValueVector();
    BigIntVector recordWrittenV = (BigIntVector) batchLoader.getValueAccessorById(BigIntVector.class, 1).getValueVector();

    // expected only one row in output
    assertEquals(1, batchLoader.getRecordCount());

    assertEquals("0_0", fragmentIdV.getAccessor().getObject(0).toString());
    assertEquals(132000, recordWrittenV.getAccessor().get(0));

    // now verify csv files are written to disk
    assertTrue(fs.exists(path));

    // expect two files
    FileStatus[] fileStatuses = fs.globStatus(new Path(path.toString(), "*.csv"));
    assertTrue(2 == fileStatuses.length);

    for (QueryDataBatch b : results) {
      b.release();
    }
    batchLoader.clear();
  }

  @Test
  public void simpleCTAS() throws Exception {
    final String tableName = "simplectas";
    runSQL("Use dfs_test.tmp");
    runSQL(ALTER_SESSION);

    final String testQuery = String.format("CREATE TABLE %s AS SELECT * FROM cp.`employee.json`", tableName);

    testCTASQueryHelper(tableName, testQuery, 1155);
  }

  @Test
  public void complex1CTAS() throws Exception {
    final String tableName = "complex1ctas";
    runSQL("Use dfs_test.tmp");
    runSQL(ALTER_SESSION);
    final String testQuery = String.format("CREATE TABLE %s AS SELECT first_name, last_name, " +
        "position_id FROM cp.`employee.json`", tableName);

    testCTASQueryHelper(tableName, testQuery, 1155);
  }

  @Test
  public void complex2CTAS() throws Exception {
    final String tableName = "complex1ctas";
    runSQL("Use dfs_test.tmp");
    runSQL(ALTER_SESSION);
    final String testQuery = String.format("CREATE TABLE %s AS SELECT CAST(`birth_date` as Timestamp) FROM " +
        "cp.`employee.json` GROUP BY birth_date", tableName);

    testCTASQueryHelper(tableName, testQuery, 52);
  }

  @Test
  public void simpleCTASWithSchemaInTableName() throws Exception {
    final String tableName = "/test/simplectas2";
    runSQL(ALTER_SESSION);
    final String testQuery =
        String.format("CREATE TABLE dfs_test.tmp.`%s` AS SELECT * FROM cp.`employee.json`",tableName);

    testCTASQueryHelper(tableName, testQuery, 1155);
  }

  @Test
  public void simpleParquetDecimal() throws Exception {
    try {
      final String tableName = "simpleparquetdecimal";
      final String testQuery = String.format("CREATE TABLE dfs_test.tmp.`%s` AS SELECT cast(salary as " +
          "decimal(30,2)) * -1 as salary FROM cp.`employee.json`", tableName);

      // enable decimal
      test(String.format("alter session set `%s` = true", PlannerSettings.ENABLE_DECIMAL_DATA_TYPE_KEY));
      testCTASQueryHelper(tableName, testQuery, 1155);

      // disable decimal
    } finally {
      test(String.format("alter session set `%s` = false", PlannerSettings.ENABLE_DECIMAL_DATA_TYPE_KEY));
    }
  }

  private void testCTASQueryHelper(String tableName, String testQuery, int expectedOutputCount) throws Exception {
    try {
      List<QueryDataBatch> results = testSqlWithResults(testQuery);

      RecordBatchLoader batchLoader = new RecordBatchLoader(getAllocator());

      int recordsWritten = 0;
      for (QueryDataBatch batch : results) {
        batchLoader.load(batch.getHeader().getDef(), batch.getData());

        if (batchLoader.getRecordCount() <= 0) {
          continue;
        }

        BigIntVector recordWrittenV = (BigIntVector) batchLoader.getValueAccessorById(BigIntVector.class, 1).getValueVector();

        for (int i = 0; i < batchLoader.getRecordCount(); i++) {
          recordsWritten += recordWrittenV.getAccessor().get(i);
        }

        batchLoader.clear();
        batch.release();
      }

      assertEquals(expectedOutputCount, recordsWritten);
    } finally {
      try {
        Path path = new Path(getDfsTestTmpSchemaLocation(), tableName);
        if (fs.exists(path)) {
          fs.delete(path, true);
        }
      } catch (Exception e) {
        // ignore exceptions.
        logger.warn("Failed to delete the table [{}, {}] created as part of the test",
            getDfsTestTmpSchemaLocation(), tableName);
      }
    }
  }
}
