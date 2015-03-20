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

package org.apache.drill.exec;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.PrintWriter;
import java.util.List;

import org.apache.drill.BaseTestQuery;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.apache.drill.exec.vector.BigIntVector;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

public class TestQueriesOnLargeFile extends BaseTestQuery {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestQueriesOnLargeFile.class);

  private static File dataFile = null;
  private static int NUM_RECORDS = 15000;

  @BeforeClass
  public static void generateTestData() throws Exception {
    // Generate a json file with NUM_RECORDS number of records
    while (true) {
      dataFile = File.createTempFile("drill-json", ".json");
      if (dataFile.exists()) {
        boolean success = dataFile.delete();
        if (success) {
          break;
        }
      }
      logger.trace("retry creating tmp file");
    }

    PrintWriter printWriter = new PrintWriter(dataFile);

    for (int i=1; i<=NUM_RECORDS; i++) {
      printWriter.println("{");
      printWriter.println("  \"id\" : " + Math.random() + ",");
      printWriter.println("  \"summary\" : \"Apache Drill provides low latency ad-hoc queries to many different data sources, "+
          "including nested data. Inspired by Google's Dremel, Drill is designed to scale to 10,000 servers and " +
          "query petabytes of data in seconds.\"");
      printWriter.println("}");
    }

    printWriter.close();
  }

  @Test
  public void testRead() throws Exception {
    List<QueryDataBatch> results = testSqlWithResults(
        String.format("SELECT count(*) FROM dfs_test.`default`.`%s`", dataFile.getPath()));

    RecordBatchLoader batchLoader = new RecordBatchLoader(getAllocator());

    for(QueryDataBatch batch : results) {
      batchLoader.load(batch.getHeader().getDef(), batch.getData());

      if (batchLoader.getRecordCount() <= 0) {
        continue;
      }

      BigIntVector countV = (BigIntVector) batchLoader.getValueAccessorById(BigIntVector.class, 0).getValueVector();
      assertTrue("Total of "+ NUM_RECORDS + " records expected in count", countV.getAccessor().get(0) == NUM_RECORDS);

      batchLoader.clear();
      batch.release();
    }
  }

  @Test
  public void testMergingReceiver() throws Exception {
    String plan = Files.toString(FileUtils.getResourceAsFile("/largefiles/merging_receiver_large_data.json"),
        Charsets.UTF_8).replace("#{TEST_FILE}", escapeJsonString(dataFile.getPath()));
    List<QueryDataBatch> results = testPhysicalWithResults(plan);

    int recordsInOutput = 0;
    for(QueryDataBatch batch : results) {
      recordsInOutput += batch.getHeader().getDef().getRecordCount();
      batch.release();
    }

    assertTrue(String.format("Number of records in output is wrong: expected=%d, actual=%s",
        NUM_RECORDS, recordsInOutput), NUM_RECORDS == recordsInOutput);
  }

  @AfterClass
  public static void deleteTestData() throws Exception {
    if (dataFile != null) {
      if (dataFile.exists()) {
        org.apache.commons.io.FileUtils.forceDelete(dataFile);
      }
    }
  }
}
