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

package org.apache.drill.exec.vector.complex.writer;

import org.apache.commons.io.FileUtils;
import org.apache.drill.BaseTestQuery;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.apache.drill.exec.vector.BigIntVector;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.PrintWriter;
import java.util.List;

import static org.junit.Assert.assertTrue;

public class TestJsonReaderLargeFile extends BaseTestQuery {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestJsonReaderLargeFile.class);

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
    String record = "{\n" +
        "\"project\" : \"Drill\", \n" +
        "\"summary\" : \"Apache Drill provides low latency ad-hoc queries to many different data sources, " +
        "including nested data. Inspired by Google's Dremel, Drill is designed to scale to 10,000 servers and " +
        "query petabytes of data in seconds.\"\n" +
        "}";

    for (int i=1; i<=NUM_RECORDS; i++) {
      printWriter.println(record);
    }

    printWriter.close();
  }

  @Test
  public void testRead() throws Exception {
    List<QueryResultBatch> results = testSqlWithResults(
        String.format("SELECT count(*) FROM dfs.`default`.`%s`", dataFile.getPath()));

    RecordBatchLoader batchLoader = new RecordBatchLoader(getAllocator());

    for(QueryResultBatch batch : results) {
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

  @AfterClass
  public static void deleteTestData() throws Exception {
    if (dataFile != null) {
      if (dataFile.exists()) {
        FileUtils.forceDelete(dataFile);
      }
    }
  }
}
