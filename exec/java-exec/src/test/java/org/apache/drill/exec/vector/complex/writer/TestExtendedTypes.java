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
package org.apache.drill.exec.vector.complex.writer;

import static org.junit.Assert.assertEquals;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.TimeZone;

import org.apache.drill.test.BaseTestQuery;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.junit.BeforeClass;
import org.junit.Test;

// TODO: Move to JSON reader package after code review
public class TestExtendedTypes extends BaseTestQuery {

  @BeforeClass
  public static void setupTestFiles() {
    dirTestWatcher.copyResourceToRoot(Paths.get("vector", "complex"));
  }

  @Test
  public void checkReadWriteExtended() throws Exception {
    runBoth(() -> doCheckReadWriteExtended());
  }

  private void doCheckReadWriteExtended() throws Exception {

    final String originalFile = "vector/complex/extended.json";
    final String newTable = "TestExtendedTypes/newjson";

    TimeZone origZone = TimeZone.getDefault();
    try {
      TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
      alterSession(ExecConstants.OUTPUT_FORMAT_OPTION, "json");
      alterSession(ExecConstants.JSON_EXTENDED_TYPES_KEY, true);

      // create table
      test("create table dfs.tmp.`%s` as select * from cp.`%s`", newTable, originalFile);
      // check query of table.
      test("select * from dfs.tmp.`%s`", newTable);

      // check that original file and new file match.
      final byte[] originalData = Files.readAllBytes(dirTestWatcher.getRootDir().toPath().resolve(originalFile));
      final byte[] newData = Files.readAllBytes(dirTestWatcher.getDfsTestTmpDir().toPath().resolve(Paths.get(newTable, "0_0_0.json")));
      assertEquals(new String(originalData), new String(newData));
    } finally {
      TimeZone.setDefault(origZone);
      resetSessionOption(ExecConstants.OUTPUT_FORMAT_OPTION);
      resetSessionOption(ExecConstants.JSON_EXTENDED_TYPES_KEY);
      test("DROP TABLE IF EXISTS dfs.tmp.`%s`", newTable);
    }
  }

  @Test
  public void testMongoExtendedTypes() throws Exception {
    runBoth(() -> doTestMongoExtendedTypes());
  }

  private void doTestMongoExtendedTypes() throws Exception {
    final String originalFile = "vector/complex/mongo_extended.json";

    TimeZone origZone = TimeZone.getDefault();
    try {
      TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
      alterSession(ExecConstants.OUTPUT_FORMAT_OPTION, "json");
      alterSession(ExecConstants.JSON_EXTENDED_TYPES_KEY, true);

      int actualRecordCount = testSql(String.format("select * from cp.`%s`", originalFile));
      assertEquals(
          String.format(
              "Received unexpected number of rows in output: expected=%d, received=%s",
              1, actualRecordCount), 1, actualRecordCount);
      List<QueryDataBatch> resultList = testSqlWithResults(String.format("select * from dfs.`%s`", originalFile));
      String actual = getResultString(resultList, ",");
      String expected = "drill_timestamp_millies,bin,bin1\n2015-07-07 03:59:43.488,drill,drill\n";
      assertEquals(expected, actual);
    } finally {
      TimeZone.setDefault(origZone);
      resetSessionOption(ExecConstants.OUTPUT_FORMAT_OPTION);
      resetSessionOption(ExecConstants.JSON_EXTENDED_TYPES_KEY);
    }
  }

  private void enableV2Reader(boolean enable) throws Exception {
    alterSession(ExecConstants.ENABLE_V2_JSON_READER_KEY, enable);
  }

  private void resetV2Reader() throws Exception {
    resetSessionOption(ExecConstants.ENABLE_V2_JSON_READER_KEY);
  }

  public interface TestWrapper {
    void apply() throws Exception;
  }

  public void runBoth(TestWrapper wrapper) throws Exception {
    try {
      enableV2Reader(false);
      wrapper.apply();
      enableV2Reader(true);
      wrapper.apply();
    } finally {
      resetV2Reader();
    }
  }
}
