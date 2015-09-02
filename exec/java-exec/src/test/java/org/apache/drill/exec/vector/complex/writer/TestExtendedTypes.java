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

import static org.junit.Assert.assertEquals;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.drill.DrillIntegrationTestBase;
import org.apache.drill.common.util.TestTools;
import org.apache.drill.exec.ExecConstants;
import org.junit.Test;

public class TestExtendedTypes extends DrillIntegrationTestBase {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestExtendedTypes.class);

  @Test
  public void checkReadWriteExtended() throws Exception {

    final String originalFile = "${WORKING_PATH}/src/test/resources/vector/complex/extended.json".replaceAll(
        Pattern.quote("${WORKING_PATH}"),
        Matcher.quoteReplacement(TestTools.getWorkingPath()));

    final String newTable = "TestExtendedTypes/newjson";
    try {
      testNoResult(String.format("ALTER SESSION SET `%s` = 'json'", ExecConstants.OUTPUT_FORMAT_VALIDATOR.getOptionName()));
      testNoResult(String.format("ALTER SESSION SET `%s` = true", ExecConstants.JSON_EXTENDED_TYPES.getOptionName()));

      // create table
      test("create table dfs_test.tmp.`%s` as select * from dfs.`%s`", newTable, originalFile);

      // check query of table.
      test("select * from dfs_test.tmp.`%s`", newTable);

      // check that original file and new file match.
      final byte[] originalData = Files.readAllBytes(Paths.get(originalFile));
      final byte[] newData = Files.readAllBytes(Paths.get(DrillIntegrationTestBase.getDfsTestTmpSchemaLocation() + '/' + newTable
          + "/0_0_0.json"));
      assertEquals(new String(originalData), new String(newData));
    } finally {
      testNoResult(String.format("ALTER SESSION SET `%s` = '%s'",
          ExecConstants.OUTPUT_FORMAT_VALIDATOR.getOptionName(),
          ExecConstants.OUTPUT_FORMAT_VALIDATOR.getDefault().getValue()));
      testNoResult(String.format("ALTER SESSION SET `%s` = %s",
          ExecConstants.JSON_EXTENDED_TYPES.getOptionName(),
          ExecConstants.JSON_EXTENDED_TYPES.getDefault().getValue()));
    }
  }
}
