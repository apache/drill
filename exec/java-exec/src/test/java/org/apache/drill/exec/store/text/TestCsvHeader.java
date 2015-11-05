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
package org.apache.drill.exec.store.text;

import org.apache.drill.BaseTestQuery;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.rpc.user.QueryDataBatch;

import java.util.List;

import org.junit.Before;
import org.junit.Test;


import static org.junit.Assert.assertEquals;

public class TestCsvHeader extends BaseTestQuery{

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestCsvHeader.class);
  final String OUTPUT_DELIMITER = "|";
  String root;

  @Before
  public void initialize() throws Exception {
    root = FileUtils.getResourceAsFile("/store/text/data/cars.csvh").toURI().toString();
    test("alter session set `exec.errors.verbose` = true ");
  }

  @Test //DRILL-951
  public void testCsvWithHeader() throws Exception {
    //Pre DRILL-951: Qry: select * from dfs_test.`%s` LIMIT 2
    String query = String.format("select * from dfs_test.`%s` LIMIT 2", root);
    List<QueryDataBatch> batches = testSqlWithResults(query);

    String expectedOutput = "Year|Make|Model|Description|Price\n" +
        "1997|Ford|E350|ac, abs, moon|3000.00\n" +
        "1999|Chevy|Venture \"Extended Edition\"||4900.00\n";

    validateResults (batches, expectedOutput);
  }

  @Test //DRILL-951
  public void testCsvWhereWithHeader() throws Exception {
    //Pre DRILL-951: Qry: select * from dfs_test.`%s` where columns[1] = 'Chevy'
    String query = String.format("select * from dfs_test.`%s` where Make = 'Chevy'", root);
    List<QueryDataBatch> batches = testSqlWithResults(query);

    String expectedOutput = "Year|Make|Model|Description|Price\n" +
        "1999|Chevy|Venture \"Extended Edition\"||4900.00\n" +
        "1999|Chevy|Venture \"Extended Edition, Very Large\"||5000.00\n";

    validateResults (batches, expectedOutput);
  }

  @Test //DRILL-951
  public void testCsvStarPlusWithHeader() throws Exception {
    //Pre DRILL-951: Qry: select *,columns[1] from dfs_test.`%s` where columns[1] = 'Chevy'
    String query = String.format("select *, Make from dfs_test.`%s` where Make = 'Chevy'", root);
    List<QueryDataBatch> batches = testSqlWithResults(query);

    String expectedOutput = "Year|Make|Model|Description|Price|Make0\n" +
        "1999|Chevy|Venture \"Extended Edition\"||4900.00|Chevy\n" +
        "1999|Chevy|Venture \"Extended Edition, Very Large\"||5000.00|Chevy\n";

    validateResults (batches, expectedOutput);
  }

  @Test //DRILL-951
  public void testCsvWhereColumnsWithHeader() throws Exception {
    //Pre DRILL-951: Qry: select columns[1] from dfs_test.`%s` where columns[1] = 'Chevy'
    String query = String.format("select Make from dfs_test.`%s` where Make = 'Chevy'", root);
    List<QueryDataBatch> batches = testSqlWithResults(query);

    String expectedOutput = "Make\n" +
        "Chevy\n" +
        "Chevy\n";

    validateResults (batches, expectedOutput);
  }

  @Test //DRILL-951
  public void testCsvColumnsWithHeader() throws Exception {
    //Pre DRILL-951: Qry: select columns[0],columns[2],columns[4] from dfs_test.`%s` where columns[1] = 'Chevy'
    String query = String.format("select `Year`, Model, Price from dfs_test.`%s` where Make = 'Chevy'", root);
    List<QueryDataBatch> batches = testSqlWithResults(query);

    String expectedOutput = "Year|Model|Price\n" +
        "1999|Venture \"Extended Edition\"|4900.00\n" +
        "1999|Venture \"Extended Edition, Very Large\"|5000.00\n";

    validateResults (batches, expectedOutput);
  }

  @Test //DRILL-951
  public void testCsvHeaderShortCircuitReads() throws Exception {
    String query = String.format("select `Year`, Model from dfs_test.`%s` where Make = 'Chevy'", root);
    List<QueryDataBatch> batches = testSqlWithResults(query);

    String expectedOutput = "Year|Model\n" +
        "1999|Venture \"Extended Edition\"\n" +
        "1999|Venture \"Extended Edition, Very Large\"\n";

    validateResults (batches, expectedOutput);
  }

  private void validateResults (List<QueryDataBatch> batches, String expectedOutput) throws SchemaChangeException {
    String actualOutput = getResultString(batches, OUTPUT_DELIMITER);
    //for your and machine's eyes
    System.out.println(actualOutput);
    assertEquals(String.format("Result mismatch.\nExpected:\n%s \nReceived:\n%s",
        expectedOutput, actualOutput), expectedOutput, actualOutput);
  }
}
