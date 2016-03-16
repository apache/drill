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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.drill.exec.hive.HiveTestBase;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestHiveProjectPushDown extends HiveTestBase {

  // enable decimal data type
  @BeforeClass
  public static void enableDecimalDataType() throws Exception {
    test(String.format("alter session set `%s` = true", PlannerSettings.ENABLE_DECIMAL_DATA_TYPE_KEY));
  }

  @AfterClass
  public static void disableDecimalDataType() throws Exception {
    test(String.format("alter session set `%s` = false", PlannerSettings.ENABLE_DECIMAL_DATA_TYPE_KEY));
  }

  private void testHelper(String query, int expectedRecordCount, String... expectedSubstrs)throws Exception {
    testPhysicalPlan(query, expectedSubstrs);

    int actualRecordCount = testSql(query);
    assertEquals(String.format("Received unexpected number of rows in output: expected=%d, received=%s",
        expectedRecordCount, actualRecordCount), expectedRecordCount, actualRecordCount);
  }

  @Test
  public void testSingleColumnProject() throws Exception {
    String query = "SELECT `value` as v FROM hive.`default`.kv";
    String expectedColNames = " \"columns\" : [ \"`value`\" ]";

    testHelper(query, 5, expectedColNames);
  }

  @Test
  public void testMultipleColumnsProject() throws Exception {
    String query = "SELECT boolean_field as b_f, tinyint_field as ti_f FROM hive.`default`.readtest";
    String expectedColNames = " \"columns\" : [ \"`boolean_field`\", \"`tinyint_field`\" ]";

    testHelper(query, 2, expectedColNames);
  }

  @Test
  public void testPartitionColumnProject() throws Exception {
    String query = "SELECT double_part as dbl_p FROM hive.`default`.readtest";
    String expectedColNames = " \"columns\" : [ \"`double_part`\" ]";

    testHelper(query, 2, expectedColNames);
  }

  @Test
  public void testMultiplePartitionColumnsProject() throws Exception {
    String query = "SELECT double_part as dbl_p, decimal0_part as dec_p FROM hive.`default`.readtest";
    String expectedColNames = " \"columns\" : [ \"`double_part`\", \"`decimal0_part`\" ]";

    testHelper(query, 2, expectedColNames);
  }

  @Test
  public void testPartitionAndRegularColumnProjectColumn() throws Exception {
    String query = "SELECT boolean_field as b_f, tinyint_field as ti_f, " +
        "double_part as dbl_p, decimal0_part as dec_p FROM hive.`default`.readtest";
    String expectedColNames = " \"columns\" : [ \"`boolean_field`\", \"`tinyint_field`\", " +
        "\"`double_part`\", \"`decimal0_part`\" ]";

    testHelper(query, 2, expectedColNames);
  }

  @Test
  public void testStarProject() throws Exception {
    String query = "SELECT * FROM hive.`default`.kv";
    String expectedColNames = " \"columns\" : [ \"`key`\", \"`value`\" ]";

    testHelper(query, 5, expectedColNames);
  }

  @Test
  public void testHiveCountStar() throws Exception {
    String query = "SELECT count(*) as cnt FROM hive.`default`.kv";
    String expectedColNames = "\"columns\" : [ ]";

    testHelper(query, 1, expectedColNames);
  }

  @Test
  public void projectPushDownOnHiveParquetTable() throws Exception {
    try {
      test(String.format("alter session set `%s` = true", ExecConstants.HIVE_OPTIMIZE_SCAN_WITH_NATIVE_READERS));
      String query = "SELECT boolean_field, boolean_part, int_field, int_part FROM hive.readtest_parquet";
      String expectedColNames = "\"columns\" : [ \"`boolean_field`\", \"`dir0`\", \"`int_field`\", \"`dir9`\" ]";

      testHelper(query, 2, expectedColNames, "hive-drill-native-parquet-scan");
    } finally {
      test(String.format("alter session set `%s` = false", ExecConstants.HIVE_OPTIMIZE_SCAN_WITH_NATIVE_READERS));
    }
  }
}
