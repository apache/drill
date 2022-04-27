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
package org.apache.drill.exec.store.json;

import static org.apache.drill.exec.ExecConstants.ENABLE_V2_JSON_READER_KEY;
import static org.apache.drill.exec.ExecConstants.JSON_READER_PRINT_INVALID_RECORDS_LINE_NOS_FLAG;
import static org.apache.drill.exec.ExecConstants.JSON_READER_SKIP_INVALID_RECORDS_FLAG;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.file.Paths;

import org.apache.drill.categories.RowSetTest;
import org.apache.drill.categories.UnlikelyTest;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.test.BaseTestQuery;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Original JSON reader tests. Left in original form; not converted
 * to the newer formats.
 */
@Category(RowSetTest.class)
public class TestJsonRecordReader extends BaseTestQuery {

  @BeforeClass
  public static void setupTestFiles() {
    dirTestWatcher.copyResourceToRoot(Paths.get("jsoninput/drill_3353"));
  }

  private void enableV2Reader(boolean enable) {
    alterSession(ENABLE_V2_JSON_READER_KEY, enable);
  }

  private void resetV2Reader() {
    resetSessionOption(ENABLE_V2_JSON_READER_KEY);
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

  @Test
  public void testComplexJsonInput() throws Exception {
    runBoth(this::doTestComplexJsonInput);
  }

  private void doTestComplexJsonInput() throws Exception {
    test("select `integer`, x['y'] as x1, x['y'] as x2, z[0], z[0]['orange'], z[1]['pink']  from cp.`jsoninput/input2.json` limit 10 ");
  }

  @Test
  public void testDateJsonInput() throws Exception {
    try{
      alterSession(ExecConstants.JSON_EXTENDED_TYPES_KEY, true);
      test("select `date`, AGE(`date`, CAST('2019-09-30 20:47:43' as timestamp)) from cp.`jsoninput/input2.json` limit 10 ");
    } finally {
      resetSessionOption(ExecConstants.JSON_EXTENDED_TYPES_KEY);
    }
  }

  @Test
  public void testContainingArray() throws Exception {
    runBoth(this::doTestContainingArray);
  }

  private void doTestContainingArray() throws Exception {
    test("select * from cp.`store/json/listdoc.json`");
  }

  @Test
  public void testComplexMultipleTimes() throws Exception {
    runBoth(this::doTestComplexMultipleTimes);
  }

  private void doTestComplexMultipleTimes() throws Exception {
    for (int i = 0; i < 5; i++) {
      test("select * from cp.`join/merge_join.json`");
    }
  }

  @Test
  public void trySimpleQueryWithLimit() throws Exception {
    runBoth(this::doTrySimpleQueryWithLimit);
  }

  private void doTrySimpleQueryWithLimit() throws Exception {
    test("select * from cp.`limit/test1.json` limit 10");
  }

  @Test
  // DRILL-1634 : retrieve an element in a nested array in a repeated map.
  // RepeatedMap (Repeated List (Repeated varchar))
  public void testNestedArrayInRepeatedMap() throws Exception {
    runBoth(this::doTestNestedArrayInRepeatedMap);
  }

  private void doTestNestedArrayInRepeatedMap() throws Exception {
    test("select a[0].b[0] from cp.`jsoninput/nestedArray.json`");
    test("select a[0].b[1] from cp.`jsoninput/nestedArray.json`");
    test("select a[1].b[1] from cp.`jsoninput/nestedArray.json`"); // index out of the range. Should return empty list.
  }

  @Test
  public void testEmptyMapDoesNotFailValueCapacityCheck() throws Exception {
    runBoth(this::doTestEmptyMapDoesNotFailValueCapacityCheck);
  }

  private void doTestEmptyMapDoesNotFailValueCapacityCheck() throws Exception {
    final String sql = "select * from cp.`store/json/value-capacity.json`";
    test(sql);
  }

  @Test
  public void testEnableAllTextMode() throws Exception {
    runBoth(this::doTestEnableAllTextMode);
  }

  private void doTestEnableAllTextMode() throws Exception {
    try{
      alterSession(ExecConstants.JSON_ALL_TEXT_MODE, true);
      test("select * from cp.`jsoninput/big_numeric.json`");
    } finally {
      resetSessionOption(ExecConstants.JSON_ALL_TEXT_MODE);
    }
  }

  @Test
  public void testExceptionHandling() throws Exception {
    runBoth(this::doTestExceptionHandling);
  }

  private void doTestExceptionHandling() throws Exception {
    try {
      test("select * from cp.`jsoninput/DRILL-2350.json`");
    } catch (UserException e) {
      Assert.assertEquals(
          UserBitShared.DrillPBError.ErrorType.UNSUPPORTED_OPERATION, e
              .getOrCreatePBError(false).getErrorType());
      String s = e.getMessage();
      assertTrue("Expected Unsupported Operation Exception.",
        s.contains("Drill does not support lists of different types."));
    }

  }

  @Test
  @Category(UnlikelyTest.class)
  // DRILL-1832
  public void testJsonWithNulls1() throws Exception {
    runBoth(this::doTestJsonWithNulls1);
  }

  private void doTestJsonWithNulls1() throws Exception {
    final String query = "select * from cp.`jsoninput/twitter_43.json`";
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .jsonBaselineFile("jsoninput/drill-1832-1-result.json")
      .go();
  }

  @Test
  @Category(UnlikelyTest.class)
  // DRILL-1832
  public void testJsonWithNulls2() throws Exception {
    runBoth(this::doTestJsonWithNulls2);
  }

  private void doTestJsonWithNulls2() throws Exception {
    final String query = "select SUM(1) as `sum_Number_of_Records_ok` from cp.`jsoninput/twitter_43.json` having (COUNT(1) > 0)";
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .jsonBaselineFile("jsoninput/drill-1832-2-result.json")
      .go();
  }

  // V1-only test. In V2, this works. See TestJsonReaderQueries.

  @Test
  public void testMixedNumberTypes() throws Exception {
    try {
      enableV2Reader(false);
      testBuilder()
        .sqlQuery("select * from cp.`jsoninput/mixed_number_types.json`")
        .unOrdered().jsonBaselineFile("jsoninput/mixed_number_types.json")
        .go();
      fail("Mixed number types verification failed, expected failure on conflicting number types.");
    } catch (Exception ex) {
      // this indicates successful completion of the test
      assertTrue(ex.getMessage()
        .contains("You tried to write a BigInt type when you are using a ValueWriter of type NullableFloat8WriterImpl."));
    } finally {
      resetV2Reader();
    }
  }

  @Test
  public void testMixedNumberTypesInAllTextMode() throws Exception {
    runBoth(this::doTestMixedNumberTypesInAllTextMode);
  }

  private void doTestMixedNumberTypesInAllTextMode() throws Exception {
    try {
      alterSession("store.json.all_text_mode", true);
      testBuilder()
        .sqlQuery("select * from cp.`jsoninput/mixed_number_types.json`")
        .unOrdered()
        .baselineColumns("a")
        .baselineValues("5.2")
        .baselineValues("6")
        .go();
    } finally {
      resetSessionOption("store.json.all_text_mode");
    }
  }

  @Test
  public void testMixedNumberTypesWhenReadingNumbersAsDouble() throws Exception {
    try {
      alterSession(ExecConstants.JSON_READ_NUMBERS_AS_DOUBLE, true);
      testBuilder()
        .sqlQuery("select * from cp.`jsoninput/mixed_number_types.json`")
        .unOrdered()
        .baselineColumns("a")
        .baselineValues(5.2D)
        .baselineValues(6D)
        .go();
    } finally {
      resetSessionOption(ExecConstants.JSON_READ_NUMBERS_AS_DOUBLE);
    }
  }

  @Test
  public void drill_3353() throws Exception {
    try {
      alterSession(ExecConstants.JSON_ALL_TEXT_MODE, true);
      test("create table dfs.tmp.drill_3353 as select a from dfs.`jsoninput/drill_3353` where e = true");
      runBoth(this::doDrill_3353);
    } finally {
      resetSessionOption(ExecConstants.JSON_ALL_TEXT_MODE);
    }
  }

  private void doDrill_3353() throws Exception {
    String query = "select t.a.d cnt from dfs.tmp.drill_3353 t where t.a.d is not null";
    test(query);
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("cnt")
      .baselineValues("1")
      .go();
  }

  @Test
  @Category(UnlikelyTest.class)
  // See DRILL-3476
  public void testNestedFilter() throws Exception {
    runBoth(this::doTestNestedFilter);
  }

  private void doTestNestedFilter() throws Exception {
    String query = "select a from cp.`jsoninput/nestedFilter.json` t where t.a.b = 1";
    String baselineQuery = "select * from cp.`jsoninput/nestedFilter.json` t where t.a.b = 1";
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .sqlBaselineQuery(baselineQuery)
      .go();
  }

  @Test
  @Category(UnlikelyTest.class)
  // See DRILL-4653
  /* Test for CountingJSONReader */
  public void testCountingQuerySkippingInvalidJSONRecords() throws Exception {
    try {
      String query = "select count(*) from cp.`jsoninput/drill4653/file.json`";

      testBuilder()
        .unOrdered()
        .disableSessionOption(ENABLE_V2_JSON_READER_KEY)
        .enableSessionOption(JSON_READER_SKIP_INVALID_RECORDS_FLAG)
        .enableSessionOption(JSON_READER_PRINT_INVALID_RECORDS_LINE_NOS_FLAG)
        .sqlQuery(query)
        .sqlBaselineQuery(query)
        .go();
    } finally {
      resetSessionOption(ENABLE_V2_JSON_READER_KEY);
      resetSessionOption(JSON_READER_PRINT_INVALID_RECORDS_LINE_NOS_FLAG);
      resetSessionOption(JSON_READER_SKIP_INVALID_RECORDS_FLAG);
    }
  }

  @Test
  @Category(UnlikelyTest.class)
  // See DRILL-4653
  /* Test for CountingJSONReader */
  public void testCountingQueryNotSkippingInvalidJSONRecords() throws Exception {
    runBoth(this::doTestCountingQueryNotSkippingInvalidJSONRecords);
  }

  private void doTestCountingQueryNotSkippingInvalidJSONRecords() throws Exception {
    try {
      String query = "select count(*) from cp.`jsoninput/drill4653/file.json`";
      testBuilder()
        .unOrdered()
        .sqlQuery(query)
        .sqlBaselineQuery(query)
        .go();
    } catch (Exception ex) {
      // do nothing just return
       return;
    }
    throw new Exception("testCountingQueryNotSkippingInvalidJSONRecords");
  }

  @Test
  @Category(UnlikelyTest.class)
  // See DRILL-4653
  /* Test for JSONReader */
  public void testNotCountingQuerySkippingInvalidJSONRecords() throws Exception {
    try {
      String query = "select sum(balance) from cp.`jsoninput/drill4653/file.json`";
      testBuilder()
        .unOrdered()
        .disableSessionOption(ENABLE_V2_JSON_READER_KEY)
        .enableSessionOption(JSON_READER_PRINT_INVALID_RECORDS_LINE_NOS_FLAG)
        .enableSessionOption(JSON_READER_SKIP_INVALID_RECORDS_FLAG)
        .sqlQuery(query)
        .sqlBaselineQuery(query)
        .go();
    }
    finally {
      resetSessionOption(ENABLE_V2_JSON_READER_KEY);
      resetSessionOption(JSON_READER_PRINT_INVALID_RECORDS_LINE_NOS_FLAG);
      resetSessionOption(JSON_READER_SKIP_INVALID_RECORDS_FLAG);
    }
  }

  @Test
  @Category(UnlikelyTest.class)
  // See DRILL-4653
  /* Test for JSONReader */
  public void testNotCountingQueryNotSkippingInvalidJSONRecords() throws Exception {
    runBoth(this::doTestNotCountingQueryNotSkippingInvalidJSONRecords);
  }

  private void doTestNotCountingQueryNotSkippingInvalidJSONRecords() throws Exception {
    try {
      String query = "select sum(balance) from cp.`jsoninput/drill4653/file.json`";
      testBuilder()
        .unOrdered()
        .sqlQuery(query)
        .sqlBaselineQuery(query)
        .go();
    } catch (Exception ex) {
      // do nothing just return
      return;
    }
    throw new Exception("testNotCountingQueryNotSkippingInvalidJSONRecords");
  }

  @Test
  @Category(UnlikelyTest.class)
  // See DRILL-7362
  /* Test for CountingJSONReader */
  public void testContainingArrayCount() throws Exception {
    runBoth(this::doTestContainingArrayCount);
  }

  private void doTestContainingArrayCount() throws Exception {
    testBuilder()
      .sqlQuery("select count(*) as cnt from cp.`store/json/listdoc.json`")
      .unOrdered()
      .baselineColumns("cnt")
      .baselineValues(2L)
      .go();
  }
}
