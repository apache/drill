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
package org.apache.drill.exec.store.json;

import org.apache.drill.BaseTestQuery;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.proto.UserBitShared;
import org.junit.Test;
import org.junit.Assert;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestJsonRecordReader extends BaseTestQuery {
  //private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestJsonRecordReader.class);

  @Test
  public void testComplexJsonInput() throws Exception {
//  test("select z[0]['orange']  from cp.`jsoninput/input2.json` limit 10");
    test("select `integer`, x['y'] as x1, x['y'] as x2, z[0], z[0]['orange'], z[1]['pink']  from cp.`jsoninput/input2.json` limit 10 ");
//    test("select x from cp.`jsoninput/input2.json`");

//    test("select z[0]  from cp.`jsoninput/input2.json` limit 10");
  }

  @Test
  public void testContainingArray() throws Exception {
    test("select * from dfs.`${WORKING_PATH}/src/test/resources/store/json/listdoc.json`");
  }

  @Test
  public void testComplexMultipleTimes() throws Exception {
    for(int i =0 ; i < 5; i++) {
    test("select * from cp.`join/merge_join.json`");
    }
  }

  @Test
  public void trySimpleQueryWithLimit() throws Exception {
    test("select * from cp.`limit/test1.json` limit 10");
  }

  @Test// DRILL-1634 : retrieve an element in a nested array in a repeated map.  RepeatedMap (Repeated List (Repeated varchar))
  public void testNestedArrayInRepeatedMap() throws Exception {
    test("select a[0].b[0] from cp.`jsoninput/nestedArray.json`");
    test("select a[0].b[1] from cp.`jsoninput/nestedArray.json`");
    test("select a[1].b[1] from cp.`jsoninput/nestedArray.json`");  // index out of the range. Should return empty list.
  }

  @Test
  public void testEmptyMapDoesNotFailValueCapacityCheck() throws Exception {
    final String sql = "select * from cp.`store/json/value-capacity.json`";
    test(sql);
  }

  @Test
  public void testEnableAllTextMode() throws Exception {
    testNoResult("alter session set `store.json.all_text_mode`= true");
    test("select * from cp.`jsoninput/big_numeric.json`");
    testNoResult("alter session set `store.json.all_text_mode`= false");
  }

  @Test
  public void testExceptionHandling() throws Exception {
    try {
      test("select * from cp.`jsoninput/DRILL-2350.json`");
    } catch(UserException e) {
      Assert.assertEquals(UserBitShared.DrillPBError.ErrorType.UNSUPPORTED_OPERATION, e.getOrCreatePBError(false).getErrorType());
      String s = e.getMessage();
      assertEquals("Expected Unsupported Operation Exception.", true, s.contains("Drill does not support lists of different types."));
    }

  }

  @Test //DRILL-1832
  public void testJsonWithNulls1() throws Exception {
    final String query="select * from cp.`jsoninput/twitter_43.json`";
    testBuilder()
            .sqlQuery(query)
            .unOrdered()
            .jsonBaselineFile("jsoninput/drill-1832-1-result.json")
            .go();
  }

  @Test //DRILL-1832
  public void testJsonWithNulls2() throws Exception {
    final String query="select SUM(1) as `sum_Number_of_Records_ok` from cp.`/jsoninput/twitter_43.json` having (COUNT(1) > 0)";
    testBuilder()
            .sqlQuery(query)
            .unOrdered()
            .jsonBaselineFile("jsoninput/drill-1832-2-result.json")
            .go();
  }

  @Test
  public void testMixedNumberTypes() throws Exception {
    try {
      testBuilder()
          .sqlQuery("select * from cp.`jsoninput/mixed_number_types.json`")
          .unOrdered()
          .jsonBaselineFile("jsoninput/mixed_number_types.json")
          .build().run();
    } catch (Exception ex) {
      assertTrue(ex.getMessage().contains("DATA_READ ERROR: Error parsing JSON - You tried to write a BigInt type when you are using a ValueWriter of type NullableFloat8WriterImpl."));
      // this indicates successful completion of the test
      return;
    }
    throw new Exception("Mixed number types verification failed, expected failure on conflicting number types.");
  }

  @Test
  public void testMixedNumberTypesInAllTextMode() throws Exception {
    testNoResult("alter session set `store.json.all_text_mode`= true");
    testBuilder()
        .sqlQuery("select * from cp.`jsoninput/mixed_number_types.json`")
        .unOrdered()
        .baselineColumns("a")
        .baselineValues("5.2")
        .baselineValues("6")
        .build().run();
  }

  @Test
  public void testMixedNumberTypesWhenReadingNumbersAsDouble() throws Exception {
    try {
    testNoResult("alter session set `store.json.read_numbers_as_double`= true");
    testBuilder()
        .sqlQuery("select * from cp.`jsoninput/mixed_number_types.json`")
        .unOrdered()
        .baselineColumns("a")
        .baselineValues(5.2D)
        .baselineValues(6D)
        .build().run();
    } finally {
      testNoResult("alter session set `store.json.read_numbers_as_double`= false");
    }
  }

  @Test
  public void drill_3353() throws Exception {
    try {
      testNoResult("alter session set `store.json.all_text_mode` = true");
      test("create table dfs_test.tmp.drill_3353 as select a from dfs.`${WORKING_PATH}/src/test/resources/jsoninput/drill_3353` where e = true");
      String query = "select t.a.d cnt from dfs_test.tmp.drill_3353 t where t.a.d is not null";
      test(query);
      testBuilder()
          .sqlQuery(query)
          .unOrdered()
          .baselineColumns("cnt")
          .baselineValues("1")
          .go();
    } finally {
      testNoResult("alter session set `store.json.all_text_mode` = false");
    }
  }

  @Test // See DRILL-3476
  public void testNestedFilter() throws Exception {
    String query = "select a from cp.`jsoninput/nestedFilter.json` t where t.a.b = 1";
    String baselineQuery = "select * from cp.`jsoninput/nestedFilter.json` t where t.a.b = 1";
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .sqlBaselineQuery(baselineQuery)
        .go();
  }
}
