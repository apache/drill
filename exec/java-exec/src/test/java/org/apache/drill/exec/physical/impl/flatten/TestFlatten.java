/*******************************************************************************
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
 ******************************************************************************/
package org.apache.drill.exec.physical.impl.flatten;

import static org.junit.Assert.assertEquals;

import org.apache.drill.BaseTestQuery;
import org.apache.drill.exec.proto.UserBitShared;
import org.junit.Ignore;
import org.junit.Test;

public class TestFlatten extends BaseTestQuery {

  /**
   *  enable this if you have the following files:
   *    - /tmp/yelp_academic_dataset_business.json
   *    - /tmp/mapkv.json
   *    - /tmp/drill1665.json
   *    - /tmp/bigfile.json
   */
  public static boolean RUN_ADVANCED_TESTS = false;


  @Test
  public void testFlattenFailure() throws Exception {
    test("select flatten(complex), rownum from cp.`/store/json/test_flatten_mappify2.json`");
//    test("select complex, rownum from cp.`/store/json/test_flatten_mappify2.json`");
  }

  @Test
  public void drill1671() throws Exception{
    int rowCount = testSql("select * from (select count(*) as cnt from (select id, flatten(evnts1), flatten(evnts2), flatten(evnts3), flatten(evnts4), flatten(evnts5), flatten(evnts6), flatten(evnts7), flatten(evnts8), flatten(evnts9), flatten(evnts10), flatten(evnts11) from cp.`/flatten/many-arrays-50.json`)x )y where cnt = 2048");
    assertEquals(rowCount, 1);
  }

  @Test
  @Ignore("not yet fixed")
  public void drill1660() throws Exception {
    test("select * from cp.`/flatten/empty-rm.json`");
  }

  @Test
  public void drill1653() throws Exception{
    int rowCount = testSql("select * from (select sum(t.flat.`value`) as sm from (select id, flatten(kvgen(m)) as flat from cp.`/flatten/missing-map.json`)t) where sm = 10 ");
    assertEquals(1, rowCount);
  }

  @Test
  public void drill1652() throws Exception {
    if(RUN_ADVANCED_TESTS){
      test("select uid, flatten(transactions) from dfs.`/tmp/bigfile.json`");
    }
  }

  @Test
  @Ignore("Still not working.")
  public void drill1649() throws Exception {
    test("select event_info.uid, transaction_info.trans_id, event_info.event.evnt_id\n" +
        "from (\n" +
        " select userinfo.transaction.trans_id trans_id, max(userinfo.event.event_time) max_event_time\n" +
        " from (\n" +
        "     select uid, flatten(events) event, flatten(transactions) transaction from cp.`/flatten/single-user-transactions.json`\n" +
        " ) userinfo\n" +
        " where userinfo.transaction.trans_time >= userinfo.event.event_time\n" +
        " group by userinfo.transaction.trans_id\n" +
        ") transaction_info\n" +
        "inner join\n" +
        "(\n" +
        " select uid, flatten(events) event\n" +
        " from cp.`/flatten/single-user-transactions.json`\n" +
        ") event_info\n" +
        "on transaction_info.max_event_time = event_info.event.event_time;");
  }



  @Test
  public void testKVGenFlatten1() throws Exception {
    // works - TODO and verify results
    test("select flatten(kvgen(f1)) as monkey, x " +
        "from cp.`/store/json/test_flatten_mapify.json`");
  }

  @Test
  public void testTwoFlattens() throws Exception {
    // second re-write rule has been added to test the fixes together, this now runs
    test("select `integer`, `float`, x, flatten(z), flatten(l) from cp.`/jsoninput/input2_modified.json`");
  }

  @Test
  public void testFlattenRepeatedMap() throws Exception {
    test("select `integer`, `float`, x, flatten(z) from cp.`/jsoninput/input2.json`");
  }

  @Test
  public void testFlattenKVGenFlatten() throws Exception {
    // currently does not fail, but produces incorrect results, requires second re-write rule to split up expressions
    // with complex outputs
    test("select `integer`, `float`, x, flatten(kvgen(flatten(z))) from cp.`/jsoninput/input2.json`");
  }

  @Test
  public void testKVGenFlatten2() throws Exception {
    // currently runs
    // TODO - re-verify results by hand
    if(RUN_ADVANCED_TESTS){
      test("select flatten(kvgen(visited_cellid_counts)) as mytb from dfs.`/tmp/mapkv.json`") ;
    }
  }

  @Test
  public void testFilterFlattenedRecords() throws Exception {
    // WORKS!!
    // TODO - hand verify results
    test("select t2.key from (select t.monkey.`value` as val, t.monkey.key as key from (select flatten(kvgen(f1)) as monkey, x " +
        "from cp.`/store/json/test_flatten_mapify.json`) as t) as t2 where t2.val > 1");
  }

  @Test
  public void testFilterFlattenedRecords2() throws Exception {
    // previously failed in generated code
    //  "value" is neither a method, a field, nor a member class of "org.apache.drill.exec.expr.holders.RepeatedVarCharHolder" [ 42eb1fa1-0742-4e4f-8723-609215c18900 on 10.250.0.86:31010 ]
    // appears to be resolving the data coming out of flatten as repeated, check fast schema stuff

    // FIXED BY RETURNING PROPER SCHEMA DURING FAST SCHEMA STEP
    // these types of problems are being solved more generally as we develp better support for chaning schema
    if(RUN_ADVANCED_TESTS){
      test("select celltbl.catl from (\n" +
          "        select flatten(categories) catl from dfs.`/tmp/yelp_academic_dataset_business.json` b limit 100\n" +
          "    )  celltbl where celltbl.catl = 'Doctors'");
    }
  }

  @Test
  public void countAggFlattened() throws Exception {
    if(RUN_ADVANCED_TESTS){
      test("select celltbl.catl, count(celltbl.catl) from ( " +
          "select business_id, flatten(categories) catl from dfs.`/tmp/yelp_academic_dataset_business.json` b limit 100 " +
          ")  celltbl group by celltbl.catl limit 10 ");
    }
  }


  @Test
  public void flattenAndAdditionalColumn() throws Exception {
    if(RUN_ADVANCED_TESTS){
      test("select business_id, flatten(categories) from dfs.`/tmp/yelp_academic_dataset_business.json` b");
    }
  }

  @Test
  public void testFailingFlattenAlone() throws Exception {
    if(RUN_ADVANCED_TESTS){
      test("select flatten(categories) from dfs.`/tmp/yelp_academic_dataset_business.json` b  ");
    }
  }

  @Test
  public void testDistinctAggrFlattened() throws Exception {
    if(RUN_ADVANCED_TESTS){
      test(" select distinct(celltbl.catl) from (\n" +
          "        select flatten(categories) catl from dfs.`/tmp/yelp_academic_dataset_business.json` b\n" +
          "    )  celltbl");
    }

  }

  @Test
  public void testDrill1665() throws Exception {
    if(RUN_ADVANCED_TESTS){
      test("select id, flatten(evnts) as rpt from dfs.`/tmp/drill1665.json`");
    }

  }

  @Test
  public void testFlattenComplexRepeatedMap() throws Exception {
    test("select a, flatten(r_map_1), flatten(r_map_2) from cp.`/store/json/complex_repeated_map.json`");
  }

  @Test
  public void testFlatten2_levelRepeatedMap() throws Exception {
    test("select flatten(rm) from cp.`/store/json/2_level_repeated_map.json`");
  }

  @Test
  public void testDrill_1770() throws Exception {
    test("select flatten(sub.fk.`value`) from (select flatten(kvgen(map)) fk from cp.`/store/json/nested_repeated_map.json`) sub");
  }


  @Test //DRILL-2254
  public void testSingleFlattenFromNestedRepeatedList() throws Exception {
    final String query = "select t.uid, flatten(t.odd) odd from cp.`project/complex/a.json` t";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .jsonBaselineFile("flatten/drill-2254-result-single.json")
        .build()
        .run();
  }

  @Test //DRILL-2254 supplementary
  public void testMultiFlattenFromNestedRepeatedList() throws Exception {
    final String query = "select t.uid, flatten(flatten(t.odd)) odd from cp.`project/complex/a.json` t";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .jsonBaselineFile("flatten/drill-2254-result-multi.json")
        .build()
        .run();
  }

  @Test //DRILL-2254 supplementary
  public void testSingleMultiFlattenFromNestedRepeatedList() throws Exception {
    final String query = "select t.uid, flatten(t.odd) once, flatten(flatten(t.odd)) twice from cp.`project/complex/a.json` t";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .jsonBaselineFile("flatten/drill-2254-result-mix.json")
        .build()
        .run();
  }


  @Test
  public void testDrill_2013() throws Exception {
    testBuilder()
            .sqlQuery("select flatten(complex), rownum from cp.`/store/json/test_flatten_mappify2.json` where rownum > 5")
            .expectsEmptyResultSet()
            .build().run();
  }

}
