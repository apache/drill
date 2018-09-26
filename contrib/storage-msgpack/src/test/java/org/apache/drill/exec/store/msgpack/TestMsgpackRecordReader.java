package org.apache.drill.exec.store.msgpack;
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


import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.drill.test.BaseTestQuery;
import org.junit.Test;

public class TestMsgpackRecordReader extends BaseTestQuery {

  @Test
  public void testBasic() throws Exception {
    ArrayList<Map<String, Object>> baseline = new ArrayList<Map<String, Object>>();
    HashMap<String, Object> map = new HashMap<String, Object>();
    map.put("`apple`", 1);
    map.put("`banana`", 2);
    map.put("`orange`", "infini!!!");
    map.put("`potato`", null);
    baseline.add(map);
    map = new HashMap<String, Object>();
    map.put("`apple`", 1);
    map.put("`banana`", 2);
    map.put("`orange`", null);
    map.put("`potato`", 12.12);
    baseline.add(map);

    testBuilder()
        .sqlQuery("select * from cp.`msgpack/testBasic.mp`")
        .ordered()
        .baselineRecords(baseline)
        .go();

  }

  @Test
  public void testBasicSelect() throws Exception {
    testBuilder()
        .sqlQuery("select apple as y from cp.`msgpack/testBasic.mp`")
        .ordered()
        .baselineColumns("y").baselineValues(1).baselineValues(1)
        .go();
  }

  @Test
  public void testSelectMap() throws Exception {
    ArrayList<Map<String, Object>> baseline = new ArrayList<Map<String, Object>>();
    HashMap<String, Object> map = new HashMap<String, Object>();
    map.put("`x`", 44);
    baseline.add(map);
    map = new HashMap<String, Object>();
    map.put("`x`", null);
    baseline.add(map);
    testBuilder()
        .sqlQuery("select root.aMap.b as x from cp.`msgpack/testNested.mp` as root")
        .ordered()
        .baselineRecords(baseline)
        .go();
  }

  @Test
  public void testSelectArray() throws Exception {
    ArrayList<Map<String, Object>> baseline = new ArrayList<Map<String, Object>>();
    HashMap<String, Object> map = new HashMap<String, Object>();
    map.put("`x`", null);
    baseline.add(map);
    map = new HashMap<String, Object>();
    map.put("`x`", 0.342);
    baseline.add(map);
    testBuilder()
        .sqlQuery("select round(root.anArray[1], 3) as x from cp.`msgpack/testNested.mp` as root")
        .ordered()
        .baselineRecords(baseline)
        .go();
  }

  @Test
  public void testSelectMapOfMap() throws Exception {
    testBuilder()
        .sqlQuery("select root.mapOfMap.aMap.x as x from cp.`msgpack/testNestedMapOfMap.mp` as root")
        .ordered()
        .baselineColumns("x")
        .baselineValues(1)
        .go();
  }
  @Test
  public void testSelectArrayOfMap() throws Exception {
    testBuilder()
        .sqlQuery("select root.arrayOfMap[0].x as x from cp.`msgpack/testNestedArrayOfMap.mp` as root")
        .ordered()
        .baselineColumns("x")
        .baselineValues(1)
        .go();
  }

  @Test
  public void testSelectArrayOfMap2() throws Exception {
    testBuilder()
        .sqlQuery("select root.arrayOfMap[1].x as x from cp.`msgpack/testNestedArrayOfMap.mp` as root")
        .ordered()
        .baselineColumns("x")
        .baselineValues(1)
        .go();
  }

  @Test
  public void testSelectMapWithArray() throws Exception {
    testBuilder()
        .sqlQuery("select root.mapWithArray.anArray[1] as x from cp.`msgpack/testNestedMapWithArray.mp` as root")
        .ordered()
        .baselineColumns("x")
        .baselineValues("v2")
        .go();
  }
}
