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

import org.apache.drill.BaseTestQuery;
import org.junit.Test;

public class TestComplexTypeReader extends BaseTestQuery{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestComplexTypeReader.class);

  @Test
  // Repeated map (map) -> json.
  public void testX() throws Exception{
    test("select convert_to(z[0], 'JSON') from cp.`jsoninput/input2.json`;");
  }

  @Test
  //map -> json.
  public void testX2() throws Exception{
    test("select convert_to(x, 'JSON') from cp.`jsoninput/input2.json`;");
  }

  @Test
  //Map (mapfield) -> json.
  public void testX3() throws Exception{
    test("select convert_to(tbl.x.y, 'JSON') from cp.`jsoninput/input2.json` tbl;");
  }

  @Test
  //float value -> json
  public void testX4() throws Exception{
    test("select convert_to(`float`, 'JSON') from cp.`jsoninput/input2.json`;");
  }

  @Test
  //integer value -> json
  public void testX5() throws Exception{
    test("select convert_to(`integer`, 'JSON') from cp.`jsoninput/input2.json`;");
  }

  @Test
  // repeated map -> json.
  public void testX6() throws Exception{
    test("select convert_to(z, 'JSON')  from cp.`jsoninput/input2.json`;");
  }

  @Test
  //repeated list (Repeated BigInt) -> json
  public void testX7() throws Exception{
    test("select convert_to(rl[1], 'JSON') from cp.`jsoninput/input2.json`;");
  }

  @Test
  //repeated list (Repeated BigInt) -> json
  public void testX8() throws Exception{
    test("select convert_to(rl[0][1], 'JSON') from cp.`jsoninput/input2.json`;");
  }

  @Test
  //repeated list -> json
  public void testX9() throws Exception{
    test("select convert_to(rl, 'JSON') from cp.`jsoninput/input2.json`;");
  }

  @Test
  public void testY() throws Exception{
    test("select z[0] from cp.`jsoninput/input2.json`;");
  }

  @Test
  public void testY2() throws Exception{
    test("select x from cp.`jsoninput/input2.json`;");
  }

  @Test
  public void testY3() throws Exception{
    test("select tbl.x.y from cp.`jsoninput/input2.json` tbl;");
  }

  @Test
  public void testY6() throws Exception{
    test("select z  from cp.`jsoninput/input2.json`;");
  }

  @Test
  //repeated list (Repeated BigInt)
  public void testZ() throws Exception{
    test("select rl[1] from cp.`jsoninput/input2.json`;");
  }

  @Test
  //repeated list (Repeated BigInt ( BigInt) ) )
  public void testZ1() throws Exception{
    test("select rl[0][1] from cp.`jsoninput/input2.json`;");
  }

  @Test
  //repeated list (Repeated BigInt ( BigInt) ) ). The first index is out of boundary
  public void testZ2() throws Exception{
    test("select rl[1000][1] from cp.`jsoninput/input2.json`;");
  }

  @Test
  //repeated list (Repeated BigInt ( BigInt) ) ). The second index is out of boundary
  public void testZ3() throws Exception{
    test("select rl[0][1000] from cp.`jsoninput/input2.json`;");
  }

  @Test
  //repeated list. The repeated list is asked for twice, hence requires copying (evaluation in ProjectRecordBatch)
  public void testZ4() throws Exception{
    test("select rl, rl from cp.`jsoninput/input2.json`;");
  }

  @Test
  //repeated map  --> Json.  It will go beyond the buffer of size 256 allocated in setup.
  public void testA0() throws Exception{
    test("  select convert_to(types, 'JSON') from cp.`jsoninput/vvtypes.json`;");
  }

  @Test
  //repeated map (map) --> Json.
  public void testA1() throws Exception{
    test("  select convert_to(types[1], 'JSON') from cp.`jsoninput/vvtypes.json`;");
  }

  @Test
  //repeated map (map (repeated map) ) --> Json.
  public void testA2() throws Exception{
    test("  select convert_to(types[1].minor, 'JSON') from cp.`jsoninput/vvtypes.json`;");
  }

  @Test
  //repeated map (map( repeated map (map (varchar)))) --> Json.
  public void testA3() throws Exception{
    test("  select convert_to(types[1].minor[0].valueHolder, 'JSON') from cp.`jsoninput/vvtypes.json`;");
  }


  @Test
  //Two complex type functions in SELECT clause : repeated map (map) --> Json,
  public void testA4() throws Exception{
    test("  select convert_to(types[1], 'JSON'), convert_to(modes[2], 'JSON') from cp.`jsoninput/vvtypes.json`;");
  }

  @Test
  //repeated map (map) .
  public void testB1() throws Exception{
    test("  select types[1] from cp.`jsoninput/vvtypes.json`;");
  }

  @Test
  //repeated map (map (repeated map) ).
  public void testB2() throws Exception{
    test("  select types[1].minor from cp.`jsoninput/vvtypes.json`;");
  }

  @Test
  //repeated map (map( repeated map (map (varchar)))).
  public void testB3() throws Exception{
    test("  select types[1].minor[0].valueholder from cp.`jsoninput/vvtypes.json`;");
  }

  @Test  // DRILL-1250
  //repeated scalar values evaluation.
  public void test_repeatedList() throws Exception{
    test("select l, l from cp.`jsoninput/input2.json`;");
  }

}
