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
import org.junit.Test;


public class JsonRecordReader2Test extends BaseTestQuery{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JsonRecordReader2Test.class);

  @Test
  public void testComplexJsonInput() throws Exception{
//  test("select z[0]['orange']  from cp.`jsoninput/input2.json` limit 10");
    test("select `integer`, x['y'] as x1, x['y'] as x2, z[0], z[0]['orange'], z[1]['pink']  from cp.`jsoninput/input2.json` limit 10 ");
//    test("select x from cp.`jsoninput/input2.json`");

//    test("select z[0]  from cp.`jsoninput/input2.json` limit 10");
  }

  @Test
  public void testComplexMultipleTimes() throws Exception{
    for(int i =0 ; i < 5; i++){
    test("select * from cp.`join/merge_join.json`");
    }
  }

  @Test
  public void trySimpleQueryWithLimit() throws Exception{
    test("select * from cp.`limit/test1.json` limit 10");
  }
}
