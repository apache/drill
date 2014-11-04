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

import org.apache.drill.BaseTestQuery;
import org.junit.Test;

public class TestFlatten extends BaseTestQuery {

  @Test
  public void testKVGenFlatten1() throws Exception {
    test("select flatten(kvgen(f1)) as monkey, x " +
        "from cp.`/store/json/test_flatten_mapify.json`");
  }

  @Test
  public void testTwoFlattens() throws Exception {
    test("select `integer`, `float`, x, flatten(z), flatten(l) from cp.`/jsoninput/input2_modified.json`");
  }

  @Test
  public void testFilterFlattenedRecords() throws Exception {
    test("select t2.key from (select t.monkey.`value` as val, t.monkey.key as key from (select flatten(kvgen(f1)) as monkey, x " +
        "from cp.`/store/json/test_flatten_mapify.json`) as t) as t2 where t2.val > 1");
  }

}
