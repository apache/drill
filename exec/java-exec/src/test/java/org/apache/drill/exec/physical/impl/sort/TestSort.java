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
package org.apache.drill.exec.physical.impl.sort;

import org.apache.drill.BaseTestQuery;
import org.junit.Test;

/**
 * Placeholder for all sort related test. Can be used as we move
 * more tests to use the new test framework
 */
public class TestSort extends BaseTestQuery {

  @Test
  public void testSortWithComplexInput() throws Exception {
    test("select t.a from cp.`jsoninput/repeatedmap_sort_bug.json` t order by t.b");
  }
}