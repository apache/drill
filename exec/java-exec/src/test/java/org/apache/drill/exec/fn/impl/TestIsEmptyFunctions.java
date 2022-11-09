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

package org.apache.drill.exec.fn.impl;

import org.apache.drill.categories.SqlFunctionTest;
import org.apache.drill.categories.UnlikelyTest;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({UnlikelyTest.class, SqlFunctionTest.class})
public class TestIsEmptyFunctions extends ClusterTest {

  @BeforeClass
  public static void setUp() throws Exception {
    startCluster(ClusterFixture.builder(dirTestWatcher));
  }

  @Test
  public void testIsEmptyWithNumericColumn() throws Exception {
    String sql = "SELECT numeric_col FROM cp.`jsoninput/is_empty_tests.json` WHERE NOT is_empty" +
      "(numeric_col)";
    testBuilder()
      .sqlQuery(sql)
      .unOrdered()
      .baselineColumns("numeric_col")
      .baselineValues(1.3)
      .baselineValues(2.3)
      .baselineValues(1.0)
      .go();
  }

  @Test
  public void testIsEmptyWithTextColumn() throws Exception {
    String sql = "SELECT text_col FROM cp.`jsoninput/is_empty_tests.json` WHERE NOT is_empty" +
      "(text_col)";
    testBuilder()
      .sqlQuery(sql)
      .unOrdered()
      .baselineColumns("text_col")
      .baselineValues("text")
      .go();
  }

  @Test
  public void testIsEmptyWithList() throws Exception {
    String sql = "SELECT COUNT(*) AS row_count FROM cp.`jsoninput/is_empty_tests.json` WHERE " +
      "is_empty(list_col)";
    testBuilder()
      .sqlQuery(sql)
      .unOrdered()
      .baselineColumns("row_count")
      .baselineValues(1L)
      .go();
  }

  @Test
  public void testIsEmptyWithMap() throws Exception {
    String sql = "SELECT COUNT(*) AS row_count FROM cp.`jsoninput/is_empty_tests.json` WHERE " +
      "is_empty(map_column)";
    testBuilder()
      .sqlQuery(sql)
      .unOrdered()
      .baselineColumns("row_count")
      .baselineValues(3L)
      .go();
  }

}
