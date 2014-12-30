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
package org.apache.drill.exec.physical.impl.window;

import org.apache.drill.BaseTestQuery;
import org.apache.drill.exec.ExecConstants;
import org.junit.Test;

public class TestWindowFrame extends BaseTestQuery {

  private void runTest(String data, String results, String window) throws Exception {
    testNoResult("alter session set `%s`= true", ExecConstants.ENABLE_WINDOW_FUNCTIONS);
    testBuilder()
      .sqlQuery("select count(*) over pos_win `count`, sum(salary) over pos_win `sum` from cp.`window/%s.json` window pos_win as (%s)", data, window)
      .ordered()
      .csvBaselineFile("window/" + results + ".tsv")
      .baselineColumns("count", "sum")
      .build().run();
  }

  /**
   * Single batch with a single partition (position_id column)
   */
  @Test
  public void testB1P1() throws Exception {
    runTest("b1.p1.data", "b1.p1", "partition by position_id order by position_id");
  }

  /**
   * Single batch with a single partition (position_id column) and multiple sub-partitions (sub column)
   */
  @Test
  public void testB1P1OrderBy() throws Exception {
    runTest("b1.p1.data", "b1.p1.subs", "partition by position_id order by sub");
  }

  /**
   * Single batch with 2 partitions (position_id column)
   */
  @Test
  public void testB1P2() throws Exception {
    runTest("b1.p2.data", "b1.p2", "partition by position_id order by position_id");
  }

  /**
   * Single batch with 2 partitions (position_id column)
   * with order by clause
   */
  @Test
  public void testB1P2OrderBy() throws Exception {
    runTest("b1.p2.data", "b1.p2.subs", "partition by position_id order by sub");
  }

  /**
   * 2 batches with 2 partitions (position_id column), each batch contains a different partition
   */
  @Test
  public void testB2P2() throws Exception {
    runTest("b2.p2.data", "b2.p2", "partition by position_id order by position_id");
  }

  @Test
  public void testB2P2OrderBy() throws Exception {
    runTest("b2.p2.data", "b2.p2.subs", "partition by position_id order by sub");
  }

  /**
   * 2 batches with 4 partitions, one partition has rows in both batches
   */
  @Test
  public void testB2P4() throws Exception {
    runTest("b2.p4.data", "b2.p4", "partition by position_id order by position_id");
  }

  /**
   * 2 batches with 4 partitions, one partition has rows in both batches
   * no sub partition has rows in both batches
   */
  @Test
  public void testB2P4OrderBy() throws Exception {
    runTest("b2.p4.data", "b2.p4.subs", "partition by position_id order by sub");
  }

  /**
   * 3 batches with 2 partitions, one partition has rows in all 3 batches
   */
  @Test
  public void testB3P2() throws Exception {
    runTest("b3.p2.data", "b3.p2", "partition by position_id order by position_id");
  }

  /**
   * 3 batches with 2 partitions, one partition has rows in all 3 batches
   * 2 subs have rows in 2 batches
   */
  @Test
  public void testB3P2OrderBy() throws Exception {
    runTest("b3.p2.data", "b3.p2.subs", "partition by position_id order by sub");
  }

  /**
   * 4 batches with 4 partitions. After processing 1st batch, when innerNext() is called again, framer can process
   * current batch without the need to call next(incoming).
   */
  @Test
  public void testb4P4() throws Exception {
    runTest("b4.p4.data", "b4.p4", "partition by position_id order by position_id");
  }

}
