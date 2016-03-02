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
package org.apache.drill.exec.store.sys;

import org.apache.drill.BaseTestQuery;
import org.apache.drill.exec.ExecConstants;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestSystemTable extends BaseTestQuery {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestSystemTable.class);

  @BeforeClass
  public static void setupMultiNodeCluster() throws Exception {
    updateTestCluster(3, null);
  }

  @Test
  public void alterSessionOption() throws Exception {

    newTest() //
      .sqlQuery("select bool_val as bool from sys.options where name = '%s' order by type desc", ExecConstants.JSON_ALL_TEXT_MODE)
      .baselineColumns("bool")
      .ordered()
      .baselineValues(false)
      .go();

    test("alter session set `%s` = true", ExecConstants.JSON_ALL_TEXT_MODE);

    newTest() //
      .sqlQuery("select bool_val as bool from sys.options where name = '%s' order by type desc ", ExecConstants.JSON_ALL_TEXT_MODE)
      .baselineColumns("bool")
      .ordered()
      .baselineValues(false)
      .baselineValues(true)
      .go();
  }

  // DRILL-2670
  @Test
  public void optionsOrderBy() throws Exception {
    test("select * from sys.options order by name");
  }

  @Test
  public void threadsTable() throws Exception {
    test("select * from sys.threads");
  }

  @Test
  public void memoryTable() throws Exception {
    test("select * from sys.memory");
  }
}
