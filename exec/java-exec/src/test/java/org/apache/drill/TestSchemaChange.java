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
package org.apache.drill;

import org.apache.drill.common.util.TestTools;
import org.junit.Ignore;
import org.junit.Test;

public class TestSchemaChange extends BaseTestQuery {

  protected static final String WORKING_PATH = TestTools.getWorkingPath();
  protected static final String TEST_RES_PATH = WORKING_PATH + "/src/test/resources";

  @Test //DRILL-1605
  @Ignore("Until DRILL-2171 is fixed")
  public void testMultiFilesWithDifferentSchema() throws Exception {
    final String query = String.format("select a, b from dfs_test.`%s/schemachange/multi/*.json`", TEST_RES_PATH);
    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("a", "b")
        .baselineValues(1L, null)
        .baselineValues(2L, null)
        .baselineValues(null, true)
        .build()
        .run();
  }
}
