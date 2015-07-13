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
package org.apache.drill.exec.server;

import org.apache.drill.BaseTestQuery;
import org.apache.drill.exec.ExecConstants;
import org.junit.Test;

public class TestOptions extends BaseTestQuery{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestOptions.class);

  @Test
  public void testDrillbits() throws Exception{
    test("select * from sys.drillbits;");
  }

  @Test
  public void testOptions() throws Exception{
    test(
        "select * from sys.options;" +
        "ALTER SYSTEM set `planner.disable_exchanges` = true;" +
        "select * from sys.options;" +
        "ALTER SESSION set `planner.disable_exchanges` = true;" +
        "select * from sys.options;"
        );
  }

  @Test // DRILL-3122
  public void checkChangedColumn() throws Exception {
    test(String.format("ALTER session SET `%s` = %d;", ExecConstants.SLICE_TARGET,
      ExecConstants.SLICE_TARGET_DEFAULT));
    testBuilder()
        .sqlQuery(String.format("SELECT status FROM sys.options WHERE name = '%s' AND type = 'SESSION'", ExecConstants.SLICE_TARGET))
        .unOrdered()
        .baselineColumns("status")
        .baselineValues("DEFAULT")
        .build()
        .run();
  }
}
