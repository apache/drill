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

import org.apache.drill.common.util.FileUtils;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.compile.ClassTransformer;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.server.options.OptionValue;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestSkipInvalidRecords extends BaseTestQuery {
  @BeforeClass
  public static void setupOptions() throws Exception {
    test(String.format("alter session set `%s` = true", ExecConstants.ENABLE_SKIP_INVALID_RECORD_KEY));
  }

  @Test
  public void testCastFailAtSomeRecords_Project() throws Exception {
    String root = FileUtils.getResourceAsFile("/testSkipInvalidRecords/testCastFailAtSomeRecords.csv").toURI().toString();
    String query = String.format("select cast(columns[0] as Integer) as col0, cast(columns[1] as Integer) as col1 \n" +
        "from dfs_test.tmp.`%s`", root);

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("col0", "col1")
        .baselineValues(2, 2)
        .baselineValues(3, 3)
        .baselineValues(4, 4)
        .build()
        .run();
  }

  @Test
  public void testCastFailAtSomeRecords_Filter() throws Exception {
    String root = FileUtils.getResourceAsFile("/testSkipInvalidRecords/testCastFailAtSomeRecords.csv").toURI().toString();
    String query = String.format("select cast(columns[0] as integer) as c0, cast(columns[1] as integer) as c1 \n" +
            "from dfs_test.`%s` \n" +
            "where columns[0] > 1", root);

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("c0", "c1")
        .baselineValues(2, 2)
        .baselineValues(3, 3)
        .baselineValues(4, 4)
        .build()
        .run();
  }

  @Test
  public void testCTAS_CastFailAtSomeRecords_Project() throws Exception {
    String root = FileUtils.getResourceAsFile("/testSkipInvalidRecords/testCastFailAtSomeRecords.csv").toURI().toString();
    String query = String.format("select cast(columns[0] as integer) c0, cast(columns[1] as integer) c1 \n" +
        "from dfs_test.`%s`", root);

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("c0", "c1")
        .baselineValues(2, 2)
        .baselineValues(3, 3)
        .baselineValues(4, 4)
        .build()
        .run();
  }

  @Test
  public void testStar() throws Exception {
    test("select * from cp.`tpch/region.parquet`");
  }

  @AfterClass
  public static void shutdownOptions() throws Exception {
    test(String.format("alter session set `%s` = %s",
        ExecConstants.ENABLE_SKIP_INVALID_RECORD_KEY,
            ExecConstants.ENABLE_SKIP_INVALID_RECORD.getDefault().bool_val));
  }
}
