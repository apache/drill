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
package org.apache.drill.exec.store.parquet;

import org.apache.drill.BaseTestQuery;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.proto.UserBitShared;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestFixedlenDecimal extends BaseTestQuery {
  // enable decimal data type
  @BeforeClass
  public static void enableDecimalDataType() throws Exception {
    test(String.format("alter session set `%s` = true", PlannerSettings.ENABLE_DECIMAL_DATA_TYPE_KEY));
  }

  private static final String DATAFILE = "cp.`parquet/fixedlenDecimal.parquet`";

  @Test
  public void testNullCount() throws Exception {
    String query = String.format("select count(*) as c from %s where department_id is null", DATAFILE);
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("c")
        .baselineValues(1L)
        .build()
        .run();
  }

  @Test
  public void testNotNullCount() throws Exception {
    String query = String.format("select count(*) as c from %s where department_id is not null", DATAFILE);
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("c")
        .baselineValues(106L)
        .build()
        .run();
  }

  @Test
  public void testSimpleQueryWithCast() throws Exception {
    String query = String.format("select cast(department_id as bigint) as c from %s where cast(employee_id as decimal) = 170", DATAFILE);
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("c")
        .baselineValues(80L)
        .build()
        .run();
  }

  @Test
  public void testSimpleQueryDrill4704Fix() throws Exception {
    String query = String.format("select cast(department_id as bigint) as c from %s where employee_id = 170", DATAFILE);
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("c")
        .baselineValues(80L)
        .build()
        .run();
  }
}
