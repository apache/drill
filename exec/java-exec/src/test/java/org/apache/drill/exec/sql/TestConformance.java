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
package org.apache.drill.exec.sql;

import org.apache.drill.PlanTestBase;
import org.apache.drill.categories.SqlTest;
import org.apache.drill.test.BaseTestQuery;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SqlTest.class)
public class TestConformance extends BaseTestQuery {

  @Test
  public void testApply() throws Exception{

    //cross join is not support yet in Drill: DRILL-1921, so we are testing OUTER APPLY only
    String query = "SELECT c.c_nationkey, o.orderdate from " +
      "cp.`tpch/customer.parquet` c outer apply " +
      "cp.`tpch/orders.parquet` o " +
      "where c.c_custkey = o.o_custkey";

    PlanTestBase.testPlanMatchingPatterns(query,
        new String[] {"Join"}, new String[] {}
    );

    return;
  }


}
