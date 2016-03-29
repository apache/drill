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
package org.apache.drill.exec.expr.fn.impl;

import org.apache.drill.BaseTestQuery;
import org.junit.Test;

public class TestStringFunctions extends BaseTestQuery {

  @Test
  public void testILike() throws Exception {
    testBuilder()
        .sqlQuery("select n_name from cp.`tpch/nation.parquet` where ilike(n_name, '%united%') = true")
        .unOrdered()
        .baselineColumns("n_name")
        .baselineValues("UNITED STATES")
        .baselineValues("UNITED KINGDOM")
        .build()
        .run();
  }

  @Test
  public void testILikeEscape() throws Exception {
    testBuilder()
        .sqlQuery("select a from (select concat(r_name , '_region') a from cp.`tpch/region.parquet`) where ilike(a, 'asia#_region', '#') = true")
        .unOrdered()
        .baselineColumns("a")
        .baselineValues("ASIA_region")
        .build()
        .run();
  }

  @Test
  public void testSubstr() throws Exception {
    testBuilder()
        .sqlQuery("select substr(n_name, 'UN.TE.') a from cp.`tpch/nation.parquet` where ilike(n_name, 'united%') = true")
        .unOrdered()
        .baselineColumns("a")
        .baselineValues("UNITED")
        .baselineValues("UNITED")
        .build()
        .run();
  }

  @Test
  public void testLpadTwoArgConvergeToLpad() throws Exception {
    final String query_1 = "SELECT lpad(r_name, 25) \n" +
        "FROM cp.`tpch/region.parquet`";


    final String query_2 = "SELECT lpad(r_name, 25, ' ') \n" +
        "FROM cp.`tpch/region.parquet`";

    testBuilder()
        .sqlQuery(query_1)
        .unOrdered()
        .sqlBaselineQuery(query_2)
        .build()
        .run();
  }

  @Test
  public void testRpadTwoArgConvergeToRpad() throws Exception {
    final String query_1 = "SELECT rpad(r_name, 25) \n" +
        "FROM cp.`tpch/region.parquet`";


    final String query_2 = "SELECT rpad(r_name, 25, ' ') \n" +
        "FROM cp.`tpch/region.parquet`";

    testBuilder()
        .sqlQuery(query_1)
        .unOrdered()
        .sqlBaselineQuery(query_2)
        .build()
        .run();
  }

  @Test
  public void testLtrimOneArgConvergeToLtrim() throws Exception {
    final String query_1 = "SELECT ltrim(concat(' ', r_name, ' ')) \n" +
        "FROM cp.`tpch/region.parquet`";


    final String query_2 = "SELECT ltrim(concat(' ', r_name, ' '), ' ') \n" +
        "FROM cp.`tpch/region.parquet`";

    testBuilder()
        .sqlQuery(query_1)
        .unOrdered()
        .sqlBaselineQuery(query_2)
        .build()
        .run();
  }

  @Test
  public void testRtrimOneArgConvergeToRtrim() throws Exception {
    final String query_1 = "SELECT rtrim(concat(' ', r_name, ' ')) \n" +
        "FROM cp.`tpch/region.parquet`";


    final String query_2 = "SELECT rtrim(concat(' ', r_name, ' '), ' ') \n" +
        "FROM cp.`tpch/region.parquet`";

    testBuilder()
        .sqlQuery(query_1)
        .unOrdered()
        .sqlBaselineQuery(query_2)
        .build()
        .run();
  }

  @Test
  public void testBtrimOneArgConvergeToBtrim() throws Exception {
    final String query_1 = "SELECT btrim(concat(' ', r_name, ' ')) \n" +
        "FROM cp.`tpch/region.parquet`";


    final String query_2 = "SELECT btrim(concat(' ', r_name, ' '), ' ') \n" +
        "FROM cp.`tpch/region.parquet`";

    testBuilder()
        .sqlQuery(query_1)
        .unOrdered()
        .sqlBaselineQuery(query_2)
        .build()
        .run();
  }
}
