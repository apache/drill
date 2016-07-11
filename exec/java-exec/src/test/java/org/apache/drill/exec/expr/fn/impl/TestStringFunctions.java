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

import static org.junit.Assert.assertTrue;

import org.apache.drill.BaseTestQuery;
import org.apache.drill.exec.util.Text;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class TestStringFunctions extends BaseTestQuery {

  @Test
  public void testStrPosMultiByte() throws Exception {
    testBuilder()
        .sqlQuery("select `position`('a', 'abc') res1 from (values(1))")
        .ordered()
        .baselineColumns("res1")
        .baselineValues(1l)
        .go();

    testBuilder()
        .sqlQuery("select `position`('\\u11E9', '\\u11E9\\u0031') res1 from (values(1))")
        .ordered()
        .baselineColumns("res1")
        .baselineValues(1l)
        .go();
  }

  @Test
  public void testSplitPart() throws Exception {
    testBuilder()
        .sqlQuery("select split_part('abc~@~def~@~ghi', '~@~', 1) res1 from (values(1))")
        .ordered()
        .baselineColumns("res1")
        .baselineValues("abc")
        .go();

    testBuilder()
        .sqlQuery("select split_part('abc~@~def~@~ghi', '~@~', 2) res1 from (values(1))")
        .ordered()
        .baselineColumns("res1")
        .baselineValues("def")
        .go();

    // invalid index
    boolean expectedErrorEncountered;
    try {
      testBuilder()
          .sqlQuery("select split_part('abc~@~def~@~ghi', '~@~', 0) res1 from (values(1))")
          .ordered()
          .baselineColumns("res1")
          .baselineValues("abc")
          .go();
      expectedErrorEncountered = false;
    } catch (Exception ex) {
      assertTrue(ex.getMessage().contains("Index in split_part must be positive, value provided was 0"));
      expectedErrorEncountered = true;
    }
    if (!expectedErrorEncountered) {
      throw new RuntimeException("Missing expected error on invalid index for split_part function");
    }

    // with a multi-byte splitter
    testBuilder()
        .sqlQuery("select split_part('abc\\u1111drill\\u1111ghi', '\\u1111', 2) res1 from (values(1))")
        .ordered()
        .baselineColumns("res1")
        .baselineValues("drill")
        .go();

    // going beyond the last available index, returns empty string
    testBuilder()
        .sqlQuery("select split_part('a,b,c', ',', 4) res1 from (values(1))")
        .ordered()
        .baselineColumns("res1")
        .baselineValues("")
        .go();

    // if the delimiter does not appear in the string, 1 returns the whole string
    testBuilder()
        .sqlQuery("select split_part('a,b,c', ' ', 1) res1 from (values(1))")
        .ordered()
        .baselineColumns("res1")
        .baselineValues("a,b,c")
        .go();
  }

  @Test
  public void testRegexpMatches() throws Exception {
    testBuilder()
        .sqlQuery("select regexp_matches(a, '^a.*') res1, regexp_matches(b, '^a.*') res2 " +
                  "from (values('abc', 'bcd'), ('bcd', 'abc')) as t(a,b)")
        .unOrdered()
        .baselineColumns("res1", "res2")
        .baselineValues(true, false)
        .baselineValues(false, true)
        .build()
        .run();
  }

  @Test
  public void testRegexpMatchesNonAscii() throws Exception {
    testBuilder()
        .sqlQuery("select regexp_matches(a, 'München') res1, regexp_matches(b, 'AMünchenA') res2 " +
            "from (values('München', 'MünchenA'), ('MünchenA', 'AMünchenA')) as t(a,b)")
        .unOrdered()
        .baselineColumns("res1", "res2")
        .baselineValues(true, false)
        .baselineValues(false, true)
        .build()
        .run();
  }

  @Test
  public void testRegexpReplace() throws Exception {
    testBuilder()
        .sqlQuery("select regexp_replace(a, 'a|c', 'x') res1, regexp_replace(b, 'd', 'zzz') res2 " +
                  "from (values('abc', 'bcd'), ('bcd', 'abc')) as t(a,b)")
        .unOrdered()
        .baselineColumns("res1", "res2")
        .baselineValues("xbx", "bczzz")
        .baselineValues("bxd", "abc")
        .build()
        .run();
  }

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

  @Test
  public void testSplit() throws Exception {
    testBuilder()
        .sqlQuery("select split(n_name, ' ') words from cp.`tpch/nation.parquet` where n_nationkey = 24")
        .unOrdered()
        .baselineColumns("words")
        .baselineValues(ImmutableList.of(new Text("UNITED"), new Text("STATES")))
        .build()
        .run();
  }

}
