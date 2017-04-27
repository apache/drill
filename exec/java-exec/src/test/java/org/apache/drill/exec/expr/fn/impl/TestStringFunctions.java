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
package org.apache.drill.exec.expr.fn.impl;

import static org.junit.Assert.assertTrue;

import mockit.Mock;
import mockit.MockUp;
import mockit.integration.junit4.JMockit;
import org.apache.calcite.util.ConversionUtil;
import org.apache.calcite.util.Util;
import org.apache.commons.io.FileUtils;
import org.apache.drill.BaseTestQuery;
import org.apache.drill.exec.util.Text;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import org.junit.runner.RunWith;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.nio.charset.Charset;

@RunWith(JMockit.class)
public class TestStringFunctions extends BaseTestQuery {

  @Test
  public void testStrPosMultiByte() throws Exception {
    testBuilder()
        .sqlQuery("select `position`('a', 'abc') res1 from (values(1))")
        .ordered()
        .baselineColumns("res1")
        .baselineValues(1L)
        .go();

    testBuilder()
        .sqlQuery("select `position`('\\u11E9', '\\u11E9\\u0031') res1 from (values(1))")
        .ordered()
        .baselineColumns("res1")
        .baselineValues(1L)
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

  @Test
  public void testReverse() throws Exception {
    testBuilder()
      .sqlQuery("select reverse('qwerty') words from (values(1))")
      .unOrdered()
      .baselineColumns("words")
      .baselineValues("ytrewq")
      .build()
      .run();
  }

  @Test // DRILL-5424
  public void testReverseLongVarChars() throws Exception {
    File path = new File(BaseTestQuery.getTempDir("input"));
    try {
      path.mkdirs();
      String pathString = path.toPath().toString();

      try (BufferedWriter writer = new BufferedWriter(new FileWriter(new File(path, "table_with_long_varchars.json")))) {
        for (int i = 0; i < 10; i++) {
          writer.write("{ \"a\": \"abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz\"}");
        }
      }

      test("select reverse(a) from dfs_test.`%s/table_with_long_varchars.json` t", pathString);

    } finally {
      FileUtils.deleteQuietly(path);
    }
  }

  @Test
  public void testLower() throws Exception {
    testBuilder()
        .sqlQuery("select\n" +
            "lower('ABC') col_upper,\n" +
            "lower('abc') col_lower,\n" +
            "lower('AbC aBc') col_space,\n" +
            "lower('123ABC$!abc123.') as col_special,\n" +
            "lower('') as col_empty,\n" +
            "lower(cast(null as varchar(10))) as col_null\n" +
            "from (values(1))")
        .unOrdered()
        .baselineColumns("col_upper", "col_lower", "col_space", "col_special", "col_empty", "col_null")
        .baselineValues("abc", "abc", "abc abc", "123abc$!abc123.", "", null)
        .build()
        .run();
  }

  @Test
  public void testUpper() throws Exception {
    testBuilder()
        .sqlQuery("select\n" +
            "upper('ABC')as col_upper,\n" +
            "upper('abc') as col_lower,\n" +
            "upper('AbC aBc') as col_space,\n" +
            "upper('123ABC$!abc123.') as col_special,\n" +
            "upper('') as col_empty,\n" +
            "upper(cast(null as varchar(10))) as col_null\n" +
            "from (values(1))")
        .unOrdered()
        .baselineColumns("col_upper", "col_lower", "col_space", "col_special", "col_empty", "col_null")
        .baselineValues("ABC", "ABC", "ABC ABC", "123ABC$!ABC123.", "", null)
        .build()
        .run();
  }

  @Test
  public void testInitcap() throws Exception {
    testBuilder()
        .sqlQuery("select\n" +
            "initcap('ABC')as col_upper,\n" +
            "initcap('abc') as col_lower,\n" +
            "initcap('AbC aBc') as col_space,\n" +
            "initcap('123ABC$!abc123.') as col_special,\n" +
            "initcap('') as col_empty,\n" +
            "initcap(cast(null as varchar(10))) as col_null\n" +
            "from (values(1))")
        .unOrdered()
        .baselineColumns("col_upper", "col_lower", "col_space", "col_special", "col_empty", "col_null")
        .baselineValues("Abc", "Abc", "Abc Abc", "123abc$!Abc123.", "", null)
        .build()
        .run();
  }

  @Ignore("DRILL-5477")
  @Test
  public void testMultiByteEncoding() throws Exception {
    // mock calcite util method to return utf charset
    // instead of setting saffron.default.charset at system level
    new MockUp<Util>()
    {
      @Mock
      Charset getDefaultCharset() {
        return Charset.forName(ConversionUtil.NATIVE_UTF16_CHARSET_NAME);
      }
    };

    testBuilder()
        .sqlQuery("select\n" +
            "upper('привет')as col_upper,\n" +
            "lower('ПРИВЕТ') as col_lower,\n" +
            "initcap('приВЕТ') as col_initcap\n" +
            "from (values(1))")
        .unOrdered()
        .baselineColumns("col_upper", "col_lower", "col_initcap")
        .baselineValues("ПРИВЕТ", "привет", "Привет")
        .build()
        .run();
  }
}
