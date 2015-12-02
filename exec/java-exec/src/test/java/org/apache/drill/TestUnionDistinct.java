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

import com.google.common.collect.Lists;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.exec.work.foreman.SqlUnsupportedException;
import org.apache.drill.exec.work.foreman.UnsupportedRelOperatorException;
import org.junit.Test;

import java.util.List;

public class TestUnionDistinct extends BaseTestQuery {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestUnionDistinct.class);

  private static final String sliceTargetSmall = "alter session set `planner.slice_target` = 1";
  private static final String sliceTargetDefault = "alter session reset `planner.slice_target`";

  @Test  // Simple Union over two scans
  public void testUnionDistinct1() throws Exception {
    String query = "(select n_regionkey from cp.`tpch/nation.parquet`) union (select r_regionkey from cp.`tpch/region.parquet`)";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .csvBaselineFile("testframework/unionDistinct/q1.tsv")
        .baselineTypes(TypeProtos.MinorType.INT)
        .baselineColumns("n_regionkey")
        .build()
        .run();
  }

  @Test  // Union over inner joins
  public void testUnionDistinct2() throws Exception {
    String query = "select n1.n_nationkey from cp.`tpch/nation.parquet` n1 inner join cp.`tpch/region.parquet` r1 on n1.n_regionkey = r1.r_regionkey where n1.n_nationkey in (1, 2) \n" +
        "union \n" +
        "select n2.n_nationkey from cp.`tpch/nation.parquet` n2 inner join cp.`tpch/region.parquet` r2 on n2.n_regionkey = r2.r_regionkey where n2.n_nationkey in (1, 2, 3, 4)";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .csvBaselineFile("testframework/unionDistinct/q2.tsv")
        .baselineTypes(TypeProtos.MinorType.INT)
        .baselineColumns("n_nationkey")
        .build()
        .run();
  }

  @Test  // Union over grouped aggregates
  public void testUnionDistinct3() throws Exception {
    String query = "select n1.n_nationkey from cp.`tpch/nation.parquet` n1 where n1.n_nationkey in (1, 2) group by n1.n_nationkey \n" +
        "union \n" +
        "select r1.r_regionkey from cp.`tpch/region.parquet` r1 group by r1.r_regionkey";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .csvBaselineFile("testframework/unionDistinct/q3.tsv")
        .baselineTypes(TypeProtos.MinorType.INT)
        .baselineColumns("n_nationkey")
        .build()
        .run();
  }

  @Test    // Chain of Unions
  public void testUnionDistinct4() throws Exception {
    String query = "select n_regionkey from cp.`tpch/nation.parquet` \n" +
            "union select r_regionkey from cp.`tpch/region.parquet` \n" +
            "union select n_nationkey from cp.`tpch/nation.parquet` \n" +
            "union select c_custkey from cp.`tpch/customer.parquet` where c_custkey < 5";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .csvBaselineFile("testframework/unionDistinct/q4.tsv")
        .baselineTypes(TypeProtos.MinorType.INT)
        .baselineColumns("n_regionkey")
        .build()
        .run();
  }

  @Test  // Union of all columns in the table
  public void testUnionDistinct5() throws Exception {
    String query = "select r_name, r_comment, r_regionkey from cp.`tpch/region.parquet` r1 \n" +
        "union \n" +
        "select r_name, r_comment, r_regionkey from cp.`tpch/region.parquet` r2";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .csvBaselineFile("testframework/unionDistinct/q5.tsv")
        .baselineTypes(TypeProtos.MinorType.VARCHAR, TypeProtos.MinorType.VARCHAR, TypeProtos.MinorType.INT)
        .baselineColumns("r_name", "r_comment", "r_regionkey")
        .build()
        .run();
  }

  @Test // Union-Distinct where same column is projected twice in right child
  public void testUnionDistinct6() throws Exception {
    String query = "select n_nationkey, n_regionkey from cp.`tpch/nation.parquet` where n_regionkey = 1 \n" +
        "union \n" +
        "select r_regionkey, r_regionkey from cp.`tpch/region.parquet` where r_regionkey = 2";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .csvBaselineFile("testframework/unionDistinct/q6.tsv")
        .baselineTypes(TypeProtos.MinorType.INT, TypeProtos.MinorType.INT)
        .baselineColumns("n_nationkey", "n_regionkey")
        .build()
        .run();
  }

  @Test // Union-Distinct where same column is projected twice in left and right child
  public void testUnionDistinct6_1() throws Exception {
    String query = "select n_nationkey, n_nationkey from cp.`tpch/nation.parquet` union \n" +
        "select r_regionkey, r_regionkey from cp.`tpch/region.parquet`";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .csvBaselineFile("testframework/unionDistinct/q6_1.tsv")
        .baselineTypes(TypeProtos.MinorType.INT, TypeProtos.MinorType.INT)
        .baselineColumns("n_nationkey", "n_nationkey0")
        .build()
        .run();
  }

  @Test  // Union-Distinct of two string literals of different lengths
  public void testUnionDistinct7() throws Exception {
    String query = "select 'abc' as col from cp.`tpch/region.parquet` union \n" +
        "select 'abcdefgh' from cp.`tpch/region.parquet`";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .csvBaselineFile("testframework/unionDistinct/q7.tsv")
        .baselineTypes(TypeProtos.MinorType.VARCHAR)
        .baselineColumns("col")
        .build()
        .run();
  }

  @Test  // Union-Distinct of two character columns of different lengths
  public void testUnionDistinct8() throws Exception {
    String query = "select n_name, n_nationkey from cp.`tpch/nation.parquet` union \n" +
        "select r_comment, r_regionkey  from cp.`tpch/region.parquet`";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .csvBaselineFile("testframework/unionDistinct/q8.tsv")
        .baselineTypes(TypeProtos.MinorType.VARCHAR, TypeProtos.MinorType.INT)
        .baselineColumns("n_name", "n_nationkey")
        .build()
        .run();
  }

  @Test // Union-Distinct of * column from JSON files in different directories
  public void testUnionDistinct9() throws Exception {
    String file0 = FileUtils.getResourceAsFile("/multilevel/json/1994/Q1/orders_94_q1.json").toURI().toString();
    String file1 = FileUtils.getResourceAsFile("/multilevel/json/1995/Q1/orders_95_q1.json").toURI().toString();
    String query = String.format("select o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment, o_orderkey from dfs_test.`%s` union \n" +
            "select o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment, o_orderkey from dfs_test.`%s`", file0, file1);

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .csvBaselineFile("testframework/unionDistinct/q9.tsv")
        .baselineTypes(TypeProtos.MinorType.BIGINT, TypeProtos.MinorType.VARCHAR, TypeProtos.MinorType.FLOAT8, TypeProtos.MinorType.VARCHAR,
            TypeProtos.MinorType.VARCHAR, TypeProtos.MinorType.VARCHAR, TypeProtos.MinorType.BIGINT,TypeProtos.MinorType.VARCHAR, TypeProtos.MinorType.BIGINT)
        .baselineColumns("o_custkey", "o_orderstatus", "o_totalprice", "o_orderdate",
            "o_orderpriority", "o_clerk", "o_shippriority", "o_comment", "o_orderkey")
        .build()
        .run();
  }

  @Test // Union-Distinct constant literals
  public void testUnionDistinct10() throws Exception {
    String query = "(select n_name, 'LEFT' as LiteralConstant, n_nationkey, '1' as NumberConstant from cp.`tpch/nation.parquet`) \n" +
        "union \n" +
        "(select 'RIGHT', r_name, '2', r_regionkey from cp.`tpch/region.parquet`)";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .csvBaselineFile("testframework/unionDistinct/q10.tsv")
        .baselineTypes(TypeProtos.MinorType.VARCHAR, TypeProtos.MinorType.VARCHAR, TypeProtos.MinorType.INT, TypeProtos.MinorType.INT)
        .baselineColumns("n_name", "LiteralConstant", "n_nationkey", "NumberConstant")
        .build()
        .run();
  }

  @Test
  public void testUnionDistinctViewExpandableStar() throws Exception {
    test("use dfs_test.tmp");
    test("create view nation_view_testunion as select n_name, n_nationkey from cp.`tpch/nation.parquet`;");
    test("create view region_view_testunion as select r_name, r_regionkey from cp.`tpch/region.parquet`;");

    String query1 = "(select * from dfs_test.tmp.`nation_view_testunion`) \n" +
        "union \n" +
        "(select * from dfs_test.tmp.`region_view_testunion`)";

    String query2 =  "(select r_name, r_regionkey from cp.`tpch/region.parquet`)  \n" +
        "union \n" +
        "(select * from dfs_test.tmp.`nation_view_testunion`)";

    try {
      testBuilder()
          .sqlQuery(query1)
          .unOrdered()
          .csvBaselineFile("testframework/unionDistinct/q11.tsv")
          .baselineTypes(TypeProtos.MinorType.VARCHAR, TypeProtos.MinorType.INT)
          .baselineColumns("n_name", "n_nationkey")
          .build()
          .run();

      testBuilder()
          .sqlQuery(query2)
          .unOrdered()
          .csvBaselineFile("testframework/unionDistinct/q12.tsv")
          .baselineTypes(TypeProtos.MinorType.VARCHAR, TypeProtos.MinorType.INT)
          .baselineColumns("r_name", "r_regionkey")
          .build()
          .run();
    } finally {
      test("drop view nation_view_testunion");
      test("drop view region_view_testunion");
    }
  }

  @Test(expected = UnsupportedRelOperatorException.class)
  public void testUnionDistinctViewUnExpandableStar() throws Exception {
    test("use dfs_test.tmp");
    test("create view nation_view_testunion as select * from cp.`tpch/nation.parquet`;");

    try {
      String query = "(select * from dfs_test.tmp.`nation_view_testunion`) \n" +
          "union (select * from cp.`tpch/region.parquet`)";
      test(query);
    } catch(UserException ex) {
      SqlUnsupportedException.errorClassNameToException(ex.getOrCreatePBError(false).getException().getExceptionClass());
      throw ex;
    } finally {
      test("drop view nation_view_testunion");
    }
  }

  @Test
  public void testDiffDataTypesAndModes() throws Exception {
    test("use dfs_test.tmp");
    test("create view nation_view_testunion as select n_name, n_nationkey from cp.`tpch/nation.parquet`;");
    test("create view region_view_testunion as select r_name, r_regionkey from cp.`tpch/region.parquet`;");

    String t1 = "(select n_comment, n_regionkey from cp.`tpch/nation.parquet` limit 5)";
    String t2 = "(select * from nation_view_testunion  limit 5)";
    String t3 = "(select full_name, store_id from cp.`employee.json` limit 5)";
    String t4 = "(select * from region_view_testunion limit 5)";

    String query1 = t1 + " union " + t2 + " union " + t3 + " union " + t4;

    try {
      testBuilder()
          .sqlQuery(query1)
          .unOrdered()
          .csvBaselineFile("testframework/unionDistinct/q13.tsv")
          .baselineTypes(TypeProtos.MinorType.VARCHAR, TypeProtos.MinorType.BIGINT)
          .baselineColumns("n_comment", "n_regionkey")
          .build()
          .run();
    } finally {
      test("drop view nation_view_testunion");
      test("drop view region_view_testunion");
    }
  }

  @Test
  public void testDistinctOverUnionDistinctwithFullyQualifiedColumnNames() throws Exception {
    String query = "select distinct sq.x1, sq.x2 \n" +
        "from \n" +
        "((select n_regionkey as a1, n_name as b1 from cp.`tpch/nation.parquet`) \n" +
        "union \n" +
        "(select r_regionkey as a2, r_name as b2 from cp.`tpch/region.parquet`)) as sq(x1,x2)";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .csvBaselineFile("testframework/unionDistinct/q14.tsv")
        .baselineTypes(TypeProtos.MinorType.INT, TypeProtos.MinorType.VARCHAR)
        .baselineColumns("x1", "x2")
        .build()
        .run();
  }

  @Test
  public void testUnionDistinctContainsColumnANumericConstant() throws Exception {
    String query = "(select n_nationkey, n_regionkey, n_name from cp.`tpch/nation.parquet`  limit 5) \n" +
        "union \n" +
        "(select 1, n_regionkey, 'abc' from cp.`tpch/nation.parquet` limit 5)";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .csvBaselineFile("testframework/unionDistinct/q15.tsv")
        .baselineTypes(TypeProtos.MinorType.INT, TypeProtos.MinorType.INT, TypeProtos.MinorType.VARCHAR)
        .baselineColumns("n_nationkey", "n_regionkey", "n_name")
        .build().run();
  }

  @Test
  public void testUnionDistinctEmptySides() throws Exception {
    String query1 = "(select n_nationkey, n_regionkey, n_name from cp.`tpch/nation.parquet`  limit 0) \n" +
        "union \n" +
        "(select 1, n_regionkey, 'abc' from cp.`tpch/nation.parquet` limit 5)";

    String query2 = "(select n_nationkey, n_regionkey, n_name from cp.`tpch/nation.parquet`  limit 5) \n" +
        "union \n" +
        "(select 1, n_regionkey, 'abc' from cp.`tpch/nation.parquet` limit 0)";

    testBuilder()
        .sqlQuery(query1)
        .unOrdered()
        .csvBaselineFile("testframework/unionDistinct/q16.tsv")
        .baselineTypes(TypeProtos.MinorType.INT, TypeProtos.MinorType.INT, TypeProtos.MinorType.VARCHAR)
        .baselineColumns("n_nationkey", "n_regionkey", "n_name")
        .build()
        .run();

    testBuilder()
        .sqlQuery(query2)
        .unOrdered()
        .csvBaselineFile("testframework/unionDistinct/q17.tsv")
        .baselineTypes(TypeProtos.MinorType.INT, TypeProtos.MinorType.INT, TypeProtos.MinorType.VARCHAR)
        .baselineColumns("n_nationkey", "n_regionkey", "n_name")
        .build()
        .run();
  }

  @Test
  public void testAggregationOnUnionDistinctOperator() throws Exception {
    String root = FileUtils.getResourceAsFile("/store/text/data/t.json").toURI().toString();
    String query1 = String.format(
        "(select calc1, max(b1) as `max`, min(b1) as `min`, count(c1) as `count` \n" +
        "from (select a1 + 10 as calc1, b1, c1 from dfs_test.`%s` \n" +
        "union \n" +
        "select a1 + 100 as diff1, b1 as diff2, c1 as diff3 from dfs_test.`%s`) \n" +
        "group by calc1 order by calc1)", root, root);

    String query2 = String.format(
        "(select calc1, min(b1) as `min`, max(b1) as `max`, count(c1) as `count` \n" +
        "from (select a1 + 10 as calc1, b1, c1 from dfs_test.`%s` \n" +
        "union \n" +
        "select a1 + 100 as diff1, b1 as diff2, c1 as diff3 from dfs_test.`%s`) \n" +
        "group by calc1 order by calc1)", root, root);

    testBuilder()
        .sqlQuery(query1)
        .unOrdered()
        .csvBaselineFile("testframework/unionDistinct/testAggregationOnUnionDistinctOperator_1.tsv")
        .baselineTypes(TypeProtos.MinorType.BIGINT, TypeProtos.MinorType.BIGINT, TypeProtos.MinorType.BIGINT, TypeProtos.MinorType.BIGINT)
        .baselineColumns("calc1", "max", "min", "count")
        .build()
        .run();

    testBuilder()
        .sqlQuery(query2)
        .unOrdered()
        .csvBaselineFile("testframework/unionDistinct/testAggregationOnUnionDistinctOperator_2.tsv")
        .baselineTypes(TypeProtos.MinorType.BIGINT, TypeProtos.MinorType.BIGINT, TypeProtos.MinorType.BIGINT, TypeProtos.MinorType.BIGINT)
        .baselineColumns("calc1", "min", "max", "count")
        .build()
        .run();
  }

  @Test(expected = UserException.class)
  public void testUnionDistinctImplicitCastingFailure() throws Exception {
    String rootInt = FileUtils.getResourceAsFile("/store/json/intData.json").toURI().toString();
    String rootBoolean = FileUtils.getResourceAsFile("/store/json/booleanData.json").toURI().toString();

    String query = String.format(
        "(select key from dfs_test.`%s` \n" +
        "union \n" +
        "select key from dfs_test.`%s` )", rootInt, rootBoolean);

    test(query);
  }

  @Test
  public void testDateAndTimestampJson() throws Exception {
    String rootDate = FileUtils.getResourceAsFile("/store/json/dateData.json").toURI().toString();
    String rootTimpStmp = FileUtils.getResourceAsFile("/store/json/timeStmpData.json").toURI().toString();

    String query1 = String.format(
        "(select max(key) as key from dfs_test.`%s` \n" +
        "union \n" +
        "select key from dfs_test.`%s`)", rootDate, rootTimpStmp);

    String query2 = String.format(
        "select key from dfs_test.`%s` \n" +
        "union \n" +
        "select max(key) as key from dfs_test.`%s`", rootDate, rootTimpStmp);

    String query3 = String.format(
        "select key from dfs_test.`%s` \n" +
        "union \n" +
        "select max(key) as key from dfs_test.`%s`", rootDate, rootTimpStmp);

    testBuilder()
        .sqlQuery(query1)
        .unOrdered()
        .csvBaselineFile("testframework/unionDistinct/q18_1.tsv")
        .baselineTypes(TypeProtos.MinorType.VARCHAR)
        .baselineColumns("key")
        .build()
        .run();

    testBuilder()
        .sqlQuery(query2)
        .unOrdered()
        .csvBaselineFile("testframework/unionDistinct/q18_2.tsv")
        .baselineTypes(TypeProtos.MinorType.VARCHAR)
        .baselineColumns("key")
        .build()
        .run();

    testBuilder()
        .sqlQuery(query3)
        .unOrdered()
        .csvBaselineFile("testframework/unionDistinct/q18_3.tsv")
        .baselineTypes(TypeProtos.MinorType.VARCHAR)
        .baselineColumns("key")
        .build()
        .run();
  }

  @Test
  public void testUnionDistinctOneInputContainsAggFunction() throws Exception {
    String root = FileUtils.getResourceAsFile("/multilevel/csv/1994/Q1/orders_94_q1.csv").toURI().toString();
    String query1 = String.format("select * from ((select count(c1) as ct from (select columns[0] c1 from dfs.`%s`)) \n" +
        "union \n" +
        "(select columns[0] c2 from dfs.`%s`)) order by ct limit 3", root, root);

    String query2 = String.format("select * from ((select columns[0] ct from dfs.`%s`) \n" +
        "union \n" +
        "(select count(c1) as c2 from (select columns[0] c1 from dfs.`%s`))) order by ct limit 3", root, root);

    String query3 = String.format("select * from ((select count(c1) as ct from (select columns[0] c1 from dfs.`%s`) )\n" +
        "union \n" +
        "(select count(c1) as c2 from (select columns[0] c1 from dfs.`%s`))) order by ct", root, root);

    testBuilder()
        .sqlQuery(query1)
        .unOrdered()
        .baselineColumns("ct")
        .baselineValues((long) 10)
        .baselineValues((long) 66)
        .baselineValues((long) 99)
        .build()
        .run();

    testBuilder()
        .sqlQuery(query2)
        .unOrdered()
        .baselineColumns("ct")
        .baselineValues((long) 10)
        .baselineValues((long) 66)
        .baselineValues((long) 99)
        .build()
        .run();

    testBuilder()
        .sqlQuery(query3)
        .unOrdered()
        .baselineColumns("ct")
        .baselineValues((long) 10)
        .build()
        .run();
  }

  @Test
  public void testUnionDistinctDiffTypesAtPlanning() throws Exception {
    String query = "select count(c1) as ct from (select cast(r_regionkey as int) c1 from cp.`tpch/region.parquet`) \n" +
        "union \n" +
        "(select cast(r_regionkey as int) c2 from cp.`tpch/region.parquet`)";
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("ct")
        .baselineValues((long) 5)
        .baselineValues((long) 0)
        .baselineValues((long) 1)
        .baselineValues((long) 2)
        .baselineValues((long) 3)
        .baselineValues((long) 4)
        .build()
        .run();
  }

  @Test
  public void testUnionDistinctRightEmptyJson() throws Exception {
    String rootEmpty = FileUtils.getResourceAsFile("/project/pushdown/empty.json").toURI().toString();
    String rootSimple = FileUtils.getResourceAsFile("/store/json/booleanData.json").toURI().toString();

    String queryRightEmpty = String.format(
        "select key from dfs_test.`%s` \n" +
        "union \n" +
        "select key from dfs_test.`%s`", rootSimple, rootEmpty);

    testBuilder()
        .sqlQuery(queryRightEmpty)
        .unOrdered()
        .baselineColumns("key")
        .baselineValues(true)
        .baselineValues(false)
        .build()
        .run();
  }

  @Test
  public void testUnionDistinctLeftEmptyJson() throws Exception {
    final String rootEmpty = FileUtils.getResourceAsFile("/project/pushdown/empty.json").toURI().toString();
    final String rootSimple = FileUtils.getResourceAsFile("/store/json/booleanData.json").toURI().toString();

    final String queryLeftEmpty = String.format(
        "select key from dfs_test.`%s` " +
            "union " +
            "select key from dfs_test.`%s`",
        rootEmpty,
        rootSimple);

    testBuilder()
        .sqlQuery(queryLeftEmpty)
        .unOrdered()
        .baselineColumns("key")
        .baselineValues(true)
        .baselineValues(false)
        .build()
        .run();
  }

  @Test
  public void testUnionDistinctBothEmptyJson() throws Exception {
    final String rootEmpty = FileUtils.getResourceAsFile("/project/pushdown/empty.json").toURI().toString();
    final String query = String.format(
        "select key from dfs_test.`%s` " +
            "union " +
            "select key from dfs_test.`%s`",
        rootEmpty,
        rootEmpty);

    final List<Pair<SchemaPath, TypeProtos.MajorType>> expectedSchema = Lists.newArrayList();
    final TypeProtos.MajorType majorType = TypeProtos.MajorType.newBuilder()
        .setMinorType(TypeProtos.MinorType.INT)
        .setMode(TypeProtos.DataMode.OPTIONAL)
        .build();
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("key"), majorType));

    testBuilder()
        .sqlQuery(query)
        .schemaBaseLine(expectedSchema)
        .build()
        .run();
  }

  @Test
  public void testUnionDistinctRightEmptyBatch() throws Exception {
    String rootSimple = FileUtils.getResourceAsFile("/store/json/booleanData.json").toURI().toString();

    String queryRightEmptyBatch = String.format(
        "select key from dfs_test.`%s` " +
            "union " +
            "select key from dfs_test.`%s` where 1 = 0",
        rootSimple,
        rootSimple);

    testBuilder()
        .sqlQuery(queryRightEmptyBatch)
        .unOrdered()
        .baselineColumns("key")
        .baselineValues(true)
        .baselineValues(false)
        .build().run();
  }

  @Test
  public void testUnionDistinctLeftEmptyBatch() throws Exception {
    String rootSimple = FileUtils.getResourceAsFile("/store/json/booleanData.json").toURI().toString();

    final String queryLeftBatch = String.format(
        "select key from dfs_test.`%s` where 1 = 0 " +
            "union " +
            "select key from dfs_test.`%s`",
        rootSimple,
        rootSimple);

    testBuilder()
        .sqlQuery(queryLeftBatch)
        .unOrdered()
        .baselineColumns("key")
        .baselineValues(true)
        .baselineValues(false)
        .build()
        .run();
  }

  @Test
  public void testUnionDistinctBothEmptyBatch() throws Exception {
    String rootSimple = FileUtils.getResourceAsFile("/store/json/booleanData.json").toURI().toString();
    final String query = String.format(
        "select key from dfs_test.`%s` where 1 = 0 " +
            "union " +
            "select key from dfs_test.`%s` where 1 = 0",
        rootSimple,
        rootSimple);

    final List<Pair<SchemaPath, TypeProtos.MajorType>> expectedSchema = Lists.newArrayList();
    final TypeProtos.MajorType majorType = TypeProtos.MajorType.newBuilder()
        .setMinorType(TypeProtos.MinorType.INT)
        .setMode(TypeProtos.DataMode.OPTIONAL)
        .build();
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("key"), majorType));

    testBuilder()
        .sqlQuery(query)
        .schemaBaseLine(expectedSchema)
        .build()
        .run();
  }

  @Test
  public void testFilterPushDownOverUnionDistinct() throws Exception {
    String query = "select n_regionkey from \n"
        + "(select n_regionkey from cp.`tpch/nation.parquet` union select r_regionkey from cp.`tpch/region.parquet`) \n"
        + "where n_regionkey > 0 and n_regionkey < 2 \n"
        + "order by n_regionkey";

    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("n_regionkey")
        .baselineValues(1)
        .build()
        .run();
  }

  @Test
  public void testInListPushDownOverUnionDistinct() throws Exception {
    String query = "select n_nationkey \n" +
        "from (select n1.n_nationkey from cp.`tpch/nation.parquet` n1 inner join cp.`tpch/region.parquet` r1 on n1.n_regionkey = r1.r_regionkey \n" +
        "union \n" +
        "select n2.n_nationkey from cp.`tpch/nation.parquet` n2 inner join cp.`tpch/region.parquet` r2 on n2.n_regionkey = r2.r_regionkey) \n" +
        "where n_nationkey in (1, 2)";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("n_nationkey")
        .baselineValues(1)
        .baselineValues(2)
        .build()
        .run();
  }

  @Test
  public void testFilterPushDownOverUnionDistinctCSV() throws Exception {
    String root = FileUtils.getResourceAsFile("/multilevel/csv/1994/Q1/orders_94_q1.csv").toURI().toString();
    String query = String.format("select ct \n" +
        "from ((select count(c1) as ct from (select columns[0] c1 from dfs.`%s`)) \n" +
        "union \n" +
        "(select columns[0] c2 from dfs.`%s`)) \n" +
        "where ct < 100", root, root);

    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("ct")
        .baselineValues((long) 10)
        .baselineValues((long) 66)
        .baselineValues((long) 99)
        .build()
        .run();
  }

  @Test
  public void testProjectPushDownOverUnionDistinctWithProject() throws Exception {
    String query = "select n_nationkey, n_name from \n" +
        "(select n_nationkey, n_name, n_comment from cp.`tpch/nation.parquet` \n" +
        "union select r_regionkey, r_name, r_comment  from cp.`tpch/region.parquet`)";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .csvBaselineFile("testframework/unionDistinct/testProjectPushDownOverUnionDistinctWithProject.tsv")
        .baselineTypes(TypeProtos.MinorType.INT, TypeProtos.MinorType.VARCHAR)
        .baselineColumns("n_nationkey", "n_name")
        .build()
        .run();
  }

  @Test
  public void testProjectPushDownOverUnionDistinctWithoutProject() throws Exception {
    String query = "select n_nationkey from \n" +
        "(select n_nationkey, n_name, n_comment from cp.`tpch/nation.parquet` \n" +
        "union select r_regionkey, r_name, r_comment  from cp.`tpch/region.parquet`)";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .csvBaselineFile("testframework/unionDistinct/testProjectPushDownOverUnionDistinctWithoutProject.tsv")
        .baselineTypes(TypeProtos.MinorType.INT)
        .baselineColumns("n_nationkey")
        .build()
        .run();
  }

  @Test
  public void testProjectWithExpressionPushDownOverUnionDistinct() throws Exception {
    String query = "select 2 * n_nationkey as col from \n" +
        "(select n_nationkey, n_name, n_comment from cp.`tpch/nation.parquet` \n" +
        "union select r_regionkey, r_name, r_comment  from cp.`tpch/region.parquet`)";

    // Validate the result
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .csvBaselineFile("testframework/unionDistinct/testProjectWithExpressionPushDownOverUnionDistinct.tsv")
        .baselineTypes(TypeProtos.MinorType.INT)
        .baselineColumns("col")
        .build()
        .run();
  }

  @Test
  public void testProjectDownOverUnionDistinctImplicitCasting() throws Exception {
    String root = FileUtils.getResourceAsFile("/store/text/data/nations.csv").toURI().toString();
    String query = String.format("select 2 * n_nationkey as col from \n" +
        "(select n_nationkey, n_name, n_comment from cp.`tpch/nation.parquet` \n" +
        "union select columns[0], columns[1], columns[2] from dfs.`%s`) \n" +
        "order by col limit 10", root);

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .csvBaselineFile("testframework/unionDistinct/testProjectDownOverUnionDistinctImplicitCasting.tsv")
        .baselineTypes(TypeProtos.MinorType.INT)
        .baselineColumns("col")
        .build()
        .run();
  }

  @Test
  public void testProjectPushDownProjectColumnReorderingAndAlias() throws Exception {
    String query = "select n_comment as col1, n_nationkey as col2, n_name as col3 from \n" +
        "(select n_nationkey, n_name, n_comment from cp.`tpch/nation.parquet` \n" +
        "union select r_regionkey, r_name, r_comment  from cp.`tpch/region.parquet`)";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .csvBaselineFile("testframework/unionDistinct/testProjectPushDownProjectColumnReorderingAndAlias.tsv")
        .baselineTypes(TypeProtos.MinorType.VARCHAR, TypeProtos.MinorType.INT, TypeProtos.MinorType.VARCHAR)
        .baselineColumns("col1", "col2", "col3")
        .build()
        .run();
  }

  @Test
  public void testProjectFiltertPushDownOverUnionDistinct() throws Exception {
    String query = "select n_nationkey from \n" +
        "(select n_nationkey, n_name, n_comment from cp.`tpch/nation.parquet` \n" +
        "union select r_regionkey, r_name, r_comment  from cp.`tpch/region.parquet`) \n" +
        "where n_nationkey > 0 and n_nationkey < 4";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .csvBaselineFile("testframework/unionDistinct/testProjectFiltertPushDownOverUnionDistinct.tsv")
        .baselineTypes(TypeProtos.MinorType.INT)
        .baselineColumns("n_nationkey")
        .build()
        .run();
  }

  @Test // DRILL-3296
  public void testGroupByUnionDistinct() throws Exception {
    String query = "select n_nationkey from \n" +
        "(select n_nationkey from cp.`tpch/nation.parquet` \n" +
        "union select n_nationkey from cp.`tpch/nation.parquet`) \n" +
        "group by n_nationkey";


    // Validate the plan
    final String[] expectedPlan = {"HashAgg.*\n" +
        ".*UnionAll"};
    final String[] excludedPlan = {"HashAgg.*\n.*HashAgg"};
    PlanTestBase.testPlanMatchingPatterns(query, expectedPlan, excludedPlan);

    // Validate the result
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .csvBaselineFile("testframework/unionDistinct/testGroupByUnionDistinct.tsv")
        .baselineTypes(TypeProtos.MinorType.INT)
        .baselineColumns("n_nationkey")
        .build()
        .run();
  }

  @Test // DRILL-4147 // union-distinct base case
  public void testDrill4147_1() throws Exception {
    final String l = FileUtils.getResourceAsFile("/multilevel/parquet/1994").toURI().toString();
    final String r = FileUtils.getResourceAsFile("/multilevel/parquet/1995").toURI().toString();

    final String query = String.format("SELECT o_custkey FROM dfs_test.`%s` \n" +
        "Union distinct SELECT o_custkey FROM dfs_test.`%s`", l, r);

    // Validate the plan
    final String[] expectedPlan = {"(?s)UnionExchange.*HashAgg.*HashToRandomExchange.*UnionAll.*"};
    final String[] excludedPlan = {};

    try {
      test(sliceTargetSmall);
      PlanTestBase.testPlanMatchingPatterns(query, expectedPlan, excludedPlan);

      testBuilder()
        .optionSettingQueriesForTestQuery(sliceTargetSmall)
        .optionSettingQueriesForBaseline(sliceTargetDefault)
        .unOrdered()
        .sqlQuery(query)
        .sqlBaselineQuery(query)
        .build()
        .run();
    } finally {
      test(sliceTargetDefault);
    }
  }

}
