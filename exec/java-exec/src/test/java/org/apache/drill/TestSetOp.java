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
package org.apache.drill;

import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.BatchSchemaBuilder;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.util.StoragePluginTestUtils;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.drill.categories.OperatorTest;
import org.apache.drill.categories.SqlTest;
import org.apache.drill.categories.UnlikelyTest;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.nio.file.Paths;
import java.util.List;

@Category({SqlTest.class, OperatorTest.class})
public class TestSetOp extends ClusterTest {
  private static final String EMPTY_DIR_NAME = "empty_directory";
  private static final String SLICE_TARGET_DEFAULT = "alter session reset `planner.slice_target`";

  @BeforeClass
  public static void setupTestFiles() throws Exception {
    startCluster(ClusterFixture.builder(dirTestWatcher));
    dirTestWatcher.copyResourceToRoot(Paths.get("multilevel", "parquet"));
    dirTestWatcher.makeTestTmpSubDir(Paths.get(EMPTY_DIR_NAME));

    // A tmp workspace with a default format defined for tests that need to
    // query empty directories without encountering an error.
    cluster.defineWorkspace(
        StoragePluginTestUtils.DFS_PLUGIN_NAME,
        "tmp_default_format",
        dirTestWatcher.getDfsTestTmpDir().getAbsolutePath(),
        "csvh"
    );
  }

  @Test
  public void TestExceptionWithSchemaLessDataSource() {
    String root = "/multilevel/csv/1994/Q1/orders_94_q1.csv";
    try {
      testBuilder()
        .sqlQuery("select * from cp.`%s` intersect select * from cp.`%s`", root, root)
        .unOrdered()
        .baselineColumns("a", "b")
        .baselineValues(1, 1)
        .go();
      Assert.fail("Missing expected exception on schema less data source");
    } catch (Exception ex) {
      Assert.assertThat(ex.getMessage(), ex.getMessage(),
        CoreMatchers.containsString("schema-less tables must specify the columns explicitly"));
    }

    try {
      testBuilder()
        .sqlQuery("select * from cp.`%s` except select * from cp.`%s`", root, root)
        .unOrdered()
        .baselineColumns("a", "b")
        .baselineValues(1, 1)
        .go();
      Assert.fail("Missing expected exception on schema less data source");
    } catch (Exception ex) {
      Assert.assertThat(ex.getMessage(), ex.getMessage(),
        CoreMatchers.containsString("schema-less tables must specify the columns explicitly"));
    }
  }

  @Test
  public void testIntersect() throws Exception {
    String query = "select * from (values(4,4), (2,2), (4,4), (1,1), (3,4), (2,2), (1,1)) t(a,b) intersect select * from (values(1,1), (1,1), (2,2), (3,3)) t(a,b)";
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("a", "b")
      .baselineValues(2, 2)
      .baselineValues(1, 1)
      .build().run();

    query = "select * from (values(4,4), (2,2), (4,4), (1,1), (3,4), (2,2), (1,1)) t(a,b) intersect all select * from (values(1,1), (1,1), (2,2), (3,3)) t(a,b)";
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("a", "b")
      .baselineValues(2, 2)
      .baselineValues(1, 1)
      .baselineValues(1, 1)
      .build().run();
  }

  @Test
  public void testExcept() throws Exception {
    String query = "select * from (values(4,4), (2,2), (4,4), (1,1), (3,4), (2,2), (1,1)) t(a,b) except select * from (values(1,1), (1,1), (2,2), (3,3)) t(a,b)";
    String aggAbovePattern = ".*Screen.*Agg.*SetOp.*";
    String aggBelowPattern = ".*SetOp.*Agg.*Values.*";

    queryBuilder()
      .sql(query)
      .planMatcher()
      .include(aggAbovePattern)
      .exclude(aggBelowPattern)
      .match(true);

    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("a", "b")
      .baselineValues(4, 4)
      .baselineValues(3, 4)
      .build().run();

    try {
      client.alterSession(ExecConstants.EXCEPT_ADD_AGG_BELOW_KEY, true);
      query = "select * from (values(4,4), (2,2), (4,4), (1,1), (3,4), (2,2), (1,1)) t(a,b) except select a, b from (values(1,1), (1,1), (2,2), (3,3)) t(a,b)";
      queryBuilder()
        .sql(query)
        .planMatcher()
        .include(aggBelowPattern)
        .exclude(aggAbovePattern)
        .match(true);

      testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("a", "b")
        .baselineValues(4, 4)
        .baselineValues(3, 4)
        .build().run();
    } finally {
      client.resetSession(ExecConstants.EXCEPT_ADD_AGG_BELOW_KEY);
    }

    query = "select * from (values(4,4), (2,2), (4,4), (1,1), (3,4), (2,2), (1,1)) t(a,b) except all select * from (values(1,1), (1,1), (2,2), (3,3)) t(a,b)";
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("a", "b")
      .baselineValues(4, 4)
      .baselineValues(4, 4)
      .baselineValues(3, 4)
      .baselineValues(2, 2)
      .build().run();
  }


  @Test
  public void testOverJoin() throws Exception {
    String query =
      "select n1.n_nationkey from cp.`tpch/nation.parquet` n1 inner join cp.`tpch/region.parquet` r1 on n1.n_regionkey = r1.r_regionkey where n1.n_nationkey in (1, 2, 3, 4) " +
      "except " +
      "select n2.n_nationkey from cp.`tpch/nation.parquet` n2 inner join cp.`tpch/region.parquet` r2 on n2.n_regionkey = r2.r_regionkey where n2.n_nationkey in (3, 4)";

    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("n_nationkey")
      .baselineValues(1)
      .baselineValues(2)
      .build().run();
  }

  @Test
  public void testExceptOverAgg() throws Exception {
    String query = "select n1.n_regionkey from cp.`tpch/nation.parquet` n1 group by n1.n_regionkey except " +
      "select r1.r_regionkey from cp.`tpch/region.parquet` r1 where r1.r_regionkey in (0, 1) group by r1.r_regionkey";

    String excludePattern = "Screen.*Agg.*SetOp";
    queryBuilder()
      .sql(query)
      .planMatcher()
      .exclude(excludePattern)
      .match(true);

    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("n_regionkey")
      .baselineValues(2)
      .baselineValues(3)
      .baselineValues(4)
      .build().run();
  }

  @Test
  public void testChain() throws Exception {
    String query = "select n_regionkey from cp.`tpch/nation.parquet` intersect " +
      "select r_regionkey from cp.`tpch/region.parquet` intersect " +
      "select n_nationkey from cp.`tpch/nation.parquet` where n_nationkey in (1,2) intersect " +
      "select c_custkey from cp.`tpch/customer.parquet` where c_custkey < 5";

    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("n_regionkey")
      .baselineValues(1)
      .baselineValues(2)
      .build().run();
  }

  @Test
  public void testSameColumn() throws Exception {
    String query = "select n_nationkey, n_regionkey from cp.`tpch/nation.parquet` where n_regionkey = 1 intersect all select r_regionkey, r_regionkey from cp.`tpch/region.parquet` where r_regionkey = 1";
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("n_nationkey", "n_regionkey")
      .baselineValues(1, 1)
      .build().run();

    query = "select n_regionkey, n_regionkey from cp.`tpch/nation.parquet` where n_regionkey = 1 except all select r_regionkey, r_regionkey from cp.`tpch/region.parquet` where r_regionkey = 1";
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("n_regionkey", "n_regionkey0")
      .baselineValues(1, 1)
      .baselineValues(1, 1)
      .baselineValues(1, 1)
      .baselineValues(1, 1)
      .build().run();
  }

  @Test
  public void testTwoStringColumns() throws Exception {
    String query = "select r_comment, r_regionkey from cp.`tpch/region.parquet` except select n_name, n_nationkey from cp.`tpch/nation.parquet`";

    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("r_comment", "r_regionkey")
      .baselineValues("lar deposits. blithely final packages cajole. regular waters are final requests. regular accounts are according to ", 0)
      .baselineValues("hs use ironic, even requests. s", 1)
      .baselineValues("ges. thinly even pinto beans ca", 2)
      .baselineValues("ly final courts cajole furiously final excuse", 3)
      .baselineValues("uickly special accounts cajole carefully blithely close requests. carefully final asymptotes haggle furiousl", 4)
      .build().run();
  }


  @Test
  public void testConstantLiterals() throws Exception {
    String query = "(select 'CONST' as LiteralConstant, 1 as NumberConstant, n_nationkey from cp.`tpch/nation.parquet`) " +
      "intersect " +
      "(select 'CONST', 1, r_regionkey from cp.`tpch/region.parquet`)";

    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("LiteralConstant", "NumberConstant", "n_nationkey")
      .baselineValues("CONST", 1, 0)
      .baselineValues("CONST", 1, 1)
      .baselineValues("CONST", 1, 2)
      .baselineValues("CONST", 1, 3)
      .baselineValues("CONST", 1, 4)
      .build().run();
  }

  @Test
  @Category(UnlikelyTest.class)
  public void testViewExpandableStar() throws Exception {
    try {
      run("use dfs.tmp");
      run("create view nation_view as select n_nationkey, n_name from (values(4,'4'), (2,'2'), (4,'4'), (1,'1'), (3,'4'), (2,'2'), (1,'1')) t(n_nationkey, n_name)");
      run("create view region_view as select r_regionkey, r_name from (values(1,'1'), (1,'1'), (2,'2'), (3,'3')) t(r_regionkey, r_name)");

      String query1 = "(select * from dfs.tmp.`nation_view`) " +
        "except " +
        "(select * from dfs.tmp.`region_view`) ";

      String query2 =  "(select r_regionkey, r_name from (values(1,'1'), (1,'1'), (2,'2'), (3,'3')) t(r_regionkey, r_name)) " +
        "intersect " +
        "(select * from dfs.tmp.`nation_view`)";

      testBuilder()
        .sqlQuery(query1)
        .unOrdered()
        .baselineColumns("n_nationkey", "n_name")
        .baselineValues(4, "4")
        .baselineValues(3, "4")
        .build().run();

      testBuilder()
        .sqlQuery(query2)
        .unOrdered()
        .baselineColumns("r_regionkey", "r_name")
        .baselineValues(1, "1")
        .baselineValues(2, "2")
        .build().run();
    } finally {
      run("drop view if exists nation_view");
      run("drop view if exists region_view");
    }
  }

  @Test
  public void testDiffDataTypesAndModes() throws Exception {
    try {
      run("use dfs.tmp");
      run("create view nation_view as select n_nationkey, n_name from (values(4,'4'), (2,'2'), (4,'4'), (1,'1'), (3,'4'), (2,'2'), (1,'1')) t(n_nationkey, n_name)");
      run("create view region_view as select r_regionkey, r_name from (values(1,'1'), (1,'1'), (2,'2'), (3,'3')) t(r_regionkey, r_name)");


      String t1 = "(select r_regionkey, r_name from (values(1,'1'), (1,'1'), (2,'2'), (3,'3')) t(r_regionkey, r_name))";
      String t2 = "(select * from nation_view)";
      String t3 = "(select * from region_view)";
      String t4 = "(select store_id, full_name from cp.`employee.json` limit 5)";

      String query1 = t1 + " intersect all " + t2 + " intersect all " + t3 + " except all " + t4;

      testBuilder()
        .sqlQuery(query1)
        .unOrdered()
        .baselineColumns("r_regionkey", "r_name")
        .baselineValues(1, "1")
        .baselineValues(1, "1")
        .baselineValues(2, "2")
        .build().run();
    } finally {
      run("drop view if exists nation_view");
      run("drop view if exists region_view");
    }
  }

  @Test
  @Category(UnlikelyTest.class)
  public void testDistinctOverIntersectAllWithFullyQualifiedColumnNames() throws Exception {
    String query = "select distinct sq.x1 " +
      "from " +
      "((select n_regionkey as a1 from cp.`tpch/nation.parquet`) " +
      "intersect all " +
      "(select r_regionkey as a2 from cp.`tpch/region.parquet`)) as sq(x1)";

    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("x1")
      .baselineValues(0)
      .baselineValues(1)
      .baselineValues(2)
      .baselineValues(3)
      .baselineValues(4)
      .build().run();
  }

  @Test
  @Category(UnlikelyTest.class)
  public void testContainsColumnAndNumericConstant() throws Exception {
    String query = "(select n_nationkey, n_regionkey, n_name from cp.`tpch/nation.parquet`) " +
      "intersect " +
      "(select 1, n_regionkey, 'ARGENTINA' from cp.`tpch/nation.parquet`)";

    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("n_nationkey", "n_regionkey", "n_name")
      .baselineValues(1, 1, "ARGENTINA")
      .build().run();
  }

  @Test
  @Category(UnlikelyTest.class)
  public void testEmptySides() throws Exception {
    String query1 = "(select n_nationkey, n_regionkey, n_name from cp.`tpch/nation.parquet` limit 0) " +
      "intersect " +
      "(select 1, n_regionkey, 'ARGENTINA' from cp.`tpch/nation.parquet`)";

    String query2 = "(select n_nationkey, n_regionkey, n_name from cp.`tpch/nation.parquet` where n_nationkey = 1) " +
      "except " +
      "(select 1, n_regionkey, 'ARGENTINA' from cp.`tpch/nation.parquet` limit 0)";

    testBuilder()
      .sqlQuery(query1)
      .unOrdered()
      .baselineColumns("n_nationkey", "n_regionkey", "n_name")
      .expectsEmptyResultSet()
      .build().run();

    testBuilder()
      .sqlQuery(query2)
      .unOrdered()
      .baselineColumns("n_nationkey", "n_regionkey", "n_name")
      .baselineValues(1, 1, "ARGENTINA")
      .build().run();
  }

  @Test
  @Category(UnlikelyTest.class)
  public void testAggregationOnIntersectOperator() throws Exception {
    String root = "/store/text/data/t.json";

    testBuilder()
      .sqlQuery("(select calc1, max(b1) as `max`, min(b1) as `min`, count(c1) as `count` " +
        "from (select a1 + 10 as calc1, b1, c1 from cp.`%s` " +
        "intersect all select a1 + 10 as diff1, b1 as diff2, c1 as diff3 from cp.`%s`) " +
        "group by calc1 order by calc1)", root, root)
      .ordered()
      .baselineColumns("calc1", "max", "min", "count")
      .baselineValues(10L, 2L, 1L, 5L)
      .baselineValues(20L, 5L, 3L, 5L)
      .build().run();

    testBuilder()
      .sqlQuery("(select calc1, min(b1) as `min`, max(b1) as `max`, count(c1) as `count` " +
        "from (select a1 + 10 as calc1, b1, c1 from cp.`%s` " +
        "intersect all select a1 + 10 as diff1, b1 as diff2, c1 as diff3 from cp.`%s`) " +
        "group by calc1 order by calc1)", root, root)
      .ordered()
      .baselineColumns("calc1", "min", "max", "count")
      .baselineValues(10L, 1L, 2L, 5L)
      .baselineValues(20L, 3L, 5L, 5L)
      .build().run();
  }

  @Test(expected = UserException.class)
  public void testImplicitCastingFailure() throws Exception {
    String rootInt = "/store/json/intData.json";
    String rootBoolean = "/store/json/booleanData.json";

    run("(select key from cp.`%s` " +
      "intersect all " +
      "select key from cp.`%s` )", rootInt, rootBoolean);
  }

  @Test
  @Category(UnlikelyTest.class)
  public void testDateAndTimestampJson() throws Exception {
    String rootDate = "/store/json/dateData.json";
    String rootTimpStmp = "/store/json/timeStmpData.json";

    testBuilder()
      .sqlQuery("(select max(key) as key from cp.`%s` " +
        "except all select key from cp.`%s`)", rootDate, rootTimpStmp)
      .unOrdered()
      .baselineColumns("key")
      .baselineValues("2011-07-26")
      .build().run();

    testBuilder()
      .sqlQuery("select key from cp.`%s` " +
        "except select max(key) as key from cp.`%s`", rootTimpStmp, rootDate)
      .unOrdered()
      .baselineColumns("key")
      .baselineValues("2015-03-26 19:04:55.542")
      .baselineValues("2015-03-26 19:04:55.543")
      .baselineValues("2015-03-26 19:04:55.544")
      .build().run();
  }

  @Test
  @Category(UnlikelyTest.class)
  public void testOneInputContainsAggFunction() throws Exception {
    String root = "/multilevel/csv/1994/Q1/orders_94_q1.csv";

    testBuilder()
      .sqlQuery("select * from ((select max(c1) as ct from (select columns[0] c1 from cp.`%s`)) \n" +
        "intersect all (select columns[0] c2 from cp.`%s`)) order by ct limit 3", root, root)
      .ordered()
      .baselineColumns("ct")
      .baselineValues("99")
      .build().run();

    testBuilder()
      .sqlQuery("select * from ((select columns[0] ct from cp.`%s`)\n" +
        "intersect all (select max(c1) as c2 from (select columns[0] c1 from cp.`%s`))) order by ct limit 3", root, root)
      .ordered()
      .baselineColumns("ct")
      .baselineValues("99")
      .build().run();

    testBuilder()
      .sqlQuery("select * from ((select max(c1) as ct from (select columns[0] c1 from cp.`%s`))\n" +
        "intersect all (select max(c1) as c2 from (select columns[0] c1 from cp.`%s`))) order by ct", root, root)
      .ordered()
      .baselineColumns("ct")
      .baselineValues("99")
      .build().run();
  }

  @Test
  @Category(UnlikelyTest.class)
  public void testInputsGroupByOnCSV() throws Exception {
    String root = "/multilevel/csv/1994/Q1/orders_94_q1.csv";

    testBuilder()
      .sqlQuery("select * from \n" +
          "((select columns[0] as col0 from cp.`%s` t1 \n" +
          "where t1.columns[0] = 66) \n" +
          "intersect all \n" +
          "(select columns[0] c2 from cp.`%s` t2 \n" +
          "where t2.columns[0] is not null \n" +
          "group by columns[0])) \n" +
          "group by col0",
        root, root)
      .unOrdered()
      .baselineColumns("col0")
      .baselineValues("66")
      .build().run();
  }

  @Test
  @Category(UnlikelyTest.class)
  public void testDiffTypesAtPlanning() throws Exception {
    testBuilder()
      .sqlQuery("select count(c1) as ct from (select cast(r_regionkey as int) c1 from cp.`tpch/region.parquet`) " +
        "intersect (select cast(r_regionkey as int) + 1 c2 from cp.`tpch/region.parquet`)")
      .ordered()
      .baselineColumns("ct")
      .baselineValues((long) 5)
      .build().run();
  }

  @Test
  @Category(UnlikelyTest.class)
  public void testRightEmptyJson() throws Exception {
    String rootEmpty = "/project/pushdown/empty.json";
    String rootSimple = "/store/json/booleanData.json";

    testBuilder()
      .sqlQuery("select key from cp.`%s` " +
          "intersect all " +
          "select key from cp.`%s`",
        rootSimple,
        rootEmpty)
      .unOrdered()
      .baselineColumns("key")
      .expectsEmptyResultSet()
      .build().run();

    testBuilder()
      .sqlQuery("select key from cp.`%s` " +
          "except all " +
          "select key from cp.`%s`",
        rootSimple,
        rootEmpty)
      .unOrdered()
      .baselineColumns("key")
      .baselineValues(true)
      .baselineValues(false)
      .build().run();
  }

  @Test
  public void testLeftEmptyJson() throws Exception {
    final String rootEmpty = "/project/pushdown/empty.json";
    final String rootSimple = "/store/json/booleanData.json";

    testBuilder()
      .sqlQuery("select key from cp.`%s` " +
          "intersect all " +
          "select key from cp.`%s`",
        rootEmpty,
        rootSimple)
      .unOrdered()
      .baselineColumns("key")
      .expectsEmptyResultSet()
      .build().run();

    testBuilder()
      .sqlQuery("select key from cp.`%s` " +
          "except all " +
          "select key from cp.`%s`",
        rootEmpty,
        rootSimple)
      .unOrdered()
      .baselineColumns("key")
      .expectsEmptyResultSet()
      .build().run();
  }

  @Test
  public void testBothEmptyJson() throws Exception {
    final String rootEmpty = "/project/pushdown/empty.json";

    final List<Pair<SchemaPath, TypeProtos.MajorType>> expectedSchema = Lists.newArrayList();
    final TypeProtos.MajorType majorType = TypeProtos.MajorType.newBuilder()
      .setMinorType(TypeProtos.MinorType.INT)
      .setMode(TypeProtos.DataMode.OPTIONAL)
      .build();
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("key"), majorType));

    testBuilder()
      .sqlQuery("select key from cp.`%s` " +
          "intersect all " +
          "select key from cp.`%s`",
        rootEmpty,
        rootEmpty)
      .schemaBaseLine(expectedSchema)
      .build()
      .run();
  }

  @Test
  public void testRightEmptyDataBatch() throws Exception {
    String rootSimple = "/store/json/booleanData.json";

    testBuilder()
      .sqlQuery("select key from cp.`%s` " +
          "except all " +
          "select key from cp.`%s` where 1 = 0",
        rootSimple,
        rootSimple)
      .unOrdered()
      .baselineColumns("key")
      .baselineValues(true)
      .baselineValues(false)
      .build().run();
  }

  @Test
  public void testLeftEmptyDataBatch() throws Exception {
    String rootSimple = "/store/json/booleanData.json";

    testBuilder()
      .sqlQuery("select key from cp.`%s` where 1 = 0 " +
          "except all " +
          "select key from cp.`%s`",
        rootSimple,
        rootSimple)
      .unOrdered()
      .baselineColumns("key")
      .expectsEmptyResultSet()
      .build()
      .run();
  }

  @Test
  public void testBothEmptyDataBatch() throws Exception {
    String rootSimple = "/store/json/booleanData.json";

    final List<Pair<SchemaPath, TypeProtos.MajorType>> expectedSchema = Lists.newArrayList();
    final TypeProtos.MajorType majorType = TypeProtos.MajorType.newBuilder()
      .setMinorType(TypeProtos.MinorType.BIT) // field "key" is boolean type
      .setMode(TypeProtos.DataMode.OPTIONAL)
      .build();
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("key"), majorType));

    testBuilder()
      .sqlQuery("select key from cp.`%s` where 1 = 0 " +
          "intersect all " +
          "select key from cp.`%s` where 1 = 0",
        rootSimple,
        rootSimple)
      .schemaBaseLine(expectedSchema)
      .build()
      .run();
  }

  @Test
  @Category(UnlikelyTest.class)
  public void testInListOnIntersect() throws Exception {
    String query = "select n_nationkey \n" +
      "from (select n1.n_nationkey from cp.`tpch/nation.parquet` n1 inner join cp.`tpch/region.parquet` r1 on n1.n_regionkey = r1.r_regionkey \n" +
      "intersect \n" +
      "select n2.n_nationkey from cp.`tpch/nation.parquet` n2 inner join cp.`tpch/region.parquet` r2 on n2.n_regionkey = r2.r_regionkey) \n" +
      "where n_nationkey in (1, 2)";

    // Validate the plan
    final String[] expectedPlan = {"Project.*\n" +
      ".*SetOp\\(all=\\[false\\], kind=\\[INTERSECT\\]\\).*\n" +
      ".*Project.*\n" +
      ".*HashJoin.*\n" +
      ".*SelectionVectorRemover.*\n" +
      ".*Filter.*\n" +
      ".*Scan.*columns=\\[`n_regionkey`, `n_nationkey`\\].*\n" +
      ".*Scan.*columns=\\[`r_regionkey`\\].*\n" +
      ".*Project.*\n" +
      ".*HashJoin.*\n" +
      ".*SelectionVectorRemover.*\n" +
      ".*Filter.*\n" +
      ".*Scan.*columns=\\[`n_regionkey`, `n_nationkey`\\].*\n" +
      ".*Scan.*columns=\\[`r_regionkey`\\].*"};
    queryBuilder()
      .sql(query)
      .planMatcher()
      .include(expectedPlan)
      .match();

    // Validate the result
    testBuilder()
      .sqlQuery(query)
      .ordered()
      .baselineColumns("n_nationkey")
      .baselineValues(1)
      .baselineValues(2)
      .build()
      .run();
  }

  @Test
  @Category(UnlikelyTest.class)
  public void testIntersectWith() throws Exception {
    final String query1 = "WITH year_total \n" +
      "     AS (SELECT c.r_regionkey    customer_id,\n" +
      "                1 year_total\n" +
      "         FROM   cp.`tpch/region.parquet` c\n" +
      "         Intersect ALL \n" +
      "         SELECT c.r_regionkey    customer_id, \n" +
      "                1 year_total\n" +
      "         FROM   cp.`tpch/region.parquet` c) \n" +
      "SELECT count(t_s_secyear.customer_id) as ct \n" +
      "FROM   year_total t_s_firstyear, \n" +
      "       year_total t_s_secyear, \n" +
      "       year_total t_w_firstyear, \n" +
      "       year_total t_w_secyear \n" +
      "WHERE  t_s_secyear.customer_id = t_s_firstyear.customer_id \n" +
      "       AND t_s_firstyear.customer_id = t_w_secyear.customer_id \n" +
      "       AND t_s_firstyear.customer_id = t_w_firstyear.customer_id \n" +
      "       AND CASE \n" +
      "             WHEN t_w_firstyear.year_total > 0 THEN t_w_secyear.year_total \n" +
      "             ELSE NULL \n" +
      "           END > -1";

    final String query2 = "WITH year_total \n" +
      "     AS (SELECT c.r_regionkey    customer_id,\n" +
      "                1 year_total\n" +
      "         FROM   cp.`tpch/region.parquet` c\n" +
      "         Intersect ALL \n" +
      "         SELECT c.r_regionkey    customer_id, \n" +
      "                1 year_total\n" +
      "         FROM   cp.`tpch/region.parquet` c) \n" +
      "SELECT count(t_w_firstyear.customer_id) as ct \n" +
      "FROM   year_total t_w_firstyear, \n" +
      "       year_total t_w_secyear \n" +
      "WHERE  t_w_firstyear.year_total = t_w_secyear.year_total \n" +
      " AND t_w_firstyear.year_total > 0 and t_w_secyear.year_total > 0";

    final String query3 = "WITH year_total_1\n" +
      "             AS (SELECT c.r_regionkey    customer_id,\n" +
      "                        1 year_total\n" +
      "                 FROM   cp.`tpch/region.parquet` c\n" +
      "                 Intersect ALL \n" +
      "                 SELECT c.r_regionkey    customer_id, \n" +
      "                        1 year_total\n" +
      "                 FROM   cp.`tpch/region.parquet` c) \n" +
      "             , year_total_2\n" +
      "             AS (SELECT c.n_nationkey    customer_id,\n" +
      "                        1 year_total\n" +
      "                 FROM   cp.`tpch/nation.parquet` c\n" +
      "                 Intersect ALL \n" +
      "                 SELECT c.n_nationkey    customer_id, \n" +
      "                        1 year_total\n" +
      "                 FROM   cp.`tpch/nation.parquet` c) \n" +
      "        SELECT count(t_w_firstyear.customer_id) as ct\n" +
      "        FROM   year_total_1 t_w_firstyear,\n" +
      "               year_total_2 t_w_secyear\n" +
      "        WHERE  t_w_firstyear.year_total = t_w_secyear.year_total\n" +
      "           AND t_w_firstyear.year_total > 0 and t_w_secyear.year_total > 0";

    final String query4 = "WITH year_total_1\n" +
      "             AS (SELECT c.n_regionkey    customer_id,\n" +
      "                        1 year_total\n" +
      "                 FROM   cp.`tpch/nation.parquet` c\n" +
      "                 Intersect ALL \n" +
      "                 SELECT c.r_regionkey    customer_id, \n" +
      "                        1 year_total\n" +
      "                 FROM   cp.`tpch/region.parquet` c), \n" +
      "             year_total_2\n" +
      "             AS (SELECT c.n_regionkey    customer_id,\n" +
      "                        1 year_total\n" +
      "                 FROM   cp.`tpch/nation.parquet` c\n" +
      "                 Intersect ALL \n" +
      "                 SELECT c.r_regionkey    customer_id, \n" +
      "                        1 year_total\n" +
      "                 FROM   cp.`tpch/region.parquet` c) \n" +
      "        SELECT count(t_w_firstyear.customer_id) as ct \n" +
      "        FROM   year_total_1 t_w_firstyear,\n" +
      "               year_total_2 t_w_secyear\n" +
      "        WHERE  t_w_firstyear.year_total = t_w_secyear.year_total\n" +
      "         AND t_w_firstyear.year_total > 0 and t_w_secyear.year_total > 0";

    testBuilder()
      .sqlQuery(query1)
      .ordered()
      .baselineColumns("ct")
      .baselineValues((long) 5)
      .build()
      .run();

    testBuilder()
      .sqlQuery(query2)
      .ordered()
      .baselineColumns("ct")
      .baselineValues((long) 25)
      .build()
      .run();

    testBuilder()
      .sqlQuery(query3)
      .ordered()
      .baselineColumns("ct")
      .baselineValues((long) 125)
      .build()
      .run();

    testBuilder()
      .sqlQuery(query4)
      .ordered()
      .baselineColumns("ct")
      .baselineValues((long) 25)
      .build()
      .run();
  }

  @Test
  @Category(UnlikelyTest.class)
  public void testFragmentNum() throws Exception {
    final String l = "/multilevel/parquet/1994";
    final String r = "/multilevel/parquet/1995";

    final String query = String.format("SELECT o_custkey FROM dfs.`%s` \n" +
      "Except All SELECT o_custkey FROM dfs.`%s`", l, r);

    // Validate the plan
    final String[] expectedPlan = {"UnionExchange.*\n",
      ".*SetOp"};

    try {
      client.alterSession(ExecConstants.SLICE_TARGET, 1);
      queryBuilder()
        .sql(query)
        .planMatcher()
        .include(expectedPlan)
        .match();

      testBuilder()
        .optionSettingQueriesForBaseline(SLICE_TARGET_DEFAULT)
        .unOrdered()
        .sqlQuery(query)
        .sqlBaselineQuery(query)
        .build()
        .run();
    } finally {
      client.resetSession(ExecConstants.SLICE_TARGET);
    }
  }

  @Test
  public void testGroupByOnSetOp() throws Exception {
    final String l = "/multilevel/parquet/1994";
    final String r = "/multilevel/parquet/1995";

    final String query = String.format("Select o_custkey, count(*) as cnt from \n" +
      " (SELECT o_custkey FROM dfs.`%s` \n" +
      "Intersect All SELECT o_custkey FROM dfs.`%s`) \n" +
      "group by o_custkey", l, r);

    // Validate the plan
    final String[] expectedPlan = {"(?s)UnionExchange.*StreamAgg.*Sort.*SetOp.*"};

    try {
      client.alterSession(ExecConstants.SLICE_TARGET, 1);
      queryBuilder()
        .sql(query)
        .planMatcher()
        .include(expectedPlan)
        .match();

      testBuilder()
        .optionSettingQueriesForBaseline(SLICE_TARGET_DEFAULT)
        .unOrdered()
        .sqlQuery(query)
        .sqlBaselineQuery(query)
        .build()
        .run();
    } finally {
      client.resetSession(ExecConstants.SLICE_TARGET);
    }
  }

  @Test
  public void testSetOpOnHashJoin() throws Exception {
    final String l = "/multilevel/parquet/1994";
    final String r = "/multilevel/parquet/1995";

    final String query = String.format("SELECT o_custkey FROM \n" +
      " (select o1.o_custkey from dfs.`%s` o1 inner join dfs.`%s` o2 on o1.o_orderkey = o2.o_custkey) \n" +
      " Intersect All SELECT o_custkey FROM dfs.`%s` where o_custkey > 10", l, r, l);

    // Validate the plan
    final String[] expectedPlan = {"(?s)UnionExchange.*SetOp.*HashJoin.*"};

    try {
      client.alterSession(ExecConstants.SLICE_TARGET, 1);
      queryBuilder()
        .sql(query)
        .planMatcher()
        .include(expectedPlan)
        .match();

      testBuilder()
        .optionSettingQueriesForBaseline(SLICE_TARGET_DEFAULT)
        .unOrdered()
        .sqlQuery(query)
        .sqlBaselineQuery(query)
        .build()
        .run();
    } finally {
      client.resetSession(ExecConstants.SLICE_TARGET);
    }
  }

  @Test
  @Category(UnlikelyTest.class)
  public void testLimitOneOnRightSide() throws Exception {
    final String l = "/multilevel/parquet/1994";
    final String r = "/multilevel/parquet/1995";

    final String query = String.format("SELECT o_custkey FROM \n" +
      " ((select o1.o_custkey from dfs.`%s` o1 inner join dfs.`%s` o2 on o1.o_orderkey = o2.o_custkey) \n" +
      " Intersect All (SELECT o_custkey FROM dfs.`%s` limit 1))", l, r, l);

    // Validate the plan
    final String[] expectedPlan = {"(?s)UnionExchange.*SetOp.*HashJoin.*"};

    try {
      client.alterSession(ExecConstants.SLICE_TARGET, 1);
      client.alterSession(PlannerSettings.UNIONALL_DISTRIBUTE_KEY, true);
      queryBuilder()
        .sql(query)
        .planMatcher()
        .include(expectedPlan)
        .match();

      testBuilder()
        .optionSettingQueriesForBaseline(SLICE_TARGET_DEFAULT)
        .unOrdered()
        .sqlQuery(query)
        .sqlBaselineQuery(query)
        .build()
        .run();
    } finally {
      client.resetSession(ExecConstants.SLICE_TARGET);
      client.resetSession(PlannerSettings.UNIONALL_DISTRIBUTE_KEY);
    }
  }

  @Test
  @Category(UnlikelyTest.class)
  public void testLimitOneOnLeftSide() throws Exception {
    final String l = "/multilevel/parquet/1994";
    final String r = "/multilevel/parquet/1995";

    final String query = String.format("SELECT o_custkey FROM \n" +
      " ((SELECT o_custkey FROM dfs.`%s` limit 1) \n" +
      " intersect all \n" +
      " (select o1.o_custkey from dfs.`%s` o1 inner join dfs.`%s` o2 on o1.o_orderkey = o2.o_custkey))", l, r, l);

    // Validate the plan
    final String[] expectedPlan = {"(?s)SetOp.*BroadcastExchange.*HashJoin.*"};

    try {
      client.alterSession(ExecConstants.SLICE_TARGET, 1);
      client.alterSession(PlannerSettings.UNIONALL_DISTRIBUTE_KEY, true);
      queryBuilder()
        .sql(query)
        .planMatcher()
        .include(expectedPlan)
        .match();

      testBuilder()
        .optionSettingQueriesForBaseline(SLICE_TARGET_DEFAULT)
        .unOrdered()
        .sqlQuery(query)
        .sqlBaselineQuery(query)
        .build()
        .run();
    } finally {
      client.resetSession(ExecConstants.SLICE_TARGET);
      client.resetSession(PlannerSettings.UNIONALL_DISTRIBUTE_KEY);
    }
  }

  @Test
  public void testIntersectAllWithValues() throws Exception {
    testBuilder()
      .sqlQuery("values('A') intersect all values('A')")
      .unOrdered()
      .baselineColumns("EXPR$0")
      .baselineValues("A")
      .go();
  }

  @Test
  @Category(UnlikelyTest.class)
  public void testFieldWithDots() throws Exception {
    String fileName = "table.json";
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(new File(dirTestWatcher.getRootDir(), fileName)))) {
      writer.write("{\"rk.q\": \"a\", \"m\": {\"a.b\":\"1\", \"a\":{\"b\":\"2\"}, \"c\":\"3\"}}");
    }

    testBuilder()
      .sqlQuery("select * from (" +
        "(select t.m.`a.b` as a,\n" +
        "t.m.a.b as b,\n" +
        "t.m['a.b'] as c,\n" +
        "t.`rk.q` as e\n" +
        "from dfs.`%1$s` t)\n" +
        "intersect all\n" +
        "(select t.m.`a.b` as a,\n" +
        "t.m.a.b as b,\n" +
        "t.m['a.b'] as c,\n" +
        "t.`rk.q` as e\n" +
        "from dfs.`%1$s` t))", fileName)
      .unOrdered()
      .baselineColumns("a", "b", "c", "e")
      .baselineValues("1", "2", "1", "a")
      .go();
  }

  @Test
  public void testExceptAllRightEmptyDir() throws Exception {
    String rootSimple = "/store/json/booleanData.json";

    testBuilder()
      .sqlQuery("SELECT key FROM cp.`%s` EXCEPT ALL SELECT key FROM dfs.tmp_default_format.`%s`",
        rootSimple, EMPTY_DIR_NAME)
      .unOrdered()
      .baselineColumns("key")
      .baselineValues(true)
      .baselineValues(false)
      .build()
      .run();
  }

  @Test
  public void testExceptAllLeftEmptyDir() throws Exception {
    final String rootSimple = "/store/json/booleanData.json";

    testBuilder()
      .sqlQuery("SELECT key FROM dfs.tmp_default_format.`%s` EXCEPT ALL SELECT key FROM cp.`%s`",
        EMPTY_DIR_NAME, rootSimple)
      .unOrdered()
      .baselineColumns("key")
      .expectsEmptyResultSet()
      .build()
      .run();
  }

  @Test
  public void testIntersectBothEmptyDirs() throws Exception {
    SchemaBuilder schemaBuilder = new SchemaBuilder()
      .addNullable("key", TypeProtos.MinorType.INT);
    BatchSchema expectedSchema = new BatchSchemaBuilder()
      .withSchemaBuilder(schemaBuilder)
      .build();

    testBuilder()
      .sqlQuery("SELECT key FROM dfs.tmp_default_format.`%1$s` INTERSECT ALL SELECT key FROM dfs.tmp_default_format.`%1$s`", EMPTY_DIR_NAME)
      .schemaBaseLine(expectedSchema)
      .build()
      .run();
  }

  @Test
  public void testSetOpMiddleEmptyDir() throws Exception {
    final String query = "(SELECT n_regionkey FROM cp.`tpch/nation.parquet` EXCEPT ALL " +
      "SELECT missing_key FROM dfs.tmp_default_format.`%s`) intersect all SELECT r_regionkey FROM cp.`tpch/region.parquet`";

    testBuilder()
      .sqlQuery(query, EMPTY_DIR_NAME)
      .unOrdered()
      .baselineColumns("n_regionkey")
      .baselineValues(0)
      .baselineValues(1)
      .baselineValues(2)
      .baselineValues(3)
      .baselineValues(4)
      .build()
      .run();
  }

  @Test
  public void testComplexQueryWithSetOpAndEmptyDir() throws Exception {
    final String rootSimple = "/store/json/booleanData.json";

    testBuilder()
      .sqlQuery("SELECT key FROM cp.`%2$s` INTERSECT ALL SELECT key FROM " +
          "(SELECT key FROM cp.`%2$s` EXCEPT ALL SELECT key FROM dfs.tmp_default_format.`%1$s`)",
        EMPTY_DIR_NAME, rootSimple)
      .unOrdered()
      .baselineColumns("key")
      .baselineValues(true)
      .baselineValues(false)
      .build()
      .run();
  }

  @Test
  public void testIntersectCancellation() throws Exception {
    String query = "WITH foo AS\n" +
      "  (SELECT 1 AS a FROM cp.`/tpch/nation.parquet`\n" +
      "   Intersect ALL\n" +
      "   SELECT 1 AS a FROM cp.`/tpch/nation.parquet`\n" +
      "   WHERE n_nationkey > (SELECT 1) )\n" +
      "SELECT * FROM foo\n" +
      "LIMIT 1";

    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("a")
      .baselineValues(1)
      .build()
      .run();
  }

  @Test
  public void testMultiBatch() throws Exception {
    String query = "(select * from (values(1,1)) t(a,b) union all select * from (values(3,3)) t(a,b) union all select * from (values(5,5)) t(a,b)) intersect all " +
      "(select * from (values(1,1)) t(a,b) union all select * from (values(3,3), (2,2)) t(a,b) union all select * from (values(6,6), (4,4), (5,5)) t(a,b)) ";
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("a", "b")
      .baselineValues(5, 5)
      .baselineValues(3, 3)
      .baselineValues(1, 1)
      .build().run();
  }

  @Test
  public void testFirstEmptyBatch() throws Exception {
    String query = "(select n_nationkey from cp.`tpch/nation.parquet` where n_nationkey < 0 union all select n_nationkey from cp.`tpch/nation.parquet` where n_nationkey < 5) intersect all " +
      "(select n_nationkey from cp.`tpch/nation.parquet` where n_nationkey < 0 union all select n_nationkey from cp.`tpch/nation.parquet` where n_nationkey < 3)";
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("n_nationkey")
      .baselineValues(0)
      .baselineValues(2)
      .baselineValues(1)
      .build().run();
  }

  @Test
  public void testUnsupportedComplexType() {
    try {
      String query = "select sia from cp.`complex/json/complex.json` intersect all select sia from cp.`complex/json/complex.json`";
      testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("sia")
        .baselineValues("[1,11,101,1001]")
        .baselineValues("[2,12,102,1002]")
        .baselineValues("[3,13,103,1003]")
        .build().run();
      Assert.fail("Missing expected exception on complex type");
    } catch (Exception ex) {
      Assert.assertThat(ex.getMessage(), ex.getMessage(),
        CoreMatchers.containsString("Map, Array, Union or repeated scalar type should not be used in group by, order by or in a comparison operator"));
    }
  }
}
