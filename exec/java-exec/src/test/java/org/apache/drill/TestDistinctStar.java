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

import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.util.FileUtils;
import org.junit.Test;

public class TestDistinctStar extends BaseTestQuery {
  @Test // DRILL-2139
  public void testSelectDistinctByStreamAgg() throws Exception {
    test("alter session set `planner.enable_hashagg` = false;");
    test("alter session set `planner.enable_streamagg` = true;");

    String root = FileUtils.getResourceAsFile("/store/text/data/repeatedRows.json").toURI().toString();
    String query = String.format("select distinct *, a1, *, b1, *, c1, * \n" +
            "from dfs.`%s`", root);

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .csvBaselineFile("testframework/testDistinctStar/testSelectDistinct.tsv")
        .baselineTypes(TypeProtos.MinorType.BIGINT, TypeProtos.MinorType.BIGINT, TypeProtos.MinorType.VARCHAR,
            TypeProtos.MinorType.BIGINT, TypeProtos.MinorType.BIGINT, TypeProtos.MinorType.BIGINT,
            TypeProtos.MinorType.VARCHAR, TypeProtos.MinorType.BIGINT, TypeProtos.MinorType.BIGINT,
            TypeProtos.MinorType.BIGINT, TypeProtos.MinorType.VARCHAR, TypeProtos.MinorType.VARCHAR,
            TypeProtos.MinorType.BIGINT, TypeProtos.MinorType.BIGINT, TypeProtos.MinorType.VARCHAR)
        .baselineColumns("a1", "b1", "c1", "a10",
            "a11", "b10", "c10", "b11",
            "a12", "b12", "c11", "c12",
            "a13", "b13", "c13")
        .build()
        .run();

    test("alter session set `planner.enable_hashagg` = true;");
  }

  @Test // see DRILL-2139
  public void testSelectDistinctByHashAgg() throws Exception {
    String root = FileUtils.getResourceAsFile("/store/text/data/repeatedRows.json").toURI().toString();
    test("alter session set `planner.enable_hashagg` = true;");
    test("alter session set `planner.enable_streamagg` = false;");

    String query = String.format("select distinct *, a1, *, b1, *, c1, * \n" +
        "from dfs.`%s`", root);

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .csvBaselineFile("testframework/testDistinctStar/testSelectDistinct.tsv")
        .baselineTypes(TypeProtos.MinorType.BIGINT, TypeProtos.MinorType.BIGINT, TypeProtos.MinorType.VARCHAR,
            TypeProtos.MinorType.BIGINT, TypeProtos.MinorType.BIGINT, TypeProtos.MinorType.BIGINT,
            TypeProtos.MinorType.VARCHAR, TypeProtos.MinorType.BIGINT, TypeProtos.MinorType.BIGINT,
            TypeProtos.MinorType.BIGINT, TypeProtos.MinorType.VARCHAR, TypeProtos.MinorType.VARCHAR,
            TypeProtos.MinorType.BIGINT, TypeProtos.MinorType.BIGINT, TypeProtos.MinorType.VARCHAR)
        .baselineColumns("a1", "b1", "c1", "a10",
            "a11", "b10", "c10", "b11",
            "a12", "b12", "c11", "c12",
            "a13", "b13", "c13")
        .build()
        .run();

    test("alter session set `planner.enable_streamagg` = true;");
  }

  @Test // DRILL-2139
  public void testSelectDistinctOverJoin() throws Exception {
    String root = FileUtils.getResourceAsFile("/store/text/data/repeatedRows.json").toURI().toString();
    String query = String.format("select distinct * \n" +
        "from dfs.`%s` t1, dfs.`%s` t2 " +
        "where (t1.a1 = t2.a1)", root, root);

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .csvBaselineFile("testframework/testDistinctStar/testSelectDistinctOverJoin.tsv")
        .baselineTypes(TypeProtos.MinorType.BIGINT, TypeProtos.MinorType.BIGINT, TypeProtos.MinorType.VARCHAR,
            TypeProtos.MinorType.BIGINT, TypeProtos.MinorType.BIGINT, TypeProtos.MinorType.VARCHAR)
        .baselineColumns("a1", "b1", "c1", "a10", "b10", "c10")
        .build()
        .run();
  }

  @Test // DRILL-2139
  public void testSelectDistinctExpression() throws Exception {
    String root = FileUtils.getResourceAsFile("/store/text/data/repeatedRows.json").toURI().toString();
    String query = String.format("select distinct *, a1 + 3 as expr \n" +
              "from dfs.`%s`", root);
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .csvBaselineFile("testframework/testDistinctStar/testSelectDistinctExpression.tsv")
        .baselineTypes(TypeProtos.MinorType.BIGINT, TypeProtos.MinorType.BIGINT,
            TypeProtos.MinorType.VARCHAR, TypeProtos.MinorType.BIGINT)
        .baselineColumns("a1", "b1", "c1", "expr")
        .build()
        .run();
  }

  @Test // DRILL-2139
  public void testSelectDistinctViewHashAgg() throws Exception {
    String root = FileUtils.getResourceAsFile("/store/text/data/repeatedRows.json").toURI().toString();
    String createView = String.format("create view nation_view_testSelectDistinctView as \n" +
        "select a1, b1 from dfs_test.`%s`", root);
    String query = "select distinct * from nation_view_testSelectDistinctView";
    try {
      test("use dfs_test.tmp");
      test(createView);
      test("alter session set `planner.enable_streamagg` = false");
      testBuilder()
          .sqlQuery(query)
          .unOrdered()
          .baselineColumns("a1", "b1")
          .baselineValues(0l, 1l)
          .baselineValues(0l, 2l)
          .baselineValues(1l, 1l)
          .baselineValues(1l, 2l)
          .build()
          .run();
    } finally {
      test("drop view nation_view_testSelectDistinctView");
      test("alter session set `planner.enable_streamagg` = true");
    }
  }

  @Test // DRILL-2139
  public void testSelectDistinctViewStreamAgg() throws Exception {
    String root = FileUtils.getResourceAsFile("/store/text/data/repeatedRows.json").toURI().toString();
    String createView = String.format("create view nation_view_testSelectDistinctView as \n" +
        "select a1, b1 from dfs_test.`%s`", root);
    String query = "select distinct * from nation_view_testSelectDistinctView";
    try {
      test("use dfs_test.tmp");
      test(createView);
      test("alter session set `planner.enable_hashagg` = false");
      testBuilder()
          .sqlQuery(query)
          .unOrdered()
          .baselineColumns("a1", "b1")
          .baselineValues(0l, 1l)
          .baselineValues(0l, 2l)
          .baselineValues(1l, 1l)
          .baselineValues(1l, 2l)
          .build()
          .run();
    } finally {
      test("drop view nation_view_testSelectDistinctView");
      test("alter session set `planner.enable_hashagg` = true");
    }
  }
}
