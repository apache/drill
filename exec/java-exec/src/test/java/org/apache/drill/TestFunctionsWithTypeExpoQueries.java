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
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.util.FileUtils;
import org.junit.Test;

import java.util.List;

public class TestFunctionsWithTypeExpoQueries extends BaseTestQuery {
  @Test
  public void testConcatWithMoreThanTwoArgs() throws Exception {
    final String query = "select concat(r_name, r_name, r_name) as col \n" +
        "from cp.`tpch/region.parquet` limit 0";

    List<Pair<SchemaPath, TypeProtos.MajorType>> expectedSchema = Lists.newArrayList();
    TypeProtos.MajorType majorType = TypeProtos.MajorType.newBuilder()
            .setMinorType(TypeProtos.MinorType.VARCHAR)
            .setMode(TypeProtos.DataMode.REQUIRED)
            .build();
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("col"), majorType));

    testBuilder()
        .sqlQuery(query)
        .schemaBaseLine(expectedSchema)
        .build()
        .run();
  }

  @Test
  public void testRow_NumberInView() throws Exception {
    try {
      test("use dfs_test.tmp;");
      final String view1 =
          "create view TestFunctionsWithTypeExpoQueries_testViewShield1 as \n" +
              "select rnum, position_id, " +
              "   ntile(4) over(order by position_id) " +
              " from (select position_id, row_number() " +
              "       over(order by position_id) as rnum " +
              "       from cp.`employee.json`)";


      final String view2 =
          "create view TestFunctionsWithTypeExpoQueries_testViewShield2 as \n" +
              "select row_number() over(order by position_id) as rnum, " +
              "    position_id, " +
              "    ntile(4) over(order by position_id) " +
              " from cp.`employee.json`";

      test(view1);
      test(view2);

      testBuilder()
          .sqlQuery("select * from TestFunctionsWithTypeExpoQueries_testViewShield1")
          .ordered()
          .sqlBaselineQuery("select * from TestFunctionsWithTypeExpoQueries_testViewShield2")
          .build()
          .run();
    } finally {
      test("drop view TestFunctionsWithTypeExpoQueries_testViewShield1;");
      test("drop view TestFunctionsWithTypeExpoQueries_testViewShield2;");
    }
  }

  @Test
  public void testLRBTrimOneArg() throws Exception {
    final String query1 = "SELECT ltrim('drill') as col FROM cp.`tpch/region.parquet` limit 0";
    final String query2 = "SELECT rtrim('drill') as col FROM cp.`tpch/region.parquet` limit 0";
    final String query3 = "SELECT btrim('drill') as col FROM cp.`tpch/region.parquet` limit 0";

    List<Pair<SchemaPath, TypeProtos.MajorType>> expectedSchema = Lists.newArrayList();
    TypeProtos.MajorType majorType = TypeProtos.MajorType.newBuilder()
        .setMinorType(TypeProtos.MinorType.VARCHAR)
        .setMode(TypeProtos.DataMode.REQUIRED)
        .build();
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("col"), majorType));

    testBuilder()
        .sqlQuery(query1)
        .schemaBaseLine(expectedSchema)
        .build()
        .run();

    testBuilder()
        .sqlQuery(query2)
        .schemaBaseLine(expectedSchema)
        .build()
        .run();

    testBuilder()
        .sqlQuery(query3)
        .schemaBaseLine(expectedSchema)
        .build()
        .run();
  }

  @Test
  public void testTrimOneArg() throws Exception {
    final String query1 = "SELECT trim(leading 'drill') as col FROM cp.`tpch/region.parquet` limit 0";
    final String query2 = "SELECT trim(trailing 'drill') as col FROM cp.`tpch/region.parquet` limit 0";
    final String query3 = "SELECT trim(both 'drill') as col FROM cp.`tpch/region.parquet` limit 0";

    List<Pair<SchemaPath, TypeProtos.MajorType>> expectedSchema = Lists.newArrayList();
    TypeProtos.MajorType majorType = TypeProtos.MajorType.newBuilder()
        .setMinorType(TypeProtos.MinorType.VARCHAR)
        .setMode(TypeProtos.DataMode.REQUIRED)
        .build();
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("col"), majorType));

    testBuilder()
        .sqlQuery(query1)
        .schemaBaseLine(expectedSchema)
        .build()
        .run();

    testBuilder()
        .sqlQuery(query2)
        .schemaBaseLine(expectedSchema)
        .build()
        .run();

    testBuilder()
        .sqlQuery(query3)
        .schemaBaseLine(expectedSchema)
        .build()
        .run();
  }

  @Test
  public void testTrimTwoArg() throws Exception {
    final String query1 = "SELECT trim(leading ' ' from 'drill') as col FROM cp.`tpch/region.parquet` limit 0";
    final String query2 = "SELECT trim(trailing ' ' from 'drill') as col FROM cp.`tpch/region.parquet` limit 0";
    final String query3 = "SELECT trim(both ' ' from 'drill') as col FROM cp.`tpch/region.parquet` limit 0";

    List<Pair<SchemaPath, TypeProtos.MajorType>> expectedSchema = Lists.newArrayList();
    TypeProtos.MajorType majorType = TypeProtos.MajorType.newBuilder()
        .setMinorType(TypeProtos.MinorType.VARCHAR)
        .setMode(TypeProtos.DataMode.REQUIRED)
        .build();
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("col"), majorType));

    testBuilder()
        .sqlQuery(query1)
        .schemaBaseLine(expectedSchema)
        .build()
        .run();

    testBuilder()
        .sqlQuery(query2)
        .schemaBaseLine(expectedSchema)
        .build()
        .run();

    testBuilder()
        .sqlQuery(query3)
        .schemaBaseLine(expectedSchema)
        .build()
        .run();
  }

  @Test
  public void tesIsNull() throws Exception {
    final String query = "select r_name is null as col from cp.`tpch/region.parquet` limit 0";
    List<Pair<SchemaPath, TypeProtos.MajorType>> expectedSchema = Lists.newArrayList();
    TypeProtos.MajorType majorType = TypeProtos.MajorType.newBuilder()
        .setMinorType(TypeProtos.MinorType.BIT)
        .setMode(TypeProtos.DataMode.REQUIRED)
        .build();
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("col"), majorType));

    testBuilder()
        .sqlQuery(query)
        .schemaBaseLine(expectedSchema)
        .build()
        .run();
  }

  /**
   * In the following query, the extract function would be borrowed from Calcite,
   * which asserts the return type as be BIG-INT
   */
  @Test
  public void testExtractSecond() throws Exception {
    String query = "select extract(second from time '02:30:45.100') as col \n" +
        "from cp.`tpch/region.parquet` \n" +
        "limit 0";

    List<Pair<SchemaPath, TypeProtos.MajorType>> expectedSchema = Lists.newArrayList();
    TypeProtos.MajorType majorType = TypeProtos.MajorType.newBuilder()
        .setMinorType(TypeProtos.MinorType.FLOAT8)
        .setMode(TypeProtos.DataMode.REQUIRED)
        .build();
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("col"), majorType));

    testBuilder()
        .sqlQuery(query)
        .schemaBaseLine(expectedSchema)
        .build()
        .run();
  }

  @Test
  public void testMetaDataExposeType() throws Exception {
    final String root = FileUtils.getResourceAsFile("/typeExposure/metadata_caching").toURI().toString();
    final String query = String.format("select count(*) as col \n" +
        "from dfs_test.`%s` \n" +
        "where concat(a, 'asdf') = 'asdf'", root);

    // Validate the plan
    final String[] expectedPlan = {"Scan.*a.parquet.*numFiles=1"};
    final String[] excludedPlan = {"Filter"};
    PlanTestBase.testPlanMatchingPatterns(query, expectedPlan, excludedPlan);

    // Validate the result
    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("col")
        .baselineValues(1l)
        .build()
        .run();
  }

  @Test
  public void testDate_Part() throws Exception {
    final String query = "select date_part('year', date '2008-2-23') as col \n" +
        "from cp.`tpch/region.parquet` \n" +
        "limit 0";

    List<Pair<SchemaPath, TypeProtos.MajorType>> expectedSchema = Lists.newArrayList();
    TypeProtos.MajorType majorType = TypeProtos.MajorType.newBuilder()
        .setMinorType(TypeProtos.MinorType.BIGINT)
        .setMode(TypeProtos.DataMode.REQUIRED)
        .build();
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("col"), majorType));

    testBuilder()
        .sqlQuery(query)
        .schemaBaseLine(expectedSchema)
        .build()
        .run();
  }

  @Test
  public void testNegativeByInterpreter() throws Exception {
    final String query = "select * from cp.`tpch/region.parquet` \n" +
        "where r_regionkey = negative(-1)";

    // Validate the plan
    final String[] expectedPlan = {"Filter.*condition=\\[=\\(.*, 1\\)\\]\\)"};
    final String[] excludedPlan = {};
    PlanTestBase.testPlanMatchingPatterns(query, expectedPlan, excludedPlan);
  }

  @Test
  public void testSumRequiredType() throws Exception {
    final String query = "SELECT \n" +
        "SUM(CASE WHEN (CAST(n_regionkey AS INT) = 1) THEN 1 ELSE 0 END) AS col \n" +
        "FROM cp.`tpch/nation.parquet` \n" +
        "GROUP BY CAST(n_regionkey AS INT) \n" +
        "limit 0";

    List<Pair<SchemaPath, TypeProtos.MajorType>> expectedSchema = Lists.newArrayList();
    TypeProtos.MajorType majorType = TypeProtos.MajorType.newBuilder()
        .setMinorType(TypeProtos.MinorType.BIGINT)
        .setMode(TypeProtos.DataMode.REQUIRED)
        .build();
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("col"), majorType));

    testBuilder()
        .sqlQuery(query)
        .schemaBaseLine(expectedSchema)
        .build()
        .run();
  }

  @Test
  public void testSQRT() throws Exception {
    final String query = "SELECT sqrt(5.1) as col \n" +
        "from cp.`tpch/nation.parquet` \n" +
        "limit 0";

    List<Pair<SchemaPath, TypeProtos.MajorType>> expectedSchema = Lists.newArrayList();
    TypeProtos.MajorType majorType = TypeProtos.MajorType.newBuilder()
        .setMinorType(TypeProtos.MinorType.FLOAT8)
        .setMode(TypeProtos.DataMode.REQUIRED)
        .build();
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("col"), majorType));

    testBuilder()
        .sqlQuery(query)
        .schemaBaseLine(expectedSchema)
        .build()
        .run();
  }
}
