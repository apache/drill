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

package org.apache.drill.exec.physical.impl.agg;

import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.apache.drill.exec.physical.config.StreamingAggregate;
import org.apache.drill.test.PhysicalOpUnitTestBase;
import org.apache.drill.exec.util.JsonStringArrayList;
import org.apache.drill.test.BaseTestQuery;
import org.apache.drill.categories.OperatorTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.drill.test.TestBuilder;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import java.math.BigDecimal;
import java.util.List;

@Category(OperatorTest.class)
@RunWith(Enclosed.class)
public class TestAggWithAnyValue {

  public static class TestAggWithAnyValueMultipleBatches extends PhysicalOpUnitTestBase {

    @Test
    public void testStreamAggWithGroupBy() {
      StreamingAggregate aggConf = new StreamingAggregate(null, parseExprs("age.`max`", "age"), parseExprs("any_value(a)", "any_a"), 2.0f);
      List<String> inputJsonBatches = Lists.newArrayList(
          "[{ \"age\": {\"min\":20, \"max\":60}, \"city\": \"San Bruno\", \"de\": \"987654321987654321987654321.10987654321\"," +
              " \"a\": [{\"b\":50, \"c\":30},{\"b\":70, \"c\":40}], \"m\": [{\"n\": [10, 11, 12]}], \"f\": [{\"g\": {\"h\": [{\"k\": 70}, {\"k\": 80}]}}]," +
              "\"p\": {\"q\": [21, 22, 23]}" + "}, " +
              "{ \"age\": {\"min\":20, \"max\":60}, \"city\": \"Castro Valley\", \"de\": \"987654321987654321987654321.12987654321\"," +
              " \"a\": [{\"b\":60, \"c\":40},{\"b\":80, \"c\":50}], \"m\": [{\"n\": [13, 14, 15]}], \"f\": [{\"g\": {\"h\": [{\"k\": 90}, {\"k\": 100}]}}]," +
              "\"p\": {\"q\": [24, 25, 26]}" + "}]",
          "[{ \"age\": {\"min\":43, \"max\":80}, \"city\": \"Palo Alto\", \"de\": \"987654321987654321987654321.00987654321\"," +
              " \"a\": [{\"b\":10, \"c\":15}, {\"b\":20, \"c\":45}], \"m\": [{\"n\": [1, 2, 3]}], \"f\": [{\"g\": {\"h\": [{\"k\": 10}, {\"k\": 20}]}}]," +
              "\"p\": {\"q\": [27, 28, 29]}" + "}, " +
              "{ \"age\": {\"min\":43, \"max\":80}, \"city\": \"San Carlos\", \"de\": \"987654321987654321987654321.11987654321\"," +
              " \"a\": [{\"b\":30, \"c\":25}, {\"b\":40, \"c\":55}], \"m\": [{\"n\": [4, 5, 6]}], \"f\": [{\"g\": {\"h\": [{\"k\": 30}, {\"k\": 40}]}}]," +
              "\"p\": {\"q\": [30, 31, 32]}" + "}, " +
              "{ \"age\": {\"min\":43, \"max\":80}, \"city\": \"Palo Alto\", \"de\": \"987654321987654321987654321.13987654321\"," +
              " \"a\": [{\"b\":70, \"c\":85}, {\"b\":90, \"c\":145}], \"m\": [{\"n\": [7, 8, 9]}], \"f\": [{\"g\": {\"h\": [{\"k\": 50}, {\"k\": 60}]}}]," +
              "\"p\": {\"q\": [33, 34, 35]}" + "}]");
      legacyOpTestBuilder()
          .physicalOperator(aggConf)
          .inputDataStreamJson(inputJsonBatches)
          .baselineColumns("age", "any_a")
          .baselineValues(60l, TestBuilder.listOf(TestBuilder.mapOf("b", 50l, "c", 30l), TestBuilder.mapOf("b", 70l, "c", 40l)))
          .baselineValues(80l, TestBuilder.listOf(TestBuilder.mapOf("b", 10l, "c", 15l), TestBuilder.mapOf("b", 20l, "c", 45l)))
          .go();
    }
  }

  public static class TestAggWithAnyValueSingleBatch extends BaseTestQuery {

    @Test
    public void testWithGroupBy() throws Exception {
      String query = "select t1.age.`max` as age, count(*) as cnt, any_value(t1.a) as any_a, any_value(t1.city) as any_city, " +
          "any_value(f) as any_f, any_value(m) as any_m, any_value(p) as any_p from  cp.`store/json/test_anyvalue.json` t1 group by t1.age.`max`";
      testBuilder()
          .sqlQuery(query)
          .unOrdered()
          .baselineColumns("age", "cnt", "any_a", "any_city", "any_f", "any_m", "any_p")
          .baselineValues(60l, 2l, TestBuilder.listOf(TestBuilder.mapOf("b", 50l, "c", 30l), TestBuilder.mapOf("b", 70l, "c", 40l)), "San Bruno",
              TestBuilder.listOf(TestBuilder.mapOf("g", TestBuilder.mapOf("h", TestBuilder.listOf(TestBuilder.mapOf("k", 70l), TestBuilder.mapOf("k", 80l))))),
              TestBuilder.listOf(TestBuilder.mapOf("n", TestBuilder.listOf(10l, 11l, 12l))),
              TestBuilder.mapOf("q", TestBuilder.listOf(21l, 22l, 23l)))
          .baselineValues(80l, 3l, TestBuilder.listOf(TestBuilder.mapOf("b", 10l, "c", 15l), TestBuilder.mapOf("b", 20l, "c", 45l)), "Palo Alto",
              TestBuilder.listOf(TestBuilder.mapOf("g", TestBuilder.mapOf("h", TestBuilder.listOf(TestBuilder.mapOf("k", 10l), TestBuilder.mapOf("k", 20l))))),
              TestBuilder.listOf(TestBuilder.mapOf("n", TestBuilder.listOf(1l, 2l, 3l))),
              TestBuilder.mapOf("q", TestBuilder.listOf(27l, 28l, 29l)))
          .go();
    }

    @Test
    public void testWithoutGroupBy() throws Exception {
      String query = "select count(*) as cnt, any_value(t1.a) as any_a, any_value(t1.city) as any_city, " +
          "any_value(f) as any_f, any_value(m) as any_m, any_value(p) as any_p from  cp.`store/json/test_anyvalue.json` t1";
      testBuilder()
          .sqlQuery(query)
          .unOrdered()
          .baselineColumns("cnt", "any_a", "any_city", "any_f", "any_m", "any_p")
          .baselineValues(5l, TestBuilder.listOf(TestBuilder.mapOf("b", 10l, "c", 15l), TestBuilder.mapOf("b", 20l, "c", 45l)), "Palo Alto",
              TestBuilder.listOf(TestBuilder.mapOf("g", TestBuilder.mapOf("h", TestBuilder.listOf(TestBuilder.mapOf("k", 10l), TestBuilder.mapOf("k", 20l))))),
              TestBuilder.listOf(TestBuilder.mapOf("n", TestBuilder.listOf(1l, 2l, 3l))),
              TestBuilder.mapOf("q", TestBuilder.listOf(27l, 28l, 29l)))
          .go();
    }

    @Test
    public void testDecimalWithGroupBy() throws Exception {
      String query = "select t1.age.`max` as age, any_value(cast(t1.de as decimal(38, 11))) as any_decimal " +
          "from  cp.`store/json/test_anyvalue.json` t1 group by t1.age.`max`";
      testBuilder()
          .sqlQuery(query)
          .unOrdered()
          .baselineColumns("age", "any_decimal")
          .baselineValues(60l, new BigDecimal("987654321987654321987654321.10987654321"))
          .baselineValues(80l, new BigDecimal("987654321987654321987654321.00987654321"))
          .go();
    }

    @Test
    public void testRepeatedDecimalWithGroupBy() throws Exception {
      JsonStringArrayList<BigDecimal> ints = new JsonStringArrayList<>();
      ints.add(new BigDecimal("999999.999"));
      ints.add(new BigDecimal("-999999.999"));
      ints.add(new BigDecimal("0.000"));

      JsonStringArrayList<BigDecimal> longs = new JsonStringArrayList<>();
      longs.add(new BigDecimal("999999999.999999999"));
      longs.add(new BigDecimal("-999999999.999999999"));
      longs.add(new BigDecimal("0.000000000"));

      JsonStringArrayList<BigDecimal> fixedLen = new JsonStringArrayList<>();
      fixedLen.add(new BigDecimal("999999999999.999999"));
      fixedLen.add(new BigDecimal("-999999999999.999999"));
      fixedLen.add(new BigDecimal("0.000000"));

      String query = "select any_value(decimal_int32) as any_dec_32, any_value(decimal_int64) as any_dec_64," +
          " any_value(decimal_fixedLen) as any_dec_fixed, any_value(decimal_binary) as any_dec_bin" +
          " from cp.`parquet/repeatedIntLondFixedLenBinaryDecimal.parquet`";
      testBuilder()
          .sqlQuery(query)
          .unOrdered()
          .baselineColumns("any_dec_32", "any_dec_64", "any_dec_fixed", "any_dec_bin")
          .baselineValues(ints, longs, fixedLen, fixedLen)
          .go();
    }
  }
}
