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

import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.test.QueryBuilder;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import static java.lang.Float.NEGATIVE_INFINITY;
import static java.lang.Float.NaN;

public class TestMathFunctionsVarcharInput extends ClusterTest {

  @BeforeClass
  public static void setup() throws Exception {
    ClusterFixtureBuilder builder = ClusterFixture.builder(dirTestWatcher);
    startCluster(builder);
  }

  @Ignore
  // The following test fails because when the output is a random number, the test cannot know what to expect.
  @Test
  public void testRandWithSeedVarcharInput() throws Exception {
    String sql = "select rand('567!') as rand1, rand(123) as rand2, rand('23') as rand3, rand('43.0') as rand4 from (values (1))";
    QueryBuilder q = client.queryBuilder().sql(sql);
    RowSet results = q.rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
      .add("rand1", TypeProtos.MinorType.FLOAT8)
      .add("rand2", TypeProtos.MinorType.FLOAT8)
      .add("rand3", TypeProtos.MinorType.FLOAT8)
      .add("rand4", TypeProtos.MinorType.FLOAT8)
      .build();

    RowSet expected = client.rowSetBuilder(expectedSchema).addRow(NaN, .7924, .356, NaN).build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Ignore
  @Test
  public void testPowerVarcharInput() throws Exception {
    String sql = "select power(2, 3) as pow1, power(2.0, '3*') as pow2, power('', '3.0') as pow3 from (values (1))";
    QueryBuilder q = client.queryBuilder().sql(sql);
    RowSet results = q.rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
      .add("pow1", TypeProtos.MinorType.FLOAT8)
      .add("pow2", TypeProtos.MinorType.FLOAT8)
      .add("pow3", TypeProtos.MinorType.FLOAT8)
      .build();

    RowSet expected = client.rowSetBuilder(expectedSchema).addRow(8.0, NaN, NaN).build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testModVarcharInput() throws Exception {
    String sql = "SELECT mod('8.0', '3') AS mod1, " +
      "mod('40', '2a*') AS mod2, " +
      "mod('3.5abc', '38.9') AS mod3, " +
      "mod(null, '8.0') AS mod4, " +
      "mod(null, null) AS mod5, " +
      "mod('8.0', null) AS mod6  " +
      "FROM (values (1))";

    RowSet results = client.queryBuilder().sql(sql).rowSet();
    results.print();

    /*TupleMetadata expectedSchema = new SchemaBuilder()
      .addNullable("mod1", TypeProtos.MinorType.FLOAT8)
      .addNullable("mod2", TypeProtos.MinorType.FLOAT8)
      .addNullable("mod3", TypeProtos.MinorType.FLOAT8)
      .build();

    RowSet expected = client.rowSetBuilder(expectedSchema).addRow(2.0, null, null).build();

    new RowSetComparison(expected).verifyAndClearAll(results);*/
  }

  @Ignore
  @Test
  public void testAbsVarcharInput() throws Exception {
    String sql = "select abs('-9.0') as abs1, abs('.5') as abs2, abs(34) as abs3, abs('asdf234') as abs4 from (values (1))";

    QueryBuilder q = client.queryBuilder().sql(sql);

    RowSet results = q.rowSet();

    results.print();

    TupleMetadata expectedSchema = new SchemaBuilder()
      .add("abs1", TypeProtos.MinorType.FLOAT8)
      .add("abs2", TypeProtos.MinorType.FLOAT8)
      .add("abs3", TypeProtos.MinorType.INT)
      .add("abs4", TypeProtos.MinorType.FLOAT8)
      .build();

    RowSet expected = client.rowSetBuilder(expectedSchema).addRow(9.0, .5, 34, NaN).build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Ignore
  @Test
  public void testCbrtVarcharInput() throws Exception {
    String sql = "select cbrt('-27.0') as cbrt1, cbrt('125') as cbrt2, " +
      "cbrt(10) as cbrt3, cbrt('abc123') as cbrt4, cbrt('') as cbrt5, " +
      "cbrt('null') as cbrt6, cbrt('  ') as cbrt7 " +
      "from (values (1))";

    QueryBuilder q = client.queryBuilder().sql(sql);

    RowSet results = q.rowSet();

    //results.print();

    TupleMetadata expectedSchema = new SchemaBuilder()
      .add("cbrt1", TypeProtos.MinorType.FLOAT8)
      .add("cbrt2", TypeProtos.MinorType.FLOAT8)
      .add("cbrt3", TypeProtos.MinorType.FLOAT8)
      .add("cbrt4", TypeProtos.MinorType.FLOAT8)
      .add("cbrt5", TypeProtos.MinorType.FLOAT8)
      .add("cbrt6", TypeProtos.MinorType.FLOAT8)
      .add("cbrt7", TypeProtos.MinorType.FLOAT8)
      .build();

    RowSet expected = client.rowSetBuilder(expectedSchema).addRow(-3.0, 5, 2.154434690031884, NaN, NaN, NaN, NaN).build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testCeilVarcharInput() throws Exception {
    String sql = "select ceil('-27.789') as ceil1, ceil('125.345') as ceil2, ceil('10.938') as ceil3, ceiling('213.456') as ceil4 from (values (1))";

    QueryBuilder q = client.queryBuilder().sql(sql);

    RowSet results = q.rowSet();

    //results.print();

    TupleMetadata expectedSchema = new SchemaBuilder().add("ceil1", TypeProtos.MinorType.FLOAT8)
      .add("ceil2", TypeProtos.MinorType.FLOAT8)
      .add("ceil3", TypeProtos.MinorType.FLOAT8)
      .add("ceil4", TypeProtos.MinorType.FLOAT8).build();

    RowSet expected = client.rowSetBuilder(expectedSchema).addRow(-27, 126, 11, 214).build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testDegreesVarcharInput() throws Exception {
    String sql = "select degrees('45') as deg1, degrees('90.0') as deg2 from (values (1))";

    QueryBuilder q = client.queryBuilder().sql(sql);

    RowSet results = q.rowSet();

    //results.print();

    TupleMetadata expectedSchema = new SchemaBuilder()
      .add("deg1", TypeProtos.MinorType.FLOAT8)
      .add("deg2", TypeProtos.MinorType.FLOAT8)
      .build();

    RowSet expected = client.rowSetBuilder(expectedSchema).addRow(2578.3100780887044, 5156.620156177409).build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testExpVarcharInput() throws Exception {
    String sql = "select exp('5.0') as exp1, exp('12') as exp2 from (values (1))";

    QueryBuilder q = client.queryBuilder().sql(sql);

    RowSet results = q.rowSet();

    //results.print();

    TupleMetadata expectedSchema = new SchemaBuilder()
      .add("exp1", TypeProtos.MinorType.FLOAT8)
      .add("exp2", TypeProtos.MinorType.FLOAT8)
      .build();

    RowSet expected = client.rowSetBuilder(expectedSchema).addRow(148.4131591025766, 162754.79141900392).build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testFloorVarcharInput() throws Exception {
    String sql = "select floor('123.456') as floor1, floor('89.876') as floor2 from (values (1))";

    QueryBuilder q = client.queryBuilder().sql(sql);

    RowSet results = q.rowSet();

    //results.print();

    TupleMetadata expectedSchema = new SchemaBuilder()
      .add("floor1", TypeProtos.MinorType.FLOAT8)
      .add("floor2", TypeProtos.MinorType.FLOAT8)
      .build();

    RowSet expected = client.rowSetBuilder(expectedSchema).addRow(123, 89).build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testLogVarcharInput() throws Exception {
    String sql = "select log('-2.55') as log1, log('0') as log2, log('145.256') as log3, log('3') as log4 from (values (1))";

    QueryBuilder q = client.queryBuilder().sql(sql);

    RowSet results = q.rowSet();

    //results.print();

    TupleMetadata expectedSchema = new SchemaBuilder()
      .add("log1", TypeProtos.MinorType.FLOAT8)
      .add("log2", TypeProtos.MinorType.FLOAT8)
      .add("log3", TypeProtos.MinorType.FLOAT8)
      .add("log4", TypeProtos.MinorType.FLOAT8)
      .build();

    RowSet expected = client.rowSetBuilder(expectedSchema).addRow(NaN, NEGATIVE_INFINITY, 4.978497702968366, 1.09861228867).build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testLogVarcharMultipleInputs() throws Exception {
    String sql = "select log('5.0', '3') as log1, log('4', '9') as log2 from (values (1))";

    QueryBuilder q = client.queryBuilder().sql(sql);

    RowSet results = q.rowSet();

    //results.print();

    TupleMetadata expectedSchema = new SchemaBuilder()
      .add("log1", TypeProtos.MinorType.FLOAT8)
      .add("log2", TypeProtos.MinorType.FLOAT8)
      .build();

    RowSet expected = client.rowSetBuilder(expectedSchema).addRow(0.68260619448, 1.58496250072).build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testLog10VarcharInput() throws Exception {
    String sql = "select log10('60984.1') as log1, log10('1000') as log2 from (values (1))";

    QueryBuilder q = client.queryBuilder().sql(sql);

    RowSet results = q.rowSet();

    //results.print();

    TupleMetadata expectedSchema = new SchemaBuilder()
      .add("log1", TypeProtos.MinorType.FLOAT8)
      .add("log2", TypeProtos.MinorType.FLOAT8)
      .build();

    RowSet expected = client.rowSetBuilder(expectedSchema).addRow(4.78521661890635, 3.0).build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testNegativeVarcharInput() throws Exception {
    String sql = "select negative('2.55') as neg1, negative('876') as neg2, negative('-145.2') as neg3 from (values (1))";

    QueryBuilder q = client.queryBuilder().sql(sql);

    RowSet results = q.rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
      .add("neg1", TypeProtos.MinorType.FLOAT8)
      .add("neg2", TypeProtos.MinorType.FLOAT8)
      .add("neg3", TypeProtos.MinorType.FLOAT8)
      .build();

    RowSet expected = client.rowSetBuilder(expectedSchema).addRow(-2.55, -876, 145.2).build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testLshift() throws Exception {
    String sql = "select lshift('10', '2') as lshift1, lshift('5', '1') as lshift2 from (values (1))";

    QueryBuilder q = client.queryBuilder().sql(sql);
    RowSet results = q.rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
      .add("lshift1", TypeProtos.MinorType.FLOAT8)
      .add("lshift2", TypeProtos.MinorType.FLOAT8)
      .build();

    RowSet expected = client.rowSetBuilder(expectedSchema).addRow(40, 10).build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testRshift() throws Exception {
    String sql = "select rshift('-4', '1') as rshift1, rshift('4', '1') as rshift2 from (values (1))";

    QueryBuilder q = client.queryBuilder().sql(sql);
    RowSet results = q.rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
      .add("rshift1", TypeProtos.MinorType.FLOAT8)
      .add("rshift2", TypeProtos.MinorType.FLOAT8)
      .build();

    RowSet expected = client.rowSetBuilder(expectedSchema).addRow(-2, 2).build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testRadians() throws Exception {
    String sql = "select radians('180.0') as radians1 from (values (1))";

    QueryBuilder q = client.queryBuilder().sql(sql);
    RowSet results = q.rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
      .add("radians1", TypeProtos.MinorType.FLOAT8)
      //.add("rshift2", TypeProtos.MinorType.FLOAT8)
      .build();

    RowSet expected = client.rowSetBuilder(expectedSchema).addRow(3.141592653589793).build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testRound() throws Exception {
    String sql = "select round('180.7865') as round1, round('76.32') as round2 from (values (1))";

    QueryBuilder q = client.queryBuilder().sql(sql);
    RowSet results = q.rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
      .add("round1", TypeProtos.MinorType.FLOAT8)
      .add("round2", TypeProtos.MinorType.FLOAT8)
      .build();

    RowSet expected = client.rowSetBuilder(expectedSchema).addRow(181, 76.0).build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testRound2() throws Exception {
    String sql = "select round('180.7865', '2') as round1, round('76.328976', '4') as round2 from (values (1))";

    QueryBuilder q = client.queryBuilder().sql(sql);
    RowSet results = q.rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
      .add("round1", TypeProtos.MinorType.FLOAT8)
      .add("round2", TypeProtos.MinorType.FLOAT8)
      .build();

    RowSet expected = client.rowSetBuilder(expectedSchema).addRow(180.79, 76.3290).build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testSign() throws Exception {
    String sql = "select sign('180.7865') as sign1, sign('-76.3') as sign2, sign('0') as sign3 from (values (1))";

    QueryBuilder q = client.queryBuilder().sql(sql);
    RowSet results = q.rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
      .add("sign1", TypeProtos.MinorType.INT)
      .add("sign2", TypeProtos.MinorType.INT)
      .add("sign3", TypeProtos.MinorType.INT)
      .build();

    RowSet expected = client.rowSetBuilder(expectedSchema).addRow(1, -1, 0).build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testSqrt() throws Exception {
    String sql = "select sqrt('9') as sqrt1, sqrt('188.0') as sqrt2 from (values (1))";

    QueryBuilder q = client.queryBuilder().sql(sql);
    RowSet results = q.rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
      .add("sqrt1", TypeProtos.MinorType.FLOAT8)
      .add("sqrt2", TypeProtos.MinorType.FLOAT8)
      .build();

    RowSet expected = client.rowSetBuilder(expectedSchema).addRow(3, 13.7113092008).build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testTrunc() throws Exception {
    String sql = "select trunc('180.7865', '2.0') as trunc1, trunc('-76.390', '1') as trunc2, trunc('0.6783', '0') as trunc3 from (values (1))";

    QueryBuilder q = client.queryBuilder().sql(sql);
    RowSet results = q.rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
      .add("trunc1", TypeProtos.MinorType.FLOAT8)
      .add("trunc2", TypeProtos.MinorType.FLOAT8)
      .add("trunc3", TypeProtos.MinorType.FLOAT8)
      .build();

    RowSet expected = client.rowSetBuilder(expectedSchema).addRow(180.78, -76.3, 0).build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }
}

