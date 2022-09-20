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

import org.apache.drill.categories.SqlTest;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.physical.rowSet.DirectRowSet;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.resolver.ResolverTypePrecedence;
import org.apache.drill.exec.resolver.TypeCastRules;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.test.rowSet.RowSetUtilities;
import static org.hamcrest.MatcherAssert.assertThat;

import org.joda.time.Period;
import org.junit.BeforeClass;
import org.junit.Test;

import org.junit.experimental.categories.Category;

import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertEquals;

@Category(SqlTest.class)
public class TestImplicitCasting extends ClusterTest {

  @BeforeClass
  public static void setup() throws Exception {
    ClusterTest.startCluster(new ClusterFixtureBuilder(dirTestWatcher));
  }

  @Test
  public void testTimeStampAndTime() {
    final TypeProtos.MinorType result = TypeCastRules.getLeastRestrictiveType(
      TypeProtos.MinorType.TIME,
      TypeProtos.MinorType.TIMESTAMP
    );

    assertEquals(TypeProtos.MinorType.TIME, result);
  }

  @Test
  /**
   * Tests path cost arithmetic computed over the graph in ResolveTypePrecedence.
   */
  public void testCastingCosts() {
    // INT -> BIGINT
    assertEquals(
      10f,
      ResolverTypePrecedence.computeCost(TypeProtos.MinorType.INT, TypeProtos.MinorType.BIGINT),
      0f
    );
    // INT -> BIGINT -> VARDECIMAL
    assertEquals(
      10f+100f,
      ResolverTypePrecedence.computeCost(TypeProtos.MinorType.INT, TypeProtos.MinorType.VARDECIMAL),
      0f
    );
    // DECIMAL9 -> DECIMAL18 -> DECIMAL28SPARSE -> DECIMAL28DENSE -> DECIMAL38SPARSE ->
    // -> DECIMAL38DENSE -> VARDECIMAL
    assertEquals(
      10f+10f+10f+10f+10f+10f,
      ResolverTypePrecedence.computeCost(TypeProtos.MinorType.DECIMAL9, TypeProtos.MinorType.VARDECIMAL),
      0f
    );
    // FLOAT4 -> FLOAT8 -> VARDECIMAL -> INT
    assertEquals(
      10f+10_000f+1_002f,
      ResolverTypePrecedence.computeCost(TypeProtos.MinorType.FLOAT4, TypeProtos.MinorType.INT),
      0f
    );
    // TIMESTAMP -> DATE
    assertEquals(
      100f,
      ResolverTypePrecedence.computeCost(TypeProtos.MinorType.TIMESTAMP, TypeProtos.MinorType.DATE),
      0f
    );
    // No path from MAP to FLOAT8
    assertEquals(
      Float.POSITIVE_INFINITY,
      ResolverTypePrecedence.computeCost(TypeProtos.MinorType.MAP, TypeProtos.MinorType.FLOAT8),
      0f
    );
    // VARCHAR -> INT -> BIGINT
    assertEquals(
      10f+10f,
      ResolverTypePrecedence.computeCost(TypeProtos.MinorType.VARCHAR, TypeProtos.MinorType.BIGINT),
      0f
    );
  }

  @Test
  public void testCastingPreferences() {
    // All of the constraints that follow should be satisfied in order for
    // queries involving implicit casts to behave as expected.
    assertThat(
      ResolverTypePrecedence.computeCost(TypeProtos.MinorType.VARCHAR, TypeProtos.MinorType.BIT),
        lessThan(ResolverTypePrecedence.computeCost(TypeProtos.MinorType.BIT, TypeProtos.MinorType.INT))
    );
    assertThat(
      ResolverTypePrecedence.computeCost(TypeProtos.MinorType.UINT1, TypeProtos.MinorType.BIGINT),
        lessThan(ResolverTypePrecedence.computeCost(TypeProtos.MinorType.UINT1, TypeProtos.MinorType.FLOAT4))
    );
    assertThat(
      ResolverTypePrecedence.computeCost(TypeProtos.MinorType.UINT1, TypeProtos.MinorType.BIGINT),
        lessThan(ResolverTypePrecedence.computeCost(TypeProtos.MinorType.UINT1, TypeProtos.MinorType.FLOAT4))
    );
    assertThat(
      ResolverTypePrecedence.computeCost(TypeProtos.MinorType.INT, TypeProtos.MinorType.FLOAT4),
        lessThan(ResolverTypePrecedence.computeCost(TypeProtos.MinorType.INT, TypeProtos.MinorType.VARDECIMAL))
    );
    assertThat(
      ResolverTypePrecedence.computeCost(TypeProtos.MinorType.VARDECIMAL, TypeProtos.MinorType.FLOAT8),
        lessThan(ResolverTypePrecedence.computeCost(TypeProtos.MinorType.FLOAT8, TypeProtos.MinorType.VARDECIMAL))
    );
    assertThat(
      ResolverTypePrecedence.computeCost(TypeProtos.MinorType.INT, TypeProtos.MinorType.VARDECIMAL),
        lessThan(ResolverTypePrecedence.computeCost(TypeProtos.MinorType.VARDECIMAL, TypeProtos.MinorType.FLOAT4))
    );
    assertThat(
      ResolverTypePrecedence.computeCost(TypeProtos.MinorType.VARCHAR, TypeProtos.MinorType.INTERVAL),
        lessThan(ResolverTypePrecedence.computeCost(TypeProtos.MinorType.INTERVAL, TypeProtos.MinorType.VARCHAR))
    );
    assertThat(
      ResolverTypePrecedence.computeCost(TypeProtos.MinorType.VARCHAR, TypeProtos.MinorType.TIME),
      lessThan(ResolverTypePrecedence.computeCost(TypeProtos.MinorType.TIME, TypeProtos.MinorType.VARCHAR))
    );
    assertThat(
      ResolverTypePrecedence.computeCost(TypeProtos.MinorType.DATE, TypeProtos.MinorType.TIMESTAMP),
        lessThan(ResolverTypePrecedence.computeCost(TypeProtos.MinorType.TIMESTAMP, TypeProtos.MinorType.DATE))
    );
    assertThat(
      ResolverTypePrecedence.computeCost(TypeProtos.MinorType.VARDECIMAL, TypeProtos.MinorType.FLOAT8),
      lessThan(ResolverTypePrecedence.computeCost(TypeProtos.MinorType.VARDECIMAL, TypeProtos.MinorType.FLOAT4))
    );
    assertThat(
      ResolverTypePrecedence.computeCost(TypeProtos.MinorType.VARCHAR, TypeProtos.MinorType.BIGINT),
      lessThan(ResolverTypePrecedence.computeCost(TypeProtos.MinorType.VARCHAR, TypeProtos.MinorType.TIMESTAMP))
    );
    assertThat(
      ResolverTypePrecedence.computeCost(TypeProtos.MinorType.VARCHAR, TypeProtos.MinorType.FLOAT8),
      lessThan(ResolverTypePrecedence.computeCost(TypeProtos.MinorType.VARCHAR, TypeProtos.MinorType.FLOAT4))
    );
    assertThat(ResolverTypePrecedence.computeCost(TypeProtos.MinorType.VARCHAR, TypeProtos.MinorType.FLOAT4) +
      ResolverTypePrecedence.computeCost(TypeProtos.MinorType.FLOAT4, TypeProtos.MinorType.FLOAT4),
      lessThan(ResolverTypePrecedence.computeCost(TypeProtos.MinorType.VARCHAR, TypeProtos.MinorType.FLOAT8) +
        ResolverTypePrecedence.computeCost(TypeProtos.MinorType.FLOAT4, TypeProtos.MinorType.FLOAT8))
    );
    assertThat(
      ResolverTypePrecedence.computeCost(TypeProtos.MinorType.NULL, TypeProtos.MinorType.FLOAT4),
      lessThan(ResolverTypePrecedence.computeCost(TypeProtos.MinorType.FLOAT4, TypeProtos.MinorType.FLOAT8))
    );
  }

  @Test
  public void testSqrtOfString() throws Exception {
    String sql = "select sqrt('5')";

    DirectRowSet results = queryBuilder().sql(sql).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
      .add("EXPR$0", TypeProtos.MinorType.FLOAT8)
      .build();

    RowSet expected = client.rowSetBuilder(expectedSchema)
      .addRow(2.23606797749979)
      .build();

    RowSetUtilities.verify(expected, results);
  }

  @Test
  public void testDateDiffOnStringDate() throws Exception {
    String sql = "select date_diff('2022-01-01', date '1970-01-01')";

    DirectRowSet results = queryBuilder().sql(sql).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
      .add("EXPR$0", TypeProtos.MinorType.INTERVALDAY)
      .build();

    RowSet expected = client.rowSetBuilder(expectedSchema)
      .addRow(new Period("P18993D"))
      .build();

    RowSetUtilities.verify(expected, results);
  }

  @Test
  public void testSubstringOfDate() throws Exception {
    String sql = "select substring(date '2022-09-09', 1, 4)";

    DirectRowSet results = queryBuilder().sql(sql).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
      .add("EXPR$0", TypeProtos.MinorType.VARCHAR, 65535)
      .build();

    RowSet expected = client.rowSetBuilder(expectedSchema)
      .addRow("2022")
      .build();

    RowSetUtilities.verify(expected, results);
  }

  @Test
  public void testBooleanStringEquality() throws Exception {
    String sql = "select true = 'true', true = 'FalsE'";

    DirectRowSet results = queryBuilder().sql(sql).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
      .add("EXPR$0", TypeProtos.MinorType.BIT)
      .add("EXPR$1", TypeProtos.MinorType.BIT)
      .build();

    RowSet expected = client.rowSetBuilder(expectedSchema)
      .addRow(true, false)
      .build();

    RowSetUtilities.verify(expected, results);
  }
}
