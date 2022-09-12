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
import org.joda.time.Period;
import org.junit.BeforeClass;
import org.junit.Test;

import org.junit.experimental.categories.Category;

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
  public void testCastingCosts() {
    // INT -> BIGINT
    assertEquals(
      1f,
      ResolverTypePrecedence.computeCost(TypeProtos.MinorType.INT, TypeProtos.MinorType.BIGINT),
      0f
    );
    // INT -> BIGINT -> VARDECIMAL
    assertEquals(
      3f,
      ResolverTypePrecedence.computeCost(TypeProtos.MinorType.INT, TypeProtos.MinorType.VARDECIMAL),
      0f
    );
    // DECIMAL9 -> DECIMAL18 -> DECIMAL28SPARSE -> DECIMAL28DENSE -> DECIMAL38SPARSE ->
    // -> DECIMAL38DENSE -> VARDECIMAL
    assertEquals(
      6f,
      ResolverTypePrecedence.computeCost(TypeProtos.MinorType.DECIMAL9, TypeProtos.MinorType.VARDECIMAL),
      0f
    );
    // FLOAT4 -> FLOAT8 -> VARDECIMAL -> INT
    assertEquals(
      4f,
      ResolverTypePrecedence.computeCost(TypeProtos.MinorType.FLOAT4, TypeProtos.MinorType.INT),
      0f
    );
    // TIMESTAMP -> DATE
    assertEquals(
      2f,
      ResolverTypePrecedence.computeCost(TypeProtos.MinorType.TIMESTAMP, TypeProtos.MinorType.DATE),
      0f
    );
    // No path from MAP to INT
    assertEquals(
      Float.POSITIVE_INFINITY,
      ResolverTypePrecedence.computeCost(TypeProtos.MinorType.MAP, TypeProtos.MinorType.INT),
      0f
    );
    // VARCHAR -> INT -> BIGINT
    assertEquals(
      2f,
      ResolverTypePrecedence.computeCost(TypeProtos.MinorType.VARCHAR, TypeProtos.MinorType.BIGINT),
      0f
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
