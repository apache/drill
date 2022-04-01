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
import org.apache.drill.exec.resolver.ResolverTypePrecedence;
import org.apache.drill.exec.resolver.TypeCastRules;
import org.apache.drill.test.BaseTest;
import org.junit.Test;

import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;

@Category(SqlTest.class)
public class TestImplicitCasting extends BaseTest {
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
      ResolverTypePrecedence.PRIMITIVE_TYPE_COST,
      ResolverTypePrecedence.computeCost(TypeProtos.MinorType.INT, TypeProtos.MinorType.BIGINT),
      0f
    );
    // INT -> BIGINT -> VARDECIMAL
    assertEquals(
      ResolverTypePrecedence.BASE_COST + ResolverTypePrecedence.PRIMITIVE_TYPE_COST,
      ResolverTypePrecedence.computeCost(TypeProtos.MinorType.INT, TypeProtos.MinorType.VARDECIMAL),
      0f
    );
    // DECIMAL9 -> DECIMAL18 -> DECIMAL28SPARSE -> DECIMAL28DENSE -> DECIMAL38SPARSE ->
    // -> DECIMAL38DENSE -> VARDECIMAL
    assertEquals(
      6*ResolverTypePrecedence.BASE_COST,
      ResolverTypePrecedence.computeCost(TypeProtos.MinorType.DECIMAL9, TypeProtos.MinorType.VARDECIMAL),
      0f
    );
    // No path from FLOAT4 to INT
    assertEquals(
      Float.POSITIVE_INFINITY,
      ResolverTypePrecedence.computeCost(TypeProtos.MinorType.FLOAT4, TypeProtos.MinorType.INT),
      0f
    );
    // TIMESTAMP -> DATE
    assertEquals(
      ResolverTypePrecedence.PRECISION_LOSS_COST,
      ResolverTypePrecedence.computeCost(TypeProtos.MinorType.TIMESTAMP, TypeProtos.MinorType.DATE),
      0f
    );
    // No path from MAP to INT
    assertEquals(
      Float.POSITIVE_INFINITY,
      ResolverTypePrecedence.computeCost(TypeProtos.MinorType.MAP, TypeProtos.MinorType.INT),
      0f
    );
    // VARCHAR -> BIGINT
    assertEquals(
      3*ResolverTypePrecedence.PRECISION_LOSS_COST,
      ResolverTypePrecedence.computeCost(TypeProtos.MinorType.VARCHAR, TypeProtos.MinorType.BIGINT),
      0f
    );
  }
}
