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
package org.apache.drill.exec.physical.resultSet.project;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.drill.categories.RowSetTests;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.test.BaseTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(RowSetTests.class)
public class TestProjectionType extends BaseTest {

  @Test
  public void testQueries() {
    assertFalse(ProjectionType.UNPROJECTED.isTuple());
    assertFalse(ProjectionType.WILDCARD.isTuple());
    assertFalse(ProjectionType.GENERAL.isTuple());
    assertFalse(ProjectionType.SCALAR.isTuple());
    assertTrue(ProjectionType.TUPLE.isTuple());
    assertFalse(ProjectionType.ARRAY.isTuple());
    assertTrue(ProjectionType.TUPLE_ARRAY.isTuple());

    assertFalse(ProjectionType.UNPROJECTED.isArray());
    assertFalse(ProjectionType.WILDCARD.isArray());
    assertFalse(ProjectionType.GENERAL.isArray());
    assertFalse(ProjectionType.SCALAR.isArray());
    assertFalse(ProjectionType.TUPLE.isArray());
    assertTrue(ProjectionType.ARRAY.isArray());
    assertTrue(ProjectionType.TUPLE_ARRAY.isArray());

    assertFalse(ProjectionType.UNPROJECTED.isMaybeScalar());
    assertFalse(ProjectionType.WILDCARD.isMaybeScalar());
    assertTrue(ProjectionType.GENERAL.isMaybeScalar());
    assertTrue(ProjectionType.SCALAR.isMaybeScalar());
    assertFalse(ProjectionType.TUPLE.isMaybeScalar());
    assertFalse(ProjectionType.ARRAY.isMaybeScalar());
    assertFalse(ProjectionType.TUPLE_ARRAY.isMaybeScalar());
  }

  @Test
  public void testLabel() {

    // Only worry about the types that could conflict and thus
    // would show up in error messages.

    assertEquals(ProjectionType.UNPROJECTED.name(), ProjectionType.UNPROJECTED.label());
    assertEquals("wildcard (*)", ProjectionType.WILDCARD.label());
    assertEquals(ProjectionType.GENERAL.name(), ProjectionType.GENERAL.label());
    assertEquals("scalar (a)", ProjectionType.SCALAR.label());
    assertEquals("tuple (a.x)", ProjectionType.TUPLE.label());
    assertEquals("array (a[n])", ProjectionType.ARRAY.label());
    assertEquals("tuple array (a[n].x)", ProjectionType.TUPLE_ARRAY.label());
  }

  @Test
  public void testTypeFor() {

    // Test the return of the projection type most specific
    // for a data type. The projection type under-specifies
    // the data type, but is a hint.

    assertEquals(ProjectionType.TUPLE, ProjectionType.typeFor(Types.required(MinorType.MAP)));
    assertEquals(ProjectionType.TUPLE_ARRAY, ProjectionType.typeFor(Types.repeated(MinorType.MAP)));
    assertEquals(ProjectionType.ARRAY, ProjectionType.typeFor(Types.repeated(MinorType.INT)));
    assertEquals(ProjectionType.ARRAY, ProjectionType.typeFor(Types.required(MinorType.LIST)));
    assertEquals(ProjectionType.SCALAR, ProjectionType.typeFor(Types.required(MinorType.INT)));
  }

  @Test
  public void testCompatibility() {

    // Only SCALAR, TUPLE, ARRAY and TUPLE_ARRAY are expected for the
    // argument, but we check all cases for completeness.
    // Note that the cases are not always symmetrical:
    // a map array column is compatible with a map projection,
    // but a map column is not compatible with a map array projection.

    assertTrue(ProjectionType.UNPROJECTED.isCompatible(ProjectionType.UNPROJECTED));
    assertTrue(ProjectionType.UNPROJECTED.isCompatible(ProjectionType.WILDCARD));
    assertTrue(ProjectionType.UNPROJECTED.isCompatible(ProjectionType.GENERAL));
    assertTrue(ProjectionType.UNPROJECTED.isCompatible(ProjectionType.SCALAR));
    assertTrue(ProjectionType.UNPROJECTED.isCompatible(ProjectionType.TUPLE));
    assertTrue(ProjectionType.UNPROJECTED.isCompatible(ProjectionType.ARRAY));
    assertTrue(ProjectionType.UNPROJECTED.isCompatible(ProjectionType.TUPLE_ARRAY));

    assertTrue(ProjectionType.WILDCARD.isCompatible(ProjectionType.UNPROJECTED));
    assertTrue(ProjectionType.WILDCARD.isCompatible(ProjectionType.WILDCARD));
    assertTrue(ProjectionType.WILDCARD.isCompatible(ProjectionType.GENERAL));
    assertTrue(ProjectionType.WILDCARD.isCompatible(ProjectionType.SCALAR));
    assertTrue(ProjectionType.WILDCARD.isCompatible(ProjectionType.TUPLE));
    assertTrue(ProjectionType.WILDCARD.isCompatible(ProjectionType.ARRAY));
    assertTrue(ProjectionType.WILDCARD.isCompatible(ProjectionType.TUPLE_ARRAY));

    assertTrue(ProjectionType.GENERAL.isCompatible(ProjectionType.UNPROJECTED));
    assertTrue(ProjectionType.GENERAL.isCompatible(ProjectionType.WILDCARD));
    assertTrue(ProjectionType.GENERAL.isCompatible(ProjectionType.GENERAL));
    assertTrue(ProjectionType.GENERAL.isCompatible(ProjectionType.SCALAR));
    assertTrue(ProjectionType.GENERAL.isCompatible(ProjectionType.TUPLE));
    assertTrue(ProjectionType.GENERAL.isCompatible(ProjectionType.ARRAY));
    assertTrue(ProjectionType.GENERAL.isCompatible(ProjectionType.TUPLE_ARRAY));

    assertTrue(ProjectionType.SCALAR.isCompatible(ProjectionType.UNPROJECTED));
    assertTrue(ProjectionType.SCALAR.isCompatible(ProjectionType.WILDCARD));
    assertTrue(ProjectionType.SCALAR.isCompatible(ProjectionType.GENERAL));
    assertTrue(ProjectionType.SCALAR.isCompatible(ProjectionType.SCALAR));
    assertFalse(ProjectionType.SCALAR.isCompatible(ProjectionType.TUPLE));
    assertFalse(ProjectionType.SCALAR.isCompatible(ProjectionType.ARRAY));
    assertFalse(ProjectionType.SCALAR.isCompatible(ProjectionType.TUPLE_ARRAY));

    assertTrue(ProjectionType.TUPLE.isCompatible(ProjectionType.UNPROJECTED));
    assertTrue(ProjectionType.TUPLE.isCompatible(ProjectionType.WILDCARD));
    assertTrue(ProjectionType.TUPLE.isCompatible(ProjectionType.GENERAL));
    assertFalse(ProjectionType.TUPLE.isCompatible(ProjectionType.SCALAR));
    assertTrue(ProjectionType.TUPLE.isCompatible(ProjectionType.TUPLE));
    assertFalse(ProjectionType.TUPLE.isCompatible(ProjectionType.ARRAY));
    assertTrue(ProjectionType.TUPLE.isCompatible(ProjectionType.TUPLE_ARRAY));

    assertTrue(ProjectionType.ARRAY.isCompatible(ProjectionType.UNPROJECTED));
    assertTrue(ProjectionType.ARRAY.isCompatible(ProjectionType.WILDCARD));
    assertTrue(ProjectionType.ARRAY.isCompatible(ProjectionType.GENERAL));
    assertFalse(ProjectionType.ARRAY.isCompatible(ProjectionType.SCALAR));
    assertFalse(ProjectionType.ARRAY.isCompatible(ProjectionType.TUPLE));
    assertTrue(ProjectionType.ARRAY.isCompatible(ProjectionType.ARRAY));
    assertTrue(ProjectionType.ARRAY.isCompatible(ProjectionType.TUPLE_ARRAY));

    assertTrue(ProjectionType.TUPLE_ARRAY.isCompatible(ProjectionType.UNPROJECTED));
    assertTrue(ProjectionType.TUPLE_ARRAY.isCompatible(ProjectionType.WILDCARD));
    assertTrue(ProjectionType.TUPLE_ARRAY.isCompatible(ProjectionType.GENERAL));
    assertFalse(ProjectionType.TUPLE_ARRAY.isCompatible(ProjectionType.SCALAR));
    assertFalse(ProjectionType.TUPLE_ARRAY.isCompatible(ProjectionType.TUPLE));
    assertFalse(ProjectionType.TUPLE_ARRAY.isCompatible(ProjectionType.ARRAY));
    assertTrue(ProjectionType.TUPLE_ARRAY.isCompatible(ProjectionType.TUPLE_ARRAY));
  }
}
