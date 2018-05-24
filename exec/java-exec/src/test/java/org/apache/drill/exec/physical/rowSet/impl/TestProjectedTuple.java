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
package org.apache.drill.exec.physical.rowSet.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.rowSet.project.ImpliedTupleRequest;
import org.apache.drill.exec.physical.rowSet.project.RequestedTuple;
import org.apache.drill.exec.physical.rowSet.project.RequestedTuple.RequestedColumn;
import org.apache.drill.exec.physical.rowSet.project.RequestedTupleImpl;
import org.apache.drill.exec.record.metadata.ProjectionType;
import org.junit.Ignore;
import org.junit.Test;

public class TestProjectedTuple {

  @Test
  public void testProjectionAll() {

    // Null map means everything is projected

    RequestedTuple projSet = RequestedTupleImpl.parse(null);
    assertTrue(projSet instanceof ImpliedTupleRequest);
    assertEquals(ProjectionType.UNSPECIFIED, projSet.projectionType("foo"));
  }

  /**
   * Test an empty projection which occurs in a
   * SELECT COUNT(*) query.
   */

  @Test
  public void testProjectionNone() {

    // Empty list means nothing is projected

    RequestedTuple projSet = RequestedTupleImpl.parse(new ArrayList<SchemaPath>());
    assertTrue(projSet instanceof ImpliedTupleRequest);
    List<RequestedColumn> cols = projSet.projections();
    assertEquals(0, cols.size());
    assertEquals(ProjectionType.UNPROJECTED, projSet.projectionType("foo"));
  }

  @Test
  public void testProjectionSimple() {

    // Simple non-map columns

    RequestedTuple projSet = RequestedTupleImpl.parse(
        RowSetTestUtils.projectList("a", "b", "c"));
    assertTrue(projSet instanceof RequestedTupleImpl);
    assertEquals(ProjectionType.UNSPECIFIED, projSet.projectionType("a"));
    assertEquals(ProjectionType.UNSPECIFIED, projSet.projectionType("b"));
    assertEquals(ProjectionType.UNPROJECTED, projSet.projectionType("d"));

    List<RequestedColumn> cols = projSet.projections();
    assertEquals(3, cols.size());

    RequestedColumn a = cols.get(0);
    assertEquals("a", a.name());
    assertEquals(ProjectionType.UNSPECIFIED, a.type());
    assertTrue(a.isSimple());
    assertFalse(a.isWildcard());
    assertNull(a.mapProjection());
    assertNull(a.indexes());

    assertEquals("b", cols.get(1).name());
    assertEquals(ProjectionType.UNSPECIFIED, cols.get(1).type());
    assertTrue(cols.get(1).isSimple());

    assertEquals("c", cols.get(2).name());
    assertEquals(ProjectionType.UNSPECIFIED, cols.get(2).type());
    assertTrue(cols.get(2).isSimple());
  }

  @Test
  public void testProjectionWholeMap() {

    // Whole-map projection (note, fully projected maps are
    // identical to projected simple columns at this level of
    // abstraction.)

    List<SchemaPath> projCols = new ArrayList<>();
    projCols.add(SchemaPath.getSimplePath("map"));
    RequestedTuple projSet = RequestedTupleImpl.parse(projCols);

    assertTrue(projSet instanceof RequestedTupleImpl);
    assertEquals(ProjectionType.UNSPECIFIED, projSet.projectionType("map"));
    assertEquals(ProjectionType.UNPROJECTED, projSet.projectionType("another"));
    RequestedTuple mapProj = projSet.mapProjection("map");
    assertNotNull(mapProj);
    assertTrue(mapProj instanceof ImpliedTupleRequest);
    assertEquals(ProjectionType.UNSPECIFIED, mapProj.projectionType("foo"));
    assertNotNull(projSet.mapProjection("another"));
    assertEquals(ProjectionType.UNPROJECTED, projSet.mapProjection("another").projectionType("anyCol"));
  }

  @Test
  public void testProjectionMapSubset() {

    // Selected map projection, multiple levels, full projection
    // at leaf level.

    List<SchemaPath> projCols = new ArrayList<>();
    projCols.add(SchemaPath.getCompoundPath("map", "a"));
    projCols.add(SchemaPath.getCompoundPath("map", "b"));
    projCols.add(SchemaPath.getCompoundPath("map", "map2", "x"));
    RequestedTuple projSet = RequestedTupleImpl.parse(projCols);
    assertTrue(projSet instanceof RequestedTupleImpl);
    assertEquals(ProjectionType.TUPLE, projSet.projectionType("map"));

    // Map: an explicit map at top level

    RequestedTuple mapProj = projSet.mapProjection("map");
    assertTrue(mapProj instanceof RequestedTupleImpl);
    assertEquals(ProjectionType.UNSPECIFIED, mapProj.projectionType("a"));
    assertEquals(ProjectionType.UNSPECIFIED, mapProj.projectionType("b"));
    assertEquals(ProjectionType.TUPLE, mapProj.projectionType("map2"));
    assertEquals(ProjectionType.UNPROJECTED, mapProj.projectionType("bogus"));

    // Map b: an implied nested map

    RequestedTuple bMapProj = mapProj.mapProjection("b");
    assertNotNull(bMapProj);
    assertTrue(bMapProj instanceof ImpliedTupleRequest);
    assertEquals(ProjectionType.UNSPECIFIED, bMapProj.projectionType("foo"));

    // Map2, an nested map, has an explicit projection

    RequestedTuple map2Proj = mapProj.mapProjection("map2");
    assertNotNull(map2Proj);
    assertTrue(map2Proj instanceof RequestedTupleImpl);
    assertEquals(ProjectionType.UNSPECIFIED, map2Proj.projectionType("x"));
    assertEquals(ProjectionType.UNPROJECTED, map2Proj.projectionType("bogus"));
  }

  @Test
  public void testProjectionMapFieldAndMap() {

    // Project both a map member and the entire map.

    {
      List<SchemaPath> projCols = new ArrayList<>();
      projCols.add(SchemaPath.getCompoundPath("map", "a"));
      projCols.add(SchemaPath.getCompoundPath("map"));

      RequestedTuple projSet = RequestedTupleImpl.parse(projCols);
      assertTrue(projSet instanceof RequestedTupleImpl);
      assertEquals(ProjectionType.TUPLE, projSet.projectionType("map"));

      RequestedTuple mapProj = projSet.mapProjection("map");
      assertTrue(mapProj instanceof ImpliedTupleRequest);
      assertEquals(ProjectionType.UNSPECIFIED, mapProj.projectionType("a"));

      // Didn't ask for b, but did ask for whole map.

      assertEquals(ProjectionType.UNSPECIFIED, mapProj.projectionType("b"));
    }

    // Now the other way around.

    {
      List<SchemaPath> projCols = new ArrayList<>();
      projCols.add(SchemaPath.getCompoundPath("map"));
      projCols.add(SchemaPath.getCompoundPath("map", "a"));

      RequestedTuple projSet = RequestedTupleImpl.parse(projCols);
      assertTrue(projSet instanceof RequestedTupleImpl);
      assertEquals(ProjectionType.TUPLE, projSet.projectionType("map"));

      RequestedTuple mapProj = projSet.mapProjection("map");
      assertTrue(mapProj instanceof ImpliedTupleRequest);
      assertEquals(ProjectionType.UNSPECIFIED, mapProj.projectionType("a"));
      assertEquals(ProjectionType.UNSPECIFIED, mapProj.projectionType("b"));
    }
  }

  @Test
  public void testMapDetails() {
    RequestedTuple projSet = RequestedTupleImpl.parse(
        RowSetTestUtils.projectList("a.b.c", "a.c", "d"));
    List<RequestedColumn> cols = projSet.projections();
    assertEquals(2, cols.size());

    RequestedColumn a = cols.get(0);
    assertEquals("a", a.name());
    assertFalse(a.isSimple());
    assertFalse(a.isArray());
    assertTrue(a.isTuple());

    {
      assertNotNull(a.mapProjection());
      List<RequestedColumn> aMembers = a.mapProjection().projections();
      assertEquals(2, aMembers.size());

      RequestedColumn a_b = aMembers.get(0);
      assertEquals("b", a_b.name());
      assertTrue(a_b.isTuple());

      {
        assertNotNull(a_b.mapProjection());
        List<RequestedColumn> a_bMembers = a_b.mapProjection().projections();
        assertEquals(1, a_bMembers.size());
        assertEquals("c", a_bMembers.get(0).name());
        assertTrue(a_bMembers.get(0).isSimple());
      }

      assertEquals("c", aMembers.get(1).name());
      assertTrue(aMembers.get(1).isSimple());
    }

    assertEquals("d", cols.get(1).name());
    assertTrue(cols.get(1).isSimple());
  }

  @Test
  public void testMapDups() {
    try {
      RequestedTupleImpl.parse(
          RowSetTestUtils.projectList("a.b", "a.c", "a.b"));
      fail();
    } catch (UserException e) {
      // Expected
    }
  }

  /**
   * When the project list includes references to both the
   * map as a whole, and members, then the parser is forgiving
   * of duplicate map members since all members are projected.
   */

  @Test
  public void testMapDupsIgnored() {
    RequestedTuple projSet = RequestedTupleImpl.parse(
          RowSetTestUtils.projectList("a", "a.b", "a.c", "a.b"));
    List<RequestedColumn> cols = projSet.projections();
    assertEquals(1, cols.size());
  }

  @Test
  public void testWildcard() {
    RequestedTuple projSet = RequestedTupleImpl.parse(
        RowSetTestUtils.projectList(SchemaPath.DYNAMIC_STAR));
    List<RequestedColumn> cols = projSet.projections();
    assertEquals(1, cols.size());

    RequestedColumn wildcard = cols.get(0);
    assertEquals(ProjectionType.WILDCARD, wildcard.type());
    assertEquals(SchemaPath.DYNAMIC_STAR, wildcard.name());
    assertTrue(! wildcard.isSimple());
    assertTrue(wildcard.isWildcard());
    assertNull(wildcard.mapProjection());
    assertNull(wildcard.indexes());
  }

  @Test
  public void testSimpleDups() {
    try {
      RequestedTupleImpl.parse(RowSetTestUtils.projectList("a", "b", "a"));
      fail();
    } catch (UserException e) {
      // Expected
    }
  }

  @Test
  public void testArray() {
    RequestedTuple projSet = RequestedTupleImpl.parse(
        RowSetTestUtils.projectList("a[1]", "a[3]"));
    List<RequestedColumn> cols = projSet.projections();
    assertEquals(1, cols.size());

    assertEquals(ProjectionType.ARRAY, projSet.projectionType("a"));
    RequestedColumn a = cols.get(0);
    assertEquals("a", a.name());
    assertTrue(a.isArray());
    assertFalse(a.isSimple());
    assertFalse(a.isTuple());
    boolean indexes[] = a.indexes();
    assertNotNull(indexes);
    assertEquals(4, indexes.length);
    assertFalse(indexes[0]);
    assertTrue(indexes[1]);
    assertFalse(indexes[2]);
    assertTrue(indexes[3]);
  }

  @Test
  public void testArrayDups() {
    try {
      RequestedTupleImpl.parse(
          RowSetTestUtils.projectList("a[1]", "a[3]", "a[1]"));
      fail();
    } catch (UserException e) {
      // Expected
    }
  }

  @Test
  public void testArrayAndSimple() {
    RequestedTuple projSet = RequestedTupleImpl.parse(
        RowSetTestUtils.projectList("a[1]", "a"));
    List<RequestedColumn> cols = projSet.projections();
    assertEquals(1, cols.size());

    RequestedColumn a = cols.get(0);
    assertEquals("a", a.name());
    assertTrue(a.isArray());
    assertNull(a.indexes());
  }

  @Test
  public void testSimpleAndArray() {
    RequestedTuple projSet = RequestedTupleImpl.parse(
        RowSetTestUtils.projectList("a", "a[1]"));
    List<RequestedColumn> cols = projSet.projections();
    assertEquals(1, cols.size());

    RequestedColumn a = cols.get(0);
    assertEquals("a", a.name());
    assertTrue(a.isArray());
    assertNull(a.indexes());
    assertEquals(ProjectionType.ARRAY, projSet.projectionType("a"));
    assertEquals(ProjectionType.UNPROJECTED, projSet.projectionType("foo"));
  }

  @Test
  @Ignore("Drill syntax does not support map arrays")
  public void testMapArray() {
    RequestedTuple projSet = RequestedTupleImpl.parse(
        RowSetTestUtils.projectList("a[1].x"));
    List<RequestedColumn> cols = projSet.projections();
    assertEquals(1, cols.size());

    assertEquals(ProjectionType.TUPLE_ARRAY, cols.get(0).type());
    assertEquals(ProjectionType.TUPLE_ARRAY, projSet.projectionType("a"));
  }
}
