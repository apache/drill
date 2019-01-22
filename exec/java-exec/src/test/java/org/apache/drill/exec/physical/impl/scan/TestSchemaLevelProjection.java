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
package org.apache.drill.exec.physical.impl.scan;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.impl.scan.project.ResolvedMapColumn;
import org.apache.drill.exec.physical.impl.scan.project.ResolvedTuple.ResolvedRow;
import org.apache.drill.exec.physical.impl.scan.project.ExplicitSchemaProjection;
import org.apache.drill.exec.physical.impl.scan.project.NullColumnBuilder;
import org.apache.drill.exec.physical.impl.scan.project.ResolvedColumn;
import org.apache.drill.exec.physical.impl.scan.project.ResolvedNullColumn;
import org.apache.drill.exec.physical.impl.scan.project.ResolvedTableColumn;
import org.apache.drill.exec.physical.impl.scan.project.ResolvedTuple;
import org.apache.drill.exec.physical.impl.scan.project.ScanLevelProjection;
import org.apache.drill.exec.physical.impl.scan.project.UnresolvedColumn;
import org.apache.drill.exec.physical.impl.scan.project.VectorSource;
import org.apache.drill.exec.physical.impl.scan.project.WildcardSchemaProjection;
import org.apache.drill.exec.physical.rowSet.impl.RowSetTestUtils;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.test.SubOperatorTest;
import org.junit.Test;

public class TestSchemaLevelProjection extends SubOperatorTest {

  @Test
  public void testWildcard() {
    ScanLevelProjection scanProj = new ScanLevelProjection(
        RowSetTestUtils.projectAll(),
        ScanTestUtils.parsers());
    assertEquals(1, scanProj.columns().size());

    TupleMetadata tableSchema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .addNullable("c", MinorType.INT)
        .addArray("d", MinorType.FLOAT8)
        .buildSchema();

    NullColumnBuilder builder = new NullColumnBuilder(null, false);
    ResolvedRow rootTuple = new ResolvedRow(builder);
    new WildcardSchemaProjection(
        scanProj, tableSchema, rootTuple,
        ScanTestUtils.resolvers());

    List<ResolvedColumn> columns = rootTuple.columns();
    assertEquals(3, columns.size());

    assertEquals("a", columns.get(0).name());
    assertEquals(0, columns.get(0).sourceIndex());
    assertSame(rootTuple, columns.get(0).source());
    assertEquals("c", columns.get(1).name());
    assertEquals(1, columns.get(1).sourceIndex());
    assertSame(rootTuple, columns.get(1).source());
    assertEquals("d", columns.get(2).name());
    assertEquals(2, columns.get(2).sourceIndex());
    assertSame(rootTuple, columns.get(2).source());
  }

  /**
   * Test SELECT list with columns defined in a order and with
   * name case different than the early-schema table.
   */

  @Test
  public void testFullList() {

    // Simulate SELECT c, b, a ...

    ScanLevelProjection scanProj = new ScanLevelProjection(
        RowSetTestUtils.projectList("c", "b", "a"),
        ScanTestUtils.parsers());
    assertEquals(3, scanProj.columns().size());

    // Simulate a data source, with early schema, of (a, b, c)

    TupleMetadata tableSchema = new SchemaBuilder()
        .add("A", MinorType.VARCHAR)
        .add("B", MinorType.VARCHAR)
        .add("C", MinorType.VARCHAR)
        .buildSchema();

    NullColumnBuilder builder = new NullColumnBuilder(null, false);
    ResolvedRow rootTuple = new ResolvedRow(builder);
    new ExplicitSchemaProjection(
        scanProj, tableSchema, rootTuple,
        ScanTestUtils.resolvers());

    List<ResolvedColumn> columns = rootTuple.columns();
    assertEquals(3, columns.size());

    assertEquals("c", columns.get(0).name());
    assertEquals(2, columns.get(0).sourceIndex());
    assertSame(rootTuple, columns.get(0).source());

    assertEquals("b", columns.get(1).name());
    assertEquals(1, columns.get(1).sourceIndex());
    assertSame(rootTuple, columns.get(1).source());

    assertEquals("a", columns.get(2).name());
    assertEquals(0, columns.get(2).sourceIndex());
    assertSame(rootTuple, columns.get(2).source());
  }

  /**
   * Test SELECT list with columns missing from the table schema.
   */

  @Test
  public void testMissing() {

    // Simulate SELECT c, v, b, w ...

    ScanLevelProjection scanProj = new ScanLevelProjection(
        RowSetTestUtils.projectList("c", "v", "b", "w"),
        ScanTestUtils.parsers());
    assertEquals(4, scanProj.columns().size());

    // Simulate a data source, with early schema, of (a, b, c)

    TupleMetadata tableSchema = new SchemaBuilder()
        .add("A", MinorType.VARCHAR)
        .add("B", MinorType.VARCHAR)
        .add("C", MinorType.VARCHAR)
        .buildSchema();

    NullColumnBuilder builder = new NullColumnBuilder(null, false);
    ResolvedRow rootTuple = new ResolvedRow(builder);
    new ExplicitSchemaProjection(
        scanProj, tableSchema, rootTuple,
        ScanTestUtils.resolvers());

    List<ResolvedColumn> columns = rootTuple.columns();
    assertEquals(4, columns.size());
    VectorSource nullBuilder = rootTuple.nullBuilder();

    assertEquals("c", columns.get(0).name());
    assertEquals(2, columns.get(0).sourceIndex());
    assertSame(rootTuple, columns.get(0).source());

    assertEquals("v", columns.get(1).name());
    assertEquals(0, columns.get(1).sourceIndex());
    assertSame(nullBuilder, columns.get(1).source());

    assertEquals("b", columns.get(2).name());
    assertEquals(1, columns.get(2).sourceIndex());
    assertSame(rootTuple, columns.get(2).source());

    assertEquals("w", columns.get(3).name());
    assertEquals(1, columns.get(3).sourceIndex());
    assertSame(nullBuilder, columns.get(3).source());
  }

  @Test
  public void testSubset() {

    // Simulate SELECT c, a ...

    ScanLevelProjection scanProj = new ScanLevelProjection(
        RowSetTestUtils.projectList("c", "a"),
        ScanTestUtils.parsers());
    assertEquals(2, scanProj.columns().size());

    // Simulate a data source, with early schema, of (a, b, c)

    TupleMetadata tableSchema = new SchemaBuilder()
        .add("A", MinorType.VARCHAR)
        .add("B", MinorType.VARCHAR)
        .add("C", MinorType.VARCHAR)
        .buildSchema();

    NullColumnBuilder builder = new NullColumnBuilder(null, false);
    ResolvedRow rootTuple = new ResolvedRow(builder);
    new ExplicitSchemaProjection(
        scanProj, tableSchema, rootTuple,
        ScanTestUtils.resolvers());

    List<ResolvedColumn> columns = rootTuple.columns();
    assertEquals(2, columns.size());

    assertEquals("c", columns.get(0).name());
    assertEquals(2, columns.get(0).sourceIndex());
    assertSame(rootTuple, columns.get(0).source());

    assertEquals("a", columns.get(1).name());
    assertEquals(0, columns.get(1).sourceIndex());
    assertSame(rootTuple, columns.get(1).source());
  }

  @Test
  public void testDisjoint() {

    // Simulate SELECT c, a ...

    ScanLevelProjection scanProj = new ScanLevelProjection(
        RowSetTestUtils.projectList("b"),
        ScanTestUtils.parsers());
    assertEquals(1, scanProj.columns().size());

    // Simulate a data source, with early schema, of (a)

    TupleMetadata tableSchema = new SchemaBuilder()
        .add("A", MinorType.VARCHAR)
        .buildSchema();

    NullColumnBuilder builder = new NullColumnBuilder(null, false);
    ResolvedRow rootTuple = new ResolvedRow(builder);
    new ExplicitSchemaProjection(
        scanProj, tableSchema, rootTuple,
        ScanTestUtils.resolvers());

    List<ResolvedColumn> columns = rootTuple.columns();
    assertEquals(1, columns.size());
    VectorSource nullBuilder = rootTuple.nullBuilder();

    assertEquals("b", columns.get(0).name());
    assertEquals(0, columns.get(0).sourceIndex());
    assertSame(nullBuilder, columns.get(0).source());
  }

  @Test
  public void testOmittedMap() {

    // Simulate SELECT a, b.c.d ...

    ScanLevelProjection scanProj = new ScanLevelProjection(
        RowSetTestUtils.projectList("a", "b.c.d"),
        ScanTestUtils.parsers());
    assertEquals(2, scanProj.columns().size());
    {
      assertEquals(UnresolvedColumn.UNRESOLVED, scanProj.columns().get(1).nodeType());
      UnresolvedColumn bCol = (UnresolvedColumn) (scanProj.columns().get(1));
      assertTrue(bCol.element().isTuple());
    }

    // Simulate a data source, with early schema, of (a)

    TupleMetadata tableSchema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .buildSchema();

    NullColumnBuilder builder = new NullColumnBuilder(null, false);
    ResolvedRow rootTuple = new ResolvedRow(builder);
    new ExplicitSchemaProjection(
        scanProj, tableSchema, rootTuple,
        ScanTestUtils.resolvers());

    List<ResolvedColumn> columns = rootTuple.columns();
    assertEquals(2, columns.size());

    // Should have resolved a to a table column, b to a missing map.

    // A is projected

    ResolvedColumn aCol = columns.get(0);
    assertEquals("a", aCol.name());
    assertEquals(ResolvedTableColumn.ID, aCol.nodeType());

    // B is not projected, is implicitly a map

    ResolvedColumn bCol = columns.get(1);
    assertEquals("b", bCol.name());
    assertEquals(ResolvedMapColumn.ID, bCol.nodeType());

    ResolvedMapColumn bMap = (ResolvedMapColumn) bCol;
    ResolvedTuple bMembers = bMap.members();
    assertNotNull(bMembers);
    assertEquals(1, bMembers.columns().size());

    // C is a map within b

    ResolvedColumn cCol = bMembers.columns().get(0);
    assertEquals(ResolvedMapColumn.ID, cCol.nodeType());

    ResolvedMapColumn cMap = (ResolvedMapColumn) cCol;
    ResolvedTuple cMembers = cMap.members();
    assertNotNull(cMembers);
    assertEquals(1, cMembers.columns().size());

    // D is an unknown column type (not a map)

    ResolvedColumn dCol = cMembers.columns().get(0);
    assertEquals(ResolvedNullColumn.ID, dCol.nodeType());
  }

  /**
   * Test of a map with missing columns.
   * table of (a{b, c}), project a.c, a.d, a.e.f
   */

  @Test
  public void testOmittedMapMembers() {

    // Simulate SELECT a.c, a.d, a.e.f ...

    ScanLevelProjection scanProj = new ScanLevelProjection(
        RowSetTestUtils.projectList("x", "a.c", "a.d", "a.e.f", "y"),
        ScanTestUtils.parsers());
    assertEquals(3, scanProj.columns().size());

    // Simulate a data source, with early schema, of (x, y, a{b, c})

    TupleMetadata tableSchema = new SchemaBuilder()
        .add("x", MinorType.VARCHAR)
        .add("y", MinorType.INT)
        .addMap("a")
          .add("b", MinorType.BIGINT)
          .add("c", MinorType.FLOAT8)
          .resumeSchema()
        .buildSchema();

    NullColumnBuilder builder = new NullColumnBuilder(null, false);
    ResolvedRow rootTuple = new ResolvedRow(builder);
    new ExplicitSchemaProjection(
        scanProj, tableSchema, rootTuple,
        ScanTestUtils.resolvers());

    List<ResolvedColumn> columns = rootTuple.columns();
    assertEquals(3, columns.size());

    // Should have resolved a.b to a map column,
    // a.d to a missing nested map, and a.e.f to a missing
    // nested map member

    // X is projected

    ResolvedColumn xCol = columns.get(0);
    assertEquals("x", xCol.name());
    assertEquals(ResolvedTableColumn.ID, xCol.nodeType());
    assertSame(rootTuple, ((ResolvedTableColumn) (xCol)).source());
    assertEquals(0, ((ResolvedTableColumn) (xCol)).sourceIndex());

    // Y is projected

    ResolvedColumn yCol = columns.get(2);
    assertEquals("y", yCol.name());
    assertEquals(ResolvedTableColumn.ID, yCol.nodeType());
    assertSame(rootTuple, ((ResolvedTableColumn) (yCol)).source());
    assertEquals(1, ((ResolvedTableColumn) (yCol)).sourceIndex());

    // A is projected

    ResolvedColumn aCol = columns.get(1);
    assertEquals("a", aCol.name());
    assertEquals(ResolvedMapColumn.ID, aCol.nodeType());

    ResolvedMapColumn aMap = (ResolvedMapColumn) aCol;
    ResolvedTuple aMembers = aMap.members();
    assertFalse(aMembers.isSimpleProjection());
    assertNotNull(aMembers);
    assertEquals(3, aMembers.columns().size());

    // a.c is projected

    ResolvedColumn acCol = aMembers.columns().get(0);
    assertEquals("c", acCol.name());
    assertEquals(ResolvedTableColumn.ID, acCol.nodeType());
    assertEquals(1, ((ResolvedTableColumn) (acCol)).sourceIndex());

    // a.d is not in the table, is null

    ResolvedColumn adCol = aMembers.columns().get(1);
    assertEquals("d", adCol.name());
    assertEquals(ResolvedNullColumn.ID, adCol.nodeType());

    // a.e is not in the table, is implicitly a map

    ResolvedColumn aeCol = aMembers.columns().get(2);
    assertEquals("e", aeCol.name());
    assertEquals(ResolvedMapColumn.ID, aeCol.nodeType());

    ResolvedMapColumn aeMap = (ResolvedMapColumn) aeCol;
    ResolvedTuple aeMembers = aeMap.members();
    assertFalse(aeMembers.isSimpleProjection());
    assertNotNull(aeMembers);
    assertEquals(1, aeMembers.columns().size());

    // a.d.f is a null column

    ResolvedColumn aefCol = aeMembers.columns().get(0);
    assertEquals("f", aefCol.name());
    assertEquals(ResolvedNullColumn.ID, aefCol.nodeType());
  }

  /**
   * Simple map project. This is an internal case in which the
   * query asks for a set of columns inside a map, and the table
   * loader produces exactly that set. No special projection is
   * needed, the map is projected as a whole.
   */

  @Test
  public void testSimpleMapProject() {

    // Simulate SELECT a.b, a.c ...

    ScanLevelProjection scanProj = new ScanLevelProjection(
        RowSetTestUtils.projectList("a.b", "a.c"),
        ScanTestUtils.parsers());
    assertEquals(1, scanProj.columns().size());

    // Simulate a data source, with early schema, of (a{b, c})

    TupleMetadata tableSchema = new SchemaBuilder()
        .addMap("a")
          .add("b", MinorType.BIGINT)
          .add("c", MinorType.FLOAT8)
          .resumeSchema()
        .buildSchema();

    NullColumnBuilder builder = new NullColumnBuilder(null, false);
    ResolvedRow rootTuple = new ResolvedRow(builder);
    new ExplicitSchemaProjection(
        scanProj, tableSchema, rootTuple,
        ScanTestUtils.resolvers());

    List<ResolvedColumn> columns = rootTuple.columns();
    assertEquals(1, columns.size());

    // Should have resolved a.b to a map column,
    // a.d to a missing nested map, and a.e.f to a missing
    // nested map member

    // a is projected as a vector, not as a structured map

    ResolvedColumn aCol = columns.get(0);
    assertEquals("a", aCol.name());
    assertEquals(ResolvedTableColumn.ID, aCol.nodeType());
    assertSame(rootTuple, ((ResolvedTableColumn) (aCol)).source());
    assertEquals(0, ((ResolvedTableColumn) (aCol)).sourceIndex());
  }

  /**
   * Project of a non-map as a map
   */

  @Test
  public void testMapMismatch() {

    // Simulate SELECT a.b ...

    ScanLevelProjection scanProj = new ScanLevelProjection(
        RowSetTestUtils.projectList("a.b"),
        ScanTestUtils.parsers());

    // Simulate a data source, with early schema, of (a)
    // where a is not a map.

    TupleMetadata tableSchema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .buildSchema();

    NullColumnBuilder builder = new NullColumnBuilder(null, false);
    ResolvedRow rootTuple = new ResolvedRow(builder);
    try {
      new ExplicitSchemaProjection(
          scanProj, tableSchema, rootTuple,
          ScanTestUtils.resolvers());
      fail();
    } catch (UserException e) {
      // Expected
    }
  }

  /**
   * Test project of an array. At the scan level, we just verify
   * that the requested column is, indeed, an array.
   */

  @Test
  public void testArrayProject() {

    // Simulate SELECT a[0] ...

    ScanLevelProjection scanProj = new ScanLevelProjection(
        RowSetTestUtils.projectList("a[0]"),
        ScanTestUtils.parsers());

    // Simulate a data source, with early schema, of (a)
    // where a is not an array.

    TupleMetadata tableSchema = new SchemaBuilder()
        .addArray("a", MinorType.VARCHAR)
        .buildSchema();

    NullColumnBuilder builder = new NullColumnBuilder(null, false);
    ResolvedRow rootTuple = new ResolvedRow(builder);
    new ExplicitSchemaProjection(
          scanProj, tableSchema, rootTuple,
          ScanTestUtils.resolvers());

    List<ResolvedColumn> columns = rootTuple.columns();
    assertEquals(1, columns.size());

    ResolvedColumn aCol = columns.get(0);
    assertEquals("a", aCol.name());
    assertEquals(ResolvedTableColumn.ID, aCol.nodeType());
    assertSame(rootTuple, ((ResolvedTableColumn) (aCol)).source());
    assertEquals(0, ((ResolvedTableColumn) (aCol)).sourceIndex());
  }

  /**
   * Project of a non-array as an array
   */

  @Test
  public void testArrayMismatch() {

    // Simulate SELECT a[0] ...

    ScanLevelProjection scanProj = new ScanLevelProjection(
        RowSetTestUtils.projectList("a[0]"),
        ScanTestUtils.parsers());

    // Simulate a data source, with early schema, of (a)
    // where a is not an array.

    TupleMetadata tableSchema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .buildSchema();

    NullColumnBuilder builder = new NullColumnBuilder(null, false);
    ResolvedRow rootTuple = new ResolvedRow(builder);
    try {
      new ExplicitSchemaProjection(
          scanProj, tableSchema, rootTuple,
          ScanTestUtils.resolvers());
      fail();
    } catch (UserException e) {
      // Expected
    }
  }
}
