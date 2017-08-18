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
package org.apache.drill.exec.record;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Iterator;
import java.util.List;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.TupleSchema.MapColumnMetadata;
import org.apache.drill.exec.record.TupleSchema.PrimitiveColumnMetadata;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.junit.Test;

/**
 * Test the tuple and column metadata, including extended attributes.
 */

public class TestTupleSchema extends SubOperatorTest {

  /**
   * Test a fixed-width, primitive, required column. Includes basic
   * tests common to all data types. (Basic tests are not repeated for
   * other types.)
   */

  @Test
  public void testRequiredFixedWidthColumn() {

    MaterializedField field = SchemaBuilder.columnSchema("c", MinorType.INT, DataMode.REQUIRED );
    ColumnMetadata col = TupleSchema.fromField(field);

    // Code may depend on the specific column class

    assertTrue(col instanceof PrimitiveColumnMetadata);

    // Generic checks

    assertEquals(ColumnMetadata.StructureType.PRIMITIVE, col.structureType());
    assertNull(col.mapSchema());
    assertSame(field, col.schema());
    assertEquals(field.getName(), col.name());
    assertEquals(field.getType(), col.majorType());
    assertEquals(field.getType().getMinorType(), col.type());
    assertEquals(field.getDataMode(), col.mode());
    assertFalse(col.isNullable());
    assertFalse(col.isArray());
    assertFalse(col.isVariableWidth());
    assertFalse(col.isMap());
    assertFalse(col.isList());
    assertTrue(col.isEquivalent(col));

    ColumnMetadata col2 = TupleSchema.fromField(field);
    assertTrue(col.isEquivalent(col2));

    MaterializedField field3 = SchemaBuilder.columnSchema("d", MinorType.INT, DataMode.REQUIRED );
    ColumnMetadata col3 = TupleSchema.fromField(field3);
    assertFalse(col.isEquivalent(col3));

    MaterializedField field4 = SchemaBuilder.columnSchema("c", MinorType.BIGINT, DataMode.REQUIRED );
    ColumnMetadata col4 = TupleSchema.fromField(field4);
    assertFalse(col.isEquivalent(col4));

    MaterializedField field5 = SchemaBuilder.columnSchema("c", MinorType.INT, DataMode.OPTIONAL );
    ColumnMetadata col5 = TupleSchema.fromField(field5);
    assertFalse(col.isEquivalent(col5));

    ColumnMetadata col6 = col.cloneEmpty();
    assertTrue(col.isEquivalent(col6));

    assertEquals(4, col.expectedWidth());
    col.setExpectedWidth(10);
    assertEquals(4, col.expectedWidth());

    assertEquals(1, col.expectedElementCount());
    col.setExpectedElementCount(2);
    assertEquals(1, col.expectedElementCount());
  }

  @Test
  public void testNullableFixedWidthColumn() {

    MaterializedField field = SchemaBuilder.columnSchema("c", MinorType.INT, DataMode.OPTIONAL );
    ColumnMetadata col = TupleSchema.fromField(field);

    assertEquals(ColumnMetadata.StructureType.PRIMITIVE, col.structureType());
    assertTrue(col.isNullable());
    assertFalse(col.isArray());
    assertFalse(col.isVariableWidth());
    assertFalse(col.isMap());
    assertFalse(col.isList());

    assertEquals(4, col.expectedWidth());
    col.setExpectedWidth(10);
    assertEquals(4, col.expectedWidth());

    assertEquals(1, col.expectedElementCount());
    col.setExpectedElementCount(2);
    assertEquals(1, col.expectedElementCount());
  }

  @Test
  public void testRepeatedFixedWidthColumn() {

    MaterializedField field = SchemaBuilder.columnSchema("c", MinorType.INT, DataMode.REPEATED );
    ColumnMetadata col = TupleSchema.fromField(field);

    assertFalse(col.isNullable());
    assertTrue(col.isArray());
    assertFalse(col.isVariableWidth());
    assertFalse(col.isMap());
    assertFalse(col.isList());

    assertEquals(4, col.expectedWidth());
    col.setExpectedWidth(10);
    assertEquals(4, col.expectedWidth());

    assertEquals(ColumnMetadata.DEFAULT_ARRAY_SIZE, col.expectedElementCount());

    col.setExpectedElementCount(2);
    assertEquals(2, col.expectedElementCount());

    col.setExpectedElementCount(0);
    assertEquals(1, col.expectedElementCount());
  }

  @Test
  public void testRequiredVariableWidthColumn() {

    MaterializedField field = SchemaBuilder.columnSchema("c", MinorType.VARCHAR, DataMode.REQUIRED );
    ColumnMetadata col = TupleSchema.fromField(field);

    assertEquals(ColumnMetadata.StructureType.PRIMITIVE, col.structureType());
    assertNull(col.mapSchema());
    assertFalse(col.isNullable());
    assertFalse(col.isArray());
    assertTrue(col.isVariableWidth());
    assertFalse(col.isMap());
    assertFalse(col.isList());

    // A different precision is a different type.

    MaterializedField field2 = new SchemaBuilder.ColumnBuilder("c", MinorType.VARCHAR)
        .setMode(DataMode.REQUIRED)
        .setPrecision(10)
        .build();

    ColumnMetadata col2 = TupleSchema.fromField(field2);
    assertFalse(col.isEquivalent(col2));

    assertEquals(50, col.expectedWidth());
    col.setExpectedWidth(10);
    assertEquals(10, col.expectedWidth());

    assertEquals(1, col.expectedElementCount());
    col.setExpectedElementCount(2);
    assertEquals(1, col.expectedElementCount());

    // If precision is provided, then that is the default width

    col = TupleSchema.fromField(field2);
    assertEquals(10, col.expectedWidth());
  }

  @Test
  public void testNullableVariableWidthColumn() {

    MaterializedField field = SchemaBuilder.columnSchema("c", MinorType.VARCHAR, DataMode.OPTIONAL );
    ColumnMetadata col = TupleSchema.fromField(field);

    assertTrue(col.isNullable());
    assertFalse(col.isArray());
    assertTrue(col.isVariableWidth());
    assertFalse(col.isMap());
    assertFalse(col.isList());

    assertEquals(50, col.expectedWidth());
    col.setExpectedWidth(10);
    assertEquals(10, col.expectedWidth());

    assertEquals(1, col.expectedElementCount());
    col.setExpectedElementCount(2);
    assertEquals(1, col.expectedElementCount());
  }

  @Test
  public void testRepeatedVariableWidthColumn() {

    MaterializedField field = SchemaBuilder.columnSchema("c", MinorType.VARCHAR, DataMode.REPEATED );
    ColumnMetadata col = TupleSchema.fromField(field);

    assertFalse(col.isNullable());
    assertTrue(col.isArray());
    assertTrue(col.isVariableWidth());
    assertFalse(col.isMap());
    assertFalse(col.isList());

    assertEquals(50, col.expectedWidth());
    col.setExpectedWidth(10);
    assertEquals(10, col.expectedWidth());

    assertEquals(ColumnMetadata.DEFAULT_ARRAY_SIZE, col.expectedElementCount());

    col.setExpectedElementCount(2);
    assertEquals(2, col.expectedElementCount());
  }

  /**
   * Tests a map column. Maps can only be required or repeated, not nullable.
   * (But, the columns in the map can be nullable.)
   */

  @Test
  public void testMapColumn() {

    MaterializedField field = SchemaBuilder.columnSchema("m", MinorType.MAP, DataMode.REQUIRED );
    ColumnMetadata col = TupleSchema.fromField(field);

    assertTrue(col instanceof MapColumnMetadata);
    assertNotNull(col.mapSchema());
    assertEquals(0, col.mapSchema().size());
    assertSame(col, col.mapSchema().parent());

    MapColumnMetadata mapCol = (MapColumnMetadata) col;
    assertNull(mapCol.parentTuple());

    assertEquals(ColumnMetadata.StructureType.TUPLE, col.structureType());
    assertFalse(col.isNullable());
    assertFalse(col.isArray());
    assertFalse(col.isVariableWidth());
    assertTrue(col.isMap());
    assertFalse(col.isList());

    assertEquals(0, col.expectedWidth());
    col.setExpectedWidth(10);
    assertEquals(0, col.expectedWidth());

    assertEquals(1, col.expectedElementCount());
    col.setExpectedElementCount(2);
    assertEquals(1, col.expectedElementCount());
  }

  @Test
  public void testRepeatedMapColumn() {

    MaterializedField field = SchemaBuilder.columnSchema("m", MinorType.MAP, DataMode.REPEATED );
    ColumnMetadata col = TupleSchema.fromField(field);

    assertTrue(col instanceof MapColumnMetadata);
    assertNotNull(col.mapSchema());
    assertEquals(0, col.mapSchema().size());

    assertFalse(col.isNullable());
    assertTrue(col.isArray());
    assertFalse(col.isVariableWidth());
    assertTrue(col.isMap());
    assertFalse(col.isList());

    assertEquals(0, col.expectedWidth());
    col.setExpectedWidth(10);
    assertEquals(0, col.expectedWidth());

    assertEquals(ColumnMetadata.DEFAULT_ARRAY_SIZE, col.expectedElementCount());

    col.setExpectedElementCount(2);
    assertEquals(2, col.expectedElementCount());
  }

    // List

    // Repeated list

  /**
   * Test the basics of an empty root tuple (i.e. row) schema.
   */

  @Test
  public void testEmptyRootTuple() {

    TupleMetadata root = new TupleSchema();

    assertEquals(0, root.size());
    assertTrue(root.isEmpty());
    assertEquals(-1, root.index("foo"));

    try {
      root.metadata(0);
      fail();
    } catch (IndexOutOfBoundsException e) {
      // Expected
    }
    assertNull(root.metadata("foo"));

    try {
      root.column(0);
      fail();
    } catch (IndexOutOfBoundsException e) {
      // Expected
    }
    assertNull(root.column("foo"));

    try {
      root.fullName(0);
      fail();
    } catch (IndexOutOfBoundsException e) {
      // Expected
    }

    // The full name method does not check if the column is actually
    // in the tuple.

    MaterializedField field = SchemaBuilder.columnSchema("c", MinorType.INT, DataMode.REQUIRED );
    ColumnMetadata col = TupleSchema.fromField(field);
    assertEquals("c", root.fullName(col));

    assertTrue(root.isEquivalent(root));
    assertNull(root.parent());
    assertTrue(root.toFieldList().isEmpty());
  }

  /**
   * Test the basics of a non-empty root tuple (i.e. a row) using a pair
   * of primitive columns.
   */

  @Test
  public void testNonEmptyRootTuple() {

    TupleMetadata root = new TupleSchema();

    MaterializedField fieldA = SchemaBuilder.columnSchema("a", MinorType.INT, DataMode.REQUIRED );
    ColumnMetadata colA = root.add(fieldA);

    assertEquals(1, root.size());
    assertFalse(root.isEmpty());
    assertEquals(0, root.index("a"));
    assertEquals(-1, root.index("b"));

    assertSame(fieldA, root.column(0));
    assertSame(fieldA, root.column("a"));
    assertSame(fieldA, root.column("A"));

    assertSame(colA, root.metadata(0));
    assertSame(colA, root.metadata("a"));

    assertEquals("a", root.fullName(0));
    assertEquals("a", root.fullName(colA));

    try {
      root.add(fieldA);
      fail();
    } catch (IllegalArgumentException e) {
      // Expected
    }

    MaterializedField fieldB = SchemaBuilder.columnSchema("b", MinorType.VARCHAR, DataMode.OPTIONAL );
    ColumnMetadata colB = TupleSchema.fromField(fieldB);
    int indexB = root.addColumn(colB);

    assertEquals(1, indexB);
    assertEquals(2, root.size());
    assertFalse(root.isEmpty());
    assertEquals(indexB, root.index("b"));

    assertSame(fieldB, root.column(1));
    assertSame(fieldB, root.column("b"));

    assertSame(colB, root.metadata(1));
    assertSame(colB, root.metadata("b"));

    assertEquals("b", root.fullName(1));
    assertEquals("b", root.fullName(colB));

    try {
      root.add(fieldB);
      fail();
    } catch (IllegalArgumentException e) {
      // Expected
    }

    List<MaterializedField> fieldList = root.toFieldList();
    assertSame(fieldA, fieldList.get(0));
    assertSame(fieldB, fieldList.get(1));

    TupleMetadata emptyRoot = new TupleSchema();
    assertFalse(emptyRoot.isEquivalent(root));

    // Same schema: the tuples are equivalent

    TupleMetadata root3 = new TupleSchema();
    root3.add(fieldA);
    root3.addColumn(colB);
    assertTrue(root3.isEquivalent(root));
    assertTrue(root.isEquivalent(root3));

    // Same columns, different order. The tuples are not equivalent.

    TupleMetadata root4 = new TupleSchema();
    root4.addColumn(colB);
    root4.add(fieldA);
    assertFalse(root4.isEquivalent(root));
    assertFalse(root.isEquivalent(root4));

    // A tuple is equivalent to its copy.

    assertTrue(root.isEquivalent(((TupleSchema) root).copy()));

    // And it is equivalent to the round trip to a batch schema.

    BatchSchema batchSchema = ((TupleSchema) root).toBatchSchema(SelectionVectorMode.NONE);
    assertTrue(root.isEquivalent(TupleSchema.fromFields(batchSchema)));
  }

  /**
   * Test a complex map schema of the form:<br>
   * a.`b.x`.`c.y`.d<br>
   * in which columns "a", "b.x" and "c.y" are maps, "b.x" and "c.y" are names
   * that contains dots, and d is primitive.
   */

  @Test
  public void testMapTuple() {

    TupleMetadata root = new TupleSchema();

    MaterializedField fieldA = SchemaBuilder.columnSchema("a", MinorType.MAP, DataMode.REQUIRED);
    ColumnMetadata colA = root.add(fieldA);
    TupleMetadata mapA = colA.mapSchema();

    MaterializedField fieldB = SchemaBuilder.columnSchema("b.x", MinorType.MAP, DataMode.REQUIRED);
    ColumnMetadata colB = mapA.add(fieldB);
    TupleMetadata mapB = colB.mapSchema();

    MaterializedField fieldC = SchemaBuilder.columnSchema("c.y", MinorType.MAP, DataMode.REQUIRED);
    ColumnMetadata colC = mapB.add(fieldC);
    TupleMetadata mapC = colC.mapSchema();

    MaterializedField fieldD = SchemaBuilder.columnSchema("d", MinorType.VARCHAR, DataMode.REQUIRED);
    ColumnMetadata colD = mapC.add(fieldD);

    MaterializedField fieldE = SchemaBuilder.columnSchema("e", MinorType.INT, DataMode.REQUIRED);
    ColumnMetadata colE = mapC.add(fieldE);

    assertEquals(1, root.size());
    assertEquals(1, mapA.size());
    assertEquals(1, mapB.size());
    assertEquals(2, mapC.size());

    assertSame(colA, root.metadata("a"));
    assertSame(colB, mapA.metadata("b.x"));
    assertSame(colC, mapB.metadata("c.y"));
    assertSame(colD, mapC.metadata("d"));
    assertSame(colE, mapC.metadata("e"));

    // The full name contains quoted names if the contain dots.
    // This name is more for diagnostic than semantic purposes.

    assertEquals("a", root.fullName(0));
    assertEquals("a.`b.x`", mapA.fullName(0));
    assertEquals("a.`b.x`.`c.y`", mapB.fullName(0));
    assertEquals("a.`b.x`.`c.y`.d", mapC.fullName(0));
    assertEquals("a.`b.x`.`c.y`.e", mapC.fullName(1));

    assertEquals(1, colA.schema().getChildren().size());
    assertEquals(1, colB.schema().getChildren().size());
    assertEquals(2, colC.schema().getChildren().size());

    // Yes, it is awful that MaterializedField does not provide indexed
    // access to its children. That's one reason we have the TupleMetadata
    // classes..

    assertSame(fieldB, colA.schema().getChildren().iterator().next());
    assertSame(fieldC, colB.schema().getChildren().iterator().next());
    Iterator<MaterializedField> iterC = colC.schema().getChildren().iterator();
    assertSame(fieldD, iterC.next());
    assertSame(fieldE, iterC.next());

    // Copying should be deep.

    TupleMetadata root2 = ((TupleSchema) root).copy();
    assertEquals(2, root2.metadata(0).mapSchema().metadata(0).mapSchema().metadata(0).mapSchema().size());
    assert(root.isEquivalent(root2));
  }
}
