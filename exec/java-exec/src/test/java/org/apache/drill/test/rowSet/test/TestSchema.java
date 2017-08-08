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
package org.apache.drill.test.rowSet.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.TupleMetadata;
import org.apache.drill.exec.record.TupleMetadata.ColumnMetadata;
import org.apache.drill.exec.record.TupleMetadata.StructureType;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.junit.Test;

public class TestSchema extends SubOperatorTest {

  /**
   * Test a simple physical schema with no maps.
   */

  @Test
  public void testSchema() {
    TupleMetadata tupleSchema = new SchemaBuilder()
        .add("c", MinorType.INT)
        .add("a", MinorType.INT, DataMode.REPEATED)
        .addNullable("b", MinorType.VARCHAR)
        .buildSchema();

    assertEquals(3, tupleSchema.size());
    assertFalse(tupleSchema.isEmpty());

    assertEquals("c", tupleSchema.column(0).getName());
    assertEquals("a", tupleSchema.column(1).getName());
    assertEquals("b", tupleSchema.column(2).getName());

    ColumnMetadata md0 = tupleSchema.metadata(0);
    assertEquals(StructureType.PRIMITIVE, md0.structureType());
    assertNull(md0.mapSchema());
    assertEquals(0, md0.index());
    assertSame(md0.schema(), tupleSchema.column(0));
    assertEquals(md0.name(), tupleSchema.column(0).getName());
    assertEquals(MinorType.INT, md0.type());
    assertEquals(DataMode.REQUIRED, md0.mode());
    assertSame(tupleSchema, md0.parent());
    assertEquals(md0.name(), md0.fullName());
    assertTrue(md0.isEquivalent(md0));
    assertFalse(md0.isEquivalent(tupleSchema.metadata(1)));

    assertEquals(1, tupleSchema.metadata(1).index());
    assertEquals(DataMode.REPEATED, tupleSchema.metadata(1).mode());
    assertEquals(2, tupleSchema.metadata(2).index());
    assertEquals(DataMode.OPTIONAL, tupleSchema.metadata(2).mode());

    assertSame(tupleSchema.column(0), tupleSchema.column("c"));
    assertSame(tupleSchema.column(1), tupleSchema.column("a"));
    assertSame(tupleSchema.column(2), tupleSchema.column("b"));

    assertSame(tupleSchema.metadata(0), tupleSchema.metadata("c"));
    assertSame(tupleSchema.metadata(1), tupleSchema.metadata("a"));
    assertSame(tupleSchema.metadata(2), tupleSchema.metadata("b"));
    assertEquals(0, tupleSchema.index("c"));
    assertEquals(1, tupleSchema.index("a"));
    assertEquals(2, tupleSchema.index("b"));

    // Test undefined column

    assertEquals(-1, tupleSchema.index("x"));
    assertNull(tupleSchema.metadata("x"));
    assertNull(tupleSchema.column("x"));

    try {
      tupleSchema.metadata(4);
      fail();
    } catch (IndexOutOfBoundsException e) {
      // Expected
    }

    try {
      tupleSchema.column(4);
      fail();
    } catch (IndexOutOfBoundsException e) {
      // Expected
    }

    // No maps. Flat schema is the same as tuple schema.

//    TupleMetadata flatSchema = tupleSchema.flatten();
//    assertEquals(3, flatSchema.size());
//
//    crossCheck(flatSchema, 0, "c", MinorType.INT);
//    assertEquals(DataMode.REQUIRED, flatSchema.column(0).getDataMode());
//    assertEquals(DataMode.REQUIRED, flatSchema.column(0).getType().getMode());
//    assertTrue(! flatSchema.column(0).isNullable());
//
//    crossCheck(flatSchema, 1, "a", MinorType.INT);
//    assertEquals(DataMode.REPEATED, flatSchema.column(1).getDataMode());
//    assertEquals(DataMode.REPEATED, flatSchema.column(1).getType().getMode());
//    assertTrue(! flatSchema.column(1).isNullable());
//
//    crossCheck(flatSchema, 2, "b", MinorType.VARCHAR);
//    assertEquals(MinorType.VARCHAR, flatSchema.column(2).getType().getMinorType());
//    assertEquals(DataMode.OPTIONAL, flatSchema.column(2).getDataMode());
//    assertEquals(DataMode.OPTIONAL, flatSchema.column(2).getType().getMode());
//    assertTrue(flatSchema.column(2).isNullable());

    // Verify batch schema
    // Tests toFieldList() internally

    BatchSchema batchSchema = new BatchSchema(SelectionVectorMode.NONE, tupleSchema.toFieldList());
    assertEquals(3, batchSchema.getFieldCount());
    assertSame(batchSchema.getColumn(0), tupleSchema.column(0));
    assertSame(batchSchema.getColumn(1), tupleSchema.column(1));
    assertSame(batchSchema.getColumn(2), tupleSchema.column(2));
  }

  @Test
  public void testEmptySchema() {
    TupleMetadata tupleSchema = new SchemaBuilder()
        .buildSchema();

    assertEquals(0, tupleSchema.size());
    assertTrue(tupleSchema.isEmpty());
  }

  @Test
  public void testDuplicateName() {
    try {
      new SchemaBuilder()
          .add("foo", MinorType.INT)
          .add("a", MinorType.INT, DataMode.REPEATED)
          .addNullable("foo", MinorType.VARCHAR)
          .buildSchema();
      fail();
    } catch (IllegalArgumentException e) {
      // Expected
      assertTrue(e.getMessage().contains("foo"));
    }
  }

  @Test
  public void testSVMode() {
    BatchSchema batchSchema = new SchemaBuilder()
        .add("c", MinorType.INT)
        .add("a", MinorType.INT, DataMode.REPEATED)
        .addNullable("b", MinorType.VARCHAR)
        .withSVMode(SelectionVectorMode.TWO_BYTE)
        .build();

    assertEquals(3, batchSchema.getFieldCount());
    assertEquals(SelectionVectorMode.TWO_BYTE, batchSchema.getSelectionVectorMode());
  }

  /**
   * Verify that a nested map schema.
   * Schema has non-repeated maps, so can be flattened.
   */

  @Test
  public void testMapSchema() {
    TupleMetadata tupleSchema = new SchemaBuilder()
        .add("c", MinorType.INT)
        .addMap("a")
          .addNullable("b", MinorType.VARCHAR)
          .add("d", MinorType.INT)
          .addMap("e")
            .add("f", MinorType.VARCHAR)
            .buildMap()
          .add("g", MinorType.INT)
          .buildMap()
        .add("h", MinorType.BIGINT)
        .buildSchema();

    assertEquals(3, tupleSchema.size());
    assertEquals("c", tupleSchema.metadata(0).name());
    assertEquals("c", tupleSchema.metadata(0).fullName());
    assertEquals(StructureType.PRIMITIVE, tupleSchema.metadata(0).structureType());
    assertNull(tupleSchema.metadata(0).mapSchema());

    assertEquals("a", tupleSchema.metadata(1).name());
    assertEquals("a", tupleSchema.metadata(1).fullName());
    assertEquals(StructureType.TUPLE, tupleSchema.metadata(1).structureType());
    assertNotNull(tupleSchema.metadata(1).mapSchema());

    assertEquals("h", tupleSchema.metadata(2).name());
    assertEquals("h", tupleSchema.metadata(2).fullName());
    assertEquals(StructureType.PRIMITIVE, tupleSchema.metadata(2).structureType());
    assertNull(tupleSchema.metadata(2).mapSchema());

    TupleMetadata aSchema = tupleSchema.metadata(1).mapSchema();
    assertEquals(4, aSchema.size());
    assertEquals("b", aSchema.metadata(0).name());
    assertEquals("a.b", aSchema.metadata(0).fullName());
    assertEquals("d", aSchema.metadata(1).name());
    assertEquals("e", aSchema.metadata(2).name());
    assertEquals("g", aSchema.metadata(3).name());

    TupleMetadata eSchema = aSchema.metadata(2).mapSchema();
    assertEquals(1, eSchema.size());
    assertEquals("f", eSchema.metadata(0).name());
    assertEquals("a.e.f", eSchema.metadata(0).fullName());

    // Flattened with maps removed. This is for testing use only
    // as it is ambiguous in production.

//    TupleMetadata flatSchema = tupleSchema.flatten();
//    assertEquals(6, flatSchema.size());
//    crossCheck(flatSchema, 0, "c", MinorType.INT);
//    crossCheck(flatSchema, 1, "a.b", MinorType.VARCHAR);
//    crossCheck(flatSchema, 2, "a.d", MinorType.INT);
//    crossCheck(flatSchema, 3, "a.e.f", MinorType.VARCHAR);
//    crossCheck(flatSchema, 4, "a.g", MinorType.INT);
//    crossCheck(flatSchema, 5, "h", MinorType.BIGINT);

    // Verify batch schema: should mirror the schema created above.

    BatchSchema batchSchema = new BatchSchema(SelectionVectorMode.NONE, tupleSchema.toFieldList());
    assertEquals(3, batchSchema.getFieldCount());
    assertSame(tupleSchema.column(0), batchSchema.getColumn(0));
    assertSame(tupleSchema.column(2), batchSchema.getColumn(2));

    assertEquals("a", batchSchema.getColumn(1).getName());
    assertEquals(MinorType.MAP, batchSchema.getColumn(1).getType().getMinorType());
    assertNotNull(batchSchema.getColumn(1).getChildren());

    List<MaterializedField> aMap = new ArrayList<>();
    for (MaterializedField field : batchSchema.getColumn(1).getChildren()) {
      aMap.add(field);
    }
    assertEquals(4, aMap.size());
    assertSame(aMap.get(0), aSchema.column(0));
    assertSame(aMap.get(1), aSchema.column(1));
    assertSame(aMap.get(2), aSchema.column(2));
    assertSame(aMap.get(3), aSchema.column(3));

    List<MaterializedField> eMap = new ArrayList<>();
    for (MaterializedField field : aMap.get(2).getChildren()) {
      eMap.add(field);
    }
    assertEquals(1, eMap.size());
    assertSame(eSchema.column(0), eMap.get(0));
  }

}
