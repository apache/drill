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
package org.apache.drill.exec.physical.impl.validate;

import static org.apache.drill.test.rowSet.RowSetUtilities.intArray;
import static org.apache.drill.test.rowSet.RowSetUtilities.strArray;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.vector.RepeatedVarCharVector;
import org.apache.drill.exec.vector.UInt4Vector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VarCharVector;
import org.apache.drill.test.BaseDirTestWatcher;
import org.apache.drill.test.LogFixture;
import org.apache.drill.test.OperatorFixture;
import org.apache.drill.test.rowSet.RowSet.SingleRowSet;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import ch.qos.logback.classic.Level;

public class TestBatchValidator /* TODO: extends SubOperatorTest */ {

  protected static OperatorFixture fixture;
  protected static LogFixture logFixture;

  @ClassRule
  public static final BaseDirTestWatcher dirTestWatcher = new BaseDirTestWatcher();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    logFixture = LogFixture.builder()
        .toConsole()
        .logger(BatchValidator.class, Level.TRACE)
        .build();
    fixture = OperatorFixture.standardFixture(dirTestWatcher);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    fixture.close();
    logFixture.close();
  }

  @Test
  public void testValidFixed() {
    BatchSchema schema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .addNullable("b", MinorType.INT)
        .build();

    SingleRowSet batch = fixture.rowSetBuilder(schema)
        .addRow(10, 100)
        .addRow(20, 120)
        .addRow(30, null)
        .addRow(40, 140)
        .build();

    BatchValidator validator = new BatchValidator(batch.vectorAccessible(), true);
    validator.validate();
    assertTrue(validator.errors().isEmpty());
    batch.clear();
  }

  @Test
  public void testValidVariable() {
    BatchSchema schema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .addNullable("b", MinorType.VARCHAR)
        .build();

    SingleRowSet batch = fixture.rowSetBuilder(schema)
        .addRow("col1.1", "col1.2")
        .addRow("col2.1", "col2.2")
        .addRow("col3.1", null)
        .addRow("col4.1", "col4.2")
        .build();

    BatchValidator validator = new BatchValidator(batch.vectorAccessible(), true);
    validator.validate();
    assertTrue(validator.errors().isEmpty());
    batch.clear();
  }

  @Test
  public void testValidRepeated() {
    BatchSchema schema = new SchemaBuilder()
        .add("a", MinorType.INT, DataMode.REPEATED)
        .add("b", MinorType.VARCHAR, DataMode.REPEATED)
        .build();

    SingleRowSet batch = fixture.rowSetBuilder(schema)
        .addRow(intArray(), strArray())
        .addRow(intArray(1, 2, 3), strArray("fred", "barney", "wilma"))
        .addRow(intArray(4), strArray("dino"))
        .build();

    BatchValidator validator = new BatchValidator(batch.vectorAccessible(), true);
    validator.validate();
    assertTrue(validator.errors().isEmpty());
    batch.clear();
  }

  @Test
  public void testVariableMissingLast() {
    BatchSchema schema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .build();

    SingleRowSet batch = fixture.rowSetBuilder(schema)
        .addRow("x")
        .addRow("y")
        .addRow("z")
        .build();

    // Here we are evil: stomp on the last offset to simulate corruption.
    // Don't do this in real code!

    VectorAccessible va = batch.vectorAccessible();
    ValueVector v = va.iterator().next().getValueVector();
    VarCharVector vc = (VarCharVector) v;
    UInt4Vector ov = vc.getOffsetVector();
    assertTrue(ov.getAccessor().get(3) > 0);
    ov.getMutator().set(3, 0);

    // Validator should catch the error.

    BatchValidator validator = new BatchValidator(batch.vectorAccessible(), true);
    validator.validate();
    List<String> errors = validator.errors();
    assertEquals(1, errors.size());
    assertTrue(errors.get(0).contains("Decreasing offsets"));
    batch.clear();
  }

  @Test
  public void testVariableCorruptFirst() {
    BatchSchema schema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .build();

    SingleRowSet batch = fixture.rowSetBuilder(schema)
        .addRow("x")
        .addRow("y")
        .addRow("z")
        .build();

    zapOffset(batch, 0, 1);

    // Validator should catch the error.

    BatchValidator validator = new BatchValidator(batch.vectorAccessible(), true);
    validator.validate();
    List<String> errors = validator.errors();
    assertEquals(1, errors.size());
    assertTrue(errors.get(0).contains("Offset (0) must be 0"));
    batch.clear();
  }

  public void zapOffset(SingleRowSet batch, int index, int bogusValue) {

    // Here we are evil: stomp on an offset to simulate corruption.
    // Don't do this in real code!

    VectorAccessible va = batch.vectorAccessible();
    ValueVector v = va.iterator().next().getValueVector();
    VarCharVector vc = (VarCharVector) v;
    UInt4Vector ov = vc.getOffsetVector();
    ov.getMutator().set(index, bogusValue);
  }

  @Test
  public void testVariableCorruptMiddleLow() {
    BatchSchema schema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .build();

    SingleRowSet batch = fixture.rowSetBuilder(schema)
        .addRow("xx")
        .addRow("yy")
        .addRow("zz")
        .build();

    zapOffset(batch, 2, 1);

    // Validator should catch the error.

    BatchValidator validator = new BatchValidator(batch.vectorAccessible(), true);
    validator.validate();
    List<String> errors = validator.errors();
    assertEquals(1, errors.size());
    assertTrue(errors.get(0).contains("Decreasing offsets"));
    batch.clear();
  }

  @Test
  public void testVariableCorruptMiddleHigh() {
    BatchSchema schema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .build();

    SingleRowSet batch = fixture.rowSetBuilder(schema)
        .addRow("xx")
        .addRow("yy")
        .addRow("zz")
        .build();

    zapOffset(batch, 1, 10);

    // Validator should catch the error.

    BatchValidator validator = new BatchValidator(batch.vectorAccessible(), true);
    validator.validate();
    List<String> errors = validator.errors();
    assertEquals(1, errors.size());
    assertTrue(errors.get(0).contains("Decreasing offsets"));
    batch.clear();
  }

  @Test
  public void testVariableCorruptLastOutOfRange() {
    BatchSchema schema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .build();

    SingleRowSet batch = fixture.rowSetBuilder(schema)
        .addRow("xx")
        .addRow("yy")
        .addRow("zz")
        .build();

    zapOffset(batch, 3, 100_000);

    // Validator should catch the error.

    BatchValidator validator = new BatchValidator(batch.vectorAccessible(), true);
    validator.validate();
    List<String> errors = validator.errors();
    assertEquals(1, errors.size());
    assertTrue(errors.get(0).contains("Invalid offset"));
    batch.clear();
  }

  @Test
  public void testRepeatedBadArrayOffset() {
    BatchSchema schema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR, DataMode.REPEATED)
        .build();

    SingleRowSet batch = fixture.rowSetBuilder(schema)
        .addRow((Object) strArray())
        .addRow((Object) strArray("fred", "barney", "wilma"))
        .addRow((Object) strArray("dino"))
        .build();

    VectorAccessible va = batch.vectorAccessible();
    ValueVector v = va.iterator().next().getValueVector();
    RepeatedVarCharVector vc = (RepeatedVarCharVector) v;
    UInt4Vector ov = vc.getOffsetVector();
    ov.getMutator().set(3, 1);

    BatchValidator validator = new BatchValidator(batch.vectorAccessible(), true);
    validator.validate();
    List<String> errors = validator.errors();
    assertEquals(1, errors.size());
    assertTrue(errors.get(0).contains("Decreasing offsets"));
    batch.clear();
  }

  @Test
  public void testRepeatedBadValueOffset() {
    BatchSchema schema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR, DataMode.REPEATED)
        .build();

    SingleRowSet batch = fixture.rowSetBuilder(schema)
        .addRow((Object) strArray())
        .addRow((Object) strArray("fred", "barney", "wilma"))
        .addRow((Object) strArray("dino"))
        .build();

    VectorAccessible va = batch.vectorAccessible();
    ValueVector v = va.iterator().next().getValueVector();
    RepeatedVarCharVector rvc = (RepeatedVarCharVector) v;
    VarCharVector vc = rvc.getDataVector();
    UInt4Vector ov = vc.getOffsetVector();
    ov.getMutator().set(4, 100_000);

    BatchValidator validator = new BatchValidator(batch.vectorAccessible(), true);
    validator.validate();
    List<String> errors = validator.errors();
    assertEquals(1, errors.size());
    assertTrue(errors.get(0).contains("Invalid offset"));
    batch.clear();
  }
}
