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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.drill.categories.RowSetTests;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.physical.impl.scan.ScanTestUtils.ScanFixture;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedReader;
import org.apache.drill.exec.physical.impl.scan.framework.SchemaNegotiator;
import org.apache.drill.exec.physical.rowSet.ResultSetLoader;
import org.apache.drill.exec.physical.rowSet.RowSetLoader;
import org.apache.drill.exec.physical.rowSet.impl.RowSetTestUtils;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.test.rowSet.RowSet.SingleRowSet;
import org.apache.drill.test.rowSet.RowSetUtilities;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test the addition of an output schema to a reader. The output schema
 * defines the schema to be output from the scan operator, and forces
 * conversions between reader and output data types.
 */
@Category(RowSetTests.class)
public class TestScanOperExecOuputSchema extends BaseScanOperatorExecTest {

  private static class MockSimpleReader implements ManagedReader<SchemaNegotiator> {

    private ResultSetLoader tableLoader;

    @Override
    public boolean open(SchemaNegotiator schemaNegotiator) {
      TupleMetadata schema = new SchemaBuilder()
          // Schema provided in test
          .add("a", MinorType.VARCHAR)
          // No schema provided
          .add("b", MinorType.VARCHAR)
          // No schema and not projected in test
          .add("c", MinorType.VARCHAR)
          .buildSchema();
      schemaNegotiator.setTableSchema(schema, true);
      tableLoader = schemaNegotiator.build();
      return true;
    }

    @Override
    public boolean next() {

      // First batch is schema only, we provide batch 2

      if (tableLoader.batchCount() > 1) {
        return false;
      }
      RowSetLoader writer = tableLoader.writer();
      writer.start();
      writer.scalar(0).setString("10");
      writer.scalar(1).setString("foo");
      writer.scalar(2).setString("bar");
      writer.save();
      return true;
    }

    @Override
    public void close() { }
  }

  /**
   * Test an output schema.
   * <ul>
   * <li>Column a has an input type of VARCHAR, and output type of INT,
   * and the framework will insert an implicit conversion.</li>
   * <li>Column b has an output type of BIGINT, is projected, but is
   * not provided by the reader. It will use the default value of 20L.</li>
   * <li>Column c is not in the output schema, is not provided by the
   * reader, but is projected, so it will use the default null type
   * of VARCHAR, with a null value.</li>
   * </ul>
   */

  @Test
  public void testOutputSchema() {
    TupleMetadata outputSchema = new SchemaBuilder()
        .add("a", MinorType.INT) // Projected, in reader
        .add("d", MinorType.BIGINT) // Projected, not in reader
        .add("e", MinorType.BIGINT) // Not projected, not in reader
        .buildSchema();
    outputSchema.metadata("d").setDefaultValue("20");
    outputSchema.metadata("e").setDefaultValue("30");

    BaseScanFixtureBuilder builder = new BaseScanFixtureBuilder();
    builder.setProjection(new String[]{"a", "b", "d", "f"});
    builder.addReader(new MockSimpleReader());
    builder.builder.typeConverterBuilder().providedSchema(outputSchema);
    builder.builder.setNullType(Types.optional(MinorType.VARCHAR));
    ScanFixture scanFixture = builder.build();
    ScanOperatorExec scan = scanFixture.scanOp;

    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("b", MinorType.VARCHAR)
        .add("d", MinorType.BIGINT)
        .addNullable("f", MinorType.VARCHAR)
        .buildSchema();

    // Initial schema

    assertTrue(scan.buildSchema());
    {
      SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
           .build();
      RowSetUtilities.verify(expected,
          fixture.wrap(scan.batchAccessor().getOutgoingContainer()));
    }

    // Batch with defaults and null types

    assertTrue(scan.next());
    {
      SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
          .addRow(10, "foo", 20L, null)
          .build();
      RowSetUtilities.verify(expected,
          fixture.wrap(scan.batchAccessor().getOutgoingContainer()));
    }

    assertFalse(scan.next());
    scanFixture.close();
  }

  /**
   * Test non-strict specified schema, with a wildcard, with extra
   * reader columns. Reader columns are included in output.
   */

  @Test
  public void testOutputSchemaWithWildcard() {
    TupleMetadata outputSchema = new SchemaBuilder()
        .add("a", MinorType.INT)    // Projected, in reader
        .add("d", MinorType.BIGINT) // Projected, not in reader
        .add("e", MinorType.BIGINT) // Not projected, not in reader
        .buildSchema();
    outputSchema.metadata("d").setDefaultValue("20");
    outputSchema.metadata("e").setDefaultValue("30");

    BaseScanFixtureBuilder builder = new BaseScanFixtureBuilder();
    builder.setProjection(RowSetTestUtils.projectAll());
    builder.addReader(new MockSimpleReader());
    builder.builder.typeConverterBuilder().providedSchema(outputSchema);
    builder.builder.setNullType(Types.optional(MinorType.VARCHAR));
    ScanFixture scanFixture = builder.build();
    ScanOperatorExec scan = scanFixture.scanOp;

    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("d", MinorType.BIGINT)
        .add("e", MinorType.BIGINT)
        .add("b", MinorType.VARCHAR)
        .add("c", MinorType.VARCHAR)
        .buildSchema();

    // Initial schema

    assertTrue(scan.buildSchema());
    {
      SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
           .build();
      RowSetUtilities.verify(expected,
          fixture.wrap(scan.batchAccessor().getOutgoingContainer()));
    }

    // Batch with defaults and null types

    assertTrue(scan.next());
    {
      SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
          .addRow(10, 20L, 30L, "foo", "bar")
          .build();
      RowSetUtilities.verify(expected,
          fixture.wrap(scan.batchAccessor().getOutgoingContainer()));
    }

    assertFalse(scan.next());
    scanFixture.close();
  }

  @Test
  public void testStrictOutputSchemaWithWildcard() {
    TupleMetadata outputSchema = new SchemaBuilder()
        .add("a", MinorType.INT)    // Projected, in reader
        .add("d", MinorType.BIGINT) // Projected, not in reader
        .add("e", MinorType.BIGINT) // Not projected, not in reader
        .buildSchema();
    outputSchema.metadata("d").setDefaultValue("20");
    outputSchema.metadata("e").setDefaultValue("30");
    outputSchema.setProperty(TupleMetadata.IS_STRICT_SCHEMA_PROP, Boolean.TRUE.toString());

    BaseScanFixtureBuilder builder = new BaseScanFixtureBuilder();
    // Project schema only
    builder.setProjection(RowSetTestUtils.projectAll());
    builder.addReader(new MockSimpleReader());
    builder.builder.typeConverterBuilder().providedSchema(outputSchema);
    builder.builder.setNullType(Types.optional(MinorType.VARCHAR));
    ScanFixture scanFixture = builder.build();
    ScanOperatorExec scan = scanFixture.scanOp;

    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("d", MinorType.BIGINT)
        .add("e", MinorType.BIGINT)
         .buildSchema();

    // Initial schema

    assertTrue(scan.buildSchema());
    {
      SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
           .build();
      RowSetUtilities.verify(expected,
          fixture.wrap(scan.batchAccessor().getOutgoingContainer()));
    }

    // Batch with defaults and null types

    assertTrue(scan.next());
    {
      SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
          .addRow(10, 20L, 30L)
          .build();
      RowSetUtilities.verify(expected,
          fixture.wrap(scan.batchAccessor().getOutgoingContainer()));
    }

    assertFalse(scan.next());
    scanFixture.close();
  }

  @Test
  public void testStrictOutputSchemaWithWildcardAndSpecialCols() {
    TupleMetadata outputSchema = new SchemaBuilder()
        .add("a", MinorType.INT)    // Projected, in reader
        .add("d", MinorType.BIGINT) // Projected, not in reader
        .add("e", MinorType.BIGINT) // Not projected, not in reader
        .buildSchema();
    outputSchema.metadata("d").setDefaultValue("20");
    outputSchema.metadata("e").setDefaultValue("30");
    outputSchema.setProperty(TupleMetadata.IS_STRICT_SCHEMA_PROP, Boolean.TRUE.toString());
    outputSchema.metadata("a").setBooleanProperty(ColumnMetadata.EXCLUDE_FROM_WILDCARD, true);

    BaseScanFixtureBuilder builder = new BaseScanFixtureBuilder();
    // Project schema only
    builder.setProjection(RowSetTestUtils.projectAll());
    builder.addReader(new MockSimpleReader());
    builder.builder.typeConverterBuilder().providedSchema(outputSchema);
    builder.builder.setNullType(Types.optional(MinorType.VARCHAR));
    ScanFixture scanFixture = builder.build();
    ScanOperatorExec scan = scanFixture.scanOp;

    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("d", MinorType.BIGINT)
        .add("e", MinorType.BIGINT)
         .buildSchema();

    // Initial schema

    assertTrue(scan.buildSchema());
    {
      SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
           .build();
      RowSetUtilities.verify(expected,
          fixture.wrap(scan.batchAccessor().getOutgoingContainer()));
    }

    // Batch with defaults and null types

    assertTrue(scan.next());
    {
      SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
          .addRow(20L, 30L)
          .build();
      RowSetUtilities.verify(expected,
          fixture.wrap(scan.batchAccessor().getOutgoingContainer()));
    }

    assertFalse(scan.next());
    scanFixture.close();
  }
}
