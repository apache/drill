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
package org.apache.drill.exec.physical.resultSet.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.impl.protocol.BatchAccessor;
import org.apache.drill.exec.physical.impl.protocol.IndirectContainerAccessor;
import org.apache.drill.exec.physical.impl.protocol.VectorContainerAccessor;
import org.apache.drill.exec.physical.resultSet.ResultSetCopier;
import org.apache.drill.exec.physical.resultSet.ResultSetLoader;
import org.apache.drill.exec.physical.resultSet.RowSetLoader;
import org.apache.drill.exec.physical.resultSet.impl.ResultSetLoaderImpl.ResultSetOptions;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.physical.rowSet.RowSetBuilder;
import org.apache.drill.exec.physical.rowSet.RowSets;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.metadata.MetadataUtils;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.record.selection.SelectionVector2Builder;
import org.apache.drill.exec.vector.accessor.ArrayWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.apache.drill.test.rowSet.RowSetUtilities;
import org.junit.Test;

public class TestResultSetCopier extends SubOperatorTest {

  private static final TupleMetadata TEST_SCHEMA =
      new SchemaBuilder()
        .add("id", MinorType.INT)
        .add("name", MinorType.VARCHAR)
        .build();

  private static class BaseDataGen {
    protected final TupleMetadata schema;
    protected final ResultSetLoader rsLoader;
    protected final VectorContainerAccessor batch = new VectorContainerAccessor();

    public BaseDataGen(TupleMetadata schema) {
      this.schema = schema;
      ResultSetOptions options = new OptionBuilder()
          .setSchema(schema)
          .setVectorCache(new ResultVectorCacheImpl(fixture.allocator()))
          .build();
      rsLoader = new ResultSetLoaderImpl(fixture.allocator(), options);
    }

    public TupleMetadata schema() { return schema; }

    public BatchAccessor batchAccessor() {
      return batch;
    }
  }

  private static class DataGen extends BaseDataGen {

    public DataGen() {
      super(TEST_SCHEMA);
    }

    public void makeBatch(int start, int end) {
      rsLoader.startBatch();
      for (int i = start; i <= end; i++) {
        rsLoader.writer().addRow(i, "Row " + i);
      }
      batch.addBatch(rsLoader.harvest());
    }
  }

  public static class DataGen2 extends DataGen {
    private final int batchCount = 2;
    private final int batchSize = 5;
    private int batchIndex;

    boolean next() {
      if (batchIndex >= batchCount) {
        return false;
      }
      int start = nextRow();
      makeBatch(start, start + batchSize - 1);
      batchIndex++;
      return true;
    }

    int nextRow() {
      return batchIndex * batchSize + 1;
    }

    int targetRowCount( ) {
      return batchCount * batchSize;
    }
  }

  public static class SchemaChangeGen extends DataGen {
    private int batchIndex;
    public final int batchSize = 5;
    private int schemaVersion = 1;

    public void makeBatch2(int start, int end) {
      rsLoader.startBatch();
      for (int i = start; i <= end; i++) {
        rsLoader.writer().addRow(i, "Row " + i, i * 10);
      }
      batch.addBatch(rsLoader.harvest());
    }

    public TupleMetadata schema2() {
      return new SchemaBuilder()
          .add("id", MinorType.INT)
          .add("name", MinorType.VARCHAR)
          .add("amount", MinorType.INT)
          .build();
    }

    public void evolveSchema() {
      rsLoader.writer().addColumn(MetadataUtils.newScalar("amount", MinorType.INT, DataMode.REQUIRED));
      schemaVersion = 2;
    }

    public void nextBatch() {
      int start = batchIndex * batchSize + 1;
      int end = start + batchSize - 1;
      if (schemaVersion == 1) {
        makeBatch(start, end);
      } else {
        makeBatch2(start, end);
      }
      batchIndex++;
    }
  }

  private static class NullableGen extends BaseDataGen {

    public NullableGen() {
      super(new SchemaBuilder()
          .add("id", MinorType.INT)
          .addNullable("name", MinorType.VARCHAR)
          .addNullable("amount", MinorType.INT)
          .build());
    }

    public void makeBatch(int start, int end) {
      rsLoader.startBatch();
      RowSetLoader writer = rsLoader.writer();
      for (int i = start; i <= end; i++) {
        writer.start();
        writer.scalar(0).setInt(i);
        if (i % 2 == 0) {
          writer.scalar(1).setString("Row " + i);
        }
        if (i % 3 == 0) {
          writer.scalar(2).setInt(i * 10);
        }
        writer.save();
      }
      batch.addBatch(rsLoader.harvest());
    }
  }

  private static class ArrayGen extends BaseDataGen {

    public ArrayGen() {
      super(new SchemaBuilder()
          .add("id", MinorType.INT)
          .addArray("name", MinorType.VARCHAR)
          .build());
    }

    public void makeBatch(int start, int end) {
      rsLoader.startBatch();
      RowSetLoader writer = rsLoader.writer();
      ArrayWriter aw = writer.array(1);
      for (int i = start; i <= end; i++) {
        writer.start();
        writer.scalar(0).setInt(i);
        int n = i % 3;
        for (int j = 0; j < n; j++) {
          aw.scalar().setString("Row " + i + "." + j);
        }
        writer.save();
      }
      batch.addBatch(rsLoader.harvest());
    }
  }

  private static class MapGen extends BaseDataGen {

    public MapGen() {
      super(new SchemaBuilder()
          .add("id", MinorType.INT)
          .addMapArray("map")
            .add("name", MinorType.VARCHAR)
            .add("amount", MinorType.INT)
            .resumeSchema()
          .build());
    }

    public void makeBatch(int start, int end) {
      rsLoader.startBatch();
      RowSetLoader writer = rsLoader.writer();
      ArrayWriter aw = writer.array(1);
      TupleWriter mw = aw.entry().tuple();
      for (int i = start; i <= end; i++) {
        writer.start();
        writer.scalar(0).setInt(i);
        int n = i % 3;
        for (int j = 0; j < n; j++) {
          mw.scalar(0).setString("Row " + i + "." + j);
          mw.scalar(1).setInt(i * 100 + j);
          aw.save();
        }
        writer.save();
      }
      batch.addBatch(rsLoader.harvest());
    }
  }

  @Test
  public void testBasics() {

    DataGen dataGen = new DataGen();
    ResultSetCopier copier = new ResultSetCopierImpl(
        fixture.allocator(), dataGen.batchAccessor());

    // Nothing should work yet

    try {
      copier.copyAllRows();
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
    try {
      copier.harvest();
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }

    // Predicates should work

    assertFalse(copier.isCopyPending());
    assertFalse(copier.hasOutputRows());
    assertFalse(copier.isOutputFull());

    // Define a schema and start an output batch.

    copier.startOutputBatch();
    assertFalse(copier.isCopyPending());
    assertFalse(copier.hasOutputRows());
    assertFalse(copier.isOutputFull());

    // Provide an input row

    dataGen.makeBatch(1, 3);
    copier.startInputBatch();
    assertFalse(copier.isCopyPending());
    assertFalse(copier.hasOutputRows());
    assertFalse(copier.isOutputFull());

    // Now can do some actual copying

    while (copier.copyNextRow()) {
      // empty
    }
    assertFalse(copier.isCopyPending());
    assertTrue(copier.hasOutputRows());
    assertFalse(copier.isOutputFull());

    // Get and verify the output batch
    // (Does not free the input batch, we reuse it
    // in the verify step below.)

    RowSet result = fixture.wrap(copier.harvest());
    new RowSetComparison(fixture.wrap(dataGen.batchAccessor().container()))
      .verifyAndClear(result);

    // Copier will release the input batch

    copier.close();
  }

  @Test
  public void testImmediateClose() {

    DataGen dataGen = new DataGen();
    ResultSetCopier copier = new ResultSetCopierImpl(
        fixture.allocator(), dataGen.batchAccessor());

    // Close OK before things got started

    copier.close();

    // Second close is benign

    copier.close();
  }

  @Test
  public void testCloseBeforeSchema() {

    DataGen dataGen = new DataGen();
    ResultSetCopier copier = new ResultSetCopierImpl(
        fixture.allocator(), dataGen.batchAccessor());

    // Start batch, no data yet.

    copier.startOutputBatch();

    // Close OK before things data arrives

    copier.close();

    // Second close is benign

    copier.close();
  }

  @Test
  public void testCloseWithData() {

    DataGen dataGen = new DataGen();
    ResultSetCopier copier = new ResultSetCopierImpl(
        fixture.allocator(), dataGen.batchAccessor());

    // Start batch, with data.

    copier.startOutputBatch();
    dataGen.makeBatch(1, 3);
    copier.startInputBatch();
    copier.copyNextRow();

    // Close OK with input and output batch allocated.

    copier.close();

    // Second close is benign

    copier.close();
  }

  /**
   * Test merging multiple batches from the same input
   * source; all batches share the same vectors, hence
   * implicitly the same schema.
   * <p>
   * This copier does not support merging from multiple
   * streams.
   */

  @Test
  public void testMerge() {
    DataGen dataGen = new DataGen();
    ResultSetCopier copier = new ResultSetCopierImpl(
        fixture.allocator(), dataGen.batchAccessor());
    copier.startOutputBatch();

    for (int i = 0; i < 5; i++) {
      int start = i * 3 + 1;
      dataGen.makeBatch(start, start + 2);
      copier.startInputBatch();
      assertFalse(copier.isOutputFull());
      copier.copyAllRows();
      copier.releaseInputBatch();
      assertFalse(copier.isOutputFull());
      assertFalse(copier.isCopyPending());
    }
    RowSet result = fixture.wrap(copier.harvest());
    dataGen.makeBatch(1, 15);
    RowSet expected = RowSets.wrap(dataGen.batchAccessor());
    RowSetUtilities.verify(expected, result);

    copier.close();
  }

  @Test
  public void testMultiOutput() {
    DataGen2 dataGen = new DataGen2();
    DataGen validatorGen = new DataGen();

    // Equivalent of operator start() method.

    OptionBuilder options = new OptionBuilder()
        .setRowCountLimit(12);
    ResultSetCopier copier = new ResultSetCopierImpl(
        fixture.allocator(), dataGen.batchAccessor(), options);

    // Equivalent of an entire operator run

    int start = 1;
    for (;;) {

      // Equivalent of operator next() method

      copier.startOutputBatch();
      while (! copier.isOutputFull()) {
        copier.releaseInputBatch();
        if (! dataGen.next()) {
          break;
        }
        copier.startInputBatch();
        copier.copyAllRows();
      }
      if (! copier.hasOutputRows()) {
        break;
      }

      // Equivalent of sending downstream

      RowSet result = fixture.wrap(copier.harvest());
       int nextRow = dataGen.nextRow();
      validatorGen.makeBatch(start, nextRow - 1);
      RowSet expected = RowSets.wrap(validatorGen.batchAccessor());
      RowSetUtilities.verify(expected, result);
      start = nextRow;
    }

    // Ensure more than one output batch.

    assertTrue(start > 1);

    // Ensure all rows generated.

    assertEquals(dataGen.targetRowCount(), start - 1);

    // Simulate operator close();

    copier.close();
  }

  @Test
  public void testCopyRecord() {
    DataGen dataGen = new DataGen();
    ResultSetCopier copier = new ResultSetCopierImpl(
        fixture.allocator(), dataGen.batchAccessor());
    copier.startOutputBatch();

    dataGen.makeBatch(1, 3);
    copier.startInputBatch();
    copier.copyRow(2);
    copier.copyRow(0);
    copier.copyRow(1);
    copier.releaseInputBatch();

    dataGen.makeBatch(4, 6);
    copier.startInputBatch();
    copier.copyRow(1);
    copier.copyRow(0);
    copier.copyRow(2);
    copier.releaseInputBatch();

    RowSet expected = new RowSetBuilder(fixture.allocator(), dataGen.schema())
        .addRow(3, "Row 3")
        .addRow(1, "Row 1")
        .addRow(2, "Row 2")
        .addRow(5, "Row 5")
        .addRow(4, "Row 4")
        .addRow(6, "Row 6")
        .build();
    RowSet result = fixture.wrap(copier.harvest());
    RowSetUtilities.verify(expected, result);

    copier.close();
  }

  @Test
  public void testSchemaChange() {
    SchemaChangeGen dataGen = new SchemaChangeGen();
    SchemaChangeGen verifierGen = new SchemaChangeGen();
    ResultSetCopier copier = new ResultSetCopierImpl(
        fixture.allocator(), dataGen.batchAccessor());

    // Copy first batch with first schema

    copier.startOutputBatch();
    dataGen.nextBatch();
    copier.startInputBatch();
    copier.copyAllRows();
    assertFalse(copier.isOutputFull());

    // Second, same schema

    copier.releaseInputBatch();
    dataGen.nextBatch();
    copier.startInputBatch();
    copier.copyAllRows();
    assertFalse(copier.isOutputFull());

    // Plenty of room. But, change the schema.

    copier.releaseInputBatch();
    dataGen.evolveSchema();
    dataGen.nextBatch();
    copier.startInputBatch();
    assertTrue(copier.isOutputFull());

    // Must harvest partial output

    RowSet result = fixture.wrap(copier.harvest());
    verifierGen.makeBatch(1, 2 * dataGen.batchSize - 1);
    RowSet expected = RowSets.wrap(verifierGen.batchAccessor());
    RowSetUtilities.verify(expected, result);

    // Start a new batch, implicitly complete pending copy

    copier.startOutputBatch();
    copier.copyAllRows();

    // Add one more of second schema

    copier.releaseInputBatch();
    dataGen.nextBatch();
    copier.startInputBatch();
    copier.copyAllRows();
    assertFalse(copier.isOutputFull());

    result = fixture.wrap(copier.harvest());
    verifierGen.evolveSchema();
    verifierGen.makeBatch2(2 * dataGen.batchSize + 1, 4 * dataGen.batchSize - 1);
    expected = RowSets.wrap(verifierGen.batchAccessor());
    RowSetUtilities.verify(expected, result);
    assertFalse(copier.isCopyPending());

    copier.close();
  }

  // TODO: Test with two consecutive schema changes in
  // same input batch: once with rows pending, another without.

  @Test
  public void testSV2() {
    DataGen dataGen = new DataGen();
    IndirectContainerAccessor filtered = new IndirectContainerAccessor();
    ResultSetCopier copier = new ResultSetCopierImpl(
        fixture.allocator(), filtered);

    copier.startOutputBatch();
    dataGen.makeBatch(1, 10);

    // Pick out every other record, in descending
    // order.

    VectorContainer container = dataGen.batchAccessor().container();
    SelectionVector2Builder sv2Builder =
        new SelectionVector2Builder(fixture.allocator(), container.getRecordCount());
    for (int i = 0; i < 5; i++) {
      sv2Builder.setNext(10 - 2 * i - 1);
    }
    container.buildSchema(SelectionVectorMode.TWO_BYTE);
    filtered.addBatch(container);
    filtered.setSelectionVector(sv2Builder.harvest(container));
    assertEquals(5, filtered.rowCount());

    copier.startInputBatch();
    copier.copyAllRows();
    copier.releaseInputBatch();

    RowSet expected = new RowSetBuilder(fixture.allocator(), TEST_SCHEMA)
        .addRow(10, "Row 10")
        .addRow(8, "Row 8")
        .addRow(6, "Row 6")
        .addRow(4, "Row 4")
        .addRow(2, "Row 2")
        .build();
    RowSet result = fixture.wrap(copier.harvest());
    RowSetUtilities.verify(expected, result);

    copier.close();
  }

  @Test
  public void testSV4() {
    // TODO
  }

  @Test
  public void testNullable() {
    NullableGen dataGen = new NullableGen();
    ResultSetCopier copier = new ResultSetCopierImpl(
        fixture.allocator(), dataGen.batchAccessor());
    copier.startOutputBatch();

    dataGen.makeBatch(1, 10);
    copier.startInputBatch();
    copier.copyAllRows();

    RowSet result = fixture.wrap(copier.harvest());
    RowSet expected = RowSets.wrap(dataGen.batchAccessor());
    RowSetUtilities.verify(expected, result);

    copier.close();
  }

  @Test
  public void testArrays() {
    ArrayGen dataGen = new ArrayGen();
    ResultSetCopier copier = new ResultSetCopierImpl(
        fixture.allocator(), dataGen.batchAccessor());
    copier.startOutputBatch();

    dataGen.makeBatch(1, 5);
    copier.startInputBatch();
    copier.copyAllRows();

    RowSet result = fixture.wrap(copier.harvest());
    RowSet expected = RowSets.wrap(dataGen.batchAccessor());
    RowSetUtilities.verify(expected, result);

    copier.close();
  }

  @Test
  public void testMaps() {
    MapGen dataGen = new MapGen();
    ResultSetCopier copier = new ResultSetCopierImpl(
        fixture.allocator(), dataGen.batchAccessor());
    copier.startOutputBatch();

    dataGen.makeBatch(1, 5);
    copier.startInputBatch();
    copier.copyAllRows();

    RowSet result = fixture.wrap(copier.harvest());
    RowSet expected = RowSets.wrap(dataGen.batchAccessor());
    RowSetUtilities.verify(expected, result);

    copier.close();
  }

  @Test
  public void testUnions() {
    // TODO
  }

  @Test
  public void testOverflow() {
    // TODO
  }
}
