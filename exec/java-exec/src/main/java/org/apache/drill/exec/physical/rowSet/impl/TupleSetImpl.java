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

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.physical.rowSet.ColumnLoader;
import org.apache.drill.exec.physical.rowSet.TupleLoader;
import org.apache.drill.exec.physical.rowSet.TupleSchema;
import org.apache.drill.exec.physical.rowSet.impl.ResultSetLoaderImpl.VectorContainerBuilder;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VectorOverflowException;
import org.apache.drill.exec.vector.accessor.impl.AbstractColumnWriter;
import org.apache.drill.exec.vector.accessor.impl.ColumnAccessorFactory;

/**
 * Implementation of a column when creating a row batch.
 * Every column resides at an index, is defined by a schema,
 * is backed by a value vector, and and is written to by a writer.
 * Each column also tracks the schema version in which it was added
 * to detect schema evolution. Each column has an optional overflow
 * vector that holds overflow record values when a batch becomes
 * full.
 * <p>
 * Overflow vectors require special consideration. The vector class itself
 * must remain constant as it is bound to the writer. To handle overflow,
 * the implementation must replace the buffer in the vector with a new
 * one, saving the full vector to return as part of the final row batch.
 * This puts the column in one of three states:
 * <ul>
 * <li>Normal: only one vector is of concern - the vector for the active
 * row batch.</li>
 * <li>Overflow: a write to a vector caused overflow. For all columns,
 * the data buffer is shifted to a harvested vector, and a new, empty
 * buffer is put into the active vector.</li>
 * <li>Excess: a (small) column received values for the row that will
 * overflow due to a later column. When overflow occurs, the excess
 * column value, from the overflow record, resides in the active
 * vector. It must be shifted from the active vector into the new
 * overflow buffer.
 */

public class TupleSetImpl implements TupleSchema {

  public static class TupleLoaderImpl implements TupleLoader {

    public TupleSetImpl tupleSet;

    public TupleLoaderImpl(TupleSetImpl tupleSet) {
      this.tupleSet = tupleSet;
    }

    @Override
    public TupleSchema schema() { return tupleSet; }

    @Override
    public ColumnLoader column(int colIndex) {
      // TODO: Cache loaders here
      return tupleSet.columnImpl(colIndex).writer;
    }

    @Override
    public ColumnLoader column(String colName) {
      ColumnImpl col = tupleSet.columnImpl(colName);
      if (col == null) {
        throw new UndefinedColumnException(colName);
      }
      return col.writer;
    }

    @Override
    public TupleLoader loadRow(Object... values) {
      tupleSet.rowSetMutator().startRow();
      for (int i = 0; i < values.length;  i++) {
        set(i, values[i]);
      }
      tupleSet.rowSetMutator().saveRow();
      return this;
    }

    @Override
    public TupleLoader loadSingletonRow(Object value) {
      tupleSet.rowSetMutator().startRow();
      set(0, value);
      tupleSet.rowSetMutator().saveRow();
      return this;
    }

    @Override
    public void set(int colIndex, Object value) {
      column(colIndex).set(value);
    }

    public void setArray(int colIndex, Object value) {
      ColumnLoader colWriter = column(colIndex);
      if (value == null) {
        colWriter.setNull();
      } else {
        colWriter.array().setArray(value);
      }
    }
  }

  public static class ColumnImpl implements TupleColumnSchema {

    private enum State {

      /**
       * Column is newly added. No data yet provided.
       */
      START,

      /**
       * Actively writing to the column. May have data.
       */
      ACTIVE,

      /**
       * After sending the current batch downstream, before starting
       * the next one.
       */
      HARVESTED,

      /**
       * Like ACTIVE, but means that the data has overflowed and the
       * column's data for the current row appears in the new,
       * overflow batch. For a reader that omits some columns, written
       * columns will be in OVERFLOW state, unwritten columns in
       * ACTIVE state.
       */
      OVERFLOW,

      /**
       * Like HARVESTED, but indicates that the column has data saved
       * in the overflow batch.
       */
      LOOK_AHEAD,

      /**
       * Like LOOK_AHEAD, but indicates the special case that the column
       * was added after overflow, so there is no vector for the column
       * in the harvested batch.
       */
      NEW_LOOK_AHEAD
    }

    final TupleSetImpl tupleSet;
    final int index;
    final MaterializedField schema;
    private State state = State.START;
    final int addVersion;
    private final int allocationWidth;
    final ValueVector vector;
    final AbstractColumnWriter columnWriter;
    final ColumnLoaderImpl writer;
    private ValueVector backupVector;

    /**
     * Build a column implementation, including vector and writers, based on the
     * schema provided.
     * @param tupleSet the tuple set that owns this column
     * @param schema the schema of the column
     * @param index the index of the column within the tuple set
     */

    public ColumnImpl(TupleSetImpl tupleSet, MaterializedField schema, int index, int allocWidth) {
      this.tupleSet = tupleSet;
      this.schema = schema;
      this.index = index;
      ResultSetLoaderImpl rowSetMutator = tupleSet.rowSetMutator();
      addVersion = rowSetMutator.bumpVersion();
      ResultVectorCache inventory = rowSetMutator.vectorInventory();
      vector = inventory.addOrGet(schema);
      columnWriter = ColumnAccessorFactory.newWriter(schema.getType());
      WriterIndexImpl writerIndex = rowSetMutator.writerIndex();
      columnWriter.bind(writerIndex, vector);
      if (schema.getDataMode() == DataMode.REPEATED) {
        writer = new ArrayColumnLoader(writerIndex, columnWriter);
      } else {
        writer = new ScalarColumnLoader(writerIndex, columnWriter);
      }
      allocationWidth = allocWidth == 0
          ? TypeHelper.getSize(schema.getType())
          : allocWidth;
      allocateVector(vector);
    }


    public ColumnImpl(TupleSetImpl tupleSet, MaterializedField schema, int index) {
      this(tupleSet, schema, index, 0);
    }

    /**
     * A column within the row batch overflowed. Prepare to absorb the rest of
     * the in-flight row by rolling values over to a new vector, saving the
     * complete vector for later. This column could have a value for the overflow
     * row, or for some previous row, depending on exactly when and where the
     * overflow occurs.
     *
     * @param overflowIndex the index of the row that caused the overflow, the
     * values of which should be copied to a new "look-ahead" vector
     */

    public void rollOver(int overflowIndex) {
      assert state == State.ACTIVE;

      // Close out the active vector, setting the record count.
      // This will be replaced later when the batch is done, with the
      // final row count. Here we set the count to fill in missing values and
      // set offsets in preparation for carving off the overflow value, if any.

      try {
        columnWriter.finishBatch();
      } catch (VectorOverflowException e) {
        throw new IllegalStateException("Vector overflow should not happen on finishBatch()");
      }

      // Switch buffers between the backup vector and the writer's output
      // vector. Done this way because writers are bound to vectors and
      // we wish to keep the binding.

      if (backupVector == null) {
        backupVector = TypeHelper.getNewVector(schema, tupleSet.rowSetMutator().allocator(), null);
      }
      allocateVector(backupVector);
      vector.exchange(backupVector);
      state = State.OVERFLOW;

      // Any overflow value(s) to copy?

      int writeIndex = writer.writeIndex();
      if (writeIndex < overflowIndex) {
        return;
      }

      // Copy overflow values from the full vector to the new
      // look-ahead vector.

      int dest = 0;
      for (int src = overflowIndex; src <= writeIndex; src++, dest++) {
        vector.copyEntry(dest, backupVector, src);
      }

      // Tell the writer the new write index. At this point, vectors will have
      // distinct write indexes depending on whether data was copied or not.

      writer.resetTo(dest);
    }

    /**
     * Writing of a row batch is complete. Prepare the vector for harvesting
     * to send downstream. If this batch encountered overflow, set aside the
     * look-ahead vector and put the full vector buffer back into the active
     * vector.
     */

    public void harvest() {
      switch (state) {
      case OVERFLOW:
        vector.exchange(backupVector);
        state = State.LOOK_AHEAD;
        break;

      case ACTIVE:

        // If added after overflow, no data to save from the complete
        // batch: the vector does not appear in the completed batch.

        if (addVersion > tupleSet.rowSetMutator().schemaVersion()) {
          state = State.NEW_LOOK_AHEAD;
          return;
        }
        try {
          columnWriter.finishBatch();
        } catch (VectorOverflowException e) {
          throw new IllegalStateException("Vector overflow should not happen on finishBatch()");
        }
        state = State.HARVESTED;
        break;
      default:
        throw new IllegalStateException("Unexpected state: " + state);
      }
    }

    /**
     * Prepare the column for a new row batch. Clear the previous values.
     * If the previous batch created a look-ahead buffer, restore that to the
     * active vector so we start writing where we left off. Else, reset the
     * write position to the start.
     */

    public void resetBatch() {
      switch (state) {
      case NEW_LOOK_AHEAD:

        // Column is new, was not exchanged with backup vector

        break;
      case LOOK_AHEAD:
        vector.exchange(backupVector);
        backupVector.clear();
        break;
      case HARVESTED:

        // Note: do not reset the writer: it is already positioned in the backup
        // vector from when we wrote the overflow row.

        vector.clear();
        // Fall through
      case START:
        allocateVector(vector);
        writer.reset();
        break;
      default:
        throw new IllegalStateException("Unexpected state: " + state);
      }
      state = State.ACTIVE;
    }

    public void allocateVector(ValueVector toAlloc) {
      AllocationHelper.allocate(toAlloc, tupleSet.rowSetMutator().targetRowCount(), allocationWidth, 10);
    }

    public void reset() {
      vector.clear();
      if (backupVector != null) {
        backupVector.clear();
        backupVector = null;
      }
    }

    public void buildContainer(VectorContainerBuilder containerBuilder) {

      // Don't add the vector if it is new in an overflow row.
      // Don't add it if it is already in the container.

      if (state != State.NEW_LOOK_AHEAD &&
          addVersion > containerBuilder.lastUpdateVersion()) {
        containerBuilder.add(vector);
      }
    }

    @Override
    public MaterializedField schema() { return schema; }

    @Override
    public boolean isSelected() { return true; }

    @Override
    public int vectorIndex() { return index; }
  }

  private final ResultSetLoaderImpl resultSetLoader;
  private final TupleSetImpl parent;
  private final TupleLoaderImpl loader;
  private final TupleNameSpace<ColumnImpl> columns = new TupleNameSpace<>();
  private final List<AbstractColumnWriter> startableWriters = new ArrayList<>();

  public TupleSetImpl(ResultSetLoaderImpl rowSetMutator) {
    this.resultSetLoader = rowSetMutator;
    parent = null;
    loader = new TupleLoaderImpl(this);
  }

  public TupleSetImpl(TupleSetImpl parent) {
    this.parent = parent;
    resultSetLoader = parent.resultSetLoader;
    loader = new TupleLoaderImpl(this);
  }

  public void start() {
    for (TupleSetImpl.ColumnImpl col : columns) {
      col.resetBatch();
    }
  }

  @Override
  public int addColumn(MaterializedField columnSchema) {

    // Verify name is not a (possibly case insensitive) duplicate.

    String lastName = columnSchema.getLastName();
    String key = resultSetLoader.toKey(lastName);
    if (column(key) != null) {
      // TODO: Include full path as context
      throw new IllegalArgumentException("Duplicate column: " + lastName);
    }

    // Verify that the cardinality (mode) is acceptable.
    // For some types, we can add a required column within the first
    // batch and zero (or blank) fill the vector. Adding a required
    // field after the first batch triggers a schema change of a form
    // that is hard to handle since downstream operators won't know
    // which value to put into the "missing" columns for this reason,
    // we forbid adding required columns after the first batch. Here
    // "first batch" excludes the possible overflow row, as that will
    // be the first row of the second batch.

    if (columnSchema.getDataMode() == DataMode.REQUIRED &&
        (resultSetLoader.batchCount() > 1  || resultSetLoader.isFull())) {
      throw new IllegalArgumentException("Cannot add a required field after the first batch: " + lastName);
    }

    // TODO: If necessary, verify path

    // Add the column, increasing the schema version to indicate the change.

    TupleSetImpl.ColumnImpl colImpl = new ColumnImpl(this, columnSchema, columnCount());
    columns.add(key, colImpl);

    // Array writers must be told about the start of each row.

    if (columnSchema.getDataMode() == DataMode.REPEATED) {
      startableWriters.add(colImpl.columnWriter);
    }

    // If a batch is active, prepare the column for writing.

    if (resultSetLoader.writeable()) {
      colImpl.resetBatch();
    }
    return colImpl.index;
  }

  @Override
  public void setSchema(BatchSchema schema) {
    if (! columns.isEmpty()) {
      throw new IllegalStateException("Can only set schema when the tuple schema is empty");
    }
    for (MaterializedField field : schema) {
      addColumn(field);
    }
  }

  public ResultSetLoaderImpl rowSetMutator() { return resultSetLoader; }

  @Override
  public int columnIndex(String colName) {
    ColumnImpl col = columnImpl(colName);
    return col == null ? -1 : col.index;
  }

  @Override
  public MaterializedField column(int colIndex) {
    return columnImpl(colIndex).schema();
  }

  @Override
  public TupleColumnSchema metadata(int colIndex) {
    return columnImpl(colIndex);
  }

  protected ColumnImpl columnImpl(String colName) {
    return columns.get(resultSetLoader.toKey(colName));
  }

  @Override
  public TupleColumnSchema metadata(String colName) {
    return columnImpl(colName);
  }

  @Override
  public MaterializedField column(String colName) {
    TupleSetImpl.ColumnImpl col = columnImpl(colName);
    return col == null ? null : col.schema;
  }

  @Override
  public int columnCount() { return columns.count(); }

  public ColumnImpl columnImpl(int colIndex) {
    // Let the list catch out-of-bounds indexes
    return columns.get(colIndex);
  }

  public void startRow() {
    for (AbstractColumnWriter writer : startableWriters) {
      writer.start();
    }
  }

  protected void rollOver(int overflowIndex) {
    for (TupleSetImpl.ColumnImpl col : columns) {
      col.rollOver(overflowIndex);
    }
  }

  protected void harvest() {
    for (TupleSetImpl.ColumnImpl col : columns) {
      col.harvest();
    }
  }

  public void buildContainer(VectorContainerBuilder containerBuilder) {
    for (TupleSetImpl.ColumnImpl col : columns) {
      col.buildContainer(containerBuilder);
    }
  }

  public TupleLoader loader() { return loader; }

  public void reset() {
    for (TupleSetImpl.ColumnImpl col : columns) {
      col.reset();
    }
  }

  public void close() {
    reset();
  }

  @Override
  public BatchSchema schema() {
    List<MaterializedField> fields = new ArrayList<>();
    for (TupleSetImpl.ColumnImpl col : columns) {
      fields.add(col.schema);
    }
    return new BatchSchema(SelectionVectorMode.NONE, fields);
  }

  @Override
  public MaterializedSchema materializedSchema() {
    MaterializedSchema schema = new MaterializedSchema();
    for (int i = 0; i < columnCount(); i++) {
      schema.add(column(i));
    }
    return schema;
  }
}
