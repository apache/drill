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
package org.apache.drill.test.rowSet;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.record.VectorAccessibleUtilities;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.TupleAccessor.TupleSchema;
import org.apache.drill.exec.vector.accessor.impl.AbstractColumnWriter;
import org.apache.drill.exec.vector.accessor.impl.ColumnAccessorFactory;
import org.apache.drill.exec.vector.accessor.impl.TupleWriterImpl;
import org.apache.drill.test.rowSet.RowSet.ExtendableRowSet;

/**
 * Implementation of a single row set with no indirection (selection)
 * vector.
 */

public class DirectRowSet extends AbstractSingleRowSet implements ExtendableRowSet {

  /**
   * Reader index that points directly to each row in the row set.
   * This index starts with pointing to the -1st row, so that the
   * reader can require a <tt>next()</tt> for every row, including
   * the first. (This is the JDBC RecordSet convention.)
   */

  private static class DirectRowIndex extends BoundedRowIndex {

    public DirectRowIndex(int rowCount) {
      super(rowCount);
    }

    @Override
    public int index() { return rowIndex; }

    @Override
    public int batch() { return 0; }
  }

  /**
   * Writer index that points to each row in the row set. The index starts at
   * the 0th row and advances one row on each increment. This allows writers to
   * start positioned at the first row. Writes happen in the current row.
   * Calling <tt>next()</tt> advances to the next position, effectively saving
   * the current row. The most recent row can be abandoned easily simply by not
   * calling <tt>next()</tt>. This means that the number of completed rows is
   * the same as the row index.
   */

  private static class ExtendableRowIndex extends RowSetIndex {

    private final int maxSize;

    public ExtendableRowIndex(int maxSize) {
      this.maxSize = maxSize;
      rowIndex = 0;
    }

    @Override
    public int index() { return rowIndex; }

    @Override
    public boolean next() {
      if (++rowIndex <= maxSize ) {
        return true;
      } else {
        rowIndex--;
        return false;
      }
    }

    @Override
    public int size() { return rowIndex; }

    @Override
    public boolean valid() { return rowIndex < maxSize; }

    @Override
    public int batch() { return 0; }
  }

  /**
   * Implementation of a row set writer. Only available for newly-created,
   * empty, direct, single row sets. Rewriting is not allowed, nor is writing
   * to a hyper row set.
   */

  public class RowSetWriterImpl extends TupleWriterImpl implements RowSetWriter {

    private final ExtendableRowIndex index;
    private final ExtendableRowSet rowSet;

    protected RowSetWriterImpl(ExtendableRowSet rowSet, TupleSchema schema, ExtendableRowIndex index, AbstractColumnWriter[] writers) {
      super(schema, writers);
      this.rowSet = rowSet;
      this.index = index;
      start();
    }

    @Override
    public void setRow(Object...values) {
      if (! index.valid()) {
        throw new IndexOutOfBoundsException("Write past end of row set");
      }
      for (int i = 0; i < values.length;  i++) {
        set(i, values[i]);
      }
      save();
    }

    @Override
    public boolean valid() { return index.valid(); }

    @Override
    public int index() { return index.position(); }

    @Override
    public void save() {
      index.next();
      start();
    }

    @Override
    public void done() {
      rowSet.setRowCount(index.size());
    }
  }

  public DirectRowSet(BufferAllocator allocator, BatchSchema schema) {
    super(allocator, schema);
  }

  public DirectRowSet(BufferAllocator allocator, VectorContainer container) {
    super(allocator, container);
  }

  public DirectRowSet(BufferAllocator allocator, VectorAccessible va) {
    super(allocator, toContainer(va, allocator));
  }

  private static VectorContainer toContainer(VectorAccessible va, BufferAllocator allocator) {
    VectorContainer container = VectorContainer.getTransferClone(va, allocator);
    container.buildSchema(SelectionVectorMode.NONE);
    container.setRecordCount(va.getRecordCount());
    return container;
  }

  @Override
  public void allocate(int recordCount) {
    for (final ValueVector v : valueVectors) {
      AllocationHelper.allocate(v, recordCount, 50, 10);
    }
  }

  @Override
  public void setRowCount(int rowCount) {
    container.setRecordCount(rowCount);
    VectorAccessibleUtilities.setValueCount(container, rowCount);
  }

  @Override
  public RowSetWriter writer() {
    return writer(10);
  }

  @Override
  public RowSetWriter writer(int initialRowCount) {
    if (container.hasRecordCount()) {
      throw new IllegalStateException("Row set already contains data");
    }
    allocate(initialRowCount);
    return buildWriter(new ExtendableRowIndex(Character.MAX_VALUE));
  }

  /**
   * Build writer objects for each column based on the column type.
   *
   * @param rowIndex the index which points to each row
   * @return an array of writers
   */

  protected RowSetWriter buildWriter(ExtendableRowIndex rowIndex) {
    ValueVector[] valueVectors = vectors();
    AbstractColumnWriter[] writers = new AbstractColumnWriter[valueVectors.length];
    for (int i = 0; i < writers.length; i++) {
      writers[i] = ColumnAccessorFactory.newWriter(valueVectors[i].getField().getType());
      writers[i].bind(rowIndex, valueVectors[i]);
    }
    TupleSchema accessSchema = schema().hierarchicalAccess();
    return new RowSetWriterImpl(this, accessSchema, rowIndex, writers);
  }

  @Override
  public RowSetReader reader() {
    return buildReader(new DirectRowIndex(rowCount()));
  }

  @Override
  public boolean isExtendable() { return true; }

  @Override
  public boolean isWritable() { return true; }

  @Override
  public SelectionVectorMode indirectionType() { return SelectionVectorMode.NONE; }

  @Override
  public SingleRowSet toIndirect() {
    return new IndirectRowSet(this);
  }

  @Override
  public SelectionVector2 getSv2() { return null; }

  @Override
  public RowSet merge(RowSet other) {
    return new DirectRowSet(allocator, container().merge(other.container()));
  }
}
