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

import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.physical.impl.spill.RecordBatchSizer;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.selection.SelectionVector2;

/**
 * Single row set coupled with an indirection (selection) vector,
 * specifically an SV2.
 */

public class IndirectRowSet extends AbstractSingleRowSet {

  /**
   * Reader index that points to each row indirectly through the
   * selection vector. The {@link #index()} method points to the
   * actual data row, while the {@link #position()} method gives
   * the position relative to the indirection vector. That is,
   * the position increases monotonically, but the index jumps
   * around as specified by the indirection vector.
   */

  private static class IndirectRowIndex extends BoundedRowIndex {

    private final SelectionVector2 sv2;

    public IndirectRowIndex(SelectionVector2 sv2) {
      super(sv2.getCount());
      this.sv2 = sv2;
    }

    @Override
    public int index() { return sv2.getIndex(rowIndex); }

    @Override
    public int batch() { return 0; }
  }

  private final SelectionVector2 sv2;

  public IndirectRowSet(BufferAllocator allocator, VectorContainer container) {
    this(allocator, container, makeSv2(allocator, container));
  }

  public IndirectRowSet(BufferAllocator allocator, VectorContainer container, SelectionVector2 sv2) {
    super(allocator, container);
    this.sv2 = sv2;
  }

  private static SelectionVector2 makeSv2(BufferAllocator allocator, VectorContainer container) {
    int rowCount = container.getRecordCount();
    SelectionVector2 sv2 = new SelectionVector2(allocator);
    if (!sv2.allocateNewSafe(rowCount)) {
      throw new OutOfMemoryException("Unable to allocate sv2 buffer");
    }
    for (int i = 0; i < rowCount; i++) {
      sv2.setIndex(i, (char) i);
    }
    sv2.setRecordCount(rowCount);
    container.buildSchema(SelectionVectorMode.TWO_BYTE);
    return sv2;
  }

  public IndirectRowSet(DirectRowSet directRowSet) {
    super(directRowSet);
    sv2 = makeSv2(allocator, container);
  }

  @Override
  public SelectionVector2 getSv2() { return sv2; }

  @Override
  public void clear() {
    super.clear();
    getSv2().clear();
  }

  @Override
  public RowSetWriter writer() {
    throw new UnsupportedOperationException("Cannot write to an existing row set");
  }

  @Override
  public RowSetReader reader() {
    return buildReader(new IndirectRowIndex(getSv2()));
  }

  @Override
  public boolean isExtendable() {return false;}

  @Override
  public boolean isWritable() { return true;}

  @Override
  public SelectionVectorMode indirectionType() { return SelectionVectorMode.TWO_BYTE; }

  @Override
  public SingleRowSet toIndirect() { return this; }

  @Override
  public int size() {
    RecordBatchSizer sizer = new RecordBatchSizer(container, sv2);
    return sizer.actualSize();
  }

  @Override
  public RowSet merge(RowSet other) {
    return new IndirectRowSet(allocator, container().merge(other.container()), sv2);
  }
}
