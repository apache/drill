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

import org.apache.drill.shaded.guava.com.google.common.collect.Sets;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.physical.rowSet.model.ReaderIndex;
import org.apache.drill.exec.physical.rowSet.model.single.SingleSchemaInference;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.RecordBatchSizer;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.selection.SelectionVector2;

import java.util.Set;

/**
 * Single row set coupled with an indirection (selection) vector,
 * specifically an SV2.
 */

public class IndirectRowSet extends AbstractSingleRowSet {

  /**
   * Reader index that points to each row indirectly through the
   * selection vector. The {@link #physicalIndex()} method points to the
   * actual data row, while the {@link #logicalIndex()} method gives
   * the position relative to the indirection vector. That is,
   * the position increases monotonically, but the index jumps
   * around as specified by the indirection vector.
   */

  private static class IndirectRowIndex extends ReaderIndex {

    private final SelectionVector2 sv2;

    public IndirectRowIndex(SelectionVector2 sv2) {
      super(sv2.getCount());
      this.sv2 = sv2;
    }

    @Override
    public int offset() { return sv2.getIndex(position); }

    @Override
    public int hyperVectorIndex() { return 0; }
  }

  private final SelectionVector2 sv2;

  private IndirectRowSet(VectorContainer container, SelectionVector2 sv2) {
    super(container, new SingleSchemaInference().infer(container));
    this.sv2 = sv2;
  }

  public IndirectRowSet(VectorContainer container) {
    this(container, makeSv2(container.getAllocator(), container, Sets.<Integer>newHashSet()));
  }

  public static IndirectRowSet fromContainer(VectorContainer container) {
    return new IndirectRowSet(container, makeSv2(container.getAllocator(), container, Sets.<Integer>newHashSet()));
  }

  public static IndirectRowSet fromSv2(VectorContainer container, SelectionVector2 sv2) {
    return new IndirectRowSet(container, sv2);
  }

  private static SelectionVector2 makeSv2(BufferAllocator allocator, VectorContainer container,
                                          Set<Integer> skipIndices) {
    int rowCount = container.getRecordCount() - skipIndices.size();
    SelectionVector2 sv2 = new SelectionVector2(allocator);
    if (!sv2.allocateNewSafe(rowCount)) {
      throw new OutOfMemoryException("Unable to allocate sv2 buffer");
    }
    for (int srcIndex = 0, destIndex = 0; srcIndex < container.getRecordCount(); srcIndex++) {
      if (skipIndices.contains(srcIndex)) {
        continue;
      }

      sv2.setIndex(destIndex, (char)srcIndex);
      destIndex++;
    }
    sv2.setRecordCount(rowCount);
    sv2.setBatchActualRecordCount(container.getRecordCount());
    container.buildSchema(SelectionVectorMode.TWO_BYTE);
    return sv2;
  }

  public IndirectRowSet(DirectRowSet directRowSet, Set<Integer> skipIndices) {
    super(directRowSet);
    sv2 = makeSv2(allocator(), container(), skipIndices);
  }

  @Override
  public SelectionVector2 getSv2() { return sv2; }

  @Override
  public void clear() {
    super.clear();
    getSv2().clear();
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
  public SingleRowSet toIndirect(Set<Integer> skipIndices) {
    return new IndirectRowSet(DirectRowSet.fromContainer(container()), skipIndices);
  }

  @Override
  public long size() {
    RecordBatchSizer sizer = new RecordBatchSizer(container(), sv2);
    return sizer.getActualSize();
  }
}
