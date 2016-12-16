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
package org.apache.drill.exec.physical.impl.xsort.managed;

import java.util.LinkedList;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.sort.RecordBatchData;
import org.apache.drill.exec.physical.impl.sort.SortRecordBatchBuilder;
import org.apache.drill.exec.physical.impl.xsort.managed.ExternalSortBatch.SortResults;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.selection.SelectionVector4;

/**
 * Wrapper around the "MSorter" (in memory merge sorter). As batches have
 * arrived to the sort, they have been individually sorted and buffered
 * in memory. At the completion of the sort, we detect that no batches
 * were spilled to disk. In this case, we can merge the in-memory batches
 * using an efficient memory-based approach implemented here.
 * <p>
 * Since all batches are in memory, we don't want to use the usual merge
 * algorithm as that makes a copy of the original batches (which were read
 * from a spill file) to produce an output batch. Instead, we want to use
 * the in-memory batches as-is. To do this, we use a selection vector 4
 * (SV4) as a global index into the collection of batches. The SV4 uses
 * the upper two bytes as the batch index, and the lower two as an offset
 * of a record within the batch.
 * <p>
 * The merger ("M Sorter") populates the SV4 by scanning the set of
 * in-memory batches, searching for the one with the lowest value of the
 * sort key. The batch number and offset are placed into the SV4. The process
 * continues until all records from all batches have an entry in the SV4.
 * <p>
 * The actual implementation uses an iterative merge to perform the above
 * efficiently.
 * <p>
 * A sort can only do a single merge. So, we do not attempt to share the
 * generated class; we just generate it internally and discard it at
 * completion of the merge.
 */

public class MergeSort implements SortResults {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MergeSort.class);

  private SortRecordBatchBuilder builder;
  private MSorter mSorter;
  private final FragmentContext context;
  private final BufferAllocator oAllocator;
  private SelectionVector4 sv4;
  private final OperatorCodeGenerator opCg;
  private int batchCount;

  public MergeSort(FragmentContext context, BufferAllocator allocator, OperatorCodeGenerator opCg) {
    this.context = context;
    this.oAllocator = allocator;
    this.opCg = opCg;
  }

  /**
   * Merge the set of in-memory batches to produce a single logical output in the given
   * destination container, indexed by an SV4.
   *
   * @param batchGroups the complete set of in-memory batches
   * @param batch the record batch (operator) for the sort operator
   * @param destContainer the vector container for the sort operator
   * @return the sv4 for this operator
   */

  public SelectionVector4 merge(LinkedList<BatchGroup.InputBatch> batchGroups, VectorAccessible batch,
                                VectorContainer destContainer) {

    // Add the buffered batches to a collection that MSorter can use.
    // The builder takes ownership of the batches and will release them if
    // an error occurs.

    builder = new SortRecordBatchBuilder(oAllocator);
    for (BatchGroup.InputBatch group : batchGroups) {
      RecordBatchData rbd = new RecordBatchData(group.getContainer(), oAllocator);
      rbd.setSv2(group.getSv2());
      builder.add(rbd);
    }
    batchGroups.clear();

    // Generate the msorter.

    try {
      builder.build(context, destContainer);
      sv4 = builder.getSv4();
      mSorter = opCg.createNewMSorter(batch);
      mSorter.setup(context, oAllocator, sv4, destContainer, sv4.getCount());
    } catch (SchemaChangeException e) {
      throw UserException.unsupportedError(e)
            .message("Unexpected schema change - likely code error.")
            .build(logger);
    }

    // For testing memory-leaks, inject exception after mSorter finishes setup
    ExternalSortBatch.injector.injectUnchecked(context.getExecutionControls(), ExternalSortBatch.INTERRUPTION_AFTER_SETUP);
    mSorter.sort(destContainer);

    // sort may have prematurely exited due to should continue returning false.
    if (!context.shouldContinue()) {
      return null;
    }

    // For testing memory-leak purpose, inject exception after mSorter finishes sorting
    ExternalSortBatch.injector.injectUnchecked(context.getExecutionControls(), ExternalSortBatch.INTERRUPTION_AFTER_SORT);
    sv4 = mSorter.getSV4();

    destContainer.buildSchema(SelectionVectorMode.FOUR_BYTE);
    return sv4;
  }

  /**
   * The SV4 provides a built-in iterator that returns a virtual set of record
   * batches so that the downstream operator need not consume the entire set
   * of accumulated batches in a single step.
   */

  @Override
  public boolean next() {
    boolean more = sv4.next();
    if (more) { batchCount++; }
    return more;
  }

  @Override
  public void close() {
    if (builder != null) {
      builder.clear();
      builder.close();
    }
    if (mSorter != null) {
      mSorter.clear();
    }
  }

  @Override
  public int getBatchCount() {
    return batchCount;
  }

  @Override
  public int getRecordCount() {
    return sv4.getTotalCount();
  }
}
