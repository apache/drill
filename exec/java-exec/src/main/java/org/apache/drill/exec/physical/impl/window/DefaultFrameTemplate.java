/**
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
package org.apache.drill.exec.physical.impl.window;

import org.apache.drill.common.exceptions.DrillException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.vector.BaseDataValueVector;
import org.apache.drill.exec.vector.BaseValueVector;
import org.apache.drill.exec.vector.ValueVector;

import javax.inject.Named;
import java.util.Iterator;
import java.util.List;


public abstract class DefaultFrameTemplate implements WindowFramer {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DefaultFrameTemplate.class);

  private VectorContainer container;
  private VectorContainer internal;
  private boolean lagCopiedToInternal;
  private List<WindowDataBatch> batches;
  private int outputCount; // number of rows in currently/last processed batch

  private int frameLastRow;

  /**
   * current partition being processed.</p>
   * Can span over multiple batches, so we may need to keep it between calls to doWork()
   */
  private Partition partition;

  @Override
  public void setup(final List<WindowDataBatch> batches, final VectorContainer container, final OperatorContext oContext)
      throws SchemaChangeException {
    this.container = container;
    this.batches = batches;

    internal = new VectorContainer(oContext);
    allocateInternal();
    lagCopiedToInternal = false;

    outputCount = 0;
    partition = null;
  }

  private void allocateOutgoing() {
    for (VectorWrapper<?> w : container) {
      w.getValueVector().allocateNew();
    }
  }

  private void allocateInternal() {
    // TODO we don't need to allocate all container's vectors, we can pass a specific list of vectors to allocate internally
    for (VectorWrapper<?> w : container) {
      ValueVector vv = internal.addOrGet(w.getField());
      vv.allocateNew();
    }
  }

  /**
   * processes all rows of current batch:
   * <ul>
   *   <li>compute aggregations</li>
   *   <li>compute window functions</li>
   *   <li>transfer remaining vectors from current batch to container</li>
   * </ul>
   */
  @Override
  public void doWork() throws DrillException {
    int currentRow = 0;

    logger.trace("WindowFramer.doWork() START, num batches {}, current batch has {} rows",
      batches.size(), batches.get(0).getRecordCount());

    allocateOutgoing();

    final WindowDataBatch current = batches.get(0);

    setupCopyFirstValue(current, internal);

    // we need to store the record count explicitly, because we release current batch at the end of this call
    outputCount = current.getRecordCount();

    while (currentRow < outputCount) {
      if (partition != null) {
        assert currentRow == 0 : "pending windows are only expected at the start of the batch";

        // we have a pending window we need to handle from a previous call to doWork()
        logger.trace("we have a pending partition {}", partition);
      } else {
        newPartition(current, currentRow);
      }

      currentRow = processPartition(currentRow);
      if (partition.isDone()) {
        cleanPartition();
      }
    }

    // transfer "non aggregated" vectors
    for (VectorWrapper<?> vw : current) {
      ValueVector v = container.addOrGet(vw.getField());
      TransferPair tp = vw.getValueVector().makeTransferPair(v);
      tp.transfer();
    }

    for (VectorWrapper<?> v : container) {
      v.getValueVector().getMutator().setValueCount(outputCount);
    }

    // because we are using the default frame, and we keep the aggregated value until we start a new frame
    // we can safely free the current batch
    batches.remove(0).clear();

    logger.trace("WindowFramer.doWork() END");
  }

  private void newPartition(final WindowDataBatch current, final int currentRow) throws SchemaChangeException {
    final long length = computePartitionSize(currentRow);
    partition = new Partition(length);
    setupPartition(current, container);
    copyFirstValueToInternal(currentRow);
  }

  private void cleanPartition() {
    partition = null;
    resetValues();
    for (VectorWrapper<?> vw : internal) {
      if ((vw.getValueVector() instanceof BaseDataValueVector)) {
        ((BaseDataValueVector) vw.getValueVector()).reset();
      }
    }
    lagCopiedToInternal = false;
  }

  /**
   * process all rows (computes and writes aggregation values) of current batch that are part of current partition.
   * @param currentRow first unprocessed row
   * @return index of next unprocessed row
   * @throws DrillException if it can't write into the container
   */
  private int processPartition(final int currentRow) throws DrillException {
    logger.trace("process partition {}, currentRow: {}, outputCount: {}", partition, currentRow, outputCount);

    final VectorAccessible current = getCurrent();
    setupCopyNext(current, container);
    setupPasteValues(internal, container);

    copyPrevFromInternal();

    // copy remaining from current
    setupCopyPrev(current, container);

    int row = currentRow;

    // process all rows except the last one of the batch/partition
    while (row < outputCount && !partition.isDone()) {
      if (row != currentRow) { // this is not the first row of the partition
        copyPrev(row - 1, row);
      }

      processRow(row);

      if (row < outputCount - 1 && !partition.isDone()) {
        copyNext(row + 1, row);
      }

      row++;
    }

    // if we didn't reach the end of partition yet
    if (!partition.isDone() && batches.size() > 1) {
      // copy next value onto the current one
      setupCopyNext(batches.get(1), container);
      copyNext(0, row - 1);

      copyPrevToInternal(current, row);
    }

    return row;
  }

  private void copyPrevToInternal(VectorAccessible current, int row) {
    logger.trace("copying {} into internal", row - 1);
    setupCopyPrev(current, internal);
    copyPrev(row - 1, 0);
    lagCopiedToInternal = true;
  }

  private void copyPrevFromInternal() {
    if (lagCopiedToInternal) {
      setupCopyFromInternal(internal, container);
      copyFromInternal(0, 0);
      lagCopiedToInternal = false;
    }
  }

  private void processRow(final int row) throws DrillException {
    if (partition.isFrameDone()) {
      // because all peer rows share the same frame, we only need to compute and aggregate the frame once
      partition.newFrame(countPeers(row));
      aggregatePeers(row);
    }

    outputRow(row, partition);
    writeLastValue(frameLastRow, row);

    partition.rowAggregated();
  }

  /**
   * @return number of rows that are part of the partition starting at row start of first batch
   */
  private long computePartitionSize(final int start) {
    logger.trace("compute partition size starting from {} on {} batches", start, batches.size());

    // current partition always starts from first batch
    final VectorAccessible first = getCurrent();

    long length = 0;

    // count all rows that are in the same partition of start
    // keep increasing length until we find first row of next partition or we reach the very
    // last batch
    for (WindowDataBatch batch : batches) {
      final int recordCount = batch.getRecordCount();

      // check first container from start row, and subsequent containers from first row
      for (int row = (batch == first) ? start : 0; row < recordCount; row++, length++) {
        if (!isSamePartition(start, first, row, batch)) {
          return length;
        }
      }
    }

    return length;
  }

  /**
   * Counts how many rows are peer with the first row of the current frame
   * @param start first row of current frame
   * @return number of peer rows
   */
  private int countPeers(final int start) {
    // current frame always starts from first batch
    final VectorAccessible first = getCurrent();

    int length = 0;

    // count all rows that are in the same frame of starting row
    // keep increasing length until we find first non peer row we reach the very
    // last batch
    for (WindowDataBatch batch : batches) {
      final int recordCount = batch.getRecordCount();

      // for every remaining row in the partition, count it if it's a peer row
      final long remaining = partition.getRemaining();
      for (int row = (batch == first) ? start : 0; row < recordCount && length < remaining; row++, length++) {
        if (!isPeer(start, first, row, batch)) {
          return length;
        }
      }
    }

    return length;
  }

  /**
   * aggregates all peer rows of current row
   * @param currentRow starting row of the current frame
   * @throws SchemaChangeException
   */
  private void aggregatePeers(final int currentRow) throws SchemaChangeException {
    logger.trace("aggregating {} rows starting from {}", partition.getPeers(), currentRow);
    assert !partition.isFrameDone() : "frame is empty!";

    // a single frame can include rows from multiple batches
    // start processing first batch and, if necessary, move to next batches
    Iterator<WindowDataBatch> iterator = batches.iterator();
    WindowDataBatch current = iterator.next();
    setupEvaluatePeer(current, container);

    final int peers = partition.getPeers();
    int row = currentRow;
    for (int i = 0; i < peers; i++, row++) {
      if (row >= current.getRecordCount()) {
        // we reached the end of the current batch, move to the next one
        current = iterator.next();
        setupEvaluatePeer(current, container);
        row = 0;
      }

      evaluatePeer(row);
    }

    // last row of current frame
    setupReadLastValue(current, container);
    frameLastRow = row - 1;
  }

  @Override
  public boolean canDoWork() {
    // check if we can process a saved batch
    if (batches.size() < 2) {
      logger.trace("we don't have enough batches to proceed, fetch next batch");
      return false;
    }

    final VectorAccessible current = getCurrent();
    final int currentSize = current.getRecordCount();
    final VectorAccessible last = batches.get(batches.size() - 1);
    final int lastSize = last.getRecordCount();

    if (!isSamePartition(currentSize - 1, current, lastSize - 1, last)
        /*|| !isPeer(currentSize - 1, current, lastSize - 1, last)*/) {
      logger.trace("partition changed, we are ready to process first saved batch");
      return true;
    } else {
      logger.trace("partition didn't change, fetch next batch");
      return false;
    }
  }

  /**
   * @return saved batch that will be processed in doWork()
   */
  private VectorAccessible getCurrent() {
    return batches.get(0);
  }

  @Override
  public int getOutputCount() {
    return outputCount;
  }

  // we need this abstract method for code generation
  @Override
  public void cleanup() {
    logger.trace("clearing internal");
    internal.clear();
  }

  /**
   * called once for each peer row of the current frame.
   * @param index of row to aggregate
   */
  public abstract void evaluatePeer(@Named("index") int index);
  public abstract void setupEvaluatePeer(@Named("incoming") VectorAccessible incoming, @Named("outgoing") VectorAccessible outgoing) throws SchemaChangeException;

  public abstract void setupReadLastValue(@Named("incoming") VectorAccessible incoming, @Named("outgoing") VectorAccessible outgoing) throws SchemaChangeException;
  public abstract void writeLastValue(@Named("index") int index, @Named("outIndex") int outIndex);

  public abstract void setupCopyFirstValue(@Named("incoming") VectorAccessible incoming, @Named("outgoing") VectorAccessible outgoing) throws SchemaChangeException;
  public abstract void copyFirstValueToInternal(@Named("index") int index);

  /**
   * called once for each row after we evaluate all peer rows. Used to write a value in the row
   *
   * @param outIndex index of row
   * @param partition object used by "computed" window functions
   */
  public abstract void outputRow(@Named("outIndex") int outIndex, @Named("partition") Partition partition);

  /**
   * Called once per partition, before processing the partition. Used to setup read/write vectors
   * @param incoming batch we will read from
   * @param outgoing batch we will be writing to
   *
   * @throws SchemaChangeException
   */
  public abstract void setupPartition(@Named("incoming") WindowDataBatch incoming,
                                      @Named("outgoing") VectorAccessible outgoing) throws SchemaChangeException;

  /**
   * copies value(s) from inIndex row to outIndex row. Mostly used by LEAD. inIndex always points to the row next to
   * outIndex
   * @param inIndex source row of the copy
   * @param outIndex destination row of the copy.
   */
  public abstract void copyNext(@Named("inIndex") int inIndex, @Named("outIndex") int outIndex);
  public abstract void setupCopyNext(@Named("incoming") VectorAccessible incoming, @Named("outgoing") VectorAccessible outgoing);

  public abstract void setupPasteValues(@Named("incoming") VectorAccessible incoming, @Named("outgoing") VectorAccessible outgoing);

  /**
   * copies value(s) from inIndex row to outIndex row. Mostly used by LAG. inIndex always points to the previous row
   *
   * @param inIndex source row of the copy
   * @param outIndex destination row of the copy.
   */
  public abstract void copyPrev(@Named("inIndex") int inIndex, @Named("outIndex") int outIndex);
  public abstract void setupCopyPrev(@Named("incoming") VectorAccessible incoming, @Named("outgoing") VectorAccessible outgoing);

  public abstract void copyFromInternal(@Named("inIndex") int inIndex, @Named("outIndex") int outIndex);
  public abstract void setupCopyFromInternal(@Named("incoming") VectorAccessible incoming, @Named("outgoing") VectorAccessible outgoing);

  /**
   * reset all window functions
   */
  public abstract boolean resetValues();

  /**
   * compares two rows from different batches (can be the same), if they have the same value for the partition by
   * expression
   * @param b1Index index of first row
   * @param b1 batch for first row
   * @param b2Index index of second row
   * @param b2 batch for second row
   * @return true if the rows are in the same partition
   */
  public abstract boolean isSamePartition(@Named("b1Index") int b1Index, @Named("b1") VectorAccessible b1,
                                          @Named("b2Index") int b2Index, @Named("b2") VectorAccessible b2);

  /**
   * compares two rows from different batches (can be the same), if they have the same value for the order by
   * expression
   * @param b1Index index of first row
   * @param b1 batch for first row
   * @param b2Index index of second row
   * @param b2 batch for second row
   * @return true if the rows are in the same partition
   */
  public abstract boolean isPeer(@Named("b1Index") int b1Index, @Named("b1") VectorAccessible b1,
                                 @Named("b2Index") int b2Index, @Named("b2") VectorAccessible b2);
}
