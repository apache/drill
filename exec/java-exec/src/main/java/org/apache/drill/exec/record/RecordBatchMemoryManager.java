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
package org.apache.drill.exec.record;

import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.drill.exec.vector.UInt4Vector;
import org.apache.drill.exec.vector.ValueVector;

import java.util.List;

public class RecordBatchMemoryManager {
  protected static final int MAX_NUM_ROWS = ValueVector.MAX_ROW_COUNT;
  protected static final int MIN_NUM_ROWS = 1;
  protected static final int DEFAULT_INPUT_INDEX = 0;
  private int outputRowCount = MAX_NUM_ROWS;
  private int outgoingRowWidth;
  private int outputBatchSize;
  private RecordBatchSizer[] sizer;
  private BatchStats[] inputBatchStats;
  private BatchStats outputBatchStats;

  // By default, we expect one input batch stream and one output batch stream.
  // Some operators can get multiple input batch streams i.e. for example
  // joins get 2 batches (left and right). Merge Receiver can get more than 2.
  private int numInputs = 1;

  private class BatchStats {
    /**
     * operator metric stats
     */
    private long numBatches;
    private long sumBatchSizes;
    private long totalRecords;

    public long getNumBatches() {
      return numBatches;
    }

    public long getTotalRecords() {
      return totalRecords;
    }

    public long getAvgBatchSize() {
      return RecordBatchSizer.safeDivide(sumBatchSizes, numBatches);
    }

    public long getAvgRowWidth() {
      return RecordBatchSizer.safeDivide(sumBatchSizes, totalRecords);
    }

    public void incNumBatches() {
      ++numBatches;
    }

    public void incSumBatchSizes(long batchSize) {
      sumBatchSizes += batchSize;
    }

    public void incTotalRecords(long numRecords) {
      totalRecords += numRecords;
    }

  }

  public long getNumOutgoingBatches() {
    return outputBatchStats.getNumBatches();
  }

  public long getTotalOutputRecords() {
    return outputBatchStats.getTotalRecords();
  }

  public long getAvgOutputBatchSize() {
    return outputBatchStats.getAvgBatchSize();
  }

  public long getAvgOutputRowWidth() {
    return outputBatchStats.getAvgRowWidth();
  }

  public long getNumIncomingBatches() {
    return inputBatchStats[DEFAULT_INPUT_INDEX] == null ? 0 : inputBatchStats[DEFAULT_INPUT_INDEX].getNumBatches();
  }

  public long getAvgInputBatchSize() {
    return inputBatchStats[DEFAULT_INPUT_INDEX] == null ? 0 : inputBatchStats[DEFAULT_INPUT_INDEX].getAvgBatchSize();
  }

  public long getAvgInputRowWidth() {
    return inputBatchStats[DEFAULT_INPUT_INDEX] == null ? 0 : inputBatchStats[DEFAULT_INPUT_INDEX].getAvgRowWidth();
  }

  public long getTotalInputRecords() {
    return inputBatchStats[DEFAULT_INPUT_INDEX] == null ? 0 : inputBatchStats[DEFAULT_INPUT_INDEX].getTotalRecords();
  }

  public long getNumIncomingBatches(int index) {
    Preconditions.checkArgument(index >= 0 && index < numInputs);
    return inputBatchStats[index] == null ? 0 : inputBatchStats[index].getNumBatches();
  }

  public long getAvgInputBatchSize(int index) {
    Preconditions.checkArgument(index >= 0 && index < numInputs);
    return inputBatchStats[index] == null ? 0 : inputBatchStats[index].getAvgBatchSize();
  }

  public long getAvgInputRowWidth(int index) {
    Preconditions.checkArgument(index >= 0 && index < numInputs);
    return inputBatchStats[index] == null ? 0 : inputBatchStats[index].getAvgRowWidth();
  }

  public long getTotalInputRecords(int index) {
    Preconditions.checkArgument(index >= 0 && index < numInputs);
    return inputBatchStats[index] == null ? 0 : inputBatchStats[index].getTotalRecords();
  }

  public RecordBatchMemoryManager(int numInputs, int configuredOutputSize) {
    this.numInputs = numInputs;
    this.outputBatchSize = configuredOutputSize;
    sizer = new RecordBatchSizer[numInputs];
    inputBatchStats = new BatchStats[numInputs];
    outputBatchStats = new BatchStats();
  }

  public RecordBatchMemoryManager(int configuredOutputSize) {
    this.outputBatchSize = configuredOutputSize;
    sizer = new RecordBatchSizer[numInputs];
    inputBatchStats = new BatchStats[numInputs];
    outputBatchStats = new BatchStats();
  }

  public int update(int inputIndex, int outputPosition) {
    // by default just return the outputRowCount
    return getOutputRowCount();
  }

  public void update(int inputIndex) {
  }

  public void update() {};

  public void update(RecordBatch recordBatch) {
  }

  public void update(RecordBatch recordBatch, int index) {
    // Get sizing information for the batch.
    setRecordBatchSizer(index, new RecordBatchSizer(recordBatch));
    setOutgoingRowWidth(getRecordBatchSizer(index).getNetRowWidth());
    // Number of rows in outgoing batch
    setOutputRowCount(getOutputBatchSize(), getRecordBatchSizer(index).getNetRowWidth());
    updateIncomingStats(index);
  }

  public int update(int inputIndex, int outputPosition, boolean useAggregate) {
    // by default just return the outputRowCount
    return getOutputRowCount();
  }

  public int update(RecordBatch batch, int inputIndex, int outputPosition) {
    return getOutputRowCount();
  }

  public int update(RecordBatch batch, int inputIndex, int outputPosition, boolean useAggregate) {
    return getOutputRowCount();
  }

  public boolean updateIfNeeded(int newOutgoingRowWidth) {
    // We do not want to keep adjusting batch holders target row count
    // for small variations in row width.
    // If row width changes, calculate actual adjusted row count i.e. row count
    // rounded down to nearest power of two and do nothing if that does not change.
    if (newOutgoingRowWidth == outgoingRowWidth ||
      computeOutputRowCount(outputBatchSize, newOutgoingRowWidth) == computeOutputRowCount(outputBatchSize, outgoingRowWidth)) {
      return false;
    }

    // Set number of rows in outgoing batch. This number will be used for new batch creation.
    setOutputRowCount(outputBatchSize, newOutgoingRowWidth);
    setOutgoingRowWidth(newOutgoingRowWidth);
    return true;
  }

  public int getOutputRowCount() {
    return outputRowCount;
  }

  /**
   * Given batchSize and rowWidth, this will set output rowCount taking into account
   * the min and max that is allowed.
   */
  public void setOutputRowCount(int targetBatchSize, int rowWidth) {
    this.outputRowCount = adjustOutputRowCount(RecordBatchSizer.safeDivide(targetBatchSize, rowWidth));
  }

  public void setOutputRowCount(int outputRowCount) {
    this.outputRowCount = outputRowCount;
  }

  /**
   * This will adjust rowCount taking into account the min and max that is allowed.
   * We will round down to nearest power of two - 1 for better memory utilization.
   * -1 is done for adjusting accounting for offset vectors.
   */
  public static int adjustOutputRowCount(int rowCount) {
    return (Math.min(MAX_NUM_ROWS, Math.max(Integer.highestOneBit(rowCount) - 1, MIN_NUM_ROWS)));
  }

  public static int computeOutputRowCount(int batchSize, int rowWidth) {
    return adjustOutputRowCount(RecordBatchSizer.safeDivide(batchSize, rowWidth));
  }

  public void setOutgoingRowWidth(int outgoingRowWidth) {
    this.outgoingRowWidth = outgoingRowWidth;
  }

  public int getOutgoingRowWidth() {
    return outgoingRowWidth;
  }

  public void setRecordBatchSizer(int index, RecordBatchSizer sizer) {
    Preconditions.checkArgument(index >= 0 && index < numInputs);
    this.sizer[index] = sizer;
    if (inputBatchStats[index] == null) {
      inputBatchStats[index] = new BatchStats();
    }
  }

  public void setRecordBatchSizer(RecordBatchSizer sizer) {
    setRecordBatchSizer(DEFAULT_INPUT_INDEX, sizer);
  }

  public RecordBatchSizer getRecordBatchSizer(int index) {
    Preconditions.checkArgument(index >= 0 && index < numInputs);
    return sizer[index];
  }

  public RecordBatchSizer getRecordBatchSizer() {
    return sizer[DEFAULT_INPUT_INDEX];
  }

  public RecordBatchSizer.ColumnSize getColumnSize(int index, String name) {
    Preconditions.checkArgument(index >= 0 && index < numInputs);
    return sizer[index].getColumn(name);
  }

  public RecordBatchSizer.ColumnSize getColumnSize(String name) {
    for (int index = 0; index < numInputs; index++) {
      if (sizer[index] == null || sizer[index].getColumn(name) == null) {
        continue;
      }
      return sizer[index].getColumn(name);
    }
    return null;
  }

  public void updateIncomingStats(int index) {
    Preconditions.checkArgument(index >= 0 && index < numInputs);
    Preconditions.checkArgument(inputBatchStats[index] != null);
    inputBatchStats[index].incNumBatches();
    inputBatchStats[index].incSumBatchSizes(sizer[index].getNetBatchSize());
    inputBatchStats[index].incTotalRecords(sizer[index].rowCount());
  }

  public void updateIncomingStats() {
    inputBatchStats[DEFAULT_INPUT_INDEX].incNumBatches();
    inputBatchStats[DEFAULT_INPUT_INDEX].incSumBatchSizes(sizer[DEFAULT_INPUT_INDEX].getNetBatchSize());
    inputBatchStats[DEFAULT_INPUT_INDEX].incTotalRecords(sizer[DEFAULT_INPUT_INDEX].rowCount());
  }

  public void updateOutgoingStats(int outputRecords) {
    outputBatchStats.incNumBatches();
    outputBatchStats.incTotalRecords(outputRecords);
    outputBatchStats.incSumBatchSizes(outgoingRowWidth * outputRecords);
  }

  public int getOutputBatchSize() {
    return outputBatchSize;
  }

  public int getOffsetVectorWidth() {
    return UInt4Vector.VALUE_WIDTH;
  }

  public void allocateVectors(VectorContainer container, int recordCount) {
    // Allocate memory for the vectors.
    // This will iteratively allocate memory for all nested columns underneath.
    for (VectorWrapper w : container) {
      RecordBatchSizer.ColumnSize colSize = getColumnSize(w.getField().getName());
      colSize.allocateVector(w.getValueVector(), recordCount);
    }
    container.setRecordCount(0);
  }

  public void allocateVectors(List<ValueVector> valueVectors, int recordCount) {
    // Allocate memory for the vectors.
    // This will iteratively allocate memory for all nested columns underneath.
    for (ValueVector v : valueVectors) {
      RecordBatchSizer.ColumnSize colSize = getColumnSize(v.getField().getName());
      colSize.allocateVector(v, recordCount);
    }
  }

  public void allocateVectors(VectorContainer container) {
    allocateVectors(container, outputRowCount);
  }

  public void allocateVectors(List<ValueVector> valueVectors) {
    allocateVectors(valueVectors, outputRowCount);
  }
}
