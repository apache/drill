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

public class JoinBatchMemoryManager extends RecordBatchMemoryManager {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JoinBatchMemoryManager.class);

  private int rowWidth[];
  private RecordBatch recordBatch[];

  private static final int numInputs = 2;
  public static final int LEFT_INDEX = 0;
  public static final int RIGHT_INDEX = 1;

  public JoinBatchMemoryManager(int outputBatchSize, RecordBatch leftBatch, RecordBatch rightBatch) {
    super(numInputs, outputBatchSize);
    recordBatch = new RecordBatch[numInputs];
    recordBatch[LEFT_INDEX] = leftBatch;
    recordBatch[RIGHT_INDEX] = rightBatch;
    rowWidth = new int[numInputs];
  }

  private int updateInternal(int inputIndex, int outputPosition,  boolean useAggregate) {
    updateIncomingStats(inputIndex);
    rowWidth[inputIndex] = useAggregate ? (int) getAvgInputRowWidth(inputIndex) : getRecordBatchSizer(inputIndex).getRowAllocSize();

    final int newOutgoingRowWidth = rowWidth[LEFT_INDEX] + rowWidth[RIGHT_INDEX];

    // If outgoing row width is 0, just return. This is possible for empty batches or
    // when first set of batches come with OK_NEW_SCHEMA and no data.
    if (newOutgoingRowWidth == 0) {
      return getOutputRowCount();
    }

    // Adjust for the current batch.
    // calculate memory used so far based on previous outgoing row width and how many rows we already processed.
    final int previousOutgoingWidth = getOutgoingRowWidth();
    final long memoryUsed = outputPosition * previousOutgoingWidth;

    final int configOutputBatchSize = getOutputBatchSize();
    // This is the remaining memory.
    final long remainingMemory = Math.max(configOutputBatchSize - memoryUsed, 0);

    // These are number of rows we can fit in remaining memory based on new outgoing row width.
    final int numOutputRowsRemaining = RecordBatchSizer.safeDivide(remainingMemory, newOutgoingRowWidth);

    // update the value to be used for next batch(es)
    setOutputRowCount(configOutputBatchSize, newOutgoingRowWidth);

    // set the new row width
    setOutgoingRowWidth(newOutgoingRowWidth);

    return adjustOutputRowCount(outputPosition + numOutputRowsRemaining);
  }

  @Override
  public int update(int inputIndex, int outputPosition, boolean useAggregate) {
    setRecordBatchSizer(inputIndex, new RecordBatchSizer(recordBatch[inputIndex]));
    return updateInternal(inputIndex, outputPosition, useAggregate);
  }

  @Override
  public int update(int inputIndex, int outputPosition) {
    return update(inputIndex, outputPosition, false);
  }

  @Override
  public int update(RecordBatch batch, int inputIndex, int outputPosition, boolean useAggregate) {
    setRecordBatchSizer(inputIndex, new RecordBatchSizer(batch));
    return updateInternal(inputIndex, outputPosition, useAggregate);
  }

  @Override
  public int update(RecordBatch batch, int inputIndex, int outputPosition) {
    return update(batch, inputIndex, outputPosition, false);
  }
}
