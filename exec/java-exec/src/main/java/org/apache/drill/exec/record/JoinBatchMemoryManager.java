/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.record;

import org.apache.drill.exec.ops.MetricDef;

public class JoinBatchMemoryManager extends RecordBatchMemoryManager {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JoinBatchMemoryManager.class);

  private int leftRowWidth;

  private int rightRowWidth;

  private RecordBatch leftIncoming;

  private RecordBatch rightIncoming;

  private static final int numInputs = 2;

  public static final int LEFT_INDEX = 0;

  public static final int RIGHT_INDEX = 1;

  public JoinBatchMemoryManager(int outputBatchSize, RecordBatch leftBatch, RecordBatch rightBatch) {
    super(numInputs, outputBatchSize);
    this.leftIncoming = leftBatch;
    this.rightIncoming = rightBatch;
  }

  @Override
  public int update(int inputIndex, int outputPosition) {
    switch (inputIndex) {
      case LEFT_INDEX:
        setRecordBatchSizer(inputIndex, new RecordBatchSizer(leftIncoming));
        leftRowWidth = getRecordBatchSizer(inputIndex).netRowWidth();
        logger.debug("left incoming batch size : {}", getRecordBatchSizer(inputIndex));
        break;
      case RIGHT_INDEX:
        setRecordBatchSizer(inputIndex, new RecordBatchSizer(rightIncoming));
        rightRowWidth = getRecordBatchSizer(inputIndex).netRowWidth();
        logger.debug("right incoming batch size : {}", getRecordBatchSizer(inputIndex));
      default:
        break;
    }

    updateIncomingStats(inputIndex);
    final int newOutgoingRowWidth = leftRowWidth + rightRowWidth;

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

    logger.debug("output batch size : {}, avg outgoing rowWidth : {}, output rowCount : {}",
      getOutputBatchSize(), getOutgoingRowWidth(), getOutputRowCount());

    return adjustOutputRowCount(outputPosition + numOutputRowsRemaining);
  }

  @Override
  public RecordBatchSizer.ColumnSize getColumnSize(String name) {
    RecordBatchSizer leftSizer = getRecordBatchSizer(LEFT_INDEX);
    RecordBatchSizer rightSizer = getRecordBatchSizer(RIGHT_INDEX);

    if (leftSizer != null && leftSizer.getColumn(name) != null) {
      return leftSizer.getColumn(name);
    }
    return rightSizer == null ? null : rightSizer.getColumn(name);
  }

  public enum Metric implements MetricDef {
    LEFT_INPUT_BATCH_COUNT,
    LEFT_AVG_INPUT_BATCH_BYTES,
    LEFT_AVG_INPUT_ROW_BYTES,
    LEFT_INPUT_RECORD_COUNT,
    RIGHT_INPUT_BATCH_COUNT,
    RIGHT_AVG_INPUT_BATCH_BYTES,
    RIGHT_AVG_INPUT_ROW_BYTES,
    RIGHT_INPUT_RECORD_COUNT,
    OUTPUT_BATCH_COUNT,
    AVG_OUTPUT_BATCH_BYTES,
    AVG_OUTPUT_ROW_BYTES,
    OUTPUT_RECORD_COUNT;

    @Override
    public int metricId() {
      return ordinal();
    }
  }
}
