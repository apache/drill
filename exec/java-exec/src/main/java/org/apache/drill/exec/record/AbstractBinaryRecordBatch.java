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

import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.base.PhysicalOperator;

public abstract class AbstractBinaryRecordBatch<T extends PhysicalOperator> extends  AbstractRecordBatch<T> {
  private static final org.slf4j.Logger logger =
    org.slf4j.LoggerFactory.getLogger(new Object() {}.getClass().getEnclosingClass());

  protected final RecordBatch left;
  protected final RecordBatch right;

  // state (IterOutcome) of the left input
  protected IterOutcome leftUpstream = IterOutcome.NONE;

  // state (IterOutcome) of the right input
  protected IterOutcome rightUpstream = IterOutcome.NONE;

  // For now only used by Lateral and Merge Join
  protected RecordBatchMemoryManager batchMemoryManager;

  protected AbstractBinaryRecordBatch(final T popConfig, final FragmentContext context, RecordBatch left,
      RecordBatch right) throws OutOfMemoryException {
    super(popConfig, context, true, context.newOperatorContext(popConfig));
    this.left = left;
    this.right = right;
  }

  protected AbstractBinaryRecordBatch(final T popConfig, final FragmentContext context, final boolean buildSchema, RecordBatch left,
      RecordBatch right) throws OutOfMemoryException {
    super(popConfig, context, buildSchema);
    this.left = left;
    this.right = right;
  }

  protected boolean verifyOutcomeToSetBatchState(IterOutcome leftOutcome, IterOutcome rightOutcome) {
    if (leftOutcome == IterOutcome.STOP || rightUpstream == IterOutcome.STOP) {
      state = BatchState.STOP;
      return false;
    }

    if (leftOutcome == IterOutcome.OUT_OF_MEMORY || rightUpstream == IterOutcome.OUT_OF_MEMORY) {
      state = BatchState.OUT_OF_MEMORY;
      return false;
    }

    if (checkForEarlyFinish(leftOutcome, rightOutcome)) {
      state = BatchState.DONE;
      return false;
    }

    // EMIT outcome is not expected as part of first batch from either side
    if (leftOutcome == IterOutcome.EMIT || rightOutcome == IterOutcome.EMIT) {
      state = BatchState.STOP;
      throw new IllegalStateException("Unexpected IterOutcome.EMIT received either from left or right side in " +
        "buildSchema phase");
    }
    return true;
  }

  /**
   * Prefetch first batch from both inputs.
   * @return true if caller should continue processing
   *         false if caller should stop and exit from processing.
   */
  protected boolean prefetchFirstBatchFromBothSides() {
    // Left can get batch with zero or more records with OK_NEW_SCHEMA outcome as first batch
    leftUpstream = next(0, left);
    rightUpstream = next(1, right);
    return verifyOutcomeToSetBatchState(leftUpstream, rightUpstream);
  }

  /*
   * Checks for the operator specific early terminal condition.
   * @return true if the further processing can stop.
   *         false if the further processing is needed.
   */
  protected boolean checkForEarlyFinish(IterOutcome leftOutcome, IterOutcome rightOutcome) {
    return (leftOutcome == IterOutcome.NONE && rightOutcome == IterOutcome.NONE);
  }

  protected void updateBatchMemoryManagerStats() {
    stats.setLongStat(JoinBatchMemoryManager.Metric.LEFT_INPUT_BATCH_COUNT,
      batchMemoryManager.getNumIncomingBatches(JoinBatchMemoryManager.LEFT_INDEX));
    stats.setLongStat(JoinBatchMemoryManager.Metric.LEFT_AVG_INPUT_BATCH_BYTES,
      batchMemoryManager.getAvgInputBatchSize(JoinBatchMemoryManager.LEFT_INDEX));
    stats.setLongStat(JoinBatchMemoryManager.Metric.LEFT_AVG_INPUT_ROW_BYTES,
      batchMemoryManager.getAvgInputRowWidth(JoinBatchMemoryManager.LEFT_INDEX));
    stats.setLongStat(JoinBatchMemoryManager.Metric.LEFT_INPUT_RECORD_COUNT,
      batchMemoryManager.getTotalInputRecords(JoinBatchMemoryManager.LEFT_INDEX));

    stats.setLongStat(JoinBatchMemoryManager.Metric.RIGHT_INPUT_BATCH_COUNT,
      batchMemoryManager.getNumIncomingBatches(JoinBatchMemoryManager.RIGHT_INDEX));
    stats.setLongStat(JoinBatchMemoryManager.Metric.RIGHT_AVG_INPUT_BATCH_BYTES,
      batchMemoryManager.getAvgInputBatchSize(JoinBatchMemoryManager.RIGHT_INDEX));
    stats.setLongStat(JoinBatchMemoryManager.Metric.RIGHT_AVG_INPUT_ROW_BYTES,
      batchMemoryManager.getAvgInputRowWidth(JoinBatchMemoryManager.RIGHT_INDEX));
    stats.setLongStat(JoinBatchMemoryManager.Metric.RIGHT_INPUT_RECORD_COUNT,
      batchMemoryManager.getTotalInputRecords(JoinBatchMemoryManager.RIGHT_INDEX));

    stats.setLongStat(JoinBatchMemoryManager.Metric.OUTPUT_BATCH_COUNT,
      batchMemoryManager.getNumOutgoingBatches());
    stats.setLongStat(JoinBatchMemoryManager.Metric.AVG_OUTPUT_BATCH_BYTES,
      batchMemoryManager.getAvgOutputBatchSize());
    stats.setLongStat(JoinBatchMemoryManager.Metric.AVG_OUTPUT_ROW_BYTES,
      batchMemoryManager.getAvgOutputRowWidth());
    stats.setLongStat(JoinBatchMemoryManager.Metric.OUTPUT_RECORD_COUNT,
      batchMemoryManager.getTotalOutputRecords());

  }
}
