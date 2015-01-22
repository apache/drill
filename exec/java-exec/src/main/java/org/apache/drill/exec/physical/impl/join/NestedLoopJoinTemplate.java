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
package org.apache.drill.exec.physical.impl.join;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.record.ExpandableHyperContainer;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;

import javax.inject.Named;
import java.util.LinkedList;
import java.util.List;

/*
 * Template class that combined with the runtime generated source implements the NestedLoopJoin interface. This
 * class contains the main nested loop join logic.
 */
public abstract class NestedLoopJoinTemplate implements NestedLoopJoin {

  // Current left input batch being processed
  private RecordBatch left = null;

  // Record count of the left batch currently being processed
  private int leftRecordCount = 0;

  // List of record counts  per batch in the hyper container
  private List<Integer> rightCounts = null;

  // Output batch
  private NestedLoopJoinBatch outgoing = null;

  // Next right batch to process
  private int nextRightBatchToProcess = 0;

  // Next record in the current right batch to process
  private int nextRightRecordToProcess = 0;

  // Next record in the left batch to process
  private int nextLeftRecordToProcess = 0;

  /**
   * Method initializes necessary state and invokes the doSetup() to set the
   * input and output value vector references
   * @param context Fragment context
   * @param left Current left input batch being processed
   * @param rightContainer Hyper container
   * @param outgoing Output batch
   */
  public void setupNestedLoopJoin(FragmentContext context, RecordBatch left,
                                  ExpandableHyperContainer rightContainer,
                                  LinkedList<Integer> rightCounts,
                                  NestedLoopJoinBatch outgoing) {
    this.left = left;
    leftRecordCount = left.getRecordCount();
    this.rightCounts = rightCounts;
    this.outgoing = outgoing;

    doSetup(context, rightContainer, left, outgoing);
  }

  /**
   * This method is the core of the nested loop join. For every record on the right we go over
   * the left batch and produce the cross product output
   * @param outputIndex index to start emitting records at
   * @return final outputIndex after producing records in the output batch
   */
  private int populateOutgoingBatch(int outputIndex) {

    // Total number of batches on the right side
    int totalRightBatches = rightCounts.size();

    // Total number of records on the left
    int localLeftRecordCount = leftRecordCount;

    /*
     * The below logic is the core of the NLJ. To have better performance we copy the instance members into local
     * method variables, once we are done with the loop we need to update the instance variables to reflect the new
     * state. To avoid code duplication of resetting the instance members at every exit point in the loop we are using
     * 'goto'
     */
    int localNextRightBatchToProcess = nextRightBatchToProcess;
    int localNextRightRecordToProcess = nextRightRecordToProcess;
    int localNextLeftRecordToProcess = nextLeftRecordToProcess;

    outer: {

      for (; localNextRightBatchToProcess< totalRightBatches; localNextRightBatchToProcess++) { // for every batch on the right
        int compositeIndexPart = localNextRightBatchToProcess << 16;
        int rightRecordCount = rightCounts.get(localNextRightBatchToProcess);

        for (; localNextRightRecordToProcess < rightRecordCount; localNextRightRecordToProcess++) { // for every record in this right batch
          for (; localNextLeftRecordToProcess < localLeftRecordCount; localNextLeftRecordToProcess++) { // for every record in the left batch

            // project records from the left and right batches
            emitLeft(localNextLeftRecordToProcess, outputIndex);
            emitRight(localNextRightBatchToProcess, localNextRightRecordToProcess, outputIndex);
            outputIndex++;

            // TODO: Optimization; We can eliminate this check and compute the limits before the loop
            if (outputIndex >= NestedLoopJoinBatch.MAX_BATCH_SIZE) {
              localNextLeftRecordToProcess++;

              // no more space left in the batch, stop processing
              break outer;
            }
          }
          localNextLeftRecordToProcess = 0;
        }
        localNextRightRecordToProcess = 0;
      }
    }

    // update the instance members
    nextRightBatchToProcess = localNextRightBatchToProcess;
    nextRightRecordToProcess = localNextRightRecordToProcess;
    nextLeftRecordToProcess = localNextLeftRecordToProcess;

    // done with the current left batch and there is space in the output batch continue processing
    return outputIndex;
  }

  /**
   * Main entry point for producing the output records. Thin wrapper around populateOutgoingBatch(), this method
   * controls which left batch we are processing and fetches the next left input batch one we exhaust
   * the current one.
   * @return the number of records produced in the output batch
   */
  public int outputRecords() {
    int outputIndex = 0;
    while (leftRecordCount != 0) {
      outputIndex = populateOutgoingBatch(outputIndex);
      if (outputIndex >= NestedLoopJoinBatch.MAX_BATCH_SIZE) {
        break;
      }
      // reset state and get next left batch
      resetAndGetNextLeft();
    }
    return outputIndex;
  }

  /**
   * Utility method to clear the memory in the left input batch once we have completed processing it. Resets some
   * internal state which indicate the next records to process in the left and right batches. Also fetches the next
   * left input batch.
   */
  private void resetAndGetNextLeft() {

    for (VectorWrapper<?> vw : left) {
      vw.getValueVector().clear();
    }
    nextRightBatchToProcess = nextRightRecordToProcess = nextLeftRecordToProcess = 0;
    RecordBatch.IterOutcome leftOutcome = outgoing.next(NestedLoopJoinBatch.LEFT_INPUT, left);
    switch (leftOutcome) {
      case OK_NEW_SCHEMA:
        throw new DrillRuntimeException("Nested loop join does not handle schema change. Schema change" +
            " found on the left side of NLJ.");
      case NONE:
      case NOT_YET:
      case STOP:
        leftRecordCount = 0;
        break;
      case OK:
        leftRecordCount = left.getRecordCount();
        break;
    }
  }

  public abstract void doSetup(@Named("context") FragmentContext context,
                               @Named("rightContainer") VectorContainer rightContainer,
                               @Named("leftBatch") RecordBatch leftBatch,
                               @Named("outgoing") RecordBatch outgoing);

  public abstract void emitRight(@Named("batchIndex") int batchIndex,
                                 @Named("recordIndexWithinBatch") int recordIndexWithinBatch,
                                 @Named("outIndex") int outIndex);

  public abstract void emitLeft(@Named("leftIndex") int leftIndex, @Named("outIndex") int outIndex);
}
