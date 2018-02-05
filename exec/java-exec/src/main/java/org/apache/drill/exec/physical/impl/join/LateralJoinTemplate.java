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
package org.apache.drill.exec.physical.impl.join;

import com.google.common.base.Preconditions;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.record.RecordBatch;

import javax.inject.Named;

/*
 * Template class that combined with the runtime generated source implements the LateralJoin interface. This
 * class contains the main lateral join logic.
 */
public abstract class LateralJoinTemplate implements LateralJoin {

  // Current right input batch being processed
  private RecordBatch right = null;

  // Index in outgoing container where new record will be inserted
  private int outputIndex = 0;

  // Keep the join type at setup phase
  private JoinRelType joinType;

  /**
   * Method initializes necessary state and invokes the doSetup() to set the input and output value vector references.
   *
   * @param context Fragment context
   * @param left Current left input batch being processed
   * @param right Current right input batch being processed
   * @param outgoing Output batch
   */
  public void setupLateralJoin(FragmentContext context,
                               RecordBatch left, RecordBatch right,
                               LateralJoinBatch outgoing, JoinRelType joinType) {
    this.right = right;
    this.joinType = joinType;
    doSetup(context, this.right, left, outgoing);
  }

  /**
   * Main entry point for producing the output records. This method populates the output batch after cross join of
   * the record in a given left batch at left index and all the corresponding right batches produced for
   * this left index. The right container is copied starting from rightIndex until number of records in the container.
   *
   * @return the number of records produced in the output batch
   */
  public int crossJoinAndOutputRecords(int leftIndex, int rightIndex) {

    final int rightRecordCount = right.getRecordCount();
    int currentOutputIndex = outputIndex;

    // If there is no record in right batch just return current index in output batch
    if (rightRecordCount <= 0) {
      return currentOutputIndex;
    }

    // Check if right batch is empty since we have to handle left join case
    Preconditions.checkState(rightIndex != -1, "Right batch record count is >0 but index is -1");
    // For every record in right side just emit left and right records in output container
    for (; rightIndex < rightRecordCount; ++rightIndex) {
      emitLeft(leftIndex, currentOutputIndex);
      emitRight(rightIndex, currentOutputIndex);
      ++currentOutputIndex;

      if (currentOutputIndex >= LateralJoinBatch.MAX_BATCH_SIZE) {
        break;
      }
    }

    updateOutputIndex(currentOutputIndex);
    return currentOutputIndex;
  }

  /**
   * If current output batch is full then reset the output index for next output batch
   * Otherwise it means we still have space left in output batch, so next call will continue populating from
   * newOutputIndex
   * @param newOutputIndex - new output index of outgoing batch after copying the records
   */
  private void updateOutputIndex(int newOutputIndex) {
    outputIndex = (newOutputIndex >= LateralJoinBatch.MAX_BATCH_SIZE) ?
      0 : newOutputIndex;
  }

  /**
   * Method to copy just the left batch record at given leftIndex, the right side records will be NULL. This is
   * used in case when Join Type is LEFT and we have only seen empty batches from right side
   * @param leftIndex - index in left batch to copy record from
   */
  public void generateLeftJoinOutput(int leftIndex) {
    int currentOutputIndex = outputIndex;

    if (JoinRelType.LEFT == joinType) {
      emitLeft(leftIndex, currentOutputIndex++);
      updateOutputIndex(currentOutputIndex);
    }
  }

  /**
   * Generated method to setup vector references in rightBatch, leftBatch and outgoing batch. It should be called
   * after initial schema build phase, when the schema for outgoing container is known. This method should also be
   * called after each New Schema discovery during execution.
   * @param context
   * @param rightBatch - right incoming batch
   * @param leftBatch - left incoming batch
   * @param outgoing - output batch
   */
  public abstract void doSetup(@Named("context") FragmentContext context,
                               @Named("rightBatch") RecordBatch rightBatch,
                               @Named("leftBatch") RecordBatch leftBatch,
                               @Named("outgoing") RecordBatch outgoing);

  /**
   * Generated method to copy the record from right batch at rightIndex to outgoing batch at outIndex
   * @param rightIndex - index to copy record from the right batch
   * @param outIndex - index to copy record to a outgoing batch
   */
  public abstract void emitRight(@Named("rightIndex") int rightIndex,
                                 @Named("outIndex") int outIndex);

  /**
   * Generated method to copy the record from left batch at leftIndex to outgoing batch at outIndex
   * @param leftIndex - index to copy record from the left batch
   * @param outIndex - index to copy record to a outgoing batch
   */
  public abstract void emitLeft(@Named("leftIndex") int leftIndex,
                                @Named("outIndex") int outIndex);
}
