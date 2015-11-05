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

import org.apache.drill.exec.physical.impl.join.JoinWorker.JoinOutcome;
import org.apache.drill.exec.record.RecordBatch.IterOutcome;
import org.apache.drill.exec.record.RecordIterator;
import org.apache.calcite.rel.core.JoinRelType;

/**
 * Maintain join state.
 */
public final class JoinStatus {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JoinStatus.class);

  private static final int OUTPUT_BATCH_SIZE = 32*1024;

  public final RecordIterator left;
  public final RecordIterator right;
  private boolean iteratorInitialized;

  private int outputPosition;
  public MergeJoinBatch outputBatch;

  private final JoinRelType joinType;
  private boolean allowMarking;

  public boolean ok = true;

  public JoinStatus(RecordIterator left, RecordIterator right, MergeJoinBatch output) {
    this.left = left;
    this.right = right;
    this.outputBatch = output;
    this.joinType = output.getJoinType();
    this.iteratorInitialized = false;
    this.allowMarking = true;
  }

  @Override
  public String toString() {
    return
      super.toString()
        + "["
        + "leftPosition = " + left.getCurrentPosition()
        + ", rightPosition = " + right.getCurrentPosition()
        + ", outputPosition = " + outputPosition
        + ", joinType = " + joinType
        + ", ok = " + ok
        + ", initialSet = " + iteratorInitialized
        + ", left = " + left
        + ", right = " + right
        + ", outputBatch = " + outputBatch
        + "]";
  }

  // Initialize left and right record iterator. We avoid doing this in constructor.
  // Callers must check state of each iterator after calling ensureInitial.
  public void initialize() {
    if (!iteratorInitialized) {
      left.next();
      right.next();
      iteratorInitialized = true;
    }
  }

  public void prepare() {
    if (!iteratorInitialized) {
      initialize();
    }
    left.prepare();
    right.prepare();
  }

  public IterOutcome getLeftStatus() { return left.getLastOutcome(); }

  public IterOutcome getRightStatus() { return right.getLastOutcome(); }

  public final int getOutPosition() {
    return outputPosition;
  }

  public final void resetOutputPos() {
    outputPosition = 0;
  }

  public final boolean isOutgoingBatchFull() {
    return outputPosition == OUTPUT_BATCH_SIZE;
  }

  public final void incOutputPos() {
    ++outputPosition;
  }

  public void disableMarking() {
    allowMarking = false;
  }

  public void enableMarking() {
    allowMarking = true;
  }

  public boolean shouldMark() {
    return allowMarking;
  }

  /**
   * Return state of join based on status of left and right iterator.
   * @return
   *  1. JoinOutcome.NO_MORE_DATA : Join is finished
   *  2. JoinOutcome.FAILURE : There is an error during join.
   *  3. JoinOutcome.BATCH_RETURNED : one of the side has data
   *  4. JoinOutcome.SCHEMA_CHANGED : one of the side has change in schema.
   */
  public JoinOutcome getOutcome() {
    if (!ok) {
      return JoinOutcome.FAILURE;
    }
    if (bothMatches(IterOutcome.NONE) ||
      (joinType == JoinRelType.INNER && eitherMatches(IterOutcome.NONE)) ||
      (joinType == JoinRelType.LEFT && getLeftStatus() == IterOutcome.NONE) ||
      (joinType == JoinRelType.RIGHT && getRightStatus() == IterOutcome.NONE)) {
      return JoinOutcome.NO_MORE_DATA;
    }
    if (bothMatches(IterOutcome.OK) ||
      (eitherMatches(IterOutcome.NONE) && eitherMatches(IterOutcome.OK))) {
      return JoinOutcome.BATCH_RETURNED;
    }
    if (eitherMatches(IterOutcome.OK_NEW_SCHEMA)) {
      return JoinOutcome.SCHEMA_CHANGED;
    }
    // should never see NOT_YET
    if (eitherMatches(IterOutcome.NOT_YET)) {
      return JoinOutcome.WAITING;
    }
    ok = false;
    // on STOP, OUT_OF_MEMORY return FAILURE.
    return JoinOutcome.FAILURE;
  }

  private boolean bothMatches(IterOutcome outcome) {
    return getLeftStatus() == outcome && getRightStatus() == outcome;
  }

  private boolean eitherMatches(IterOutcome outcome) {
    return getLeftStatus() == outcome || getRightStatus() == outcome;
  }

}