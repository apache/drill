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
 * Maintain join state using RecordIterator.
 */
public final class JoinStatus {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JoinStatus.class);

  private static final int OUTPUT_BATCH_SIZE = 32*1024;

  private static enum InitState {
    INIT, // initial state
    CHECK, // need to check if batches are empty
    READY // read to do work
  }

  public final RecordIterator left;
  public final RecordIterator right;

  private int outputPosition;
  public MergeJoinBatch outputBatch;

  private final JoinRelType joinType;

  public boolean ok = true;
  private InitState initialSet = InitState.INIT;

  public JoinStatus(RecordIterator left, RecordIterator right, MergeJoinBatch output) {
    super();
    this.left = left;
    this.right = right;
    this.outputBatch = output;
    this.joinType = output.getJoinType();
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
        + ", initialSet = " + initialSet
        + ", left = " + left
        + ", right = " + right
        + ", outputBatch = " + outputBatch
        + "]";
  }


  public final void ensureInitial() {
    switch(initialSet) {
      case INIT:
        advanceLeft();
        advanceRight();
        initialSet = InitState.CHECK;
        break;
      case CHECK:
        if (getLeftStatus() != IterOutcome.NONE && left.getInnerRecordCount() == 0) {
          advanceLeft();
        }
        if (getRightStatus() != IterOutcome.NONE && right.getInnerRecordCount() == 0) {
          advanceRight();
        }
        initialSet = InitState.READY;
        break;
      default:
        break;
    }
  }

  public final void advanceLeft() {
    left.next();
  }

  public final void advanceRight() {
    right.next();
  }

  public final boolean leftFinished() {
     return left.finished();
  }

  public final boolean rightFinished() {
    return right.finished();
  }

  public final int getLeftPosition() {
    return left.getCurrentPosition();
  }

  public final int getRightPosition() {
    return right.getCurrentPosition();
  }

  public IterOutcome getLeftStatus() { return left.getLastOutcome(); }

  public IterOutcome getRightStatus() { return right.getLastOutcome(); }

  public final void markRight() {
    right.mark();
  }

  public final void resetRight() {
    right.reset();
  }

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
    outputPosition++;
  }

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
    if (eitherMatches(IterOutcome.NOT_YET)) {
      return JoinOutcome.WAITING;
    }
    return JoinOutcome.FAILURE;
  }

  private boolean bothMatches(IterOutcome outcome) {
    return getLeftStatus() == outcome && getRightStatus() == outcome;
  }

  private boolean eitherMatches(IterOutcome outcome) {
    return getLeftStatus() == outcome || getRightStatus() == outcome;
  }

}
