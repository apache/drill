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
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.RecordBatch.IterOutcome;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.eigenbase.rel.JoinRelType;

/**
 * The status of the current join.  Maintained outside the individually compiled join templates so that we can carry status across multiple schemas.
 */
public final class JoinStatus {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JoinStatus.class);

  public static enum RightSourceMode {
    INCOMING, SV4;
  }

  private static enum InitState {
    INIT, // initial state
    CHECK, // need to check if batches are empty
    READY // read to do work
  }

  private static final int LEFT_INPUT = 0;
  private static final int RIGHT_INPUT = 1;

  public final RecordBatch left;
  private int leftPosition;
  private IterOutcome lastLeft;

  public final RecordBatch right;
  private int rightPosition;
  private int svRightPosition;
  private IterOutcome lastRight;

  private int outputPosition;
  public RightSourceMode rightSourceMode = RightSourceMode.INCOMING;
  public MergeJoinBatch outputBatch;
  public SelectionVector4 sv4;

  private final JoinRelType joinType;

  public boolean ok = true;
  private InitState initialSet = InitState.INIT;
  private boolean leftRepeating = false;

  public JoinStatus(RecordBatch left, RecordBatch right, MergeJoinBatch output) {
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
        + "leftPosition = " + leftPosition
        + ", rightPosition = " + rightPosition
        + ", svRightPosition = " + svRightPosition
        + ", outputPosition = " + outputPosition
        + ", lastLeft = " + lastLeft
        + ", lastRight = " + lastRight
        + ", rightSourceMode = " + rightSourceMode
        + ", sv4 = " + sv4
        + ", joinType = " + joinType
        + ", ok = " + ok
        + ", initialSet = " + initialSet
        + ", leftRepeating = " + leftRepeating
        + ", left = " + left
        + ", right = " + right
        + ", outputBatch = " + outputBatch
        + "]";
  }

  private final IterOutcome nextLeft() {
    return outputBatch.next(LEFT_INPUT, left);
  }

  private final IterOutcome nextRight() {
    return outputBatch.next(RIGHT_INPUT, right);
  }

  public final void ensureInitial() {
    switch(initialSet) {
      case INIT:
        this.lastLeft = nextLeft();
        this.lastRight = nextRight();
        initialSet = InitState.CHECK;
        break;
      case CHECK:
        if (lastLeft != IterOutcome.NONE && left.getRecordCount() == 0) {
          this.lastLeft = nextLeft();
        }
        if (lastRight != IterOutcome.NONE && right.getRecordCount() == 0) {
          this.lastRight = nextRight();
        }
        initialSet = InitState.READY;
        // fall through
      default:
        break;
    }
  }

  public final void advanceLeft() {
    leftPosition++;
  }

  public final void advanceRight() {
    if (rightSourceMode == RightSourceMode.INCOMING) {
      rightPosition++;
    } else {
      svRightPosition++;
    }
  }

  public final int getLeftPosition() {
    return leftPosition;
  }

  public final int getRightPosition() {
    return (rightSourceMode == RightSourceMode.INCOMING) ? rightPosition : svRightPosition;
  }

  public final int getRightCount() {
    return right.getRecordCount();
  }

  public final void setRightPosition(int pos) {
    rightPosition = pos;
  }


  public final int getOutPosition() {
    return outputPosition;
  }

  public final int fetchAndIncOutputPos() {
    return outputPosition++;
  }

  public final void resetOutputPos() {
    outputPosition = 0;
  }

  public final void incOutputPos() {
    outputPosition++;
  }

  public final void notifyLeftRepeating() {
    leftRepeating = true;
    outputBatch.resetBatchBuilder();
  }

  public final void notifyLeftStoppedRepeating() {
    leftRepeating = false;
    svRightPosition = 0;
  }

  public final boolean isLeftRepeating() {
    return leftRepeating;
  }

  public void setDefaultAdvanceMode() {
    rightSourceMode = RightSourceMode.INCOMING;
  }

  public void setSV4AdvanceMode() {
    rightSourceMode = RightSourceMode.SV4;
    svRightPosition = 0;
  }

  /**
   * Check if the left record position can advance by one.
   * Side effect: advances to next left batch if current left batch size is exceeded.
   */
  public final boolean isLeftPositionAllowed() {
    if (lastLeft == IterOutcome.NONE) {
      return false;
    }
    if (!isLeftPositionInCurrentBatch()) {
      leftPosition = 0;
      releaseData(left);
      lastLeft = nextLeft();
      return lastLeft == IterOutcome.OK;
    }
    lastLeft = IterOutcome.OK;
    return true;
  }

  /**
   * Check if the right record position can advance by one.
   * Side effect: advances to next right batch if current right batch size is exceeded
   */
  public final boolean isRightPositionAllowed() {
    if (rightSourceMode == RightSourceMode.SV4) {
      return svRightPosition < sv4.getCount();
    }
    if (lastRight == IterOutcome.NONE) {
      return false;
    }
    if (!isRightPositionInCurrentBatch()) {
      rightPosition = 0;
      releaseData(right);
      lastRight = nextRight();
      return lastRight == IterOutcome.OK;
    }
    lastRight = IterOutcome.OK;
    return true;
  }

  private void releaseData(RecordBatch b) {
    for (VectorWrapper<?> v : b) {
      v.clear();
    }
    if (b.getSchema().getSelectionVectorMode() == SelectionVectorMode.TWO_BYTE) {
      b.getSelectionVector2().clear();
    }
  }

  /**
   * Check if the left record position can advance by one in the current batch.
   */
  public final boolean isLeftPositionInCurrentBatch() {
    return leftPosition < left.getRecordCount();
  }

  /**
   * Check if the right record position can advance by one in the current batch.
   */
  public final boolean isRightPositionInCurrentBatch() {
    return rightPosition < right.getRecordCount();
  }

  /**
   * Check if the next left record position can advance by one in the current batch.
   */
  public final boolean isNextLeftPositionInCurrentBatch() {
    return leftPosition + 1 < left.getRecordCount();
  }

  public IterOutcome getLastRight() {
    return lastRight;
  }

  public IterOutcome getLastLeft() {
    return lastLeft;
  }

  /**
   * Check if the next left record position can advance by one in the current batch.
   */
  public final boolean isNextRightPositionInCurrentBatch() {
    return rightPosition + 1 < right.getRecordCount();
  }

  public JoinOutcome getOutcome() {
    if (!ok) {
      return JoinOutcome.FAILURE;
    }
    if (bothMatches(IterOutcome.NONE) ||
            (joinType == JoinRelType.INNER && eitherMatches(IterOutcome.NONE)) ||
            (joinType == JoinRelType.LEFT && lastLeft == IterOutcome.NONE) ||
            (joinType == JoinRelType.RIGHT && lastRight == IterOutcome.NONE)) {
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
    return lastLeft == outcome && lastRight == outcome;
  }

  private boolean eitherMatches(IterOutcome outcome) {
    return lastLeft == outcome || lastRight == outcome;
  }

}
