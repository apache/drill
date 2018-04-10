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

package org.apache.drill.exec.record;

import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.base.PhysicalOperator;

public abstract class AbstractBinaryRecordBatch<T extends PhysicalOperator> extends  AbstractRecordBatch<T> {
  protected final RecordBatch left;
  protected final RecordBatch right;

  // state (IterOutcome) of the left input
  protected IterOutcome leftUpstream = IterOutcome.NONE;

  // state (IterOutcome) of the right input
  protected IterOutcome rightUpstream = IterOutcome.NONE;

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

  /**
   * Prefetch first batch from both inputs.
   * @return true if caller should continue processing
   *         false if caller should stop and exit from processing.
   */
  protected boolean prefetchFirstBatchFromBothSides() {
    leftUpstream = next(0, left);
    rightUpstream = next(1, right);

    if (leftUpstream == IterOutcome.STOP || rightUpstream == IterOutcome.STOP) {
      state = BatchState.STOP;
      return false;
    }

    if (leftUpstream == IterOutcome.OUT_OF_MEMORY || rightUpstream == IterOutcome.OUT_OF_MEMORY) {
      state = BatchState.OUT_OF_MEMORY;
      return false;
    }

    if (checkForEarlyFinish()) {
      state = BatchState.DONE;
      return false;
    }

    return true;
  }

  /*
   * Checks for the operator specific early terminal condition.
   * @return true if the further processing can stop.
   *         false if the further processing is needed.
   */
  protected boolean checkForEarlyFinish() {
    return (leftUpstream == IterOutcome.NONE && rightUpstream == IterOutcome.NONE);
  }
}
