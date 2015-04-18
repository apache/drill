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
package org.apache.drill.exec.physical.impl;

import java.util.List;

import org.apache.drill.exec.memory.OutOfMemoryException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OpProfileDef;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.ops.OperatorStats;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.record.CloseableRecordBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.RecordBatch.IterOutcome;

public abstract class BaseRootExec implements RootExec {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BaseRootExec.class);

  protected OperatorStats stats = null;
  protected OperatorContext oContext = null;
  protected FragmentContext fragmentContext = null;
  private List<CloseableRecordBatch> operators;

  public BaseRootExec(final FragmentContext fragmentContext, final PhysicalOperator config) throws OutOfMemoryException {
    this.oContext = fragmentContext.newOperatorContext(config, stats, true);
    stats = new OperatorStats(new OpProfileDef(config.getOperatorId(),
        config.getOperatorType(), OperatorContext.getChildCount(config)),
        oContext.getAllocator());
    fragmentContext.getStats().addOperatorStats(this.stats);
    this.fragmentContext = fragmentContext;
  }

  public BaseRootExec(final FragmentContext fragmentContext, final OperatorContext oContext,
      final PhysicalOperator config) throws OutOfMemoryException {
    this.oContext = oContext;
    stats = new OperatorStats(new OpProfileDef(config.getOperatorId(),
        config.getOperatorType(), OperatorContext.getChildCount(config)),
      oContext.getAllocator());
    fragmentContext.getStats().addOperatorStats(this.stats);
    this.fragmentContext = fragmentContext;
  }

  void setOperators(List<CloseableRecordBatch> operators) {
    this.operators = operators;
  }

  @Override
  public final boolean next() {
    // Stats should have been initialized
    assert stats != null;
    if (!fragmentContext.shouldContinue()) {
      return false;
    }
    try {
      stats.startProcessing();
      return innerNext();
    } finally {
      stats.stopProcessing();
    }
  }

  public final IterOutcome next(final RecordBatch b){
    stats.stopProcessing();
    IterOutcome next;
    try {
      next = b.next();
    } finally {
      stats.startProcessing();
    }

    switch(next){
      case OK_NEW_SCHEMA:
        stats.batchReceived(0, b.getRecordCount(), true);
        break;
      case OK:
        stats.batchReceived(0, b.getRecordCount(), false);
        break;
    }
    return next;
  }

  public abstract boolean innerNext();

  @Override
  public void receivingFragmentFinished(final FragmentHandle handle) {
    logger.warn("Currently not handling FinishedFragment message");
  }

  @Override
  public void close() throws Exception {
    // We want to account for the time spent waiting here as Wait time in the operator profile
    try {
      stats.startProcessing();
      stats.startWait();
      fragmentContext.waitForSendComplete();
    } finally {
      stats.stopWait();
      stats.stopProcessing();
    }

    // close all operators.
    if (operators != null) {
      for (CloseableRecordBatch b : operators) {
        try {
          b.close();
        } catch (Exception e) {
          fragmentContext.fail(e);
        }
      }
    }
  }
}
