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

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.memory.OutOfMemoryException;
import org.apache.drill.exec.ops.AccountingDataTunnel;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.MetricDef;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.config.SingleSender;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.FragmentWritableBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.RecordBatch.IterOutcome;

public class SingleSenderCreator implements RootCreator<SingleSender>{

  @Override
  public RootExec getRoot(FragmentContext context, SingleSender config, List<RecordBatch> children)
      throws ExecutionSetupException {
    assert children != null && children.size() == 1;
    return new SingleSenderRootExec(context, children.iterator().next(), config);
  }

  private static class SingleSenderRootExec extends BaseRootExec {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SingleSenderRootExec.class);

    private final FragmentHandle oppositeHandle;

    private RecordBatch incoming;
    private AccountingDataTunnel tunnel;
    private FragmentHandle handle;
    private SingleSender config;
    private int recMajor;
    private volatile boolean ok = true;
    private volatile boolean done = false;

    public enum Metric implements MetricDef {
      BYTES_SENT;

      @Override
      public int metricId() {
        return ordinal();
      }
    }

    public SingleSenderRootExec(FragmentContext context, RecordBatch batch, SingleSender config) throws OutOfMemoryException {
      super(context, new OperatorContext(config, context, null, false), config);
      this.incoming = batch;
      assert(incoming != null);
      this.handle = context.getHandle();
      this.config = config;
      this.recMajor = config.getOppositeMajorFragmentId();
      this.tunnel = context.getDataTunnel(config.getDestination());
      oppositeHandle = handle.toBuilder()
          .setMajorFragmentId(config.getOppositeMajorFragmentId())
          .setMinorFragmentId(config.getOppositeMinorFragmentId())
          .build();
      tunnel = context.getDataTunnel(config.getDestination());
    }

    @Override
    public boolean innerNext() {
      if (!ok) {
        incoming.kill(false);

        return false;
      }

      IterOutcome out;
      if (!done) {
        out = next(incoming);
      } else {
        incoming.kill(true);
        out = IterOutcome.NONE;
      }
//      logger.debug("Outcome of sender next {}", out);
      switch (out) {
      case STOP:
      case NONE:
        // if we didn't do anything yet, send an empty schema.
        final BatchSchema sendSchema = incoming.getSchema() == null ? BatchSchema.newBuilder().build() : incoming
            .getSchema();

        FragmentWritableBatch b2 = FragmentWritableBatch.getEmptyLastWithSchema(handle.getQueryId(),
            handle.getMajorFragmentId(), handle.getMinorFragmentId(), recMajor, oppositeHandle.getMinorFragmentId(),
            sendSchema);
        stats.startWait();
        try {
          tunnel.sendRecordBatch(b2);
        } finally {
          stats.stopWait();
        }
        return false;

      case OK_NEW_SCHEMA:
      case OK:
        FragmentWritableBatch batch = new FragmentWritableBatch(false, handle.getQueryId(), handle.getMajorFragmentId(),
                handle.getMinorFragmentId(), recMajor, oppositeHandle.getMinorFragmentId(), incoming.getWritableBatch());
        updateStats(batch);
        stats.startWait();
        try {
          tunnel.sendRecordBatch(batch);
        } finally {
          stats.stopWait();
        }
        return true;

      case NOT_YET:
      default:
        throw new IllegalStateException();
      }
    }

    public void updateStats(FragmentWritableBatch writableBatch) {
      stats.addLongStat(Metric.BYTES_SENT, writableBatch.getByteCount());
    }

    @Override
    public void stop() {
      super.stop();
      oContext.close();
      incoming.cleanup();
    }

    @Override
    public void receivingFragmentFinished(FragmentHandle handle) {
      done = true;
    }
  }

}
