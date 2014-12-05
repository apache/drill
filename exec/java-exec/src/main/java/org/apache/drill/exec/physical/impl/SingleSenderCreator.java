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

import io.netty.buffer.ByteBuf;

import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.OutOfMemoryException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.MetricDef;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.config.SingleSender;
import org.apache.drill.exec.physical.impl.materialize.QueryWritableBatch;
import org.apache.drill.exec.physical.impl.materialize.VectorRecordMaterializer;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.GeneralRPCProtos.Ack;
import org.apache.drill.exec.record.FragmentWritableBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.RecordBatch.IterOutcome;
import org.apache.drill.exec.rpc.BaseRpcOutcomeListener;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.data.DataTunnel;

public class SingleSenderCreator implements RootCreator<SingleSender>{

  @Override
  public RootExec getRoot(FragmentContext context, SingleSender config, List<RecordBatch> children)
      throws ExecutionSetupException {
    assert children != null && children.size() == 1;
    return new SingleSenderRootExec(context, children.iterator().next(), config);
  }



  private static class SingleSenderRootExec extends BaseRootExec {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SingleSenderRootExec.class);
    private RecordBatch incoming;
    private DataTunnel tunnel;
    private FragmentHandle handle;
    private SingleSender config;
    private int recMajor;
    private FragmentContext context;
    private volatile boolean ok = true;
    private volatile boolean done = false;
    private final SendingAccountor sendCount = new SendingAccountor();

    public enum Metric implements MetricDef {
      BYTES_SENT;

      @Override
      public int metricId() {
        return ordinal();
      }
    }

    public SingleSenderRootExec(FragmentContext context, RecordBatch batch, SingleSender config) throws OutOfMemoryException {
      super(context, new OperatorContext(config, context, null, false), config);
      //super(context, config);
      this.incoming = batch;
      assert(incoming != null);
      this.handle = context.getHandle();
      this.config = config;
      this.recMajor = config.getOppositeMajorFragmentId();
      FragmentHandle opposite = handle.toBuilder().setMajorFragmentId(config.getOppositeMajorFragmentId()).setMinorFragmentId(0).build();
      this.tunnel = context.getDataTunnel(config.getDestination(), opposite);
      this.context = context;
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
        FragmentWritableBatch b2 = FragmentWritableBatch.getEmptyLastWithSchema(handle.getQueryId(), handle.getMajorFragmentId(),
                handle.getMinorFragmentId(), recMajor, 0, incoming.getSchema());
        sendCount.increment();
        stats.startWait();
        try {
          tunnel.sendRecordBatch(new RecordSendFailure(), b2);
        } finally {
          stats.stopWait();
        }
        return false;

      case OK_NEW_SCHEMA:
      case OK:
        FragmentWritableBatch batch = new FragmentWritableBatch(false, handle.getQueryId(), handle.getMajorFragmentId(),
                handle.getMinorFragmentId(), recMajor, 0, incoming.getWritableBatch());
        updateStats(batch);
        sendCount.increment();
        stats.startWait();
        try {
          tunnel.sendRecordBatch(new RecordSendFailure(), batch);
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
      ok = false;
      sendCount.waitForSendComplete();
      oContext.close();
      incoming.cleanup();
    }

    @Override
    public void receivingFragmentFinished(FragmentHandle handle) {
      done = true;
    }

    private class RecordSendFailure extends BaseRpcOutcomeListener<Ack>{

      @Override
      public void failed(RpcException ex) {
        sendCount.decrement();
        if (!context.isCancelled() && !context.isFailed()) {
          context.fail(ex);
        }
        done = true;
      }

      @Override
      public void success(Ack value, ByteBuf buf) {
        sendCount.decrement();
        if (value.getOk()) {
          return;
        }

        logger.error("Downstream fragment was not accepted.  Stopping future sends.");
        // if we didn't get ack ok, we'll need to kill the query.
        context.fail(new RpcException("A downstream fragment batch wasn't accepted.  This fragment thus fails."));
        done = true;
      }

    }

  }

}
