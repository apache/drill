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
package org.apache.drill.exec.physical.impl.unorderedreceiver;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.Iterator;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.OutOfMemoryException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.MetricDef;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.ops.OperatorStats;
import org.apache.drill.exec.ops.OpProfileDef;
import org.apache.drill.exec.physical.config.UnorderedReceiver;
import org.apache.drill.exec.proto.BitControl.FinishedReceiver;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.GeneralRPCProtos.Ack;
import org.apache.drill.exec.proto.UserBitShared.RecordBatchDef;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.RawFragmentBatch;
import org.apache.drill.exec.record.RawFragmentBatchProvider;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.WritableBatch;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.RpcOutcomeListener;

public class UnorderedReceiverBatch implements RecordBatch {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(UnorderedReceiverBatch.class);

  private RecordBatchLoader batchLoader;
  private RawFragmentBatchProvider fragProvider;
  private FragmentContext context;
  private BatchSchema schema;
  private OperatorStats stats;
  private boolean first = true;
  private UnorderedReceiver config;
  OperatorContext oContext;

  public enum Metric implements MetricDef {
    BYTES_RECEIVED,
    NUM_SENDERS;

    @Override
    public int metricId() {
      return ordinal();
    }
  }

  public UnorderedReceiverBatch(FragmentContext context, RawFragmentBatchProvider fragProvider, UnorderedReceiver config) throws OutOfMemoryException {
    this.fragProvider = fragProvider;
    this.context = context;
    // In normal case, batchLoader does not require an allocator. However, in case of splitAndTransfer of a value vector,
    // we may need an allocator for the new offset vector. Therefore, here we pass the context's allocator to batchLoader.
    oContext = new OperatorContext(config, context, false);
    this.batchLoader = new RecordBatchLoader(oContext.getAllocator());

    this.stats = context.getStats().getOperatorStats(new OpProfileDef(config.getOperatorId(), config.getOperatorType(), 1), null);
    this.stats.setLongStat(Metric.NUM_SENDERS, config.getNumSenders());
    this.config = config;
  }

  @Override
  public FragmentContext getContext() {
    return context;
  }

  @Override
  public BatchSchema getSchema() {
    return schema;
  }

  @Override
  public int getRecordCount() {
    return batchLoader.getRecordCount();
  }

  @Override
  public void kill(boolean sendUpstream) {
    if (sendUpstream) {
      informSenders();
    }
    fragProvider.kill(context);
  }

  @Override
  public Iterator<VectorWrapper<?>> iterator() {
    return batchLoader.iterator();
  }

  @Override
  public SelectionVector2 getSelectionVector2() {
    throw new UnsupportedOperationException();
  }

  @Override
  public SelectionVector4 getSelectionVector4() {
    throw new UnsupportedOperationException();
  }

  @Override
  public TypedFieldId getValueVectorId(SchemaPath path) {
    return batchLoader.getValueVectorId(path);
  }

  @Override
  public VectorWrapper<?> getValueAccessorById(Class<?> clazz, int... ids) {
    return batchLoader.getValueAccessorById(clazz, ids);
  }

  @Override
  public IterOutcome next() {
    stats.startProcessing();
    try{
      RawFragmentBatch batch;
      try {
        stats.startWait();
        batch = fragProvider.getNext();

        // skip over empty batches. we do this since these are basically control messages.
        while (batch != null && !batch.getHeader().getIsOutOfMemory() && batch.getHeader().getDef().getRecordCount() == 0 && (!first || batch.getHeader().getDef().getFieldCount() == 0)) {
          batch = fragProvider.getNext();
        }
      } finally {
        stats.stopWait();
      }

      first = false;

      if (batch == null) {
        batchLoader.clear();
        if (context.isCancelled()) {
          return IterOutcome.STOP;
        }
        return IterOutcome.NONE;
      }

      if (batch.getHeader().getIsOutOfMemory()) {
        return IterOutcome.OUT_OF_MEMORY;
      }


//      logger.debug("Next received batch {}", batch);

      RecordBatchDef rbd = batch.getHeader().getDef();
      boolean schemaChanged = batchLoader.load(rbd, batch.getBody());
      stats.addLongStat(Metric.BYTES_RECEIVED, batch.getByteCount());

      batch.release();
      if(schemaChanged) {
        this.schema = batchLoader.getSchema();
        stats.batchReceived(0, rbd.getRecordCount(), true);
        return IterOutcome.OK_NEW_SCHEMA;
      } else {
        stats.batchReceived(0, rbd.getRecordCount(), false);
        return IterOutcome.OK;
      }
    } catch(SchemaChangeException | IOException ex) {
      context.fail(ex);
      return IterOutcome.STOP;
    } finally {
      stats.stopProcessing();
    }
  }

  @Override
  public WritableBatch getWritableBatch() {
    return batchLoader.getWritableBatch();
  }

  @Override
  public void cleanup() {
    batchLoader.clear();
    fragProvider.cleanup();
    oContext.close();
  }

  @Override
  public VectorContainer getOutgoingContainer() {
    throw new UnsupportedOperationException(String.format(" You should not call getOutgoingContainer() for class %s", this.getClass().getCanonicalName()));
  }

  private void informSenders() {
    FragmentHandle handlePrototype = FragmentHandle.newBuilder()
            .setMajorFragmentId(config.getOppositeMajorFragmentId())
            .setQueryId(context.getHandle().getQueryId())
            .build();
    for (int i = 0; i < config.getNumSenders(); i++) {
      FragmentHandle sender = FragmentHandle.newBuilder(handlePrototype)
              .setMinorFragmentId(i)
              .build();
      FinishedReceiver finishedReceiver = FinishedReceiver.newBuilder()
              .setReceiver(context.getHandle())
              .setSender(sender)
              .build();
      context.getControlTunnel(config.getProvidingEndpoints().get(i)).informReceiverFinished(new OutcomeListener(), finishedReceiver);
    }
  }

  private class OutcomeListener implements RpcOutcomeListener<Ack> {

    @Override
    public void failed(RpcException ex) {
      logger.warn("Failed to inform upstream that receiver is finished");
    }

    @Override
    public void success(Ack value, ByteBuf buffer) {
      // Do nothing
    }
  }

}
