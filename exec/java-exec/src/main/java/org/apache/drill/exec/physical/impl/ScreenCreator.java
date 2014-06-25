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
import org.apache.drill.exec.memory.OutOfMemoryException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.MetricDef;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.config.Screen;
import org.apache.drill.exec.physical.impl.materialize.QueryWritableBatch;
import org.apache.drill.exec.physical.impl.materialize.RecordMaterializer;
import org.apache.drill.exec.physical.impl.materialize.VectorRecordMaterializer;
import org.apache.drill.exec.proto.GeneralRPCProtos.Ack;
import org.apache.drill.exec.proto.UserBitShared.QueryResult;
import org.apache.drill.exec.proto.UserBitShared.RecordBatchDef;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.RecordBatch.IterOutcome;
import org.apache.drill.exec.rpc.BaseRpcOutcomeListener;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.user.UserServer.UserClientConnection;
import org.apache.drill.exec.work.ErrorHelper;

import com.google.common.base.Preconditions;

public class ScreenCreator implements RootCreator<Screen>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ScreenCreator.class);



  @Override
  public RootExec getRoot(FragmentContext context, Screen config, List<RecordBatch> children) throws ExecutionSetupException {
    Preconditions.checkNotNull(children);
    Preconditions.checkArgument(children.size() == 1);
    return new ScreenRoot(context, children.iterator().next(), config);
  }


  static class ScreenRoot extends BaseRootExec {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ScreenRoot.class);
    volatile boolean ok = true;

    private final SendingAccountor sendCount = new SendingAccountor();

    final RecordBatch incoming;
    final FragmentContext context;
    final UserClientConnection connection;
    private RecordMaterializer materializer;
    private boolean first = true;

    public enum Metric implements MetricDef {
      BYTES_SENT;
      
      @Override
      public int metricId() {
        return ordinal();
      }
    }
    
    public ScreenRoot(FragmentContext context, RecordBatch incoming, Screen config) throws OutOfMemoryException {
      super(context, config);
      assert context.getConnection() != null : "A screen root should only be run on the driving node which is connected directly to the client.  As such, this should always be true.";
      this.context = context;
      this.incoming = incoming;
      this.connection = context.getConnection();
    }


    @Override
    public boolean innerNext() {
      if(!ok){
        stop();
        context.fail(this.listener.ex);
        return false;
      }

      IterOutcome outcome = next(incoming);
//      logger.debug("Screen Outcome {}", outcome);
      switch(outcome){
      case STOP: {
          sendCount.waitForSendComplete();
          QueryResult header = QueryResult.newBuilder() //
              .setQueryId(context.getHandle().getQueryId()) //
              .setRowCount(0) //
              .addError(ErrorHelper.logAndConvertError(context.getIdentity(), "Screen received stop request sent.", context.getFailureCause(), logger))
              .setDef(RecordBatchDef.getDefaultInstance()) //
              .setIsLastChunk(true) //
              .build();
          QueryWritableBatch batch = new QueryWritableBatch(header);
          stats.startWait();
          try {
            connection.sendResult(listener, batch);
          } finally {
            stats.stopWait();
          }
          sendCount.increment();

          return false;
      }
      case NONE: {
        sendCount.waitForSendComplete();
//        context.getStats().batchesCompleted.inc(1);
        QueryWritableBatch batch;
        if (!first) {
          QueryResult header = QueryResult.newBuilder() //
              .setQueryId(context.getHandle().getQueryId()) //
              .setRowCount(0) //
              .setDef(RecordBatchDef.getDefaultInstance()) //
              .setIsLastChunk(true) //
              .build();
          batch = new QueryWritableBatch(header);
        } else {
          batch = QueryWritableBatch.getEmptyBatchWithSchema(context.getHandle().getQueryId(), 0, true, incoming.getSchema());
        }
        stats.startWait();
        try {
          connection.sendResult(listener, batch);
        } finally {
          stats.stopWait();
        }
        sendCount.increment();

        return false;
      }
      case OK_NEW_SCHEMA:
        materializer = new VectorRecordMaterializer(context, incoming);
        // fall through.
      case OK:
//        context.getStats().batchesCompleted.inc(1);
//        context.getStats().recordsCompleted.inc(incoming.getRecordCount());
        QueryWritableBatch batch = materializer.convertNext(false);
        updateStats(batch);
        stats.startWait();
        try {
          connection.sendResult(listener, batch);
        } finally {
          stats.stopWait();
        }
        sendCount.increment();

        first = false;
        return true;
      default:
        throw new UnsupportedOperationException();
      }
    }
    
    public void updateStats(QueryWritableBatch queryBatch) {
      stats.addLongStat(Metric.BYTES_SENT, queryBatch.getByteCount());
    }

    @Override
    public void stop() {
      sendCount.waitForSendComplete();
      oContext.close();
      incoming.cleanup();
    }

    private SendListener listener = new SendListener();

    private class SendListener extends BaseRpcOutcomeListener<Ack>{
      volatile RpcException ex; 


      @Override
      public void success(Ack value, ByteBuf buffer) {
        super.success(value, buffer);
        sendCount.decrement();
      }

      @Override
      public void failed(RpcException ex) {
        sendCount.decrement();
        logger.error("Failure while sending data to user.", ex);
        ErrorHelper.logAndConvertError(context.getIdentity(), "Failure while sending fragment to client.", ex, logger);
        ok = false;
        this.ex = ex;
      }

    }

    RecordBatch getIncoming() {
      return incoming;
    }



  }


}
