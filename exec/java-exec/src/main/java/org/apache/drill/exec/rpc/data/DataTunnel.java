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
package org.apache.drill.exec.rpc.data;

import io.netty.buffer.ByteBuf;

import java.util.concurrent.Semaphore;

import org.apache.drill.exec.proto.BitData.RpcType;
import org.apache.drill.exec.proto.GeneralRPCProtos.Ack;
import org.apache.drill.exec.record.FragmentWritableBatch;
import org.apache.drill.exec.rpc.ListeningCommand;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.RpcOutcomeListener;
import org.apache.drill.exec.testing.ControlsInjector;
import org.apache.drill.exec.testing.ExecutionControls;


public class DataTunnel {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DataTunnel.class);

  private final DataConnectionManager manager;
  private final Semaphore sendingSemaphore = new Semaphore(3);

  // Needed for injecting a test pause
  private boolean isInjectionControlSet;
  private ControlsInjector testInjector;
  private ExecutionControls testControls;
  private org.slf4j.Logger testLogger;


  public DataTunnel(DataConnectionManager manager) {
    this.manager = manager;
  }

  /**
   * Once a DataTunnel is created, clients of DataTunnel can pass injection controls to enable setting injections at
   * pre-defined places. Currently following injection sites are available.
   *
   * 1. In method {@link #sendRecordBatch(RpcOutcomeListener, FragmentWritableBatch)}, an interruptible pause injection
   *    is available before acquiring the sending slot. Site name is: "data-tunnel-send-batch-wait-for-interrupt"
   *
   * @param testInjector
   * @param testControls
   * @param testLogger
   */
  public void setTestInjectionControls(final ControlsInjector testInjector,
      final ExecutionControls testControls, final org.slf4j.Logger testLogger) {
    isInjectionControlSet = true;
    this.testInjector = testInjector;
    this.testControls = testControls;
    this.testLogger = testLogger;
  }

  public void sendRecordBatch(RpcOutcomeListener<Ack> outcomeListener, FragmentWritableBatch batch) {
    SendBatchAsyncListen b = new SendBatchAsyncListen(outcomeListener, batch);
    try{
      if (isInjectionControlSet) {
        // Wait for interruption if set. Used to simulate the fragment interruption while the fragment is waiting for
        // semaphore acquire. We expect the
        testInjector.injectInterruptiblePause(testControls, "data-tunnel-send-batch-wait-for-interrupt", testLogger);
      }

      sendingSemaphore.acquire();
      manager.runCommand(b);
    }catch(final InterruptedException e){
      // Release the buffers first before informing the listener about the interrupt.
      for(ByteBuf buffer : batch.getBuffers()) {
        buffer.release();
      }

      outcomeListener.interrupted(e);

      // Preserve evidence that the interruption occurred so that code higher up on the call stack can learn of the
      // interruption and respond to it if it wants to.
      Thread.currentThread().interrupt();
    }
  }

  private class ThrottlingOutcomeListener implements RpcOutcomeListener<Ack>{
    RpcOutcomeListener<Ack> inner;

    public ThrottlingOutcomeListener(RpcOutcomeListener<Ack> inner) {
      super();
      this.inner = inner;
    }

    @Override
    public void failed(RpcException ex) {
      sendingSemaphore.release();
      inner.failed(ex);
    }

    @Override
    public void success(Ack value, ByteBuf buffer) {
      sendingSemaphore.release();
      inner.success(value, buffer);
    }

    @Override
    public void interrupted(InterruptedException e) {
      sendingSemaphore.release();
      inner.interrupted(e);
    }
  }

  private class SendBatchAsyncListen extends ListeningCommand<Ack, DataClientConnection> {
    final FragmentWritableBatch batch;

    public SendBatchAsyncListen(RpcOutcomeListener<Ack> listener, FragmentWritableBatch batch) {
      super(listener);
      this.batch = batch;
    }

    @Override
    public void doRpcCall(RpcOutcomeListener<Ack> outcomeListener, DataClientConnection connection) {
      connection.send(new ThrottlingOutcomeListener(outcomeListener), RpcType.REQ_RECORD_BATCH, batch.getHeader(), Ack.class, batch.getBuffers());
    }

    @Override
    public String toString() {
      return "SendBatch [batch.header=" + batch.getHeader() + "]";
    }

    @Override
    public void connectionFailed(FailureType type, Throwable t) {
      for(ByteBuf buffer : batch.getBuffers()) {
        buffer.release();
      }
      super.connectionFailed(type, t);
    }
  }
}
