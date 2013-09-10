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
package org.apache.drill.exec.rpc.bit;

import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.ExecProtos.FragmentStatus;
import org.apache.drill.exec.proto.ExecProtos.PlanFragment;
import org.apache.drill.exec.proto.ExecProtos.RpcType;
import org.apache.drill.exec.proto.GeneralRPCProtos.Ack;
import org.apache.drill.exec.record.FragmentWritableBatch;
import org.apache.drill.exec.rpc.DrillRpcFuture;
import org.apache.drill.exec.rpc.RpcOutcomeListener;


public class BitTunnel {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BitTunnel.class);

  private final BitConnectionManager manager;
  private final DrillbitEndpoint endpoint;

  public BitTunnel(DrillbitEndpoint endpoint, BitConnectionManager manager) {
    this.manager = manager;
    this.endpoint = endpoint;
  }
  
  public DrillbitEndpoint getEndpoint(){
    return manager.getEndpoint();
  }

  public void sendRecordBatch(RpcOutcomeListener<Ack> outcomeListener, FragmentContext context, FragmentWritableBatch batch) {
    SendBatch b = new SendBatch(outcomeListener, batch, context);
    manager.runCommand(b);
  }

  public void sendFragment(RpcOutcomeListener<Ack> outcomeListener, PlanFragment fragment){
    SendFragment b = new SendFragment(outcomeListener, fragment);
    manager.runCommand(b);
  }
  
  public DrillRpcFuture<Ack> cancelFragment(FragmentHandle handle){
    CancelFragment b = new CancelFragment(handle);
    manager.runCommand(b);
    return b.getFuture();
  }
  
  public DrillRpcFuture<Ack> sendFragmentStatus(FragmentStatus status){
    SendFragmentStatus b = new SendFragmentStatus(status);
    manager.runCommand(b);
    return b.getFuture();
  }

  public static class SendBatch extends ListeningBitCommand<Ack> {
    final FragmentWritableBatch batch;
    final FragmentContext context;

    public SendBatch(RpcOutcomeListener<Ack> listener, FragmentWritableBatch batch, FragmentContext context) {
      super(listener);
      this.batch = batch;
      this.context = context;
    }

    @Override
    public void doRpcCall(RpcOutcomeListener<Ack> outcomeListener, BitConnection connection) {
      connection.send(outcomeListener, RpcType.REQ_RECORD_BATCH, batch.getHeader(), Ack.class, batch.getBuffers());
    }

    @Override
    public String toString() {
      return "SendBatch [batch.header=" + batch.getHeader() + "]";
    }
    
    
  }

  public static class SendFragmentStatus extends FutureBitCommand<Ack> {
    final FragmentStatus status;

    public SendFragmentStatus(FragmentStatus status) {
      super();
      this.status = status;
    }

    @Override
    public void doRpcCall(RpcOutcomeListener<Ack> outcomeListener, BitConnection connection) {
      connection.send(outcomeListener, RpcType.REQ_FRAGMENT_STATUS, status, Ack.class);
    }

  }

  public static class CancelFragment extends FutureBitCommand<Ack> {
    final FragmentHandle handle;

    public CancelFragment(FragmentHandle handle) {
      super();
      this.handle = handle;
    }

    @Override
    public void doRpcCall(RpcOutcomeListener<Ack> outcomeListener, BitConnection connection) {
      connection.send(outcomeListener, RpcType.REQ_CANCEL_FRAGMENT, handle,  Ack.class);
    }

  }

  public static class SendFragment extends ListeningBitCommand<Ack> {
    final PlanFragment fragment;

    public SendFragment(RpcOutcomeListener<Ack> listener, PlanFragment fragment) {
      super(listener);
      this.fragment = fragment;
    }

    @Override
    public void doRpcCall(RpcOutcomeListener<Ack> outcomeListener, BitConnection connection) {
      connection.send(outcomeListener, RpcType.REQ_INIATILIZE_FRAGMENT, fragment, Ack.class);
    }

  }

}
