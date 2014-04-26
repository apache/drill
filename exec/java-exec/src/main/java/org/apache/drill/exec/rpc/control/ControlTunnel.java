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
package org.apache.drill.exec.rpc.control;

import org.apache.drill.exec.proto.BitControl.FragmentStatus;
import org.apache.drill.exec.proto.BitControl.PlanFragment;
import org.apache.drill.exec.proto.BitControl.RpcType;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.GeneralRPCProtos.Ack;
import org.apache.drill.exec.rpc.DrillRpcFuture;
import org.apache.drill.exec.rpc.FutureBitCommand;
import org.apache.drill.exec.rpc.ListeningCommand;
import org.apache.drill.exec.rpc.RpcOutcomeListener;


public class ControlTunnel {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ControlTunnel.class);

  private final ControlConnectionManager manager;
  private final DrillbitEndpoint endpoint;

  public ControlTunnel(DrillbitEndpoint endpoint, ControlConnectionManager manager) {
    this.manager = manager;
    this.endpoint = endpoint;
  }
  
  public DrillbitEndpoint getEndpoint(){
    return manager.getEndpoint();
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


  public static class SendFragmentStatus extends FutureBitCommand<Ack, ControlConnection> {
    final FragmentStatus status;

    public SendFragmentStatus(FragmentStatus status) {
      super();
      this.status = status;
    }

    @Override
    public void doRpcCall(RpcOutcomeListener<Ack> outcomeListener, ControlConnection connection) {
      connection.send(outcomeListener, RpcType.REQ_FRAGMENT_STATUS, status, Ack.class);
    }

  }

  public static class CancelFragment extends FutureBitCommand<Ack, ControlConnection> {
    final FragmentHandle handle;

    public CancelFragment(FragmentHandle handle) {
      super();
      this.handle = handle;
    }

    @Override
    public void doRpcCall(RpcOutcomeListener<Ack> outcomeListener, ControlConnection connection) {
      connection.send(outcomeListener, RpcType.REQ_CANCEL_FRAGMENT, handle,  Ack.class);
    }

  }

  public static class SendFragment extends ListeningCommand<Ack, ControlConnection> {
    final PlanFragment fragment;

    public SendFragment(RpcOutcomeListener<Ack> listener, PlanFragment fragment) {
      super(listener);
      this.fragment = fragment;
    }

    @Override
    public void doRpcCall(RpcOutcomeListener<Ack> outcomeListener, ControlConnection connection) {
      connection.send(outcomeListener, RpcType.REQ_INIATILIZE_FRAGMENT, fragment, Ack.class);
    }

  }

}
