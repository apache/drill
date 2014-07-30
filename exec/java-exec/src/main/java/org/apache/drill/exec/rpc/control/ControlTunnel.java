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

import org.apache.drill.exec.proto.BitControl.FinishedReceiver;
import org.apache.drill.exec.proto.BitControl.FragmentStatus;
import org.apache.drill.exec.proto.BitControl.PlanFragment;
import org.apache.drill.exec.proto.BitControl.RpcType;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.GeneralRPCProtos.Ack;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.UserBitShared.QueryProfile;
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

  public void cancelFragment(RpcOutcomeListener<Ack> outcomeListener, FragmentHandle handle){
    CancelFragment b = new CancelFragment(outcomeListener, handle);
    manager.runCommand(b);
  }

  public void informReceiverFinished(RpcOutcomeListener<Ack> outcomeListener, FinishedReceiver finishedReceiver){
    ReceiverFinished b = new ReceiverFinished(outcomeListener, finishedReceiver);
    manager.runCommand(b);
  }

  public DrillRpcFuture<Ack> sendFragmentStatus(FragmentStatus status){
    SendFragmentStatus b = new SendFragmentStatus(status);
    manager.runCommand(b);
    return b.getFuture();
  }

  public DrillRpcFuture<QueryProfile> requestQueryProfile(QueryId queryId) {
    RequestProfile b = new RequestProfile(queryId);
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
      connection.sendUnsafe(outcomeListener, RpcType.REQ_FRAGMENT_STATUS, status, Ack.class);
    }

  }


  public static class ReceiverFinished extends ListeningCommand<Ack, ControlConnection> {
    final FinishedReceiver finishedReceiver;

    public ReceiverFinished(RpcOutcomeListener<Ack> listener, FinishedReceiver finishedReceiver) {
      super(listener);
      this.finishedReceiver = finishedReceiver;
    }

    @Override
    public void doRpcCall(RpcOutcomeListener<Ack> outcomeListener, ControlConnection connection) {
      connection.send(outcomeListener, RpcType.REQ_RECEIVER_FINISHED, finishedReceiver, Ack.class);
    }
  }

  public static class CancelFragment extends ListeningCommand<Ack, ControlConnection> {
    final FragmentHandle handle;

    public CancelFragment(RpcOutcomeListener<Ack> listener, FragmentHandle handle) {
      super(listener);
      this.handle = handle;
    }

    @Override
    public void doRpcCall(RpcOutcomeListener<Ack> outcomeListener, ControlConnection connection) {
      connection.sendUnsafe(outcomeListener, RpcType.REQ_CANCEL_FRAGMENT, handle, Ack.class);
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

  public static class RequestProfile extends FutureBitCommand<QueryProfile, ControlConnection> {
    final QueryId queryId;

    public RequestProfile(QueryId queryId) {
      super();
      this.queryId = queryId;
    }

    @Override
    public void doRpcCall(RpcOutcomeListener<QueryProfile> outcomeListener, ControlConnection connection) {
      connection.send(outcomeListener, RpcType.REQ_QUERY_STATUS, queryId, QueryProfile.class);
    }
  }
}
