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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.DrillBuf;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.drill.exec.proto.BitControl.CustomMessage;
import org.apache.drill.exec.proto.BitControl.FinishedReceiver;
import org.apache.drill.exec.proto.BitControl.FragmentStatus;
import org.apache.drill.exec.proto.BitControl.InitializeFragments;
import org.apache.drill.exec.proto.BitControl.RpcType;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.GeneralRPCProtos.Ack;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.UserBitShared.QueryProfile;
import org.apache.drill.exec.rpc.DrillRpcFuture;
import org.apache.drill.exec.rpc.FutureBitCommand;
import org.apache.drill.exec.rpc.ListeningCommand;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.RpcOutcomeListener;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;


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

  public void sendFragments(RpcOutcomeListener<Ack> outcomeListener, InitializeFragments fragments){
    SendFragment b = new SendFragment(outcomeListener, fragments);
    manager.runCommand(b);
  }

  public void cancelFragment(RpcOutcomeListener<Ack> outcomeListener, FragmentHandle handle){
    final SignalFragment b = new SignalFragment(outcomeListener, handle, RpcType.REQ_CANCEL_FRAGMENT);
    manager.runCommand(b);
  }

  public void unpauseFragment(final RpcOutcomeListener<Ack> outcomeListener, final FragmentHandle handle) {
    final SignalFragment b = new SignalFragment(outcomeListener, handle, RpcType.REQ_UNPAUSE_FRAGMENT);
    manager.runCommand(b);
  }

  public DrillRpcFuture<Ack> requestCancelQuery(QueryId queryId){
    CancelQuery c = new CancelQuery(queryId);
    manager.runCommand(c);
    return c.getFuture();
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

  public static class SignalFragment extends ListeningCommand<Ack, ControlConnection> {
    final FragmentHandle handle;
    final RpcType type;

    public SignalFragment(RpcOutcomeListener<Ack> listener, FragmentHandle handle, RpcType type) {
      super(listener);
      this.handle = handle;
      this.type = type;
    }

    @Override
    public void doRpcCall(RpcOutcomeListener<Ack> outcomeListener, ControlConnection connection) {
      connection.sendUnsafe(outcomeListener, type, handle, Ack.class);
    }

  }

  public static class SendFragment extends ListeningCommand<Ack, ControlConnection> {
    final InitializeFragments fragments;

    public SendFragment(RpcOutcomeListener<Ack> listener, InitializeFragments fragments) {
      super(listener);
      this.fragments = fragments;
    }

    @Override
    public void doRpcCall(RpcOutcomeListener<Ack> outcomeListener, ControlConnection connection) {
      connection.send(outcomeListener, RpcType.REQ_INITIALIZE_FRAGMENTS, fragments, Ack.class);
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

  public static class CancelQuery extends FutureBitCommand<Ack, ControlConnection> {
    final QueryId queryId;

    public CancelQuery(QueryId queryId) {
      super();
      this.queryId = queryId;
    }

    @Override
    public void doRpcCall(RpcOutcomeListener<Ack> outcomeListener, ControlConnection connection) {
      connection.send(outcomeListener, RpcType.REQ_QUERY_CANCEL, queryId, Ack.class);
    }
  }

  public <SEND extends Message, RECEIVE extends Message> CustomTunnel<SEND, RECEIVE> getCustomTunnel(
      int messageTypeId, Class<SEND> clazz, Parser<RECEIVE> parser) {
    return new CustomTunnel<SEND, RECEIVE>(messageTypeId, parser);
  }

  private static class CustomMessageSender extends ListeningCommand<CustomMessage, ControlConnection> {

    private CustomMessage message;
    private ByteBuf[] dataBodies;

    public CustomMessageSender(RpcOutcomeListener<CustomMessage> listener, CustomMessage message, ByteBuf[] dataBodies) {
      super(listener);
      this.message = message;
      this.dataBodies = dataBodies;
    }

    @Override
    public void doRpcCall(RpcOutcomeListener<CustomMessage> outcomeListener, ControlConnection connection) {
      connection.send(outcomeListener, RpcType.REQ_CUSTOM, message, CustomMessage.class, dataBodies);
    }

  }

  private static class SyncCustomMessageSender extends FutureBitCommand<CustomMessage, ControlConnection> {

    private CustomMessage message;
    private ByteBuf[] dataBodies;

    public SyncCustomMessageSender(CustomMessage message, ByteBuf[] dataBodies) {
      super();
      this.message = message;
      this.dataBodies = dataBodies;
    }

    @Override
    public void doRpcCall(RpcOutcomeListener<CustomMessage> outcomeListener, ControlConnection connection) {
      connection.send(outcomeListener, RpcType.REQ_CUSTOM, message, CustomMessage.class, dataBodies);
    }
  }

  /**
   * A class used to return a synchronous future when doing custom rpc messages.
   * @param <RECEIVE>
   *          The type of message that will be returned.
   */
  public class CustomFuture<RECEIVE> {

    private Parser<RECEIVE> parser;
    private DrillRpcFuture<CustomMessage> future;

    public CustomFuture(Parser<RECEIVE> parser, DrillRpcFuture<CustomMessage> future) {
      super();
      this.parser = parser;
      this.future = future;
    }

    public RECEIVE get() throws RpcException, InvalidProtocolBufferException {
      CustomMessage message = future.checkedGet();
      return parser.parseFrom(message.getMessage());
    }

    public RECEIVE get(long timeout, TimeUnit unit) throws RpcException, TimeoutException,
        InvalidProtocolBufferException {
      CustomMessage message = future.checkedGet(timeout, unit);
      return parser.parseFrom(message.getMessage());
    }

    public DrillBuf getBuffer() throws RpcException {
      return (DrillBuf) future.getBuffer();
    }

  }

  /**
   * A special tunnel that can be used for custom types of messages. Its lifecycle is tied to the underlying
   * ControlTunnel.
   * @param <SEND>
   *          The type of message the control tunnel will be able to send.
   * @param <RECEIVE>
   *          The expected response the control tunnel expects to receive.
   */
  public class CustomTunnel<SEND extends Message, RECEIVE extends Message> {
    private int messageTypeId;
    private Parser<RECEIVE> parser;

    private CustomTunnel(int messageTypeId, Parser<RECEIVE> parser) {
      super();
      this.messageTypeId = messageTypeId;
      this.parser = parser;
    }

    /**
     * Send a message and receive a future for monitoring the outcome.
     * @param messageToSend
     *          The structured message to send.
     * @param dataBodies
     *          One or more optional unstructured messages to append to the structure message.
     * @return The CustomFuture that can be used to wait for the response.
     */
    public CustomFuture<RECEIVE> send(SEND messageToSend, ByteBuf... dataBodies) {
      final CustomMessage customMessage = CustomMessage.newBuilder()
          .setMessage(messageToSend.toByteString())
          .setType(messageTypeId)
          .build();
      final SyncCustomMessageSender b = new SyncCustomMessageSender(customMessage, dataBodies);
      manager.runCommand(b);
      DrillRpcFuture<CustomMessage> innerFuture = b.getFuture();
      return new CustomFuture<RECEIVE>(parser, innerFuture);
    }

    /**
     * Send a message using a custom listener.
     * @param listener
     *          The listener to inform of the outcome of the sent message.
     * @param messageToSend
     *          The structured message to send.
     * @param dataBodies
     *          One or more optional unstructured messages to append to the structure message.
     */
    public void send(RpcOutcomeListener<RECEIVE> listener, SEND messageToSend, ByteBuf... dataBodies) {
      final CustomMessage customMessage = CustomMessage.newBuilder()
          .setMessage(messageToSend.toByteString())
          .setType(messageTypeId)
          .build();
      manager.runCommand(new CustomMessageSender(new CustomTunnelListener(listener), customMessage, dataBodies));
    }

    private class CustomTunnelListener implements RpcOutcomeListener<CustomMessage> {
      final RpcOutcomeListener<RECEIVE> innerListener;

      public CustomTunnelListener(RpcOutcomeListener<RECEIVE> innerListener) {
        super();
        this.innerListener = innerListener;
      }

      @Override
      public void failed(RpcException ex) {
        innerListener.failed(ex);
      }

      @Override
      public void success(CustomMessage value, ByteBuf buffer) {
        try {
          RECEIVE message = parser.parseFrom(value.getMessage());
          innerListener.success(message, buffer);
        } catch (InvalidProtocolBufferException e) {
          innerListener.failed(new RpcException("Failure while parsing message locally.", e));
        }

      }

      @Override
      public void interrupted(InterruptedException e) {
        innerListener.interrupted(e);
      }

    }
  }


}
