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
import io.netty.channel.ChannelFuture;
import io.netty.channel.socket.SocketChannel;
import io.netty.util.concurrent.GenericFutureListener;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.BitControl.BitControlHandshake;
import org.apache.drill.exec.proto.BitControl.RpcType;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.rpc.BasicClient;
import org.apache.drill.exec.rpc.OutOfMemoryHandler;
import org.apache.drill.exec.rpc.ProtobufLengthDecoder;
import org.apache.drill.exec.rpc.Response;
import org.apache.drill.exec.rpc.RpcBus;
import org.apache.drill.exec.rpc.RpcConnectionHandler;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.server.BootStrapContext;
import org.apache.drill.exec.work.batch.ControlMessageHandler;

import com.google.protobuf.MessageLite;

public class ControlClient extends BasicClient<RpcType, ControlConnection, BitControlHandshake, BitControlHandshake>{

  // private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ControlClient.class);

  private final ControlMessageHandler handler;
  private final DrillbitEndpoint remoteEndpoint;
  private volatile ControlConnection connection;
  private final ControlConnectionManager.CloseHandlerCreator closeHandlerFactory;
  private final DrillbitEndpoint localIdentity;
  private final BufferAllocator allocator;

  public ControlClient(BufferAllocator allocator, DrillbitEndpoint remoteEndpoint, DrillbitEndpoint localEndpoint,
      ControlMessageHandler handler,
      BootStrapContext context, ControlConnectionManager.CloseHandlerCreator closeHandlerFactory) {
    super(ControlRpcConfig.getMapping(context.getConfig(), context.getExecutor()),
        allocator.getAsByteBufAllocator(),
        context.getBitLoopGroup(),
        RpcType.HANDSHAKE,
        BitControlHandshake.class,
        BitControlHandshake.PARSER);
    this.localIdentity = localEndpoint;
    this.remoteEndpoint = remoteEndpoint;
    this.handler = handler;
    this.closeHandlerFactory = closeHandlerFactory;
    this.allocator = context.getAllocator();
  }

  public void connect(RpcConnectionHandler<ControlConnection> connectionHandler) {
    connectAsClient(connectionHandler, BitControlHandshake.newBuilder().setRpcVersion(ControlRpcConfig.RPC_VERSION).setEndpoint(localIdentity).build(), remoteEndpoint.getAddress(), remoteEndpoint.getControlPort());
  }

  @SuppressWarnings("unchecked")
  @Override
  public ControlConnection initRemoteConnection(SocketChannel channel) {
    super.initRemoteConnection(channel);
    this.connection = new ControlConnection("control client", channel,
        (RpcBus<RpcType, ControlConnection>) (RpcBus<?, ?>) this, allocator);
    return connection;
  }

  @Override
  protected GenericFutureListener<ChannelFuture> getCloseHandler(SocketChannel ch, ControlConnection clientConnection) {
    return closeHandlerFactory.getHandler(clientConnection, super.getCloseHandler(ch, clientConnection));
  }

  @Override
  public MessageLite getResponseDefaultInstance(int rpcType) throws RpcException {
    return DefaultInstanceHandler.getResponseDefaultInstance(rpcType);
  }

  @Override
  protected Response handle(ControlConnection connection, int rpcType, ByteBuf pBody, ByteBuf dBody) throws RpcException {
    return handler.handle(connection, rpcType, pBody, dBody);
  }

  @Override
  protected void validateHandshake(BitControlHandshake handshake) throws RpcException {
    if (handshake.getRpcVersion() != ControlRpcConfig.RPC_VERSION) {
      throw new RpcException(String.format("Invalid rpc version.  Expected %d, actual %d.", handshake.getRpcVersion(), ControlRpcConfig.RPC_VERSION));
    }
  }

  @Override
  protected void finalizeConnection(BitControlHandshake handshake, ControlConnection connection) {
    connection.setEndpoint(handshake.getEndpoint());
  }

  public ControlConnection getConnection() {
    return this.connection;
  }

  @Override
  public ProtobufLengthDecoder getDecoder(BufferAllocator allocator) {
    return new ControlProtobufLengthDecoder(allocator, OutOfMemoryHandler.DEFAULT_INSTANCE);
  }

}
