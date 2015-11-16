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
import io.netty.channel.ChannelFuture;
import io.netty.channel.socket.SocketChannel;
import io.netty.util.concurrent.GenericFutureListener;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.BitData.BitClientHandshake;
import org.apache.drill.exec.proto.BitData.BitServerHandshake;
import org.apache.drill.exec.proto.BitData.RpcType;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.rpc.BasicClient;
import org.apache.drill.exec.rpc.OutOfMemoryHandler;
import org.apache.drill.exec.rpc.ProtobufLengthDecoder;
import org.apache.drill.exec.rpc.Response;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.server.BootStrapContext;

import com.google.protobuf.MessageLite;

public class DataClient extends BasicClient<RpcType, DataClientConnection, BitClientHandshake, BitServerHandshake>{

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DataClient.class);

  private volatile DataClientConnection connection;
  private final BufferAllocator allocator;
  private final DataConnectionManager.CloseHandlerCreator closeHandlerFactory;


  public DataClient(DrillbitEndpoint remoteEndpoint, BootStrapContext context, DataConnectionManager.CloseHandlerCreator closeHandlerFactory) {
    super(
        DataRpcConfig.getMapping(context.getConfig(), context.getExecutor()),
        context.getAllocator().getAsByteBufAllocator(),
        context.getBitClientLoopGroup(),
        RpcType.HANDSHAKE,
        BitServerHandshake.class,
        BitServerHandshake.PARSER);
    this.closeHandlerFactory = closeHandlerFactory;
    this.allocator = context.getAllocator();
  }

  @Override
  public DataClientConnection initRemoteConnection(SocketChannel channel) {
    super.initRemoteConnection(channel);
    this.connection = new DataClientConnection(channel, this);
    return connection;
  }

  @Override
  protected GenericFutureListener<ChannelFuture> getCloseHandler(SocketChannel ch, DataClientConnection clientConnection) {
    return closeHandlerFactory.getHandler(clientConnection, super.getCloseHandler(ch, clientConnection));
  }

  @Override
  public MessageLite getResponseDefaultInstance(int rpcType) throws RpcException {
    return DataDefaultInstanceHandler.getResponseDefaultInstanceClient(rpcType);
  }

  @Override
  protected Response handle(DataClientConnection connection, int rpcType, ByteBuf pBody, ByteBuf dBody) throws RpcException {
    throw new UnsupportedOperationException("DataClient is unidirectional by design.");
  }

  BufferAllocator getAllocator() {
    return allocator;
  }

  @Override
  protected void validateHandshake(BitServerHandshake handshake) throws RpcException {
    if (handshake.getRpcVersion() != DataRpcConfig.RPC_VERSION) {
      throw new RpcException(String.format("Invalid rpc version.  Expected %d, actual %d.", handshake.getRpcVersion(), DataRpcConfig.RPC_VERSION));
    }
  }

  @Override
  protected void finalizeConnection(BitServerHandshake handshake, DataClientConnection connection) {
  }

  public DataClientConnection getConnection() {
    return this.connection;
  }

  @Override
  public ProtobufLengthDecoder getDecoder(BufferAllocator allocator) {
    return new DataProtobufLengthDecoder.Client(allocator, OutOfMemoryHandler.DEFAULT_INSTANCE);
  }
}
