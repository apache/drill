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
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.GenericFutureListener;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.BitData.BitClientHandshake;
import org.apache.drill.exec.proto.BitData.BitServerHandshake;
import org.apache.drill.exec.proto.BitData.FragmentRecordBatch;
import org.apache.drill.exec.proto.BitData.RpcType;
import org.apache.drill.exec.proto.UserBitShared.RpcChannel;
import org.apache.drill.exec.rpc.BasicServer;
import org.apache.drill.exec.rpc.ProtobufLengthDecoder;
import org.apache.drill.exec.rpc.Response;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.control.WorkEventBus;
import org.apache.drill.exec.server.BootStrapContext;

import com.google.protobuf.MessageLite;

public class DataServer extends BasicServer<RpcType, BitServerConnection> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DataServer.class);

  private volatile ProxyCloseHandler proxyCloseHandler;
  private final BootStrapContext context;
  private final WorkEventBus workBus;
  private final DataResponseHandler dataHandler;

  public DataServer(BootStrapContext context, WorkEventBus workBus, DataResponseHandler dataHandler) {
    super(DataRpcConfig.MAPPING, context.getAllocator().getUnderlyingAllocator(), context.getBitLoopGroup());
    this.context = context;
    this.workBus = workBus;
    this.dataHandler = dataHandler;
  }

  @Override
  public MessageLite getResponseDefaultInstance(int rpcType) throws RpcException {
    return DataDefaultInstanceHandler.getResponseDefaultInstanceServer(rpcType);
  }

  @Override
  protected GenericFutureListener<ChannelFuture> getCloseHandler(BitServerConnection connection) {
    this.proxyCloseHandler = new ProxyCloseHandler(super.getCloseHandler(connection));
    return proxyCloseHandler;
  }

  @Override
  public BitServerConnection initRemoteConnection(Channel channel) {
    return new BitServerConnection(channel, context.getAllocator());
  }

  @Override
  protected ServerHandshakeHandler<BitClientHandshake> getHandshakeHandler(final BitServerConnection connection) {
    return new ServerHandshakeHandler<BitClientHandshake>(RpcType.HANDSHAKE, BitClientHandshake.PARSER) {

      @Override
      public MessageLite getHandshakeResponse(BitClientHandshake inbound) throws Exception {
        // logger.debug("Handling handshake from other bit. {}", inbound);
        if (inbound.getRpcVersion() != DataRpcConfig.RPC_VERSION)
          throw new RpcException(String.format("Invalid rpc version.  Expected %d, actual %d.",
              inbound.getRpcVersion(), DataRpcConfig.RPC_VERSION));
        if (inbound.getChannel() != RpcChannel.BIT_DATA)
          throw new RpcException(String.format("Invalid NodeMode.  Expected BIT_DATA but received %s.",
              inbound.getChannel()));

        connection.setManager(workBus.getOrCreateFragmentManager(inbound.getHandle()));
        return BitServerHandshake.newBuilder().setRpcVersion(DataRpcConfig.RPC_VERSION).build();
      }

    };
  }

  @Override
  protected Response handle(BitServerConnection connection, int rpcType, ByteBuf pBody, ByteBuf body) throws RpcException {
    assert rpcType == RpcType.REQ_RECORD_BATCH_VALUE;

    FragmentRecordBatch fragmentBatch = get(pBody, FragmentRecordBatch.PARSER);
    return dataHandler.handle(connection, connection.getFragmentManager(), fragmentBatch, body);
  }

  private class ProxyCloseHandler implements GenericFutureListener<ChannelFuture> {

    private volatile GenericFutureListener<ChannelFuture> handler;

    public ProxyCloseHandler(GenericFutureListener<ChannelFuture> handler) {
      super();
      this.handler = handler;
    }

    public GenericFutureListener<ChannelFuture> getHandler() {
      return handler;
    }

    public void setHandler(GenericFutureListener<ChannelFuture> handler) {
      this.handler = handler;
    }

    @Override
    public void operationComplete(ChannelFuture future) throws Exception {
      handler.operationComplete(future);
    }

  }

  @Override
  public ProtobufLengthDecoder getDecoder(BufferAllocator allocator) {
    return new DataProtobufLengthDecoder(allocator);
  }
}
