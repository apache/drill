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
import org.apache.drill.exec.rpc.BasicServer;
import org.apache.drill.exec.rpc.OutOfMemoryHandler;
import org.apache.drill.exec.rpc.ProtobufLengthDecoder;
import org.apache.drill.exec.rpc.Response;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.server.BootStrapContext;
import org.apache.drill.exec.work.batch.ControlMessageHandler;

import com.google.protobuf.MessageLite;

public class ControlServer extends BasicServer<RpcType, ControlConnection>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ControlServer.class);

  private final ControlMessageHandler handler;
  private final ConnectionManagerRegistry connectionRegistry;
  private volatile ProxyCloseHandler proxyCloseHandler;
  private BufferAllocator allocator;

  public ControlServer(ControlMessageHandler handler, BootStrapContext context, ConnectionManagerRegistry connectionRegistry) {
    super(
        ControlRpcConfig.getMapping(context.getConfig(), context.getExecutor()),
        context.getAllocator().getAsByteBufAllocator(),
        context.getBitLoopGroup());
    this.handler = handler;
    this.connectionRegistry = connectionRegistry;
    this.allocator = context.getAllocator();
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
  protected GenericFutureListener<ChannelFuture> getCloseHandler(SocketChannel ch, ControlConnection connection) {
    this.proxyCloseHandler = new ProxyCloseHandler(super.getCloseHandler(ch, connection));
    return proxyCloseHandler;
  }

  @Override
  public ControlConnection initRemoteConnection(SocketChannel channel) {
    super.initRemoteConnection(channel);
    return new ControlConnection("control server", channel, this, allocator);
  }


  @Override
  protected ServerHandshakeHandler<BitControlHandshake> getHandshakeHandler(final ControlConnection connection) {
    return new ServerHandshakeHandler<BitControlHandshake>(RpcType.HANDSHAKE, BitControlHandshake.PARSER) {

      @Override
      public MessageLite getHandshakeResponse(BitControlHandshake inbound) throws Exception {
//        logger.debug("Handling handshake from other bit. {}", inbound);
        if (inbound.getRpcVersion() != ControlRpcConfig.RPC_VERSION) {
          throw new RpcException(String.format("Invalid rpc version.  Expected %d, actual %d.", inbound.getRpcVersion(), ControlRpcConfig.RPC_VERSION));
        }
        if (!inbound.hasEndpoint() || inbound.getEndpoint().getAddress().isEmpty() || inbound.getEndpoint().getControlPort() < 1) {
          throw new RpcException(String.format("RPC didn't provide valid counter endpoint information.  Received %s.", inbound.getEndpoint()));
        }
        connection.setEndpoint(inbound.getEndpoint());

        // add the
        ControlConnectionManager manager = connectionRegistry.getConnectionManager(inbound.getEndpoint());

        // update the close handler.
        proxyCloseHandler.setHandler(manager.getCloseHandlerCreator().getHandler(connection, proxyCloseHandler.getHandler()));

        // add to the connection manager.
        manager.addExternalConnection(connection);

        return BitControlHandshake.newBuilder().setRpcVersion(ControlRpcConfig.RPC_VERSION).build();
      }

    };
  }

  @Override
  public ProtobufLengthDecoder getDecoder(BufferAllocator allocator, OutOfMemoryHandler outOfMemoryHandler) {
    return new ControlProtobufLengthDecoder(allocator, outOfMemoryHandler);
  }

  private class ProxyCloseHandler implements GenericFutureListener<ChannelFuture> {

    private volatile GenericFutureListener<ChannelFuture>  handler;

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

}
