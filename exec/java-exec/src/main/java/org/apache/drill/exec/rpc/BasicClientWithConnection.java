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
package org.apache.drill.exec.rpc;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.GenericFutureListener;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.rpc.BasicClientWithConnection.ServerConnection;
import org.apache.drill.exec.rpc.user.ConnectionThrottle;

import com.google.protobuf.Internal.EnumLite;
import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;

public abstract class BasicClientWithConnection<T extends EnumLite, HANDSHAKE_SEND extends MessageLite, HANDSHAKE_RESPONSE extends MessageLite> extends BasicClient<T, ServerConnection, HANDSHAKE_SEND, HANDSHAKE_RESPONSE>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BasicClientWithConnection.class);

  private BufferAllocator alloc;

  public BasicClientWithConnection(RpcConfig rpcMapping, BufferAllocator alloc, EventLoopGroup eventLoopGroup, T handshakeType,
      Class<HANDSHAKE_RESPONSE> responseClass, Parser<HANDSHAKE_RESPONSE> handshakeParser) {
    super(rpcMapping, alloc.getUnderlyingAllocator(), eventLoopGroup, handshakeType, responseClass, handshakeParser);
    this.alloc = alloc;
  }

  @Override
  protected GenericFutureListener<ChannelFuture> getCloseHandler(ServerConnection clientConnection) {
    return getCloseHandler(clientConnection.getChannel());
  }

  @Override
  protected Response handle(ServerConnection connection, int rpcType, ByteBuf pBody, ByteBuf dBody) throws RpcException {
    return handleReponse( (ConnectionThrottle) connection, rpcType, pBody, dBody);
  }

  protected abstract Response handleReponse(ConnectionThrottle throttle, int rpcType, ByteBuf pBody, ByteBuf dBody) throws RpcException ;

  @Override
  public ServerConnection initRemoteConnection(Channel channel) {
    return new ServerConnection(channel, alloc);
  }

  public static class ServerConnection extends RemoteConnection{

    private final BufferAllocator alloc;

    public ServerConnection(Channel channel, BufferAllocator alloc) {
      super(channel);
      this.alloc = alloc;
    }

    @Override
    public BufferAllocator getAllocator() {
      return alloc;
    }



  }


}
