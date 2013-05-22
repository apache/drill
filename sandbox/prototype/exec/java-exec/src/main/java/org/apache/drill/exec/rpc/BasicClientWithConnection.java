/*******************************************************************************
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
 ******************************************************************************/
package org.apache.drill.exec.rpc;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.GenericFutureListener;

import org.apache.drill.exec.rpc.BasicClientWithConnection.ServerConnection;

import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;
import com.google.protobuf.Internal.EnumLite;

public abstract class BasicClientWithConnection<T extends EnumLite, HANDSHAKE_SEND extends MessageLite, HANDSHAKE_RESPONSE extends MessageLite> extends BasicClient<T, ServerConnection, HANDSHAKE_SEND, HANDSHAKE_RESPONSE>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BasicClientWithConnection.class);

  public BasicClientWithConnection(RpcConfig rpcMapping, ByteBufAllocator alloc, EventLoopGroup eventLoopGroup, T handshakeType,
      Class<HANDSHAKE_RESPONSE> responseClass, Parser<HANDSHAKE_RESPONSE> handshakeParser) {
    super(rpcMapping, alloc, eventLoopGroup, handshakeType, responseClass, handshakeParser);
  }
  
  @Override
  protected GenericFutureListener<ChannelFuture> getCloseHandler(ServerConnection clientConnection) {
    return getCloseHandler(clientConnection.getChannel());
  }
  
  @Override
  protected Response handle(ServerConnection connection, int rpcType, ByteBuf pBody, ByteBuf dBody) throws RpcException {
    return handle(rpcType, pBody, dBody);
  }
  
  protected abstract Response handle(int rpcType, ByteBuf pBody, ByteBuf dBody) throws RpcException ;
    
  @Override
  public ServerConnection initRemoteConnection(Channel channel) {
    return new ServerConnection(channel);
  }

  public static class ServerConnection extends RemoteConnection{

    public ServerConnection(Channel channel) {
      super(channel);
    }

  }

  
}
