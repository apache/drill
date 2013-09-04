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
package org.apache.drill.exec.rpc.bit;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.GenericFutureListener;

import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.ExecProtos.BitHandshake;
import org.apache.drill.exec.proto.ExecProtos.RpcType;
import org.apache.drill.exec.rpc.BasicClient;
import org.apache.drill.exec.rpc.Response;
import org.apache.drill.exec.rpc.RpcBus;
import org.apache.drill.exec.rpc.RpcConnectionHandler;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.bit.BitConnectionManager.CloseHandlerCreator;
import org.apache.drill.exec.server.BootStrapContext;
import org.apache.drill.exec.work.batch.BitComHandler;

import com.google.protobuf.MessageLite;

public class BitClient  extends BasicClient<RpcType, BitConnection, BitHandshake, BitHandshake>{

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BitClient.class);

  private final BitComHandler handler;
  private final DrillbitEndpoint remoteEndpoint;
  private volatile BitConnection connection;
  private final ListenerPool listeners;
  private final CloseHandlerCreator closeHandlerFactory;
  private final DrillbitEndpoint localIdentity;
  
  public BitClient(DrillbitEndpoint remoteEndpoint, DrillbitEndpoint localEndpoint, BitComHandler handler, BootStrapContext context, CloseHandlerCreator closeHandlerFactory, ListenerPool listeners) {
    super(BitRpcConfig.MAPPING, context.getAllocator().getUnderlyingAllocator(), context.getBitLoopGroup(), RpcType.HANDSHAKE, BitHandshake.class, BitHandshake.PARSER);
    this.localIdentity = localEndpoint;
    this.remoteEndpoint = remoteEndpoint;
    this.handler = handler;
    this.listeners = listeners;
    this.closeHandlerFactory = closeHandlerFactory;
  }
  
  public void connect(RpcConnectionHandler<BitConnection> connectionHandler) {
    connectAsClient(connectionHandler, BitHandshake.newBuilder().setRpcVersion(BitRpcConfig.RPC_VERSION).setEndpoint(localIdentity).build(), remoteEndpoint.getAddress(), remoteEndpoint.getBitPort());
  }

  @SuppressWarnings("unchecked")
  @Override
  public BitConnection initRemoteConnection(Channel channel) {
    this.connection = new BitConnection(channel, (RpcBus<RpcType, BitConnection>) (RpcBus<?, ?>) this, listeners);
    return connection;
  }

  @Override
  protected GenericFutureListener<ChannelFuture> getCloseHandler(BitConnection clientConnection) {
    return closeHandlerFactory.getHandler(clientConnection, super.getCloseHandler(clientConnection));
  }

  @Override
  public MessageLite getResponseDefaultInstance(int rpcType) throws RpcException {
    return BitComDefaultInstanceHandler.getResponseDefaultInstance(rpcType);
  }

  @Override
  protected Response handle(BitConnection connection, int rpcType, ByteBuf pBody, ByteBuf dBody) throws RpcException {
    return handler.handle(connection, rpcType, pBody, dBody);
  }

  @Override
  protected void validateHandshake(BitHandshake handshake) throws RpcException {
    if(handshake.getRpcVersion() != BitRpcConfig.RPC_VERSION) throw new RpcException(String.format("Invalid rpc version.  Expected %d, actual %d.", handshake.getRpcVersion(), BitRpcConfig.RPC_VERSION));
  }

  @Override
  protected void finalizeConnection(BitHandshake handshake, BitConnection connection) {
    connection.setEndpoint(handshake.getEndpoint());
  }

  public BitConnection getConnection(){
    return this.connection;
  }
  
}
