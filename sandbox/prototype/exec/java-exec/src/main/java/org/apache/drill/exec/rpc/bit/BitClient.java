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

import java.util.concurrent.ConcurrentMap;

import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.ExecProtos.BitHandshake;
import org.apache.drill.exec.proto.ExecProtos.RpcType;
import org.apache.drill.exec.rpc.BasicClient;
import org.apache.drill.exec.rpc.Response;
import org.apache.drill.exec.rpc.RpcBus;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.server.BootStrapContext;
import org.apache.drill.exec.work.batch.BitComHandler;

import com.google.protobuf.MessageLite;

public class BitClient  extends BasicClient<RpcType, BitConnection>{

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BitClient.class);

  private final BitComHandler handler;
  private final DrillbitEndpoint endpoint;
  private BitConnection connection;
  private final AvailabilityListener openListener;
  private final ConcurrentMap<DrillbitEndpoint, BitConnection> registry;
  private final ListenerPool listeners;
  
  public BitClient(DrillbitEndpoint endpoint, AvailabilityListener openListener, BitComHandler handler, BootStrapContext context, ConcurrentMap<DrillbitEndpoint, BitConnection> registry, ListenerPool listeners) {
    super(BitRpcConfig.MAPPING, context.getAllocator().getUnderlyingAllocator(), context.getBitLoopGroup());

    this.endpoint = endpoint;
    this.handler = handler;
    this.openListener = openListener;
    this.registry = registry;
    this.listeners = listeners;
  }
  
  public BitHandshake connect() throws RpcException, InterruptedException{
    BitHandshake bs = connectAsClient(RpcType.HANDSHAKE, BitHandshake.newBuilder().setRpcVersion(BitRpcConfig.RPC_VERSION).build(), endpoint.getAddress(), endpoint.getBitPort(), BitHandshake.class);
    connection.setEndpoint(endpoint);
    return bs;
  }

  @SuppressWarnings("unchecked")
  @Override
  public BitConnection initRemoteConnection(Channel channel) {
    this.connection = new BitConnection(openListener, channel, (RpcBus<RpcType, BitConnection>) (RpcBus<?, ?>) this, registry, listeners);
    return connection;
  }

  @Override
  protected GenericFutureListener<ChannelFuture> getCloseHandler(BitConnection clientConnection) {
    return clientConnection.getCloseHandler(super.getCloseHandler(clientConnection));
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
  protected ClientHandshakeHandler<BitHandshake> getHandshakeHandler() {
    return new ClientHandshakeHandler<BitHandshake>(RpcType.HANDSHAKE, BitHandshake.class, BitHandshake.PARSER){

      @Override
      protected void validateHandshake(BitHandshake inbound) throws Exception {
        logger.debug("Handling handshake from bit server to bit client. {}", inbound);
        if(inbound.getRpcVersion() != BitRpcConfig.RPC_VERSION) throw new RpcException(String.format("Invalid rpc version.  Expected %d, actual %d.", inbound.getRpcVersion(), BitRpcConfig.RPC_VERSION));
      }

    };
  }
  
  public BitConnection getConnection(){
    return this.connection;
  }
  
}
