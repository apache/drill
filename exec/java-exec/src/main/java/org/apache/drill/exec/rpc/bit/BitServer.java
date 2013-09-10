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
package org.apache.drill.exec.rpc.bit;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.GenericFutureListener;

import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.ExecProtos.BitHandshake;
import org.apache.drill.exec.proto.ExecProtos.RpcType;
import org.apache.drill.exec.rpc.BasicServer;
import org.apache.drill.exec.rpc.Response;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.bit.BitConnectionManager.CloseHandlerCreator;
import org.apache.drill.exec.server.BootStrapContext;
import org.apache.drill.exec.work.batch.BitComHandler;

import com.google.protobuf.MessageLite;

public class BitServer extends BasicServer<RpcType, BitConnection>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BitServer.class);
  
  private final BitComHandler handler;
  private final ListenerPool listeners;
  private final ConnectionManagerRegistry connectionRegistry;
  private volatile ProxyCloseHandler proxyCloseHandler;
  
  public BitServer(BitComHandler handler, BootStrapContext context, ConnectionManagerRegistry connectionRegistry, ListenerPool listeners) {
    super(BitRpcConfig.MAPPING, context.getAllocator().getUnderlyingAllocator(), context.getBitLoopGroup());
    this.handler = handler;
    this.connectionRegistry = connectionRegistry;
    this.listeners = listeners;
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
  protected GenericFutureListener<ChannelFuture> getCloseHandler(BitConnection connection) {
    this.proxyCloseHandler = new ProxyCloseHandler(super.getCloseHandler(connection));
    return proxyCloseHandler;
  }

  @Override
  public BitConnection initRemoteConnection(Channel channel) {
    return new BitConnection(channel, this, listeners);
  }
  
  
  @Override
  protected ServerHandshakeHandler<BitHandshake> getHandshakeHandler(final BitConnection connection) {
    return new ServerHandshakeHandler<BitHandshake>(RpcType.HANDSHAKE, BitHandshake.PARSER){
      
      @Override
      public MessageLite getHandshakeResponse(BitHandshake inbound) throws Exception {
//        logger.debug("Handling handshake from other bit. {}", inbound);
        if(inbound.getRpcVersion() != BitRpcConfig.RPC_VERSION) throw new RpcException(String.format("Invalid rpc version.  Expected %d, actual %d.", inbound.getRpcVersion(), BitRpcConfig.RPC_VERSION));
        if(!inbound.hasEndpoint() || inbound.getEndpoint().getAddress().isEmpty() || inbound.getEndpoint().getBitPort() < 1) throw new RpcException(String.format("RPC didn't provide valid counter endpoint information.  Received %s.", inbound.getEndpoint()));
        connection.setEndpoint(inbound.getEndpoint());

        // add the 
        BitConnectionManager manager = connectionRegistry.getConnectionManager(inbound.getEndpoint());
        
        // update the close handler.
        proxyCloseHandler.setHandler(manager.getCloseHandlerCreator().getHandler(connection, proxyCloseHandler.getHandler()));
        
        // add to the connection manager. 
        manager.addServerConnection(connection);

        return BitHandshake.newBuilder().setRpcVersion(BitRpcConfig.RPC_VERSION).build();
      }

    };
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
