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
import io.netty.channel.ChannelFuture;
import io.netty.channel.socket.SocketChannel;

import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.ExecProtos.BitBatchChunk;
import org.apache.drill.exec.proto.ExecProtos.BitHandshake;
import org.apache.drill.exec.proto.ExecProtos.BitStatus;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.ExecProtos.FragmentStatus;
import org.apache.drill.exec.proto.ExecProtos.RpcType;
import org.apache.drill.exec.proto.GeneralRPCProtos.Ack;
import org.apache.drill.exec.rpc.Response;
import org.apache.drill.exec.rpc.RpcBus;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.bit.BitCom.TunnelListener;
import org.apache.drill.exec.rpc.bit.BitComImpl.TunnelModifier;
import org.apache.drill.exec.server.DrillbitContext;

import com.google.protobuf.MessageLite;

public class BitComHandler {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BitComHandler.class);
  
  private final TunnelModifier modifier;
  
  public BitComHandler(TunnelModifier modifier){
    this.modifier = modifier;
  }
  
  public TunnelListener getTunnelListener(RpcBus<?>.ChannelClosedHandler internalHandler){
    return new Listener(internalHandler);
  }
  
  public class Listener implements TunnelListener {
    final RpcBus<?>.ChannelClosedHandler internalHandler;

    public Listener(RpcBus<?>.ChannelClosedHandler internalHandler) {
      this.internalHandler = internalHandler;
    }

    @Override
    public void operationComplete(ChannelFuture future) throws Exception {
      logger.debug("BitTunnel closed, removing from BitCom.");
      internalHandler.operationComplete(future);
      BitTunnel t = modifier.remove(future.channel());
      if(t != null) t.shutdownIfClient();
    }

    @Override
    public void connectionEstablished(SocketChannel channel, DrillbitEndpoint endpoint, RpcBus<?> bus) {
      modifier.create(channel, endpoint, bus);
    }

  }

  


  public MessageLite getResponseDefaultInstance(int rpcType) throws RpcException {
    switch (rpcType) {
    case RpcType.ACK_VALUE:
      return Ack.getDefaultInstance();
    case RpcType.HANDSHAKE_VALUE:
      return BitHandshake.getDefaultInstance();
    case RpcType.RESP_FRAGMENT_HANDLE_VALUE:
      return FragmentHandle.getDefaultInstance();
    case RpcType.RESP_FRAGMENT_STATUS_VALUE:
      return FragmentStatus.getDefaultInstance();
    case RpcType.RESP_BIT_STATUS_VALUE:
      return BitStatus.getDefaultInstance();
    case RpcType.RESP_BATCH_CHUNK_VALUE:
      return BitBatchChunk.getDefaultInstance();
      
    default:
      throw new UnsupportedOperationException();
    }
  }

  protected Response handle(DrillbitContext context, int rpcType, ByteBuf pBody, ByteBuf dBody) throws RpcException {
    switch (rpcType) {
    
    case RpcType.HANDSHAKE_VALUE:
      // parse incoming handshake.
      // get endpoint information.
      // record endpoint information in registry.
      // respond with our handshake info.
      return new Response(RpcType.HANDSHAKE, BitHandshake.getDefaultInstance(), null);
      
    case RpcType.REQ_BATCH_CHUNK_VALUE:
      return new Response(RpcType.RESP_BATCH_CHUNK, BitBatchChunk.getDefaultInstance(), null);
      
    case RpcType.REQ_BIT_STATUS_VALUE:
      return new Response(RpcType.RESP_BIT_STATUS, BitStatus.getDefaultInstance(), null);
      
    case RpcType.REQ_CANCEL_FRAGMENT_VALUE:
      return new Response(RpcType.ACK, Ack.getDefaultInstance(), null);

    case RpcType.REQ_FRAGMENT_STATUS_VALUE:
      return new Response(RpcType.RESP_FRAGMENT_STATUS, FragmentStatus.getDefaultInstance(), null);
      
    case RpcType.REQ_INIATILIZE_FRAGMENT_VALUE:
      return new Response(RpcType.ACK, Ack.getDefaultInstance(), null);
      
    case RpcType.REQ_RECORD_BATCH_VALUE:
      return new Response(RpcType.RESP_BATCH_CHUNK, BitBatchChunk.getDefaultInstance(), null);
      
    default:
      throw new UnsupportedOperationException();
    }

  }
  

  
  
}
