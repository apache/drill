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
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.util.concurrent.GenericFutureListener;

import org.apache.drill.exec.proto.ExecProtos.RpcType;
import org.apache.drill.exec.rpc.BasicClient;
import org.apache.drill.exec.rpc.Response;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.server.DrillbitContext;

import com.google.protobuf.MessageLite;

public class BitClient  extends BasicClient<RpcType>{
  
  private final DrillbitContext context;
  private final BitComHandler handler;
  
  public BitClient(BitComHandler handler, ByteBufAllocator alloc, EventLoopGroup eventLoopGroup, DrillbitContext context) {
    super(alloc, eventLoopGroup);
    this.context = context;
    this.handler = handler;
  }
  
  @Override
  protected MessageLite getResponseDefaultInstance(int rpcType) throws RpcException {
    return handler.getResponseDefaultInstance(rpcType);
  }

  @Override
  protected Response handle(SocketChannel ch, int rpcType, ByteBuf pBody, ByteBuf dBody) throws RpcException {
    return handler.handle(context, rpcType, pBody, dBody);
  }

  @Override
  protected GenericFutureListener<ChannelFuture> getCloseHandler(SocketChannel ch) {
    return super.getCloseHandler(ch);
  }

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BitClient.class);
}
