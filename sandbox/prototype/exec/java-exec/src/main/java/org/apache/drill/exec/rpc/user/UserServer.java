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
package org.apache.drill.exec.rpc.user;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;

import org.apache.drill.exec.proto.GeneralRPCProtos.Ack;
import org.apache.drill.exec.proto.UserProtos.BitToUserHandshake;
import org.apache.drill.exec.proto.UserProtos.QueryHandle;
import org.apache.drill.exec.proto.UserProtos.QueryResult;
import org.apache.drill.exec.proto.UserProtos.RpcType;
import org.apache.drill.exec.proto.UserProtos.RunQuery;
import org.apache.drill.exec.rpc.BasicServer;
import org.apache.drill.exec.rpc.DrillRpcFuture;
import org.apache.drill.exec.rpc.Response;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.server.DrillbitContext;

import com.google.protobuf.MessageLite;

public class UserServer extends BasicServer<RpcType> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(UserServer.class);
  
  final DrillbitContext context;
  
  public UserServer(ByteBufAllocator alloc, EventLoopGroup eventLoopGroup, DrillbitContext context) {
    super(alloc, eventLoopGroup);
    this.context = context;
  }

  @Override
  protected MessageLite getResponseDefaultInstance(int rpcType) throws RpcException {
    // a user server only expects acknowledgements on messages it creates.
    switch (rpcType) {
    case RpcType.ACK_VALUE:
      return Ack.getDefaultInstance();
    default:
      throw new UnsupportedOperationException();
    }

  }

  public DrillRpcFuture<QueryResult> sendResult(RunQuery query, ByteBuf data) throws RpcException {
    return this.send(RpcType.QUERY_RESULT, query, QueryResult.class, data);
  }
  
  
  @Override
  protected Response handle(SocketChannel channel, int rpcType, ByteBuf pBody, ByteBuf dBody) throws RpcException {
    switch (rpcType) {

    case RpcType.HANDSHAKE_VALUE:
//      logger.debug("Received handshake, responding in kind.");
      return new Response(RpcType.HANDSHAKE, BitToUserHandshake.getDefaultInstance(), null);
      
    case RpcType.RUN_QUERY_VALUE:
//      logger.debug("Received query to run.  Returning query handle.");
      return new Response(RpcType.QUERY_HANDLE, QueryHandle.newBuilder().setQueryId(1).build(), null);
      
    case RpcType.REQUEST_RESULTS_VALUE:
//      logger.debug("Received results requests.  Returning empty query result.");
      return new Response(RpcType.QUERY_RESULT, QueryResult.getDefaultInstance(), null);
      
    default:
      throw new UnsupportedOperationException();
    }

  }
  
  

}
