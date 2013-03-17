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
import org.apache.drill.exec.rpc.BasicClient;
import org.apache.drill.exec.rpc.DrillRpcFuture;
import org.apache.drill.exec.rpc.Response;
import org.apache.drill.exec.rpc.RpcException;

import com.google.protobuf.MessageLite;

public class UserClient extends BasicClient<RpcType> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(UserClient.class);
  
  public UserClient(ByteBufAllocator alloc, EventLoopGroup eventLoopGroup) {
    super(alloc, eventLoopGroup);
  }


  public DrillRpcFuture<QueryHandle> submitQuery(RunQuery query, ByteBuf data) throws RpcException {
    return this.send(RpcType.RUN_QUERY, query, QueryHandle.class, data);
  }

  @Override
  protected MessageLite getResponseDefaultInstance(int rpcType) throws RpcException {
    switch(rpcType){
    case RpcType.ACK_VALUE:
      return Ack.getDefaultInstance();
    case RpcType.HANDSHAKE_VALUE:
      return BitToUserHandshake.getDefaultInstance();
    case RpcType.QUERY_HANDLE_VALUE:
      return QueryHandle.getDefaultInstance();
    case RpcType.QUERY_RESULT_VALUE:
      return QueryResult.getDefaultInstance();
    }
    throw new RpcException(String.format("Unable to deal with RpcType of %d", rpcType));
  }


  @Override
  protected Response handle(SocketChannel ch, int rpcType, ByteBuf pBody, ByteBuf dBody) throws RpcException {
    logger.debug("Received a server > client message of type " + rpcType);
    return new Response(RpcType.ACK, Ack.getDefaultInstance(), null);
  }

}
