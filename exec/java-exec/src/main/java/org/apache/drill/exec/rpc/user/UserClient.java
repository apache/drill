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
package org.apache.drill.exec.rpc.user;

import io.netty.buffer.ByteBuf;
import io.netty.channel.EventLoopGroup;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.GeneralRPCProtos.Ack;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.UserProtos.BitToUserHandshake;
import org.apache.drill.exec.proto.UserProtos.QueryResult;
import org.apache.drill.exec.proto.UserProtos.RpcType;
import org.apache.drill.exec.proto.UserProtos.RunQuery;
import org.apache.drill.exec.proto.UserProtos.UserToBitHandshake;
import org.apache.drill.exec.rpc.BasicClientWithConnection;
import org.apache.drill.exec.rpc.ProtobufLengthDecoder;
import org.apache.drill.exec.rpc.Response;
import org.apache.drill.exec.rpc.RpcConnectionHandler;
import org.apache.drill.exec.rpc.RpcException;

import com.google.protobuf.MessageLite;

public class UserClient extends BasicClientWithConnection<RpcType, UserToBitHandshake, BitToUserHandshake> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(UserClient.class);

  private final QueryResultHandler queryResultHandler = new QueryResultHandler();

  public UserClient(BufferAllocator alloc, EventLoopGroup eventLoopGroup) {
    super(UserRpcConfig.MAPPING, alloc, eventLoopGroup, RpcType.HANDSHAKE, BitToUserHandshake.class, BitToUserHandshake.PARSER);
  }

  public void submitQuery(UserResultsListener resultsListener, RunQuery query) {
    send(queryResultHandler.getWrappedListener(resultsListener), RpcType.RUN_QUERY, query, QueryId.class);
  }

  public void connect(RpcConnectionHandler<ServerConnection> handler, DrillbitEndpoint endpoint) throws RpcException, InterruptedException {
    UserToBitHandshake hs = UserToBitHandshake.newBuilder().setRpcVersion(UserRpcConfig.RPC_VERSION).setSupportListening(true).build();
    this.connectAsClient(handler, hs, endpoint.getAddress(), endpoint.getUserPort());
  }

  @Override
  protected MessageLite getResponseDefaultInstance(int rpcType) throws RpcException {
    switch (rpcType) {
    case RpcType.ACK_VALUE:
      return Ack.getDefaultInstance();
    case RpcType.HANDSHAKE_VALUE:
      return BitToUserHandshake.getDefaultInstance();
    case RpcType.QUERY_HANDLE_VALUE:
      return QueryId.getDefaultInstance();
    case RpcType.QUERY_RESULT_VALUE:
      return QueryResult.getDefaultInstance();
    }
    throw new RpcException(String.format("Unable to deal with RpcType of %d", rpcType));
  }

  protected Response handle(int rpcType, ByteBuf pBody, ByteBuf dBody) throws RpcException {
    switch (rpcType) {
    case RpcType.QUERY_RESULT_VALUE:
      queryResultHandler.batchArrived(pBody, dBody);
      return new Response(RpcType.ACK, Ack.getDefaultInstance());
    default:
      throw new RpcException(String.format("Unknown Rpc Type %d. ", rpcType));
    }

  }

  @Override
  protected void validateHandshake(BitToUserHandshake inbound) throws RpcException {
//    logger.debug("Handling handshake from bit to user. {}", inbound);
    if (inbound.getRpcVersion() != UserRpcConfig.RPC_VERSION)
      throw new RpcException(String.format("Invalid rpc version.  Expected %d, actual %d.", inbound.getRpcVersion(),
          UserRpcConfig.RPC_VERSION));

  }

  @Override
  protected void finalizeConnection(BitToUserHandshake handshake, BasicClientWithConnection.ServerConnection connection) {
  }
  
  @Override
  public ProtobufLengthDecoder getDecoder(BufferAllocator allocator) {
    return new UserProtobufLengthDecoder(allocator);
  }

}
