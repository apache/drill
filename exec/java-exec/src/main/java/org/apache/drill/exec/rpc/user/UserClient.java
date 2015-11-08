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

import java.util.concurrent.Executor;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.GeneralRPCProtos.Ack;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.proto.UserBitShared.QueryData;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.UserBitShared.QueryResult;
import org.apache.drill.exec.proto.UserProtos.BitToUserHandshake;
import org.apache.drill.exec.proto.UserProtos.HandshakeStatus;
import org.apache.drill.exec.proto.UserProtos.RpcType;
import org.apache.drill.exec.proto.UserProtos.RunQuery;
import org.apache.drill.exec.proto.UserProtos.UserProperties;
import org.apache.drill.exec.proto.UserProtos.UserToBitHandshake;
import org.apache.drill.exec.rpc.Acks;
import org.apache.drill.exec.rpc.BasicClientWithConnection;
import org.apache.drill.exec.rpc.ConnectionThrottle;
import org.apache.drill.exec.rpc.OutOfMemoryHandler;
import org.apache.drill.exec.rpc.ProtobufLengthDecoder;
import org.apache.drill.exec.rpc.Response;
import org.apache.drill.exec.rpc.RpcConnectionHandler;
import org.apache.drill.exec.rpc.RpcException;

import com.google.protobuf.MessageLite;

public class UserClient extends BasicClientWithConnection<RpcType, UserToBitHandshake, BitToUserHandshake> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(UserClient.class);

  private final QueryResultHandler queryResultHandler = new QueryResultHandler();
  private boolean supportComplexTypes = true;

  public UserClient(DrillConfig config, boolean supportComplexTypes, BufferAllocator alloc,
      EventLoopGroup eventLoopGroup, Executor eventExecutor) {
    super(
        UserRpcConfig.getMapping(config, eventExecutor),
        alloc,
        eventLoopGroup,
        RpcType.HANDSHAKE,
        BitToUserHandshake.class,
        BitToUserHandshake.PARSER,
        "user client");
    this.supportComplexTypes = supportComplexTypes;
  }

  public void submitQuery(UserResultsListener resultsListener, RunQuery query) {
    send(queryResultHandler.getWrappedListener(connection, resultsListener), RpcType.RUN_QUERY, query, QueryId.class);
  }

  public void connect(RpcConnectionHandler<ServerConnection> handler, DrillbitEndpoint endpoint,
      UserProperties props, UserBitShared.UserCredentials credentials) {
    UserToBitHandshake.Builder hsBuilder = UserToBitHandshake.newBuilder()
        .setRpcVersion(UserRpcConfig.RPC_VERSION)
        .setSupportListening(true)
        .setSupportComplexTypes(supportComplexTypes)
        .setSupportTimeout(true)
        .setCredentials(credentials);

    if (props != null) {
      hsBuilder.setProperties(props);
    }

    this.connectAsClient(handler, hsBuilder.build(), endpoint.getAddress(), endpoint.getUserPort());
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
    case RpcType.QUERY_DATA_VALUE:
      return QueryData.getDefaultInstance();
    }
    throw new RpcException(String.format("Unable to deal with RpcType of %d", rpcType));
  }

  @Override
  protected Response handleReponse(ConnectionThrottle throttle, int rpcType, ByteBuf pBody, ByteBuf dBody) throws RpcException {
    switch (rpcType) {
    case RpcType.QUERY_DATA_VALUE:
      queryResultHandler.batchArrived(throttle, pBody, dBody);
      return new Response(RpcType.ACK, Acks.OK);
    case RpcType.QUERY_RESULT_VALUE:
      queryResultHandler.resultArrived(pBody);
      return new Response(RpcType.ACK, Acks.OK);
    default:
      throw new RpcException(String.format("Unknown Rpc Type %d. ", rpcType));
    }
  }

  @Override
  protected void validateHandshake(BitToUserHandshake inbound) throws RpcException {
//    logger.debug("Handling handshake from bit to user. {}", inbound);
    if (inbound.getStatus() != HandshakeStatus.SUCCESS) {
      final String errMsg = String.format("Status: %s, Error Id: %s, Error message: %s",
          inbound.getStatus(), inbound.getErrorId(), inbound.getErrorMessage());
      logger.error(errMsg);
      throw new RpcException(errMsg);
    }
  }

  @Override
  protected void finalizeConnection(BitToUserHandshake handshake, BasicClientWithConnection.ServerConnection connection) {
  }

  @Override
  public ProtobufLengthDecoder getDecoder(BufferAllocator allocator) {
    return new UserProtobufLengthDecoder(allocator, OutOfMemoryHandler.DEFAULT_INSTANCE);
  }
}
