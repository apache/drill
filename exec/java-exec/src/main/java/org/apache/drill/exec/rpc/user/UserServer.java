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
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;

import java.io.IOException;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.physical.impl.materialize.QueryWritableBatch;
import org.apache.drill.exec.proto.GeneralRPCProtos.Ack;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.UserProtos.BitToUserHandshake;
import org.apache.drill.exec.proto.UserProtos.RequestResults;
import org.apache.drill.exec.proto.UserProtos.RpcType;
import org.apache.drill.exec.proto.UserProtos.RunQuery;
import org.apache.drill.exec.proto.UserProtos.UserToBitHandshake;
import org.apache.drill.exec.rpc.BasicServer;
import org.apache.drill.exec.rpc.OutOfMemoryHandler;
import org.apache.drill.exec.rpc.ProtobufLengthDecoder;
import org.apache.drill.exec.rpc.RemoteConnection;
import org.apache.drill.exec.rpc.Response;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.RpcOutcomeListener;
import org.apache.drill.exec.work.user.UserWorker;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;

public class UserServer extends BasicServer<RpcType, UserServer.UserClientConnection> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(UserServer.class);

  final UserWorker worker;
  final BufferAllocator alloc;

  public UserServer(BufferAllocator alloc, EventLoopGroup eventLoopGroup, UserWorker worker) {
    super(UserRpcConfig.MAPPING, alloc.getUnderlyingAllocator(), eventLoopGroup);
    this.worker = worker;
    this.alloc = alloc;
  }

  @Override
  protected MessageLite getResponseDefaultInstance(int rpcType) throws RpcException {
    // a user server only expects acknowledgments on messages it creates.
    switch (rpcType) {
    case RpcType.ACK_VALUE:
      return Ack.getDefaultInstance();
    default:
      throw new UnsupportedOperationException();
    }
  }

  @Override
  protected Response handle(UserClientConnection connection, int rpcType, ByteBuf pBody, ByteBuf dBody)
      throws RpcException {
    switch (rpcType) {

    case RpcType.RUN_QUERY_VALUE:
      logger.debug("Received query to run.  Returning query handle.");
      try {
        RunQuery query = RunQuery.PARSER.parseFrom(new ByteBufInputStream(pBody));
        return new Response(RpcType.QUERY_HANDLE, worker.submitWork(connection, query));
      } catch (InvalidProtocolBufferException e) {
        throw new RpcException("Failure while decoding RunQuery body.", e);
      }

    case RpcType.CANCEL_QUERY_VALUE:
      try {
        QueryId queryId = QueryId.PARSER.parseFrom(new ByteBufInputStream(pBody));
        return new Response(RpcType.ACK, worker.cancelQuery(queryId));
      } catch (InvalidProtocolBufferException e) {
        throw new RpcException("Failure while decoding QueryId body.", e);
      }

    default:
      throw new UnsupportedOperationException(String.format("UserServer received rpc of unknown type.  Type was %d.", rpcType));
    }

  }


  public class UserClientConnection extends RemoteConnection {

    private UserSession session;

    public UserClientConnection(Channel channel) {
      super(channel);
    }

    void setUser(UserToBitHandshake inbound) throws IOException {
      session = UserSession.Builder.newBuilder()
          .withCredentials(inbound.getCredentials())
          .withOptionManager(worker.getSystemOptions())
          .withUserProperties(inbound.getProperties())
          .setSupportComplexTypes(inbound.getSupportComplexTypes())
          .build();
    }

    public UserSession getSession(){
      return session;
    }

    public void sendResult(RpcOutcomeListener<Ack> listener, QueryWritableBatch result){
      logger.trace("Sending result to client with {}", result);
      send(listener, this, RpcType.QUERY_RESULT, result.getHeader(), Ack.class, false, result.getBuffers());
    }

    public void sendResult(RpcOutcomeListener<Ack> listener, QueryWritableBatch result, boolean allowInEventThread){
      logger.trace("Sending result to client with {}", result);
      send(listener, this, RpcType.QUERY_RESULT, result.getHeader(), Ack.class, allowInEventThread, result.getBuffers());
    }

    @Override
    public BufferAllocator getAllocator() {
      return alloc;
    }

  }

  @Override
  public UserClientConnection initRemoteConnection(Channel channel) {
    return new UserClientConnection(channel);
  }

  @Override
  protected ServerHandshakeHandler<UserToBitHandshake> getHandshakeHandler(final UserClientConnection connection) {

    return new ServerHandshakeHandler<UserToBitHandshake>(RpcType.HANDSHAKE, UserToBitHandshake.PARSER){

      @Override
      public MessageLite getHandshakeResponse(UserToBitHandshake inbound) throws Exception {
        logger.trace("Handling handshake from user to bit. {}", inbound);
        if(inbound.getRpcVersion() != UserRpcConfig.RPC_VERSION) {
          throw new RpcException(String.format("Invalid rpc version. Expected %d, actual %d.", inbound.getRpcVersion(), UserRpcConfig.RPC_VERSION));
        }

        connection.setUser(inbound);

        return BitToUserHandshake.newBuilder().setRpcVersion(UserRpcConfig.RPC_VERSION).build();
      }

    };

  }

  @Override
  public ProtobufLengthDecoder getDecoder(BufferAllocator allocator, OutOfMemoryHandler outOfMemoryHandler) {
    return new UserProtobufLengthDecoder(allocator, outOfMemoryHandler);
  }
}
