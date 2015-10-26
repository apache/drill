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
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;

import java.io.IOException;
import java.util.UUID;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.scanner.persistence.ScanResult;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.DrillbitStartupException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.physical.impl.materialize.QueryWritableBatch;
import org.apache.drill.exec.proto.GeneralRPCProtos.Ack;
import org.apache.drill.exec.proto.GeneralRPCProtos.RpcMode;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.UserBitShared.QueryResult;
import org.apache.drill.exec.proto.UserProtos.BitToUserHandshake;
import org.apache.drill.exec.proto.UserProtos.HandshakeStatus;
import org.apache.drill.exec.proto.UserProtos.Property;
import org.apache.drill.exec.proto.UserProtos.RpcType;
import org.apache.drill.exec.proto.UserProtos.RunQuery;
import org.apache.drill.exec.proto.UserProtos.UserProperties;
import org.apache.drill.exec.proto.UserProtos.UserToBitHandshake;
import org.apache.drill.exec.rpc.BasicServer;
import org.apache.drill.exec.rpc.OutOfMemoryHandler;
import org.apache.drill.exec.rpc.OutboundRpcMessage;
import org.apache.drill.exec.rpc.ProtobufLengthDecoder;
import org.apache.drill.exec.rpc.RemoteConnection;
import org.apache.drill.exec.rpc.Response;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.RpcOutcomeListener;
import org.apache.drill.exec.rpc.user.security.UserAuthenticationException;
import org.apache.drill.exec.rpc.user.security.UserAuthenticator;
import org.apache.drill.exec.rpc.user.security.UserAuthenticatorFactory;
import org.apache.drill.exec.work.user.UserWorker;

import com.google.common.io.Closeables;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;

public class UserServer extends BasicServer<RpcType, UserServer.UserClientConnection> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(UserServer.class);

  final UserWorker worker;
  final BufferAllocator alloc;
  final UserAuthenticator authenticator;

  public UserServer(DrillConfig config, ScanResult classpathScan, BufferAllocator alloc, EventLoopGroup eventLoopGroup,
      UserWorker worker) throws DrillbitStartupException {
    super(UserRpcConfig.getMapping(config), alloc.getUnderlyingAllocator(), eventLoopGroup);
    this.worker = worker;
    this.alloc = alloc;
    // TODO: move this up
    if (config.getBoolean(ExecConstants.USER_AUTHENTICATION_ENABLED)) {
      authenticator = UserAuthenticatorFactory.createAuthenticator(config, classpathScan);
    } else {
      authenticator = null;
    }
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
        final RunQuery query = RunQuery.PARSER.parseFrom(new ByteBufInputStream(pBody));
        final QueryId queryId = worker.submitWork(connection, query);
        return new Response(RpcType.QUERY_HANDLE, queryId);
      } catch (InvalidProtocolBufferException e) {
        throw new RpcException("Failure while decoding RunQuery body.", e);
      }

    case RpcType.CANCEL_QUERY_VALUE:
      try {
        final QueryId queryId = QueryId.PARSER.parseFrom(new ByteBufInputStream(pBody));
        final Ack ack = worker.cancelQuery(queryId);
        return new Response(RpcType.ACK, ack);
      } catch (InvalidProtocolBufferException e) {
        throw new RpcException("Failure while decoding QueryId body.", e);
      }

    case RpcType.RESUME_PAUSED_QUERY_VALUE:
      try {
        final QueryId queryId = QueryId.PARSER.parseFrom(new ByteBufInputStream(pBody));
        final Ack ack = worker.resumeQuery(queryId);
        return new Response(RpcType.ACK, ack);
      } catch (final InvalidProtocolBufferException e) {
        throw new RpcException("Failure while decoding QueryId body.", e);
      }

    default:
      throw new UnsupportedOperationException(String.format("UserServer received rpc of unknown type.  Type was %d.", rpcType));
    }

  }

  public class UserClientConnection extends RemoteConnection {

    private UserSession session;

    public UserClientConnection(SocketChannel channel) {
      super(channel, "user client");
    }

    void disableReadTimeout() {
      getChannel().pipeline().remove(BasicServer.TIMEOUT_HANDLER);
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

    public void sendResult(RpcOutcomeListener<Ack> listener, QueryResult result, boolean allowInEventThread){
      logger.trace("Sending result to client with {}", result);
      send(listener, this, RpcType.QUERY_RESULT, result, Ack.class, allowInEventThread);
    }

    public void sendData(RpcOutcomeListener<Ack> listener, QueryWritableBatch result){
      sendData(listener, result, false);
    }

    public void sendData(RpcOutcomeListener<Ack> listener, QueryWritableBatch result, boolean allowInEventThread){
      logger.trace("Sending data to client with {}", result);
      send(listener, this, RpcType.QUERY_DATA, result.getHeader(), Ack.class, allowInEventThread, result.getBuffers());
    }
    @Override
    public BufferAllocator getAllocator() {
      return alloc;
    }

  }

  @Override
  public UserClientConnection initRemoteConnection(SocketChannel channel) {
    super.initRemoteConnection(channel);
    return new UserClientConnection(channel);
  }

  @Override
  protected ServerHandshakeHandler<UserToBitHandshake> getHandshakeHandler(final UserClientConnection connection) {

    return new ServerHandshakeHandler<UserToBitHandshake>(RpcType.HANDSHAKE, UserToBitHandshake.PARSER){

      @Override
      protected void consumeHandshake(ChannelHandlerContext ctx, UserToBitHandshake inbound) throws Exception {
        BitToUserHandshake handshakeResp = getHandshakeResponse(inbound);
        OutboundRpcMessage msg = new OutboundRpcMessage(RpcMode.RESPONSE, this.handshakeType, coordinationId, handshakeResp);
        ctx.writeAndFlush(msg);

        if (handshakeResp.getStatus() != HandshakeStatus.SUCCESS) {
          // If handling handshake results in an error, throw an exception to terminate the connection.
          throw new RpcException("Handshake request failed: " + handshakeResp.getErrorMessage());
        }
      }

      @Override
      public BitToUserHandshake getHandshakeResponse(UserToBitHandshake inbound) throws Exception {
        logger.trace("Handling handshake from user to bit. {}", inbound);


        // if timeout is unsupported or is set to false, disable timeout.
        if (!inbound.hasSupportTimeout() || !inbound.getSupportTimeout()) {
          connection.disableReadTimeout();
          logger.warn("Timeout Disabled as client doesn't support it.", connection.getName());
        }

        BitToUserHandshake.Builder respBuilder = BitToUserHandshake.newBuilder()
            .setRpcVersion(UserRpcConfig.RPC_VERSION);

        try {
          if (inbound.getRpcVersion() != UserRpcConfig.RPC_VERSION) {
            final String errMsg = String.format("Invalid rpc version. Expected %d, actual %d.",
                UserRpcConfig.RPC_VERSION, inbound.getRpcVersion());

            return handleFailure(respBuilder, HandshakeStatus.RPC_VERSION_MISMATCH, errMsg, null);
          }

          if (authenticator != null) {
            try {
              String password = "";
              final UserProperties props = inbound.getProperties();
              for (int i = 0; i < props.getPropertiesCount(); i++) {
                Property prop = props.getProperties(i);
                if (UserSession.PASSWORD.equalsIgnoreCase(prop.getKey())) {
                  password = prop.getValue();
                  break;
                }
              }
              authenticator.authenticate(inbound.getCredentials().getUserName(), password);
            } catch (UserAuthenticationException ex) {
              return handleFailure(respBuilder, HandshakeStatus.AUTH_FAILED, ex.getMessage(), ex);
            }
          }

          connection.setUser(inbound);

          return respBuilder.setStatus(HandshakeStatus.SUCCESS).build();
        } catch (Exception e) {
          return handleFailure(respBuilder, HandshakeStatus.UNKNOWN_FAILURE, e.getMessage(), e);
        }
      }
    };
  }

  /**
   * Complete building the given builder for <i>BitToUserHandshake</i> message with given status and error details.
   *
   * @param respBuilder Instance of {@link org.apache.drill.exec.proto.UserProtos.BitToUserHandshake} builder which
   *                    has RPC version field already set.
   * @param status  Status of handling handshake request.
   * @param errMsg  Error message.
   * @param exception Optional exception.
   * @return
   */
  private static BitToUserHandshake handleFailure(BitToUserHandshake.Builder respBuilder, HandshakeStatus status,
      String errMsg, Exception exception) {
    final String errorId = UUID.randomUUID().toString();

    if (exception != null) {
      logger.error("Error {} in Handling handshake request: {}, {}", errorId, status, errMsg, exception);
    } else {
      logger.error("Error {} in Handling handshake request: {}, {}", errorId, status, errMsg);
    }

    return respBuilder
        .setStatus(status)
        .setErrorId(errorId)
        .setErrorMessage(errMsg)
        .build();
  }

  @Override
  public ProtobufLengthDecoder getDecoder(BufferAllocator allocator, OutOfMemoryHandler outOfMemoryHandler) {
    return new UserProtobufLengthDecoder(allocator, outOfMemoryHandler);
  }

  @Override
  public void close() throws IOException {
    Closeables.closeQuietly(authenticator);
    super.close();
  }
}
