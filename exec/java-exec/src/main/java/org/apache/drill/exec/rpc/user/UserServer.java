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
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.UUID;
import java.util.concurrent.Executor;

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
import org.apache.drill.exec.proto.UserProtos.CreatePreparedStatementReq;
import org.apache.drill.exec.proto.UserProtos.GetCatalogsReq;
import org.apache.drill.exec.proto.UserProtos.GetColumnsReq;
import org.apache.drill.exec.proto.UserProtos.GetQueryPlanFragments;
import org.apache.drill.exec.proto.UserProtos.GetSchemasReq;
import org.apache.drill.exec.proto.UserProtos.GetTablesReq;
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
import org.apache.drill.exec.rpc.ResponseSender;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.RpcOutcomeListener;
import org.apache.drill.exec.rpc.user.UserServer.UserClientConnectionImpl;
import org.apache.drill.exec.rpc.user.security.UserAuthenticationException;
import org.apache.drill.exec.rpc.user.security.UserAuthenticator;
import org.apache.drill.exec.rpc.user.security.UserAuthenticatorFactory;
import org.apache.drill.exec.work.user.UserWorker;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;

public class UserServer extends BasicServer<RpcType, UserClientConnectionImpl> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(UserServer.class);

  final UserWorker worker;
  final BufferAllocator alloc;
  final UserAuthenticator authenticator;
  final InboundImpersonationManager impersonationManager;

  public UserServer(DrillConfig config, ScanResult classpathScan, BufferAllocator alloc, EventLoopGroup eventLoopGroup,
      UserWorker worker, Executor executor) throws DrillbitStartupException {
    super(UserRpcConfig.getMapping(config, executor),
        alloc.getAsByteBufAllocator(),
        eventLoopGroup);
    this.worker = worker;
    this.alloc = alloc;
    // TODO: move this up
    if (config.getBoolean(ExecConstants.USER_AUTHENTICATION_ENABLED)) {
      authenticator = UserAuthenticatorFactory.createAuthenticator(config, classpathScan);
    } else {
      authenticator = null;
    }
    if (config.getBoolean(ExecConstants.IMPERSONATION_ENABLED)) {
      impersonationManager = new InboundImpersonationManager();
    } else {
      impersonationManager = null;
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
  protected void handle(UserClientConnectionImpl connection, int rpcType, ByteBuf pBody, ByteBuf dBody,
      ResponseSender responseSender) throws RpcException {
    switch (rpcType) {

    case RpcType.RUN_QUERY_VALUE:
      logger.debug("Received query to run.  Returning query handle.");
      try {
        final RunQuery query = RunQuery.PARSER.parseFrom(new ByteBufInputStream(pBody));
        final QueryId queryId = worker.submitWork(connection, query);
        responseSender.send(new Response(RpcType.QUERY_HANDLE, queryId));
        break;
      } catch (InvalidProtocolBufferException e) {
        throw new RpcException("Failure while decoding RunQuery body.", e);
      }

    case RpcType.CANCEL_QUERY_VALUE:
      try {
        final QueryId queryId = QueryId.PARSER.parseFrom(new ByteBufInputStream(pBody));
        final Ack ack = worker.cancelQuery(queryId);
        responseSender.send(new Response(RpcType.ACK, ack));
        break;
      } catch (InvalidProtocolBufferException e) {
        throw new RpcException("Failure while decoding QueryId body.", e);
      }

    case RpcType.RESUME_PAUSED_QUERY_VALUE:
      try {
        final QueryId queryId = QueryId.PARSER.parseFrom(new ByteBufInputStream(pBody));
        final Ack ack = worker.resumeQuery(queryId);
        responseSender.send(new Response(RpcType.ACK, ack));
        break;
      } catch (final InvalidProtocolBufferException e) {
        throw new RpcException("Failure while decoding QueryId body.", e);
      }
    case RpcType.GET_QUERY_PLAN_FRAGMENTS_VALUE:
      try {
        final GetQueryPlanFragments req = GetQueryPlanFragments.PARSER.parseFrom(new ByteBufInputStream(pBody));
        responseSender.send(new Response(RpcType.QUERY_PLAN_FRAGMENTS, worker.getQueryPlan(connection, req)));
        break;
      } catch(final InvalidProtocolBufferException e) {
        throw new RpcException("Failure while decoding GetQueryPlanFragments body.", e);
      }
    case RpcType.GET_CATALOGS_VALUE:
      try {
        final GetCatalogsReq req = GetCatalogsReq.PARSER.parseFrom(new ByteBufInputStream(pBody));
        worker.submitCatalogMetadataWork(connection.getSession(), req, responseSender);
        break;
      } catch (final InvalidProtocolBufferException e) {
        throw new RpcException("Failure while decoding GetCatalogsReq body.", e);
      }
    case RpcType.GET_SCHEMAS_VALUE:
      try {
        final GetSchemasReq req = GetSchemasReq.PARSER.parseFrom(new ByteBufInputStream(pBody));
        worker.submitSchemasMetadataWork(connection.getSession(), req, responseSender);
        break;
      } catch (final InvalidProtocolBufferException e) {
        throw new RpcException("Failure while decoding GetSchemasReq body.", e);
      }
    case RpcType.GET_TABLES_VALUE:
      try {
        final GetTablesReq req = GetTablesReq.PARSER.parseFrom(new ByteBufInputStream(pBody));
        worker.submitTablesMetadataWork(connection.getSession(), req, responseSender);
        break;
      } catch (final InvalidProtocolBufferException e) {
        throw new RpcException("Failure while decoding GetTablesReq body.", e);
      }
    case RpcType.GET_COLUMNS_VALUE:
      try {
        final GetColumnsReq req = GetColumnsReq.PARSER.parseFrom(new ByteBufInputStream(pBody));
        worker.submitColumnsMetadataWork(connection.getSession(), req, responseSender);
        break;
      } catch (final InvalidProtocolBufferException e) {
        throw new RpcException("Failure while decoding GetColumnsReq body.", e);
      }
    case RpcType.CREATE_PREPARED_STATEMENT_VALUE:
      try {
        final CreatePreparedStatementReq req =
            CreatePreparedStatementReq.PARSER.parseFrom(new ByteBufInputStream(pBody));
        worker.submitPreparedStatementWork(connection, req, responseSender);
        break;
      } catch (final InvalidProtocolBufferException e) {
        throw new RpcException("Failure while decoding CreatePreparedStatementReq body.", e);
      }
    default:
      throw new UnsupportedOperationException(String.format("UserServer received rpc of unknown type.  Type was %d.", rpcType));
    }
  }

  /**
   * Interface for getting user session properties and interacting with user connection. Separating this interface from
   * {@link RemoteConnection} implementation for user connection:
   * <p><ul>
   *   <li> Connection is passed to Foreman and Screen operators. Instead passing this interface exposes few details.
   *   <li> Makes it easy to have wrappers around user connection which can be helpful to tap the messages and data
   *        going to the actual client.
   * </ul>
   */
  public interface UserClientConnection {
    /**
     * @return User session object.
     */
    UserSession getSession();

    /**
     * Send query result outcome to client. Outcome is returned through <code>listener</code>
     * @param listener
     * @param result
     */
    void sendResult(RpcOutcomeListener<Ack> listener, QueryResult result);

    /**
     * Send query data to client. Outcome is returned through <code>listener</code>
     * @param listener
     * @param result
     */
    void sendData(RpcOutcomeListener<Ack> listener, QueryWritableBatch result);

    /**
     * Returns the {@link ChannelFuture} which will be notified when this
     * channel is closed.  This method always returns the same future instance.
     */
    ChannelFuture getChannelClosureFuture();

    /**
     * @return Return the client node address.
     */
    SocketAddress getRemoteAddress();
  }

  /**
   * {@link RemoteConnection} implementation for user connection. Also implements {@link UserClientConnection}.
   */
  public class UserClientConnectionImpl extends RemoteConnection implements UserClientConnection {

    private UserSession session;

    public UserClientConnectionImpl(SocketChannel channel) {
      super(channel, "user client");
    }

    void disableReadTimeout() {
      getChannel().pipeline().remove(BasicServer.TIMEOUT_HANDLER);
    }

    void setUser(final UserToBitHandshake inbound) throws IOException {
      session = UserSession.Builder.newBuilder()
          .withCredentials(inbound.getCredentials())
          .withOptionManager(worker.getSystemOptions())
          .withUserProperties(inbound.getProperties())
          .setSupportComplexTypes(inbound.getSupportComplexTypes())
          .build();
      final String targetName = session.getTargetUserName();
      if (impersonationManager != null && targetName != null) {
        impersonationManager.replaceUserOnSession(targetName, session);
      }
    }

    @Override
    public UserSession getSession(){
      return session;
    }

    @Override
    public void sendResult(final RpcOutcomeListener<Ack> listener, final QueryResult result) {
      logger.trace("Sending result to client with {}", result);
      send(listener, this, RpcType.QUERY_RESULT, result, Ack.class, true);
    }

    @Override
    public void sendData(final RpcOutcomeListener<Ack> listener, final QueryWritableBatch result) {
      logger.trace("Sending data to client with {}", result);
      send(listener, this, RpcType.QUERY_DATA, result.getHeader(), Ack.class, false, result.getBuffers());
    }

    @Override
    public BufferAllocator getAllocator() {
      return alloc;
    }

    @Override
    public ChannelFuture getChannelClosureFuture() {
      return getChannel().closeFuture();
    }

    @Override
    public SocketAddress getRemoteAddress() {
      return getChannel().remoteAddress();
    }
  }

  @Override
  public UserClientConnectionImpl initRemoteConnection(SocketChannel channel) {
    super.initRemoteConnection(channel);
    return new UserClientConnectionImpl(channel);
  }

  @Override
  protected ServerHandshakeHandler<UserToBitHandshake> getHandshakeHandler(final UserClientConnectionImpl connection) {

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
    try {
      if (authenticator != null) {
        authenticator.close();
      }
    } catch (Exception e) {
      logger.warn("Failure closing authenticator.", e);
    }
    super.close();
  }
}
