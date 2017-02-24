/*
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

import java.io.IOException;
import java.net.SocketAddress;
import java.util.UUID;

import javax.security.sasl.SaslException;

import org.apache.drill.common.config.DrillProperties;
import org.apache.drill.exec.exception.DrillbitStartupException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.physical.impl.materialize.QueryWritableBatch;
import org.apache.drill.exec.proto.GeneralRPCProtos.Ack;
import org.apache.drill.exec.proto.GeneralRPCProtos.RpcMode;
import org.apache.drill.exec.proto.UserBitShared.QueryResult;
import org.apache.drill.exec.proto.UserBitShared.UserCredentials;
import org.apache.drill.exec.proto.UserProtos.BitToUserHandshake;
import org.apache.drill.exec.proto.UserProtos.HandshakeStatus;
import org.apache.drill.exec.proto.UserProtos.Property;
import org.apache.drill.exec.proto.UserProtos.RpcType;
import org.apache.drill.exec.proto.UserProtos.SaslSupport;
import org.apache.drill.exec.proto.UserProtos.UserProperties;
import org.apache.drill.exec.proto.UserProtos.UserToBitHandshake;
import org.apache.drill.exec.rpc.AbstractRemoteConnection;
import org.apache.drill.exec.rpc.AbstractServerConnection;
import org.apache.drill.exec.rpc.BasicServer;
import org.apache.drill.exec.rpc.OutOfMemoryHandler;
import org.apache.drill.exec.rpc.OutboundRpcMessage;
import org.apache.drill.exec.rpc.ProtobufLengthDecoder;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.RpcOutcomeListener;
import org.apache.drill.exec.rpc.security.ServerAuthenticationHandler;
import org.apache.drill.exec.rpc.security.plain.PlainFactory;
import org.apache.drill.exec.rpc.user.UserServer.BitToUserConnection;
import org.apache.drill.exec.rpc.user.security.UserAuthenticationException;
import org.apache.drill.exec.server.BootStrapContext;
import org.apache.drill.exec.work.user.UserWorker;
import org.apache.hadoop.security.HadoopKerberosName;
import org.slf4j.Logger;

import com.google.protobuf.MessageLite;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

public class UserServer extends BasicServer<RpcType, BitToUserConnection> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(UserServer.class);
  private static final String SERVER_NAME = "Apache Drill Server";

  private final UserConnectionConfig config;
  private final UserWorker userWorker;

  public UserServer(BootStrapContext context, BufferAllocator allocator, EventLoopGroup eventLoopGroup,
                    UserWorker worker) throws DrillbitStartupException {
    super(UserRpcConfig.getMapping(context.getConfig(), context.getExecutor()),
        allocator.getAsByteBufAllocator(),
        eventLoopGroup);
    this.config = new UserConnectionConfig(allocator, context, new UserServerRequestHandler(worker));
    this.userWorker = worker;
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

  /**
   * Interface for getting user session properties and interacting with user connection. Separating this interface from
   * {@link AbstractRemoteConnection} implementation for user connection:
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
   * {@link AbstractRemoteConnection} implementation for user connection. Also implements {@link UserClientConnection}.
   */
  public class BitToUserConnection extends AbstractServerConnection<BitToUserConnection>
      implements UserClientConnection {

    private UserSession session;
    private UserToBitHandshake inbound;

    BitToUserConnection(SocketChannel channel) {
      super(channel, config, !config.isAuthEnabled()
          ? config.getMessageHandler()
          : new ServerAuthenticationHandler<>(config.getMessageHandler(),
          RpcType.SASL_MESSAGE_VALUE, RpcType.SASL_MESSAGE));
    }

    void disableReadTimeout() {
      getChannel().pipeline().remove(BasicServer.TIMEOUT_HANDLER);
    }

    void setHandshake(final UserToBitHandshake inbound) {
      this.inbound = inbound;
    }

    @Override
    public void finalizeSaslSession() throws IOException {
      final String authorizationID = getSaslServer().getAuthorizationID();
      final String userName = new HadoopKerberosName(authorizationID).getShortName();
      logger.debug("Created session for {}", userName);
      finalizeSession(userName);
    }

    /**
     * Sets the user on the session, and finalizes the session.
     *
     * @param userName user name to set on the session
     *
     */
    void finalizeSession(String userName) {
      // create a session
      session = UserSession.Builder.newBuilder()
          .withCredentials(UserCredentials.newBuilder()
              .setUserName(userName)
              .build())
          .withOptionManager(userWorker.getSystemOptions())
          .withUserProperties(inbound.getProperties())
          .setSupportComplexTypes(inbound.getSupportComplexTypes())
          .build();

      // if inbound impersonation is enabled and a target is mentioned
      final String targetName = session.getTargetUserName();
      if (config.getImpersonationManager() != null && targetName != null) {
        config.getImpersonationManager().replaceUserOnSession(targetName, session);
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
    protected Logger getLogger() {
      return logger;
    }

    @Override
    public ChannelFuture getChannelClosureFuture() {
      return getChannel().closeFuture()
          .addListener(new GenericFutureListener<Future<? super Void>>() {
            @Override
            public void operationComplete(Future<? super Void> future) throws Exception {
              cleanup();
            }
          });
    }

    @Override
    public SocketAddress getRemoteAddress() {
      return getChannel().remoteAddress();
    }

    private void cleanup() {
      if (session != null) {
        session.close();
      }
    }

    @Override
    public void close() {
      cleanup();
      super.close();
    }
  }

  @Override
  protected BitToUserConnection initRemoteConnection(SocketChannel channel) {
    super.initRemoteConnection(channel);
    return new BitToUserConnection(channel);
  }

  @Override
  protected ServerHandshakeHandler<UserToBitHandshake> getHandshakeHandler(final BitToUserConnection connection) {

    return new ServerHandshakeHandler<UserToBitHandshake>(RpcType.HANDSHAKE, UserToBitHandshake.PARSER){

      @Override
      protected void consumeHandshake(ChannelHandlerContext ctx, UserToBitHandshake inbound) throws Exception {
        BitToUserHandshake handshakeResp = getHandshakeResponse(inbound);
        OutboundRpcMessage msg = new OutboundRpcMessage(RpcMode.RESPONSE, this.handshakeType, coordinationId, handshakeResp);
        ctx.writeAndFlush(msg);

        if (handshakeResp.getStatus() != HandshakeStatus.SUCCESS &&
            handshakeResp.getStatus() != HandshakeStatus.AUTH_REQUIRED) {
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
            .setRpcVersion(UserRpcConfig.RPC_VERSION)
            .setServerInfos(UserRpcUtils.getRpcEndpointInfos(SERVER_NAME))
            .addAllSupportedMethods(UserRpcConfig.SUPPORTED_SERVER_METHODS);

        try {
          if (inbound.getRpcVersion() != UserRpcConfig.RPC_VERSION) {
            final String errMsg = String.format("Invalid rpc version. Expected %d, actual %d.",
                UserRpcConfig.RPC_VERSION, inbound.getRpcVersion());

            return handleFailure(respBuilder, HandshakeStatus.RPC_VERSION_MISMATCH, errMsg, null);
          }

          connection.setHandshake(inbound);

          if (!config.isAuthEnabled()) {
            connection.finalizeSession(inbound.getCredentials().getUserName());
            respBuilder.setStatus(HandshakeStatus.SUCCESS);
            return respBuilder.build();
          }

          final boolean clientSupportsSasl = inbound.hasSaslSupport() &&
              (inbound.getSaslSupport().ordinal() >= SaslSupport.SASL_AUTH.ordinal());
          if (!clientSupportsSasl) { // for backward compatibility < 1.10
            final String userName = inbound.getCredentials().getUserName();
            if (logger.isTraceEnabled()) {
              logger.trace("User {} on connection {} is likely using an older client.",
                  userName, connection.getRemoteAddress());
            }
            try {
              String password = "";
              final UserProperties props = inbound.getProperties();
              for (int i = 0; i < props.getPropertiesCount(); i++) {
                Property prop = props.getProperties(i);
                if (DrillProperties.PASSWORD.equalsIgnoreCase(prop.getKey())) {
                  password = prop.getValue();
                  break;
                }
              }
              final PlainFactory plainFactory;
              try {
                plainFactory = (PlainFactory) config.getAuthProvider()
                    .getAuthenticatorFactory(PlainFactory.SIMPLE_NAME);
              } catch (final SaslException e) {
                throw new UserAuthenticationException("The server no longer supports username/password" +
                    " based authentication. Please talk to your system administrator.");
              }
              plainFactory.getAuthenticator()
                  .authenticate(userName, password);
              connection.changeHandlerTo(config.getMessageHandler());
              connection.finalizeSession(userName);
              respBuilder.setStatus(HandshakeStatus.SUCCESS);
              if (logger.isTraceEnabled()) {
                logger.trace("Authenticated {} successfully using PLAIN from {}", userName,
                    connection.getRemoteAddress());
              }
              return respBuilder.build();
            } catch (UserAuthenticationException ex) {
              return handleFailure(respBuilder, HandshakeStatus.AUTH_FAILED, ex.getMessage(), ex);
            }
          }

          // mention server's authentication capabilities
          respBuilder.addAllAuthenticationMechanisms(config.getAuthProvider().getAllFactoryNames());

          // for now, this means PLAIN credentials will be sent over twice
          // (during handshake and during sasl exchange)
          respBuilder.setStatus(HandshakeStatus.AUTH_REQUIRED);
          return respBuilder.build();
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
  protected ProtobufLengthDecoder getDecoder(BufferAllocator allocator, OutOfMemoryHandler outOfMemoryHandler) {
    return new UserProtobufLengthDecoder(allocator, outOfMemoryHandler);
  }

}
