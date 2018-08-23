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
package org.apache.drill.exec.rpc;

import com.google.common.base.Preconditions;
import com.google.protobuf.Internal.EnumLite;
import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.GeneralRPCProtos.RpcMode;
import org.apache.drill.exec.rpc.security.AuthenticationOutcomeListener;
import org.apache.drill.exec.rpc.security.AuthenticatorFactory;
import org.apache.hadoop.security.UserGroupInformation;

import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *
 * @param <T> handshake rpc type
 * @param <CC> Client connection type
 * @param <HS> Handshake send type
 * @param <HR> Handshake receive type
 */
public abstract class BasicClient<T extends EnumLite, CC extends ClientConnection,
                                  HS extends MessageLite, HR extends MessageLite>
    extends RpcBus<T, CC> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BasicClient.class);

  // The percentage of time that should pass before sending a ping message to ensure server doesn't time us out. For
  // example, if timeout is set to 30 seconds and we set percentage to 0.5, then if no write has happened within 15
  // seconds, the idle state handler will send a ping message.
  private static final double PERCENT_TIMEOUT_BEFORE_SENDING_PING = 0.5;

  private final Bootstrap b;
  protected CC connection;
  private final T handshakeType;
  private final Class<HR> responseClass;
  private final Parser<HR> handshakeParser;

  private final IdlePingHandler pingHandler;
  private ConnectionMultiListener.SSLHandshakeListener sslHandshakeListener = null;

  // Determines if authentication is completed between client and server
  private boolean authComplete = true;

  public BasicClient(RpcConfig rpcMapping, ByteBufAllocator alloc, EventLoopGroup eventLoopGroup, T handshakeType,
                     Class<HR> responseClass, Parser<HR> handshakeParser) {
    super(rpcMapping);
    this.responseClass = responseClass;
    this.handshakeType = handshakeType;
    this.handshakeParser = handshakeParser;
    final long timeoutInMillis = rpcMapping.hasTimeout() ?
        (long) (rpcMapping.getTimeout() * 1000.0 * PERCENT_TIMEOUT_BEFORE_SENDING_PING) :
        -1;
    this.pingHandler = rpcMapping.hasTimeout() ? new IdlePingHandler(timeoutInMillis) : null;

    b = new Bootstrap() //
        .group(eventLoopGroup) //
        .channel(TransportCheck.getClientSocketChannel()) //
        .option(ChannelOption.ALLOCATOR, alloc) //
        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 30 * 1000)
        .option(ChannelOption.SO_REUSEADDR, true)
        .option(ChannelOption.SO_RCVBUF, 1 << 17) //
        .option(ChannelOption.SO_SNDBUF, 1 << 17) //
        .option(ChannelOption.TCP_NODELAY, true)
        .handler(new ChannelInitializer<SocketChannel>() {

          @Override
          protected void initChannel(SocketChannel ch) throws Exception {
            // logger.debug("initializing client connection.");
            connection = initRemoteConnection(ch);

            ch.closeFuture().addListener(getCloseHandler(ch, connection));

            final ChannelPipeline pipe = ch.pipeline();
            // Make sure that the SSL handler is the first handler in the pipeline so everything is encrypted
            if (isSslEnabled()) {
              setupSSL(pipe, sslHandshakeListener);
            }

            pipe.addLast(RpcConstants.PROTOCOL_DECODER, getDecoder(connection.getAllocator()));
            pipe.addLast(RpcConstants.MESSAGE_DECODER, new RpcDecoder("c-" + rpcConfig.getName()));
            pipe.addLast(RpcConstants.PROTOCOL_ENCODER, new RpcEncoder("c-" + rpcConfig.getName()));
            pipe.addLast(RpcConstants.HANDSHAKE_HANDLER, new ClientHandshakeHandler(connection));

            if(pingHandler != null){
              pipe.addLast(RpcConstants.IDLE_STATE_HANDLER, pingHandler);
            }

            pipe.addLast(RpcConstants.MESSAGE_HANDLER, new InboundHandler(connection));
            pipe.addLast(RpcConstants.EXCEPTION_HANDLER, new RpcExceptionHandler<>(connection));
          }
        }); //

    // if(TransportCheck.SUPPORTS_EPOLL){
    // b.option(EpollChannelOption.SO_REUSEPORT, true); //
    // }
  }

  // Adds a SSL handler if enabled. Required only for client and server communications, so
  // a real implementation is only available for UserClient
  protected void setupSSL(ChannelPipeline pipe, ConnectionMultiListener.SSLHandshakeListener sslHandshakeListener) {
    throw new UnsupportedOperationException("SSL is implemented only by the User Client.");
  }

  protected boolean isSslEnabled() {
    return false;
  }

  /**
   * Set's the state for authentication complete.
   * @param authComplete - state to set. True means authentication between client and server is completed, false
   *                     means authentication is in progress.
   */
  protected void setAuthComplete(boolean authComplete) {
    this.authComplete = authComplete;
  }

  protected boolean isAuthComplete() {
    return authComplete;
  }

  // Save the SslChannel after the SSL handshake so it can be closed later
  public void setSslChannel(Channel c) {

  }

  @Override
  protected CC initRemoteConnection(SocketChannel channel){
    local=channel.localAddress();
    remote=channel.remoteAddress();
    return null;
  }

  private static final OutboundRpcMessage PING_MESSAGE = new OutboundRpcMessage(RpcMode.PING, 0, 0, Acks.OK);

  /**
   * Handler that watches for situations where we haven't read from the socket in a certain timeout. If we exceed this
   * timeout, we send a PING message to the server to state that we are still alive.
   */
  private class IdlePingHandler extends IdleStateHandler {

    private GenericFutureListener<Future<? super Void>> pingFailedHandler = new GenericFutureListener<Future<? super Void>>() {
      public void operationComplete(Future<? super Void> future) throws Exception {
        if (!future.isSuccess()) {
          logger.error("Unable to maintain connection {}.  Closing connection.", connection.getName());
          connection.close();
        }
      }
    };

    IdlePingHandler(long idleWaitInMillis) {
      super(0, idleWaitInMillis, 0, TimeUnit.MILLISECONDS);
    }

    @Override
    protected void channelIdle(ChannelHandlerContext ctx, IdleStateEvent evt) throws Exception {
      if (evt.state() == IdleState.WRITER_IDLE) {
        ctx.writeAndFlush(PING_MESSAGE).addListener(pingFailedHandler);
      }
    }
  }

  public abstract ProtobufLengthDecoder getDecoder(BufferAllocator allocator);

  public boolean isActive() {
    return (connection != null) && connection.isActive();
  }

  protected abstract List<String> validateHandshake(HR validateHandshake) throws RpcException;

  /**
   * Creates various instances needed to start the SASL handshake. This is called from
   * {@link BasicClient#validateHandshake(MessageLite)} if authentication is required from server side.
   * @param connectionHandler - Connection handler used by client's to know about success/failure conditions.
   * @param serverAuthMechanisms - List of auth mechanisms configured on server side
   */
  protected abstract void prepareSaslHandshake(final RpcConnectionHandler<CC> connectionHandler,
                                               List<String> serverAuthMechanisms) throws RpcException;

  /**
   * Main method which starts the SASL handshake for all client channels (user/data/control) once it's determined
   * after regular RPC handshake that authentication is required by server side. Once authentication is completed
   * then only the underlying channel is made available to clients to send other RPC messages. Success and failure
   * events are notified to the connection handler on which client waits.
   * @param connectionHandler - Connection handler used by client's to know about success/failure conditions.
   * @param saslProperties - SASL related properties needed to create SASL client.
   * @param ugi - UserGroupInformation with logged in client side user
   * @param authFactory - Authentication factory to use for this SASL handshake.
   * @param rpcType - SASL_MESSAGE rpc type.
   */
  protected void startSaslHandshake(final RpcConnectionHandler<CC> connectionHandler,
                                    Map<String, ?> saslProperties, UserGroupInformation ugi,
                                    AuthenticatorFactory authFactory, T rpcType) {
    final String mechanismName = authFactory.getSimpleName();
    try {
      final SaslClient saslClient = authFactory.createSaslClient(ugi, saslProperties);
      if (saslClient == null) {
        final Exception ex = new SaslException(String.format("Cannot initiate authentication using %s mechanism. " +
          "Insufficient credentials or selected mechanism doesn't support configured security layers?", mechanismName));
        connectionHandler.connectionFailed(RpcConnectionHandler.FailureType.AUTHENTICATION, ex);
        return;
      }
      connection.setSaslClient(saslClient);
    } catch (final SaslException e) {
      logger.error("Failed while creating SASL client for SASL handshake for connection", connection.getName());
      connectionHandler.connectionFailed(RpcConnectionHandler.FailureType.AUTHENTICATION, e);
      return;
    }

    logger.debug("Initiating SASL exchange.");
    new AuthenticationOutcomeListener<>(this, connection, rpcType, ugi,
      new RpcOutcomeListener<Void>() {
      @Override
      public void failed(RpcException ex) {
        connectionHandler.connectionFailed(RpcConnectionHandler.FailureType.AUTHENTICATION, ex);
      }

      @Override
      public void success(Void value, ByteBuf buffer) {
        authComplete = true;
        connectionHandler.connectionSucceeded(connection);
      }

      @Override
      public void interrupted(InterruptedException ex) {
        connectionHandler.connectionFailed(RpcConnectionHandler.FailureType.AUTHENTICATION, ex);
      }
    }).initiate(mechanismName);
  }

  protected void finalizeConnection(HR handshake, CC connection) {
    // no-op
  }

  public <SEND extends MessageLite, RECEIVE extends MessageLite>
  void send(RpcOutcomeListener<RECEIVE> listener, T rpcType, SEND protobufBody,
            Class<RECEIVE> clazz, ByteBuf... dataBodies) {
    super.send(listener, connection, rpcType, protobufBody, clazz, dataBodies);
  }

  public <SEND extends MessageLite, RECEIVE extends MessageLite>
  DrillRpcFuture<RECEIVE> send(T rpcType, SEND protobufBody, Class<RECEIVE> clazz, ByteBuf... dataBodies) {
    return super.send(connection, rpcType, protobufBody, clazz, dataBodies);
  }

  public <SEND extends MessageLite, RECEIVE extends MessageLite>
  void send(RpcOutcomeListener<RECEIVE> listener, SEND protobufBody, boolean allowInEventLoop,
      ByteBuf... dataBodies) {
    super.send(listener, connection, handshakeType, protobufBody, (Class<RECEIVE>) responseClass,
        allowInEventLoop, dataBodies);
  }

  protected void connectAsClient(RpcConnectionHandler<CC> connectionListener, HS handshakeValue,
                                 String host, int port) {
    ConnectionMultiListener<T, CC, HS, HR, BasicClient<T, CC, HS, HR>> cml;
    ConnectionMultiListener.Builder<T, CC, HS, HR, BasicClient<T, CC, HS, HR> > builder =
        ConnectionMultiListener.newBuilder(connectionListener, handshakeValue, this);
    if (isSslEnabled()) {
      cml = builder.enableSSL().build();
      sslHandshakeListener = new ConnectionMultiListener.SSLHandshakeListener();
      sslHandshakeListener.setParent(cml);
    } else {
      cml = builder.build();
    }
    b.connect(host, port).addListener(cml.connectionHandler);
  }

  private class ClientHandshakeHandler extends AbstractHandshakeHandler<HR> {

    private final CC connection;

    ClientHandshakeHandler(CC connection) {
      super(BasicClient.this.handshakeType, BasicClient.this.handshakeParser);
      Preconditions.checkNotNull(connection);
      this.connection = connection;
    }

    @Override
    protected final void consumeHandshake(ChannelHandlerContext ctx, HR msg) throws Exception {
      // remove the handshake information from the queue so it doesn't sit there forever.
      final RpcOutcome<HR> response =
          connection.getAndRemoveRpcOutcome(handshakeType.getNumber(), coordinationId, responseClass);
      response.set(msg, null);
    }

  }

  public void setAutoRead(boolean enableAutoRead) {
    connection.setAutoRead(enableAutoRead);
  }

  public void close() {
    logger.debug("Closing client");

    if (connection != null) {
      connection.close();
      connection = null;
    }
  }
}
