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
package org.apache.drill.exec.rpc;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelFuture;
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

import java.net.SocketAddress;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.GeneralRPCProtos.RpcMode;
import org.apache.drill.exec.rpc.RpcConnectionHandler.FailureType;

import com.google.common.base.Preconditions;
import com.google.protobuf.Internal.EnumLite;
import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;

public abstract class BasicClient<T extends EnumLite, R extends RemoteConnection, HANDSHAKE_SEND extends MessageLite, HANDSHAKE_RESPONSE extends MessageLite>
    extends RpcBus<T, R> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BasicClient.class);

  // The percentage of time that should pass before sending a ping message to ensure server doesn't time us out. For
  // example, if timeout is set to 30 seconds and we set percentage to 0.5, then if no write has happened within 15
  // seconds, the idle state handler will send a ping message.
  private static final double PERCENT_TIMEOUT_BEFORE_SENDING_PING = 0.5;

  private final Bootstrap b;
  protected R connection;
  private final T handshakeType;
  private final Class<HANDSHAKE_RESPONSE> responseClass;
  private final Parser<HANDSHAKE_RESPONSE> handshakeParser;

  private final IdlePingHandler pingHandler;

  public BasicClient(RpcConfig rpcMapping, ByteBufAllocator alloc, EventLoopGroup eventLoopGroup, T handshakeType,
      Class<HANDSHAKE_RESPONSE> responseClass, Parser<HANDSHAKE_RESPONSE> handshakeParser) {
    super(rpcMapping);
    this.responseClass = responseClass;
    this.handshakeType = handshakeType;
    this.handshakeParser = handshakeParser;
    final long timeoutInMillis = rpcMapping.hasTimeout() ? (long) (rpcMapping.getTimeout() * 1000.0 * PERCENT_TIMEOUT_BEFORE_SENDING_PING)
        : -1;
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

            pipe.addLast("protocol-decoder", getDecoder(connection.getAllocator()));
            pipe.addLast("message-decoder", new RpcDecoder("c-" + rpcConfig.getName()));
            pipe.addLast("protocol-encoder", new RpcEncoder("c-" + rpcConfig.getName()));
            pipe.addLast("handshake-handler", new ClientHandshakeHandler(connection));

            if(pingHandler != null){
              pipe.addLast("idle-state-handler", pingHandler);
            }

            pipe.addLast("message-handler", new InboundHandler(connection));
            pipe.addLast("exception-handler", new RpcExceptionHandler<R>(connection));
          }
        }); //

    // if(TransportCheck.SUPPORTS_EPOLL){
    // b.option(EpollChannelOption.SO_REUSEPORT, true); //
    // }
  }

  public R initRemoteConnection(SocketChannel channel){
    local=channel.localAddress();
    remote=channel.remoteAddress();
    return null;
  };

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

    public IdlePingHandler(long idleWaitInMillis) {
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
    return connection != null
        && connection.getChannel() != null
        && connection.getChannel().isActive();
  }

  protected abstract void validateHandshake(HANDSHAKE_RESPONSE validateHandshake) throws RpcException;

  protected abstract void finalizeConnection(HANDSHAKE_RESPONSE handshake, R connection);

  public <SEND extends MessageLite, RECEIVE extends MessageLite> void send(RpcOutcomeListener<RECEIVE> listener,
      T rpcType, SEND protobufBody, Class<RECEIVE> clazz, ByteBuf... dataBodies) {
    super.send(listener, connection, rpcType, protobufBody, clazz, dataBodies);
  }

  public <SEND extends MessageLite, RECEIVE extends MessageLite> DrillRpcFuture<RECEIVE> send(T rpcType,
      SEND protobufBody, Class<RECEIVE> clazz, ByteBuf... dataBodies) {
    return super.send(connection, rpcType, protobufBody, clazz, dataBodies);
  }

  protected void connectAsClient(RpcConnectionHandler<R> connectionListener, HANDSHAKE_SEND handshakeValue,
      String host, int port) {
    ConnectionMultiListener cml = new ConnectionMultiListener(connectionListener, handshakeValue);
    b.connect(host, port).addListener(cml.connectionHandler);
  }

  private class ConnectionMultiListener {
    private final RpcConnectionHandler<R> l;
    private final HANDSHAKE_SEND handshakeValue;

    public ConnectionMultiListener(RpcConnectionHandler<R> l, HANDSHAKE_SEND handshakeValue) {
      assert l != null;
      assert handshakeValue != null;

      this.l = l;
      this.handshakeValue = handshakeValue;
    }

    public final ConnectionHandler connectionHandler = new ConnectionHandler();
    public final HandshakeSendHandler handshakeSendHandler = new HandshakeSendHandler();

    /**
     * Manages connection establishment outcomes.
     */
    private class ConnectionHandler implements GenericFutureListener<ChannelFuture> {

      @Override
      public void operationComplete(ChannelFuture future) throws Exception {
        boolean isInterrupted = false;

        // We want to wait for at least 120 secs when interrupts occur. Establishing a connection fails/succeeds quickly,
        // So there is no point propagating the interruption as failure immediately.
        long remainingWaitTimeMills = 120000;
        long startTime = System.currentTimeMillis();
        // logger.debug("Connection operation finished.  Success: {}", future.isSuccess());
        while(true) {
          try {
            future.get(remainingWaitTimeMills, TimeUnit.MILLISECONDS);
            if (future.isSuccess()) {
              SocketAddress remote = future.channel().remoteAddress();
              SocketAddress local = future.channel().localAddress();
              setAddresses(remote, local);
              // send a handshake on the current thread. This is the only time we will send from within the event thread.
              // We can do this because the connection will not be backed up.
              send(handshakeSendHandler, connection, handshakeType, handshakeValue, responseClass, true);
            } else {
              l.connectionFailed(FailureType.CONNECTION, new RpcException("General connection failure."));
            }
            // logger.debug("Handshake queued for send.");
            break;
          } catch (final InterruptedException interruptEx) {
            remainingWaitTimeMills -= (System.currentTimeMillis() - startTime);
            startTime = System.currentTimeMillis();
            isInterrupted = true;
            if (remainingWaitTimeMills < 1) {
              l.connectionFailed(FailureType.CONNECTION, interruptEx);
              break;
            }
            // Ignore the interrupt and continue to wait until we elapse remainingWaitTimeMills.
          } catch (final Exception ex) {
            logger.error("Failed to establish connection", ex);
            l.connectionFailed(FailureType.CONNECTION, ex);
            break;
          }
        }

        if (isInterrupted) {
          // Preserve evidence that the interruption occurred so that code higher up on the call stack can learn of the
          // interruption and respond to it if it wants to.
          Thread.currentThread().interrupt();
        }
      }
    }

    /**
     * manages handshake outcomes.
     */
    private class HandshakeSendHandler implements RpcOutcomeListener<HANDSHAKE_RESPONSE> {

      @Override
      public void failed(RpcException ex) {
        logger.debug("Failure while initiating handshake", ex);
        l.connectionFailed(FailureType.HANDSHAKE_COMMUNICATION, ex);
      }

      @Override
      public void success(HANDSHAKE_RESPONSE value, ByteBuf buffer) {
        // logger.debug("Handshake received. {}", value);
        try {
          BasicClient.this.validateHandshake(value);
          BasicClient.this.finalizeConnection(value, connection);
          l.connectionSucceeded(connection);
          // logger.debug("Handshake completed succesfully.");
        } catch (RpcException ex) {
          l.connectionFailed(FailureType.HANDSHAKE_VALIDATION, ex);
        }
      }

      @Override
      public void interrupted(final InterruptedException ex) {
        logger.warn("Interrupted while waiting for handshake response", ex);
        l.connectionFailed(FailureType.HANDSHAKE_COMMUNICATION, ex);
      }
    }

  }

  private class ClientHandshakeHandler extends AbstractHandshakeHandler<HANDSHAKE_RESPONSE> {

    private final R connection;

    public ClientHandshakeHandler(R connection) {
      super(BasicClient.this.handshakeType, BasicClient.this.handshakeParser);
      Preconditions.checkNotNull(connection);
      this.connection = connection;
    }

    @Override
    protected final void consumeHandshake(ChannelHandlerContext ctx, HANDSHAKE_RESPONSE msg) throws Exception {
      // remove the handshake information from the queue so it doesn't sit there forever.
      final RpcOutcome<HANDSHAKE_RESPONSE> response =
          connection.getAndRemoveRpcOutcome(handshakeType.getNumber(), coordinationId, responseClass);
      response.set(msg, null);
    }

  }

  public void setAutoRead(boolean enableAutoRead) {
    connection.setAutoRead(enableAutoRead);
  }

  public void close() {
    logger.debug("Closing client");
    try {
      connection.getChannel().close().get();
    } catch (final InterruptedException | ExecutionException e) {
      logger.warn("Failure while shutting {}", this.getClass().getName(), e);

      // Preserve evidence that the interruption occurred so that code higher up on the call stack can learn of the
      // interruption and respond to it if it wants to.
      Thread.currentThread().interrupt();
    }
  }

}
