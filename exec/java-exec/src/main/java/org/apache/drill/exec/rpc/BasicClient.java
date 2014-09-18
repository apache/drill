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
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.util.concurrent.GenericFutureListener;

import java.util.concurrent.ExecutionException;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.rpc.RpcConnectionHandler.FailureType;

import com.google.protobuf.Internal.EnumLite;
import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;

public abstract class BasicClient<T extends EnumLite, R extends RemoteConnection, HANDSHAKE_SEND extends MessageLite, HANDSHAKE_RESPONSE extends MessageLite>
    extends RpcBus<T, R> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BasicClient.class);

  private final Bootstrap b;
  private volatile boolean connect = false;
  protected R connection;
  private final T handshakeType;
  private final Class<HANDSHAKE_RESPONSE> responseClass;
  private final Parser<HANDSHAKE_RESPONSE> handshakeParser;

  public BasicClient(RpcConfig rpcMapping, ByteBufAllocator alloc, EventLoopGroup eventLoopGroup, T handshakeType,
      Class<HANDSHAKE_RESPONSE> responseClass, Parser<HANDSHAKE_RESPONSE> handshakeParser) {
    super(rpcMapping);
    this.responseClass = responseClass;
    this.handshakeType = handshakeType;
    this.handshakeParser = handshakeParser;

    b = new Bootstrap() //
        .group(eventLoopGroup) //
        .channel(TransportCheck.getClientSocketChannel()) //
        .option(ChannelOption.ALLOCATOR, alloc) //
        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 30*1000)
        .option(ChannelOption.SO_REUSEADDR, true)
        .option(ChannelOption.SO_RCVBUF, 1 << 17) //
        .option(ChannelOption.SO_SNDBUF, 1 << 17) //
        .option(ChannelOption.TCP_NODELAY, true)
        .handler(new ChannelInitializer<SocketChannel>() {

          @Override
          protected void initChannel(SocketChannel ch) throws Exception {
//            logger.debug("initializing client connection.");
            connection = initRemoteConnection(ch);
            ch.closeFuture().addListener(getCloseHandler(connection));

            ch.pipeline().addLast( //
                getDecoder(connection.getAllocator()), //
                new RpcDecoder("c-" + rpcConfig.getName()), //
                new RpcEncoder("c-" + rpcConfig.getName()), //
                new ClientHandshakeHandler(), //
                new InboundHandler(connection), //
                new RpcExceptionHandler() //
                );
            connect = true;
          }
        }); //

//    if(TransportCheck.SUPPORTS_EPOLL){
//      b.option(EpollChannelOption.SO_REUSEPORT, true); //
//    }
  }

  public abstract ProtobufLengthDecoder getDecoder(BufferAllocator allocator);

  public boolean isActive(){
    return connection != null
        && connection.getChannel() != null
        && connection.getChannel().isActive() ;
  }

  protected abstract void validateHandshake(HANDSHAKE_RESPONSE validateHandshake) throws RpcException;
  protected abstract void finalizeConnection(HANDSHAKE_RESPONSE handshake, R connection);

  protected GenericFutureListener<ChannelFuture> getCloseHandler(Channel channel) {
    return new ChannelClosedHandler();
  }

  public <SEND extends MessageLite, RECEIVE extends MessageLite> void send(RpcOutcomeListener<RECEIVE> listener,
      T rpcType, SEND protobufBody, Class<RECEIVE> clazz, ByteBuf... dataBodies) {
    super.send(listener, connection, rpcType, protobufBody, clazz, dataBodies);
  }

  public <SEND extends MessageLite, RECEIVE extends MessageLite> DrillRpcFuture<RECEIVE> send(T rpcType,
      SEND protobufBody, Class<RECEIVE> clazz, ByteBuf... dataBodies) {
    return super.send(connection, rpcType, protobufBody, clazz, dataBodies);
  }

  @Override
  public boolean isClient() {
    return true;
  }

  protected void connectAsClient(RpcConnectionHandler<R> connectionListener, HANDSHAKE_SEND handshakeValue, String host, int port){
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
//        logger.debug("Connection operation finished.  Success: {}", future.isSuccess());
        try {
          future.get();
          if (future.isSuccess()) {
            // send a handshake on the current thread.  This is the only time we will send from within the event thread.  We can do this because the connection will not be backed up.
            send(handshakeSendHandler, connection, handshakeType, handshakeValue, responseClass, true);
          } else {
            l.connectionFailed(FailureType.CONNECTION, new RpcException("General connection failure."));
          }
//          logger.debug("Handshake queued for send.");
        } catch (Exception ex) {
          l.connectionFailed(FailureType.CONNECTION, ex);
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
//        logger.debug("Handshake received. {}", value);
        try {
          BasicClient.this.validateHandshake(value);
          BasicClient.this.finalizeConnection(value, connection);
          BasicClient.this.connect = true;
          l.connectionSucceeded(connection);
//          logger.debug("Handshake completed succesfully.");
        } catch (RpcException ex) {
          l.connectionFailed(FailureType.HANDSHAKE_VALIDATION, ex);
        }
      }

    }

  }

  private class ClientHandshakeHandler extends AbstractHandshakeHandler<HANDSHAKE_RESPONSE> {

    public ClientHandshakeHandler() {
      super(BasicClient.this.handshakeType, BasicClient.this.handshakeParser);
    }

    @Override
    protected final void consumeHandshake(ChannelHandlerContext ctx, HANDSHAKE_RESPONSE msg) throws Exception {
      // remove the handshake information from the queue so it doesn't sit there forever.
      RpcOutcome<HANDSHAKE_RESPONSE> response = queue.getFuture(handshakeType.getNumber(), coordinationId,
          responseClass);
      response.set(msg, null);
    }

  }

  public void setAutoRead(boolean enableAutoRead){
    connection.setAutoRead(enableAutoRead);
  }

  public void close() {
    logger.debug("Closing client");
    try {
      connection.getChannel().close().get();
    } catch (InterruptedException | ExecutionException e) {
      logger.warn("Failure whiel shutting {}", this.getClass().getName(), e);
    }
  }

}
