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
package org.apache.drill.exec.rpc;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.GenericFutureListener;

import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.Internal.EnumLite;
import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;

public abstract class BasicClient<T extends EnumLite, R extends RemoteConnection> extends RpcBus<T, R> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BasicClient.class);

  private Bootstrap b;
  private volatile boolean connect = false;
  protected R connection;
  private EventLoopGroup eventLoop;

  public BasicClient(RpcConfig rpcMapping, ByteBufAllocator alloc, EventLoopGroup eventLoopGroup) {
    super(rpcMapping);
    this.eventLoop = eventLoopGroup;
    
    b = new Bootstrap() //
        .group(eventLoopGroup) //
        .channel(NioSocketChannel.class) //
        .option(ChannelOption.ALLOCATOR, alloc) //
        .option(ChannelOption.SO_RCVBUF, 1 << 17) //
        .option(ChannelOption.SO_SNDBUF, 1 << 17) //
        .handler(new ChannelInitializer<SocketChannel>() {

          @Override
          protected void initChannel(SocketChannel ch) throws Exception {
            logger.debug("initializing client connection.");
            connection = initRemoteConnection(ch);
            ch.closeFuture().addListener(getCloseHandler(connection));

            ch.pipeline().addLast( //
                new ZeroCopyProtobufLengthDecoder(), //
                new RpcDecoder(rpcConfig.getName()), //
                new RpcEncoder(rpcConfig.getName()), //
                getHandshakeHandler(), //
                new InboundHandler(connection), //
                new RpcExceptionHandler() //
                );
            connect = true;
          }
        }) //

    ;
  }

  protected abstract ClientHandshakeHandler<?> getHandshakeHandler();

  protected abstract class ClientHandshakeHandler<T extends MessageLite> extends AbstractHandshakeHandler<T> {
    private Class<T> responseType;

    public ClientHandshakeHandler(EnumLite handshakeType, Class<T> responseType, Parser<T> parser) {
      super(handshakeType, parser);
      this.responseType = responseType;
    }

    @Override
    protected final void consumeHandshake(Channel c, T msg) throws Exception {
      validateHandshake(msg);
      queue.getFuture(handshakeType.getNumber(), coordinationId, responseType).setValue(msg);
    }

    protected abstract void validateHandshake(T msg) throws Exception;

  }

  protected GenericFutureListener<ChannelFuture> getCloseHandler(Channel channel) {
    return new ChannelClosedHandler();
  }

  protected final <SEND extends MessageLite, RECEIVE extends MessageLite> DrillRpcFutureImpl<RECEIVE> send(
      T connection, T rpcType, SEND protobufBody, Class<RECEIVE> clazz, ByteBuf... dataBodies) throws RpcException {
    throw new UnsupportedOperationException(
        "This shouldn't be used in client mode as a client only has a single connection.");
  }

  protected <SEND extends MessageLite, RECEIVE extends MessageLite> DrillRpcFuture<RECEIVE> send(T rpcType,
      SEND protobufBody, Class<RECEIVE> clazz, ByteBuf... dataBodies) throws RpcException {
    return super.send(connection, rpcType, protobufBody, clazz, dataBodies);
  }

  @Override
  public boolean isClient() {
    return true;
  }

  /**
   * TODO: This is a horrible hack to manage deadlock caused by creation of BitClient within BitCom.  Should be cleaned up.
   */
  private class HandshakeThread<SEND extends MessageLite, RECEIVE extends MessageLite> extends Thread {
    final SettableFuture<RECEIVE> future;
    T handshakeType;
    SEND handshakeValue;
    String host;
    int port;
    Class<RECEIVE> responseClass;

    public HandshakeThread(T handshakeType, SEND handshakeValue, String host, int port, Class<RECEIVE> responseClass) {
      super();
      assert host != null && !host.isEmpty();
      assert port > 0;
      logger.debug("Creating new handshake thread to connec to {}:{}", host, port);
      this.setName(String.format("handshake thread for %s", handshakeType.getClass().getCanonicalName()));
      future = SettableFuture.create();
      this.handshakeType = handshakeType;
      this.handshakeValue = handshakeValue;
      this.host = host;
      this.port = port;
      this.responseClass = responseClass;
    }

    @Override
    public void run() {
      try {
        logger.debug("Starting to get client connection on host {}, port {}.", host, port);
        
        ChannelFuture f = b.connect(host, port);
        f.sync();
        if (connection == null) throw new RpcException("Failure while attempting to connect to server.");
        connect = !connect;
        logger.debug("Client connected, sending handshake.");
        DrillRpcFuture<RECEIVE> fut = send(handshakeType, handshakeValue, responseClass);
        future.set(fut.checkedGet());
        logger.debug("Got bit client connection.");
      } catch (Exception e) {
        logger.debug("Failed to get client connection.", e);
        future.setException(e);
      }
    }

  }

  protected <SEND extends MessageLite, RECEIVE extends MessageLite> RECEIVE connectAsClient(T handshakeType,
      SEND handshakeValue, String host, int port, Class<RECEIVE> responseClass) throws InterruptedException,
      RpcException {
    
    
    HandshakeThread<SEND, RECEIVE> ht = new HandshakeThread<SEND, RECEIVE>(handshakeType, handshakeValue, host, port, responseClass);
    ht.start();
    try{
      return ht.future.get();  
    }catch(Exception e){
      throw new RpcException(e);
    }
    
  }

  public void close() {
    logger.debug("Closing client");
    connection.getChannel().close();
  }

}
