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
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.util.concurrent.atomic.AtomicBoolean;

public abstract class BasicClient<T extends Enum<T>> extends RpcBus<T> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BasicClient.class);

  private Bootstrap b;
  private volatile boolean connect = false;

  public BasicClient(ByteBufAllocator alloc, EventLoopGroup eventLoopGroup) {
    b = new Bootstrap() //
        .group(eventLoopGroup) //
        .channel(NioSocketChannel.class) //
        .option(ChannelOption.ALLOCATOR, alloc) //
        .option(ChannelOption.SO_RCVBUF, 1 << 17) //
        .option(ChannelOption.SO_SNDBUF, 1 << 17) //
        .handler(new ChannelInitializer<SocketChannel>() {
          
          @Override
          protected void initChannel(SocketChannel ch) throws Exception {
            ch.closeFuture().addListener(getCloseHandler(ch));
            
            ch.pipeline().addLast( //
                new ZeroCopyProtobufLengthDecoder(), //
                new RpcDecoder(), //
                new RpcEncoder(), //
                new InboundHandler(ch), //
                new RpcExceptionHandler() //
                );
            channel = ch;
            connect = true;
          }
        }) //
        
        ;
  }

  @Override
  public boolean isClient() {
    return true;
  }
  
  public ChannelFuture connectAsClient(String host, int port) throws InterruptedException {
    ChannelFuture f = b.connect(host, port).sync();
    connect = !connect;
    return f;
  }

  public void close() {
    logger.debug("Closing client");
    b.shutdown();
  }

}
