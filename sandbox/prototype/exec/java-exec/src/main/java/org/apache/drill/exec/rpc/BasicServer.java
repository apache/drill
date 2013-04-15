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

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

import java.io.IOException;

import org.apache.drill.exec.exception.DrillbitStartupException;

import com.google.protobuf.Internal.EnumLite;

/**
 * A server is bound to a port and is responsible for responding to various type of requests. In some cases, the inbound
 * requests will generate more than one outbound request.
 */
public abstract class BasicServer<T extends EnumLite> extends RpcBus<T>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BasicServer.class);

  private ServerBootstrap b;
  private volatile boolean connect = false;

  public BasicServer(ByteBufAllocator alloc, EventLoopGroup eventLoopGroup) {

    b = new ServerBootstrap() //
        .channel(NioServerSocketChannel.class) //
        .option(ChannelOption.SO_BACKLOG, 100) //
        .option(ChannelOption.SO_RCVBUF, 1 << 17) //
        .option(ChannelOption.SO_SNDBUF, 1 << 17) //
        .group(eventLoopGroup) //
        .childOption(ChannelOption.ALLOCATOR, alloc) //
        .handler(new LoggingHandler(LogLevel.INFO)) //
        .childHandler(new ChannelInitializer<SocketChannel>() {
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
        });
  }
 
  @Override
  public boolean isClient() {
    return false;
  }


  public int bind(final int initialPort) throws InterruptedException, DrillbitStartupException{
    boolean ok = false;
    int port = initialPort;
    for(; port < Character.MAX_VALUE; port++){
      if(b.bind(port).sync().isSuccess()){
        ok = true;
        break;
      }
    }
    if(!ok){
      throw new DrillbitStartupException(String.format("Unable to find available port for Drillbit server starting at port %d.", initialPort));
    }
    
    connect = !connect;
    return port;    
  }

  @Override
  public void close() throws IOException {
    if(b != null) b.shutdown();
  }
  
  

}
