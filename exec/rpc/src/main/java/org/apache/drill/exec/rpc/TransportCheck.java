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

import com.google.common.base.Stopwatch;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.internal.SystemPropertyUtil;
import org.slf4j.Logger;

import java.util.Locale;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * TransportCheck decides whether or not to use the native EPOLL mechanism for communication.
 */
public class TransportCheck {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TransportCheck.class);

  private static final String USE_LINUX_EPOLL = "drill.exec.enable-epoll";

  public static final boolean SUPPORTS_EPOLL;

  static{

    String name = SystemPropertyUtil.get("os.name").toLowerCase(Locale.US).trim();

    SUPPORTS_EPOLL = name.startsWith("linux") && SystemPropertyUtil.getBoolean(USE_LINUX_EPOLL, false);
  }

  public static Class<? extends ServerSocketChannel> getServerSocketChannel(){
    if(SUPPORTS_EPOLL){
      return EpollServerSocketChannel.class;
    }else{
      return NioServerSocketChannel.class;
    }
  }

  public static Class<? extends SocketChannel> getClientSocketChannel(){
    if(SUPPORTS_EPOLL){
      return EpollSocketChannel.class;
    }else{
      return NioSocketChannel.class;
    }
  }

  public static EventLoopGroup createEventLoopGroup(int nThreads, String prefix) {
     if(SUPPORTS_EPOLL){
       return new EpollEventLoopGroup(nThreads, new NamedThreadFactory(prefix));
     }else{
       return new NioEventLoopGroup(nThreads, new NamedThreadFactory(prefix));
     }
  }

  /**
   * Shuts down the given event loop group gracefully.
   *
   * @param eventLoopGroup event loop group to shutdown
   * @param groupName group name
   * @param logger logger
   */
  public static void shutDownEventLoopGroup(final EventLoopGroup eventLoopGroup,
                                            final String groupName,
                                            final Logger logger) {
    try {
      final Stopwatch watch = Stopwatch.createStarted();
      // this takes 1s to complete
      // known issue: https://github.com/netty/netty/issues/2545
      eventLoopGroup.shutdownGracefully(0, 0, TimeUnit.SECONDS).get();
      final long elapsed = watch.elapsed(MILLISECONDS);
      if (elapsed > 500) {
        logger.info("Closed " + groupName + " in " + elapsed + " ms.");
      }
    } catch (final InterruptedException | ExecutionException e) {
      logger.warn("Failure while shutting down {}.", groupName, e);

      // Preserve evidence that the interruption occurred so that code higher up on the call stack can learn of the
      // interruption and respond to it if it wants to.
      Thread.currentThread().interrupt();
    }
  }

  // prevents instantiation
  private TransportCheck() {
  }
}
