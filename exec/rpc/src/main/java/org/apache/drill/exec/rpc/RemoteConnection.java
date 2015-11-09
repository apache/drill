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

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.socket.SocketChannel;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

import java.util.concurrent.ExecutionException;

import org.apache.drill.exec.memory.BufferAllocator;

public abstract class RemoteConnection implements ConnectionThrottle, AutoCloseable {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RemoteConnection.class);
  private final Channel channel;
  private final WriteManager writeManager;
  private String name;
  private final String clientName;

  public boolean inEventLoop(){
    return channel.eventLoop().inEventLoop();
  }

  public RemoteConnection(SocketChannel channel, String name) {
    super();
    this.channel = channel;
    this.clientName = name;
    this.writeManager = new WriteManager();
    channel.pipeline().addLast(new BackPressureHandler());
    channel.closeFuture().addListener(new GenericFutureListener<Future<? super Void>>() {
      public void operationComplete(Future<? super Void> future) throws Exception {
        // this could possibly overrelease but it doesn't matter since we're only going to do this to ensure that we
        // fail out any pending messages
        writeManager.disable();
        writeManager.setWritable(true);
      }
    });

  }

  public String getName() {
    if(name == null){
      name = String.format("%s <--> %s (%s)", channel.localAddress(), channel.remoteAddress(), clientName);
    }
    return name;
  }

  public abstract BufferAllocator getAllocator();

  public final Channel getChannel() {
    return channel;
  }

  public boolean blockOnNotWritable(RpcOutcomeListener<?> listener){
    try{
      writeManager.waitForWritable();
      return true;
    }catch(final InterruptedException e){
      listener.interrupted(e);

      // Preserve evidence that the interruption occurred so that code higher up on the call stack can learn of the
      // interruption and respond to it if it wants to.
      Thread.currentThread().interrupt();

      return false;
    }
  }

  public void setAutoRead(boolean enableAutoRead){
    channel.config().setAutoRead(enableAutoRead);
  }

  public boolean isActive(){
    return channel.isActive();
  }

  /**
   * The write manager is responsible for controlling whether or not a write can be sent.  It controls whether or not to block a sender if we have tcp backpressure on the receive side.
   */
  private static class WriteManager{
    private final ResettableBarrier barrier = new ResettableBarrier();
    private volatile boolean disabled = false;

    public WriteManager(){
      barrier.openBarrier();
    }

    public void waitForWritable() throws InterruptedException{
      barrier.await();
    }

    public void setWritable(boolean isWritable){
      if(isWritable){
        barrier.openBarrier();
      } else if (!disabled) {
        barrier.closeBarrier();
      }

    }

    public void disable() {
      disabled = true;
    }
  }

  private class BackPressureHandler extends ChannelInboundHandlerAdapter{

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
//      logger.debug("Channel writability changed.", ctx.channel().isWritable());
      writeManager.setWritable(ctx.channel().isWritable());
      ctx.fireChannelWritabilityChanged();
    }

  }

  @Override
  public void close() {
    try {
      if (channel.isActive()) {
        channel.close().get();
      }
    } catch (final InterruptedException | ExecutionException e) {
      logger.warn("Caught exception while closing channel.", e);

      // Preserve evidence that the interruption occurred so that code higher up on the call stack can learn of the
      // interruption and respond to it if it wants to.
      Thread.currentThread().interrupt();
    }
  }

}
