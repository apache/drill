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

import java.util.concurrent.ExecutionException;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.rpc.user.ConnectionThrottle;

public abstract class RemoteConnection implements ConnectionThrottle, AutoCloseable {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RemoteConnection.class);
  private final Channel channel;
  private final WriteManager writeManager;

  public boolean inEventLoop(){
    return channel.eventLoop().inEventLoop();
  }

  public RemoteConnection(Channel channel) {
    super();
    this.channel = channel;
    this.writeManager = new WriteManager();
    channel.pipeline().addLast(new BackPressureHandler());
  }

  public abstract BufferAllocator getAllocator();

  public final Channel getChannel() {
    return channel;
  }

  public boolean blockOnNotWritable(RpcOutcomeListener<?> listener){
    try{
      writeManager.waitForWritable();
      return true;
    }catch(InterruptedException e){
      listener.failed(new RpcException(e));
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

    public WriteManager(){
      barrier.openBarrier();
    }

    public void waitForWritable() throws InterruptedException{
      barrier.await();
    }

    public void setWritable(boolean isWritable){
//      logger.debug("Set writable: {}", isWritable);
      if(isWritable){
        barrier.openBarrier();
      }else{
        barrier.closeBarrier();
      }

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
      channel.close().get();
    } catch (InterruptedException | ExecutionException e) {
      logger.warn("Caught exception while closing channel.", e);
    }
  }

}