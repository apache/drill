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
import java.util.concurrent.ExecutionException;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.UserBitShared.DrillPBError;

public abstract class RemoteConnection implements ConnectionThrottle, AutoCloseable {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RemoteConnection.class);
  private final Channel channel;
  private final WriteManager writeManager;
  private final RequestIdMap requestIdMap = new RequestIdMap();
  private final String clientName;

  private String name;

  public boolean inEventLoop() {
    return channel.eventLoop().inEventLoop();
  }

  public RemoteConnection(SocketChannel channel, String name) {
    super();
    this.channel = channel;
    this.clientName = name;
    this.writeManager = new WriteManager();
    channel.pipeline().addLast(new BackPressureHandler());
  }

  public String getName() {
    if (name == null) {
      name = String.format("%s <--> %s (%s)", channel.localAddress(), channel.remoteAddress(), clientName);
    }
    return name;
  }

  public abstract BufferAllocator getAllocator();

  public final Channel getChannel() {
    return channel;
  }

  public boolean blockOnNotWritable(RpcOutcomeListener<?> listener) {
    try {
      writeManager.waitForWritable();
      return true;
    } catch (final InterruptedException e) {
      listener.interrupted(e);

      // Preserve evidence that the interruption occurred so that code higher up
      // on the call stack can learn of the
      // interruption and respond to it if it wants to.
      Thread.currentThread().interrupt();

      return false;
    }
  }

  public void setAutoRead(boolean enableAutoRead) {
    channel.config().setAutoRead(enableAutoRead);
  }

  public boolean isActive() {
    return channel.isActive();
  }

  /**
   * The write manager is responsible for controlling whether or not a write can
   * be sent. It controls whether or not to block a sender if we have tcp
   * backpressure on the receive side.
   */
  private static class WriteManager {
    private final ResettableBarrier barrier = new ResettableBarrier();
    private volatile boolean disabled = false;

    public WriteManager() {
      barrier.openBarrier();
    }

    public void waitForWritable() throws InterruptedException {
      barrier.await();
    }

    public void setWritable(boolean isWritable) {
      if (isWritable) {
        barrier.openBarrier();
      } else if (!disabled) {
        barrier.closeBarrier();
      }

    }

    public void disable() {
      disabled = true;
    }
  }

  private class BackPressureHandler extends ChannelInboundHandlerAdapter {

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
      writeManager.setWritable(ctx.channel().isWritable());
      ctx.fireChannelWritabilityChanged();
    }

  }

  /**
   * For incoming messages, remove the outcome listener and return it. Can only be done once per coordinationId
   * creation. CoordinationId's are recycled so they will show up once we run through all 4B of them.
   * @param rpcType The rpc type associated with the coordination.
   * @param coordinationId The coordination id that was returned with the listener was created.
   * @param clazz The class that is expected in response.
   * @return An RpcOutcome associated with the provided coordinationId.
   */
  <V> RpcOutcome<V> getAndRemoveRpcOutcome(int rpcType, int coordinationId, Class<V> clazz) {
    return requestIdMap.getAndRemoveRpcOutcome(rpcType, coordinationId, clazz);
  }

  /**
   * Create a new rpc listener that will be notified when the response is returned.
   * @param handler The outcome handler to be notified when the response arrives.
   * @param clazz The Class associated with the response object.
   * @return The new listener. Also carries the coordination id for use in the rpc message.
   */
  <V> ChannelListenerWithCoordinationId createNewRpcListener(RpcOutcomeListener<V> handler, Class<V> clazz) {
    return requestIdMap.createNewRpcListener(handler, clazz, this);
  }

  /**
   * Inform the local outcome listener that the remote operation could not be handled.
   * @param coordinationId The id that failed.
   * @param failure The failure that occurred.
   */
  void recordRemoteFailure(int coordinationId, DrillPBError failure) {
    requestIdMap.recordRemoteFailure(coordinationId, failure);
  }

  /**
   * Called from the RpcBus's channel close handler to close all remaining
   * resources associated with this connection. Ensures that any pending
   * back-pressure items are also unblocked so they can be thrown away.
   *
   * @param ex
   *          The exception that caused the channel to close.
   */
  void channelClosed(RpcException ex) {
    // this could possibly overrelease but it doesn't matter since we're only
    // going to do this to ensure that we
    // fail out any pending messages
    writeManager.disable();
    writeManager.setWritable(true);

    // ensure outstanding requests are cleaned up.
    requestIdMap.channelClosed(ex);
  }

  /**
   * Connection consumer wants to close connection. Initiate connection close
   * and complete. This is a blocking call that ensures that the connection is
   * closed before returning. As part of this call, the channel close handler
   * will be triggered which will call channelClosed() above. The latter will
   * happen in a separate thread while this method is blocking.
   */
  @Override
  public void close() {
    try {
      if (channel.isActive()) {
        channel.close().get();
      }
    } catch (final InterruptedException | ExecutionException e) {
      logger.warn("Caught exception while closing channel.", e);

      // Preserve evidence that the interruption occurred so that code higher up
      // on the call stack can learn of the
      // interruption and respond to it if it wants to.
      Thread.currentThread().interrupt();
    }
  }

}
