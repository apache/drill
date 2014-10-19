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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.drill.exec.proto.UserBitShared.DrillPBError;

/**
 * Manages the creation of rpc futures for a particular socket.
 */
public class CoordinationQueue {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CoordinationQueue.class);

  private final PositiveAtomicInteger circularInt = new PositiveAtomicInteger();
  private final Map<Integer, RpcOutcome<?>> map;

  public CoordinationQueue(int segmentSize, int segmentCount) {
    map = new ConcurrentHashMap<Integer, RpcOutcome<?>>(segmentSize, 0.75f, segmentCount);
  }

  void channelClosed(Throwable ex) {
    if (ex != null) {
      RpcException e;
      if (ex instanceof RpcException) {
        e = (RpcException) ex;
      } else {
        e = new RpcException(ex);
      }
      for (RpcOutcome<?> f : map.values()) {
        f.setException(e);
      }
    }
  }

  public <V> ChannelListenerWithCoordinationId get(RpcOutcomeListener<V> handler, Class<V> clazz, RemoteConnection connection) {
    int i = circularInt.getNext();
    RpcListener<V> future = new RpcListener<V>(handler, clazz, i, connection);
    Object old = map.put(i, future);
    if (old != null) {
      throw new IllegalStateException(
          "You attempted to reuse a coordination id when the previous coordination id has not been removed.  This is likely rpc future callback memory leak.");
    }
    return future;
  }

  private class RpcListener<T> implements ChannelListenerWithCoordinationId, RpcOutcome<T>{
    final RpcOutcomeListener<T> handler;
    final Class<T> clazz;
    final int coordinationId;
    final RemoteConnection connection;

    public RpcListener(RpcOutcomeListener<T> handler, Class<T> clazz, int coordinationId, RemoteConnection connection) {
      super();
      this.handler = handler;
      this.clazz = clazz;
      this.coordinationId = coordinationId;
      this.connection = connection;
    }

    @Override
    public void operationComplete(ChannelFuture future) throws Exception {

      if (!future.isSuccess()) {
        removeFromMap(coordinationId);
        if (future.channel().isActive()) {
           throw new RpcException("Future failed") ;
        } else {
          throw  new ChannelClosedException();
        }
      }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void set(Object value, ByteBuf buffer) {
      assert clazz.isAssignableFrom(value.getClass());
      handler.success( (T) value, buffer);
    }

    @Override
    public void setException(Throwable t) {
      handler.failed(RpcException.mapException(t));
    }

    @Override
    public Class<T> getOutcomeType() {
      return clazz;
    }

    @Override
    public int getCoordinationId() {
      return coordinationId;
    }

  }

  private RpcOutcome<?> removeFromMap(int coordinationId) {
    RpcOutcome<?> rpc = map.remove(coordinationId);
    if (rpc == null) {
      throw new IllegalStateException(
          "Attempting to retrieve an rpc that wasn't first stored in the rpc coordination queue.  This would most likely happen if you're opposite endpoint sent multiple messages on the same coordination id.");
    }
    return rpc;
  }

  public <V> RpcOutcome<V> getFuture(int rpcType, int coordinationId, Class<V> clazz) {

    RpcOutcome<?> rpc = removeFromMap(coordinationId);
    // logger.debug("Got rpc from map {}", rpc);
    Class<?> outcomeClass = rpc.getOutcomeType();

    if (outcomeClass != clazz) {
      throw new IllegalStateException(
          String
              .format(
                  "RPC Engine had a submission and response configuration mismatch.  The RPC request that you submitted was defined with an expected response type of %s.  However, "
                      + "when the response returned, a call to getResponseDefaultInstance() with Rpc number %d provided an expected class of %s.  This means either your submission uses the wrong type definition"
                      + "or your getResponseDefaultInstance() method responds the wrong instance type ",
                  clazz.getCanonicalName(), rpcType, outcomeClass.getCanonicalName()));
    }

    @SuppressWarnings("unchecked")
    RpcOutcome<V> crpc = (RpcOutcome<V>) rpc;

    // logger.debug("Returning casted future");
    return crpc;
  }

  public void updateFailedFuture(int coordinationId, DrillPBError failure) {
    // logger.debug("Updating failed future.");
    try {
      RpcOutcome<?> rpc = removeFromMap(coordinationId);
      rpc.setException(new RemoteRpcException(failure));
    } catch(Exception ex) {
      logger.warn("Failed to remove from map.  Not a problem since we were updating on failed future.", ex);
    }
  }

}
