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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.drill.exec.proto.GeneralRPCProtos.RpcFailure;

/**
 * Manages the creation of rpc futures for a particular socket.
 */
public class CoordinationQueue {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CoordinationQueue.class);

  private final PositiveAtomicInteger circularInt = new PositiveAtomicInteger();
  private final Map<Integer, DrillRpcFuture<?>> map;

  public CoordinationQueue(int segmentSize, int segmentCount) {
    map = new ConcurrentHashMap<Integer, DrillRpcFuture<?>>(segmentSize, 0.75f, segmentCount);
  }

  void channelClosed(Exception ex) {
    for (DrillRpcFuture<?> f : map.values()) {
      f.setException(ex);
    }
  }

  public <V> DrillRpcFuture<V> getNewFuture(Class<V> clazz) {
    int i = circularInt.getNext();
    DrillRpcFuture<V> future = DrillRpcFuture.getNewFuture(i, clazz);
    // logger.debug("Writing to map coord {}, future {}", i, future);
    Object old = map.put(i, future);
    if (old != null)
      throw new IllegalStateException(
          "You attempted to reuse a coordination id when the previous coordination id has not been removed.  This is likely rpc future callback memory leak.");
    return future;
  }

  private DrillRpcFuture<?> removeFromMap(int coordinationId) {
    DrillRpcFuture<?> rpc = map.remove(coordinationId);
    if (rpc == null) {
      logger.error("Rpc is null.");
      throw new IllegalStateException(
          "Attempting to retrieve an rpc that wasn't first stored in the rpc coordination queue.  This would most likely happen if you're opposite endpoint sent multiple messages on the same coordination id.");
    }
    return rpc;
  }

  public <V> DrillRpcFuture<V> getFuture(int coordinationId, Class<V> clazz) {
    // logger.debug("Getting future for coordinationId {} and class {}", coordinationId, clazz);
    DrillRpcFuture<?> rpc = removeFromMap(coordinationId);
    // logger.debug("Got rpc from map {}", rpc);
    Class<?> outcomeClass = rpc.getOutcomeClass();
    if (outcomeClass != clazz) {
      logger.error("Rpc class is not expected class. Original: {}, requested: {}", outcomeClass.getCanonicalName(), clazz.getCanonicalName());
      throw new IllegalStateException(
          String
              .format(
                  "You attempted to request a future for a coordination id that has a different value class than was used when you "
                      + "initially created the coordination id.  Requested class %s, originally expected class %s.  This shouldn't happen.  ",
                  clazz.getCanonicalName(), outcomeClass.getCanonicalName()));
    }

    @SuppressWarnings("unchecked")
    DrillRpcFuture<V> crpc = (DrillRpcFuture<V>) rpc;

    // logger.debug("Returning casted future");
    return crpc;
  }

  public void updateFailedFuture(int coordinationId, RpcFailure failure) {
    // logger.debug("Updating failed future.");
    DrillRpcFuture<?> rpc = removeFromMap(coordinationId);
    rpc.setException(new RemoteRpcException(failure));
  }
}
