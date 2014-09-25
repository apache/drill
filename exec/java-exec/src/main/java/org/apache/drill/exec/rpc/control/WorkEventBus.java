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
package org.apache.drill.exec.rpc.control;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.apache.drill.exec.cache.DistributedMap;
import org.apache.drill.exec.exception.FragmentSetupException;
import org.apache.drill.exec.proto.BitControl.FragmentStatus;
import org.apache.drill.exec.proto.BitControl.PlanFragment;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.work.WorkManager.WorkerBee;
import org.apache.drill.exec.work.foreman.Foreman;
import org.apache.drill.exec.work.foreman.FragmentStatusListener;
import org.apache.drill.exec.work.fragment.FragmentManager;
import org.apache.drill.exec.work.fragment.NonRootFragmentManager;
import org.apache.drill.exec.work.fragment.RootFragmentManager;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Maps;

public class WorkEventBus {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(WorkEventBus.class);

  private final ConcurrentMap<FragmentHandle, FragmentManager> managers = Maps.newConcurrentMap();
  private final ConcurrentMap<QueryId, FragmentStatusListener> listeners = new ConcurrentHashMap<QueryId, FragmentStatusListener>(
      16, 0.75f, 16);
  private final WorkerBee bee;
  private final Cache<FragmentHandle,Void> cancelledFragments = CacheBuilder.newBuilder()
          .maximumSize(10000)
          .expireAfterWrite(10, TimeUnit.MINUTES)
          .build();

  public WorkEventBus(WorkerBee bee) {
    this.bee = bee;
  }

  public void removeFragmentStatusListener(QueryId queryId) {
    logger.debug("Removing framgent status listener for queryId {}.", queryId);
    listeners.remove(queryId);
  }

  public void setFragmentStatusListener(QueryId queryId, FragmentStatusListener listener) throws RpcException {
    logger.debug("Adding fragment status listener for queryId {}.", queryId);
    FragmentStatusListener old = listeners.putIfAbsent(queryId, listener);
    if (old != null) {
      throw new RpcException(
          "Failure.  The provided handle already exists in the listener pool.  You need to remove one listener before adding another.");
    }
  }

  public void status(FragmentStatus status) {
    FragmentStatusListener l = listeners.get(status.getHandle().getQueryId());
    if (l == null) {

      logger.error("A fragment message arrived but there was no registered listener for that message for handle {}.",
          status.getHandle());
      return;
    } else {
      l.statusUpdate(status);
    }
  }

  public void setRootFragmentManager(RootFragmentManager fragmentManager) {
    FragmentManager old = managers.putIfAbsent(fragmentManager.getHandle(), fragmentManager);
    if (old != null) {
      throw new IllegalStateException(
          "Tried to set fragment manager when has already been set for the provided fragment handle.");
    }
  }

  public FragmentManager getFragmentManager(FragmentHandle handle) {
    return managers.get(handle);
  }

  public void cancelFragment(FragmentHandle handle) {
    cancelledFragments.put(handle, null);
    removeFragmentManager(handle);
  }

  public FragmentManager getOrCreateFragmentManager(FragmentHandle handle) throws FragmentSetupException{
    if (cancelledFragments.asMap().containsKey(handle)) {
      logger.debug("Fragment: {} was cancelled. Ignoring fragment handle", handle);
      return null;
    }
    // We need to synchronize this part. Without that, multiple bit servers will be creating a Fragment manager and the
    // corresponding FragmentContext object. Each FragmentContext object registers with the TopLevelAllocator so that
    // the allocator can manage fragment resources across all fragments. So we need to make sure only one
    // FragmentManager is actually created and used for a given FragmentHandle.
    FragmentManager newManager;
    FragmentManager manager;

    manager = managers.get(handle);
    if (manager != null) {
      return manager;
    }
    if (logger.isDebugEnabled()) {
      String fragHandles = "Looking for Fragment handle: " + handle.toString() + "(Hash Code:" + handle.hashCode()
        + ")\n Fragment Handles in Fragment manager: ";
      for (FragmentHandle h : managers.keySet()) {
        fragHandles += h.toString() + "\n";
        fragHandles += "[Hash Code: " + h.hashCode() + "]\n";
      }
      logger.debug(fragHandles);
    }
    DistributedMap<FragmentHandle, PlanFragment> planCache = bee.getContext().getCache().getMap(Foreman.FRAGMENT_CACHE);
//      for (Map.Entry<FragmentHandle, PlanFragment> e : planCache.getLocalEntries()) {
//      logger.debug("Key: {}", e.getKey());
//      logger.debug("Value: {}", e.getValue());
//      }
    PlanFragment fragment = bee.getContext().getCache().getMap(Foreman.FRAGMENT_CACHE).get(handle);

    if (fragment == null) {
      throw new FragmentSetupException("Received batch where fragment was not in cache.");
    }
    logger.debug("Allocating new non root fragment manager: " + handle.toString());
    newManager = new NonRootFragmentManager(fragment, bee);
    logger.debug("Allocated new non root fragment manager: " + handle.toString());

    manager = managers.putIfAbsent(fragment.getHandle(), newManager);
    if (manager == null) {
      // we added a handler, inform the bee that we did so. This way, the foreman can track status.
      bee.addFragmentPendingRemote(newManager);
      manager = newManager;
    }else{
      // prevent a leak of the initial allocation.
      // Also the fragment context is registered with the top level allocator.
      // This will unregister the unused fragment context as well.
      newManager.getFragmentContext().close();
    }

    return manager;
  }

  public void removeFragmentManager(FragmentHandle handle) {
    managers.remove(handle);
  }

}
