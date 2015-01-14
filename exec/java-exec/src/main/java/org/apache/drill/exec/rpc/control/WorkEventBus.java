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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.apache.drill.exec.exception.FragmentSetupException;
import org.apache.drill.exec.proto.BitControl.FragmentStatus;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.helper.QueryIdHelper;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.work.WorkManager.WorkerBee;
import org.apache.drill.exec.work.foreman.FragmentStatusListener;
import org.apache.drill.exec.work.foreman.ForemanSetupException;
import org.apache.drill.exec.work.fragment.FragmentManager;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Maps;

public class WorkEventBus {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(WorkEventBus.class);

  private final ConcurrentMap<FragmentHandle, FragmentManager> managers = Maps.newConcurrentMap();
  private final ConcurrentMap<QueryId, FragmentStatusListener> listeners = new ConcurrentHashMap<QueryId, FragmentStatusListener>(
      16, 0.75f, 16);
  private final WorkerBee bee;
  private final Cache<FragmentHandle,Integer> recentlyFinishedFragments = CacheBuilder.newBuilder()
          .maximumSize(10000)

          .expireAfterWrite(10, TimeUnit.MINUTES)
          .build();

  public WorkEventBus(WorkerBee bee) {
    this.bee = bee;
  }

  public void removeFragmentStatusListener(QueryId queryId) {
    logger.debug("Removing fragment status listener for queryId {}.", queryId);
    listeners.remove(queryId);
  }

  public void setFragmentStatusListener(QueryId queryId, FragmentStatusListener listener) throws ForemanSetupException {
    logger.debug("Adding fragment status listener for queryId {}.", queryId);
    FragmentStatusListener old = listeners.putIfAbsent(queryId, listener);
    if (old != null) {
      throw new ForemanSetupException (
          "Failure.  The provided handle already exists in the listener pool.  You need to remove one listener before adding another.");
    }
  }

  public void status(FragmentStatus status) {
    FragmentStatusListener l = listeners.get(status.getHandle().getQueryId());
    if (l == null) {
      logger.warn("A fragment message arrived but there was no registered listener for that message: {}.", status);
    } else {
      l.statusUpdate(status);
    }
  }

  public void setFragmentManager(FragmentManager fragmentManager) {
    logger.debug("Manager created: {}", QueryIdHelper.getQueryIdentifier(fragmentManager.getHandle()));
    FragmentManager old = managers.putIfAbsent(fragmentManager.getHandle(), fragmentManager);
      if (old != null) {
        throw new IllegalStateException(
            "Tried to set fragment manager when has already been set for the provided fragment handle.");
    }
  }

  public FragmentManager getFragmentManagerIfExists(FragmentHandle handle){
    return managers.get(handle);

  }

  public FragmentManager getFragmentManager(FragmentHandle handle) throws FragmentSetupException {
    // check if this was a recently canceled fragment.  If so, throw away message.
    if (recentlyFinishedFragments.asMap().containsKey(handle)) {
      logger.debug("Fragment: {} was cancelled. Ignoring fragment handle", handle);
      return null;
    }

    // since non-leaf fragments are sent first, it is an error condition if the manager is unavailable.
    FragmentManager m = managers.get(handle);
    if(m != null) {
      return m;
    }
    throw new FragmentSetupException("Failed to receive plan fragment that was required for id: " + QueryIdHelper.getQueryIdentifier(handle));
  }

  public void removeFragmentManager(FragmentHandle handle) {
    logger.debug("Removing fragment manager: {}", QueryIdHelper.getQueryIdentifier(handle));
    recentlyFinishedFragments.put(handle,  1);
    managers.remove(handle);
  }

}
