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
import org.apache.drill.exec.work.foreman.FragmentStatusListener;
import org.apache.drill.exec.work.foreman.ForemanSetupException;
import org.apache.drill.exec.work.fragment.FragmentManager;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Maps;

public class WorkEventBus {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(WorkEventBus.class);
  private final ConcurrentMap<FragmentHandle, FragmentManager> managers = Maps.newConcurrentMap();
  private final ConcurrentMap<QueryId, FragmentStatusListener> listeners =
      new ConcurrentHashMap<>(16, 0.75f, 16);
  private final Cache<FragmentHandle, Integer> recentlyFinishedFragments = CacheBuilder.newBuilder()
          .maximumSize(10000)
          .expireAfterWrite(10, TimeUnit.MINUTES)
          .build();

  public void removeFragmentStatusListener(final QueryId queryId) {
    if (logger.isDebugEnabled()) {
      logger.debug("Removing fragment status listener for queryId {}.", QueryIdHelper.getQueryId(queryId));
    }
    listeners.remove(queryId);
  }

  public void addFragmentStatusListener(final QueryId queryId, final FragmentStatusListener listener)
      throws ForemanSetupException {
    if (logger.isDebugEnabled()) {
      logger.debug("Adding fragment status listener for queryId {}.", QueryIdHelper.getQueryId(queryId));
    }
    final FragmentStatusListener old = listeners.putIfAbsent(queryId, listener);
    if (old != null) {
      throw new ForemanSetupException (
          "Failure.  The provided handle already exists in the listener pool.  You need to remove one listener before adding another.");
    }
  }

  public void statusUpdate(final FragmentStatus status) {
    final FragmentStatusListener listener = listeners.get(status.getHandle().getQueryId());
    if (listener == null) {
      logger.warn("A fragment message arrived but there was no registered listener for that message: {}.", status);
    } else {
      listener.statusUpdate(status);
    }
  }

  public void addFragmentManager(final FragmentManager fragmentManager) {
    if (logger.isDebugEnabled()) {
      logger.debug("Manager created: {}", QueryIdHelper.getQueryIdentifier(fragmentManager.getHandle()));
    }
    final FragmentManager old = managers.putIfAbsent(fragmentManager.getHandle(), fragmentManager);
      if (old != null) {
        throw new IllegalStateException(
            "Tried to set fragment manager when has already been set for the provided fragment handle.");
    }
  }

  public FragmentManager getFragmentManagerIfExists(final FragmentHandle handle) {
    synchronized (this) {
      return managers.get(handle);
    }
  }

  public FragmentManager getFragmentManager(final FragmentHandle handle) throws FragmentSetupException {
    synchronized (this) {
      // Check if this was a recently finished (completed or cancelled) fragment.  If so, throw away message.
      if (recentlyFinishedFragments.asMap().containsKey(handle)) {
        if (logger.isDebugEnabled()) {
          logger.debug("Fragment: {} was cancelled. Ignoring fragment handle", handle);
        }
        return null;
      }

      // since non-leaf fragments are sent first, it is an error condition if the manager is unavailable.
      final FragmentManager m = managers.get(handle);
      if (m != null) {
        return m;
      }
    }
    throw new FragmentSetupException("Failed to receive plan fragment that was required for id: "
        + QueryIdHelper.getQueryIdentifier(handle));
  }

  /**
   * Removes fragment manager (for the corresponding the handle) from the work event bus. This method can be called
   * multiple times. The manager will be removed only once (the first call).
   * @param handle the handle to the fragment
   */
  public void removeFragmentManager(final FragmentHandle handle) {
    if (logger.isDebugEnabled()) {
      logger.debug("Removing fragment manager: {}", QueryIdHelper.getQueryIdentifier(handle));
    }

    synchronized (this) {
      final FragmentManager manager = managers.get(handle);
      if (manager != null) {
        recentlyFinishedFragments.put(handle, 1);
        managers.remove(handle);
      } else {
        logger.warn("Fragment {} not found in the work bus.", QueryIdHelper.getQueryIdentifier(handle));
      }
    }
  }

  /**
   * Cancels and removes fragment manager (for the corresponding the handle) from the work event bus, Currently, used
   * for fragments waiting on data (root and intermediate).
   * @param handle the handle to the fragment
   * @return if the fragment was found and removed from the event bus
   */
  public boolean cancelAndRemoveFragmentManagerIfExists(final FragmentHandle handle) {
    if (logger.isDebugEnabled()) {
      logger.debug("Cancelling and removing fragment manager: {}", QueryIdHelper.getQueryIdentifier(handle));
    }

    synchronized (this) {
      final FragmentManager manager = managers.get(handle);
      if (manager == null) {
        return false;
      }

      manager.cancel();
      recentlyFinishedFragments.put(handle, 1);
      managers.remove(handle);
      return true;
    }
  }
}
