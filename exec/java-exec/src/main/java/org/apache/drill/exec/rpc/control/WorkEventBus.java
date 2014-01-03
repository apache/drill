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

import org.apache.drill.exec.exception.FragmentSetupException;
import org.apache.drill.exec.proto.BitControl.FragmentStatus;
import org.apache.drill.exec.proto.BitControl.PlanFragment;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.work.WorkManager.WorkerBee;
import org.apache.drill.exec.work.foreman.FragmentStatusListener;
import org.apache.drill.exec.work.fragment.FragmentManager;
import org.apache.drill.exec.work.fragment.NonRootFragmentManager;
import org.apache.drill.exec.work.fragment.RootFragmentManager;

import com.google.common.collect.Maps;

public class WorkEventBus {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(WorkEventBus.class);

  private final ConcurrentMap<FragmentHandle, FragmentManager> managers = Maps.newConcurrentMap();
  private final ConcurrentMap<QueryId, FragmentStatusListener> listeners = new ConcurrentHashMap<QueryId, FragmentStatusListener>(
      16, 0.75f, 16);
  private final WorkerBee bee;

  public WorkEventBus(WorkerBee bee) {
    this.bee = bee;
  }

  public void removeFragmentStatusListener(FragmentHandle handle) throws RpcException {
    logger.debug("Removing framgent status listener for handle {}.", handle);
    listeners.remove(handle);
  }

  public void setFragmentStatusListener(QueryId queryId, FragmentStatusListener listener) throws RpcException {
    logger.debug("Adding fragment status listener for queryId {}.", queryId);
    FragmentStatusListener old = listeners.putIfAbsent(queryId, listener);
    if (old != null)
      throw new RpcException(
          "Failure.  The provided handle already exists in the listener pool.  You need to remove one listener before adding another.");
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
    if (old != null)
      throw new IllegalStateException(
          "Tried to set fragment manager when has already been set for the provided fragment handle.");
  }

  public FragmentManager getFragmentManager(FragmentHandle handle){
    return managers.get(handle);
  }
  
  public FragmentManager getOrCreateFragmentManager(FragmentHandle handle) throws FragmentSetupException{
    FragmentManager manager = managers.get(handle);
    if (manager != null) return manager;

    PlanFragment fragment = bee.getContext().getCache().getFragment(handle);

    if (fragment == null) {
      throw new FragmentSetupException("Received batch where fragment was not in cache.");
    }

    FragmentManager newManager = new NonRootFragmentManager(fragment, bee.getContext());

    // since their could be a race condition on the check, we'll use putIfAbsent so we don't have two competing
    // handlers.
    manager = managers.putIfAbsent(fragment.getHandle(), newManager);

    if (manager == null) {
      // we added a handler, inform the bee that we did so. This way, the foreman can track status.
      bee.addFragmentPendingRemote(newManager);
      manager = newManager;
    }

    return manager;
  }
  
  public void removeFragmentManager(FragmentHandle handle){
    managers.remove(handle);
  }
}
