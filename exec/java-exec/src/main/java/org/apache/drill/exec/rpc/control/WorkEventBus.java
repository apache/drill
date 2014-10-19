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
import org.apache.drill.exec.proto.helper.QueryIdHelper;
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

  public void setFragmentManager(FragmentManager fragmentManager) {
    logger.debug("Manager created: {}", QueryIdHelper.getQueryIdentifier(fragmentManager.getHandle()));

    synchronized (managers) {
      FragmentManager old = managers.putIfAbsent(fragmentManager.getHandle(), fragmentManager);
      managers.notifyAll();
      if (old != null) {
        throw new IllegalStateException(
            "Tried to set fragment manager when has already been set for the provided fragment handle.");
      }
    }
  }

  public FragmentManager getFragmentManagerIfExists(FragmentHandle handle){
    return managers.get(handle);

  }

  public FragmentManager getFragmentManager(FragmentHandle handle) throws FragmentSetupException {

    // check if this was a recently canceled fragment.  If so, throw away message.
    if (cancelledFragments.asMap().containsKey(handle)) {
      logger.debug("Fragment: {} was cancelled. Ignoring fragment handle", handle);
      return null;
    }

    // chm manages concurrency better then everyone fighting for the same lock so we'll do a double check.
    FragmentManager m = managers.get(handle);
    if(m != null){
      return m;
    }

    logger.debug("Fragment was requested but no manager exists.  Waiting for manager for fragment: {}", QueryIdHelper.getQueryIdentifier(handle));
    try{
    // We need to handle the race condition between the fragments being sent to leaf nodes and intermediate nodes.  It is possible that a leaf node would send a data batch to a intermediate node before the intermediate node received the associated plan.  As such, we will wait here for a bit to see if the appropriate fragment shows up.
    long expire = System.currentTimeMillis() + 30*1000;
    synchronized(managers){

      // we loop because we may be woken up by some other, unrelated manager insertion.
      while(true){
        m = managers.get(handle);
        if(m != null) {
          return m;
        }
        long timeToWait = expire - System.currentTimeMillis();
        if(timeToWait <= 0){
          break;
        }

        managers.wait(timeToWait);
      }

      throw new FragmentSetupException("Failed to receive plan fragment that was required for id: " + QueryIdHelper.getQueryIdentifier(handle));
    }
    }catch(InterruptedException e){
      throw new FragmentSetupException("Interrupted while waiting to receive plan fragment..");
    }
  }

  public void cancelFragment(FragmentHandle handle) {
    logger.debug("Fragment canceled: {}", QueryIdHelper.getQueryIdentifier(handle));
    cancelledFragments.put(handle, null);
    removeFragmentManager(handle);
  }

  public void removeFragmentManager(FragmentHandle handle) {
    logger.debug("Removing fragment manager: {}", QueryIdHelper.getQueryIdentifier(handle));
    managers.remove(handle);
  }

}
