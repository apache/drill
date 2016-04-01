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
package org.apache.drill.exec.coord;

import java.io.Closeable;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.drill.exec.coord.store.TransientStore;
import org.apache.drill.exec.coord.store.TransientStoreConfig;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.work.foreman.DrillbitStatusListener;

/**
 * Pluggable interface built to manage cluster coordination. Allows Drillbit or DrillClient to register its capabilities
 * as well as understand other node's existence and capabilities.
 **/
public abstract class ClusterCoordinator implements AutoCloseable {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ClusterCoordinator.class);

  protected ConcurrentHashMap<DrillbitStatusListener, DrillbitStatusListener> listeners = new ConcurrentHashMap<>(
      16, 0.75f, 16);

  /**
   * Start the cluster coordinator.  Millis to wait is
   * @param millisToWait The maximum time to wait before throwing an exception if the cluster coordination service has not successfully started.  Use 0 to wait indefinitely.
   * @throws Exception
   */
  public abstract void start(long millisToWait) throws Exception;

  public abstract RegistrationHandle register(DrillbitEndpoint data);

  public abstract void unregister(RegistrationHandle handle);

  /**
   * Get a collection of avialable Drillbit endpoints, Thread-safe.
   * Could be slightly out of date depending on refresh policy.
   *
   * @return A collection of available endpoints.
   */
  public abstract Collection<DrillbitEndpoint> getAvailableEndpoints();

  public interface RegistrationHandle {
  }

  public abstract DistributedSemaphore getSemaphore(String name, int maximumLeases);

  /**
   * Returns a {@link TransientStore store} instance with the given {@link TransientStoreConfig configuration}.
   *
   * Note that implementor might cache the instance so new instance creation is not guaranteed.
   *
   * @param config  store configuration
   * @param <V>  value type for this store
   */
  public abstract <V> TransientStore<V> getOrCreateTransientStore(TransientStoreConfig<V> config);

  /**
   * Actions to take when there are a set of new de-active drillbits.
   * @param unregisteredBits
   */
  protected void drillbitUnregistered(Set<DrillbitEndpoint> unregisteredBits) {
    for (DrillbitStatusListener listener : listeners.keySet()) {
      listener.drillbitUnregistered(unregisteredBits);
    }
  }

  protected void drillbitRegistered(Set<DrillbitEndpoint> registeredBits) {
    for (DrillbitStatusListener listener : listeners.keySet()) {
      listener.drillbitRegistered(registeredBits);
    }
  }

  /**
   * Register a DrillbitStatusListener.
   * Note : the listeners are not guaranteed to be called in the order in which they call this method, since all the listeners are in a ConcurrentHashMap.
   * @param listener
   */
  public void addDrillbitStatusListener(DrillbitStatusListener listener) {
    listeners.putIfAbsent(listener, listener);
  }

  /**
   * Unregister a DrillbitStatusListener.
   * @param listener
   */
  public void removeDrillbitStatusListener(DrillbitStatusListener listener) {
    listeners.remove(listener);
  }

}
