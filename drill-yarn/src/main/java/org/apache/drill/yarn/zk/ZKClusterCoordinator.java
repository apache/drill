/*
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
package org.apache.drill.yarn.zk;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.x.discovery.ServiceCache;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.details.ServiceCacheListener;
import org.apache.drill.common.AutoCloseables;
import org.apache.drill.exec.coord.ClusterCoordinator;
import org.apache.drill.exec.coord.DistributedSemaphore;
import org.apache.drill.exec.coord.DrillServiceInstanceHelper;
import org.apache.drill.exec.coord.store.CachingTransientStoreFactory;
import org.apache.drill.exec.coord.store.TransientStore;
import org.apache.drill.exec.coord.store.TransientStoreConfig;
import org.apache.drill.exec.coord.store.TransientStoreFactory;
import org.apache.drill.exec.coord.zk.ZKRegistrationHandle;
import org.apache.drill.exec.coord.zk.ZkDistributedSemaphore;
import org.apache.drill.exec.coord.zk.ZkEphemeralStore;
import org.apache.drill.exec.coord.zk.ZkTransientStoreFactory;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint.State;
import org.apache.drill.shaded.guava.com.google.common.base.Throwables;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Manages cluster coordination utilizing zookeeper.
 * <p>
 * This is a clone of the Drill class
 * org.apache.drill.exec.coord.zk.ZKClusterCoordinator with a number of
 * modifications:
 * <ul>
 * <li>Removed dependency on the Drill config system. That system uses Google's
 * Guava library version 18, which conflicts with the earlier versions used by
 * YARN and Hadoop, which resulted in runtime undefined method exceptions.</li>
 * <li>Instead of getting config information out of the Drill config, the
 * parameters are instead passed directly.</li>
 * <li>Adds support for the drillbits registered event which was neither needed
 * nor implemented by Drill.</li>
 * <li>Use the YARN logging system instead of Drill's.</li>
 * </ul>
 * <p>
 * This class should be replaced by the Drill version if/when the Guava
 * conflicts can be resolved (and when registered Drillbit notifications are
 * added to the Drill version.)
 */

public class ZKClusterCoordinator extends ClusterCoordinator {

  protected static final Log logger = LogFactory
      .getLog(ZKClusterCoordinator.class);

  private CuratorFramework curator;
  private ServiceDiscovery<DrillbitEndpoint> discovery;
  private volatile Map<String, DrillbitEndpoint> endpointsMap = Collections.emptyMap();
  private final String serviceName;
  private final CountDownLatch initialConnection = new CountDownLatch(1);
  private final TransientStoreFactory factory;
  private ServiceCache<DrillbitEndpoint> serviceCache;

  public ZKClusterCoordinator(String connect, String zkRoot, String clusterId,
      int retryCount, int retryDelayMs, int connectTimeoutMs)
      throws IOException {
    logger.debug("ZK connect: " + connect + ", zkRoot: " + zkRoot
        + ", clusterId: " + clusterId);

    this.serviceName = clusterId;
    RetryPolicy rp = new RetryNTimes(retryCount, retryDelayMs);
    curator = CuratorFrameworkFactory.builder().namespace(zkRoot)
        .connectionTimeoutMs(connectTimeoutMs).retryPolicy(rp)
        .connectString(connect).build();
    curator.getConnectionStateListenable()
        .addListener(new InitialConnectionListener());
    curator.start();
    discovery = newDiscovery();
    factory = CachingTransientStoreFactory
        .of(new ZkTransientStoreFactory(curator));
  }

  public CuratorFramework getCurator() {
    return curator;
  }

  @Override
  public void start(long millisToWait) throws Exception {
    logger.debug("Starting ZKClusterCoordination.");
    discovery.start();

    if (millisToWait != 0) {
      boolean success = this.initialConnection.await(millisToWait,
          TimeUnit.MILLISECONDS);
      if (!success) {
        throw new IOException(String.format(
            "Failure to connect to the zookeeper cluster service within the allotted time of %d milliseconds.",
            millisToWait));
      }
    } else {
      this.initialConnection.await();
    }

    serviceCache = discovery.serviceCacheBuilder().name(serviceName).build();
    serviceCache.addListener(new EndpointListener());
    serviceCache.start();
    updateEndpoints();
  }

  private class InitialConnectionListener implements ConnectionStateListener {

    @Override
    public void stateChanged(CuratorFramework client,
        ConnectionState newState) {
      if (newState == ConnectionState.CONNECTED) {
        initialConnection.countDown();
        client.getConnectionStateListenable().removeListener(this);
      }
    }

  }

  private class EndpointListener implements ServiceCacheListener {
    @Override
    public void stateChanged(CuratorFramework client,
        ConnectionState newState) {
    }

    @Override
    public void cacheChanged() {
      logger.debug("Got cache changed --> updating endpoints");
      updateEndpoints();
    }
  }

  @Override
  public void close() throws Exception {
    // discovery attempts to close its caches(ie serviceCache) already. however,
    // being good citizens we make sure to
    // explicitly close serviceCache. Not only that we make sure to close
    // serviceCache before discovery to prevent
    // double releasing and disallowing jvm to spit bothering warnings. simply
    // put, we are great!
    AutoCloseables.close(serviceCache, discovery, curator, factory);
  }

  @Override
  public RegistrationHandle register(DrillbitEndpoint data) {
    try {
      ServiceInstance<DrillbitEndpoint> serviceInstance = newServiceInstance(
          data);
      discovery.registerService(serviceInstance);
      return new ZKRegistrationHandle(serviceInstance.getId(), data);
    } catch (Exception e) {
      Throwables.throwIfUnchecked(e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void unregister(RegistrationHandle handle) {
    if (!(handle instanceof ZKRegistrationHandle)) {
      throw new UnsupportedOperationException(
          "Unknown handle type: " + handle.getClass().getName());
    }

    // when Drillbit is unregistered, clean all the listeners registered in CC.
    this.listeners.clear();

    ZKRegistrationHandle h = (ZKRegistrationHandle) handle;
    try {
      ServiceInstance<DrillbitEndpoint> serviceInstance = ServiceInstance
          .<DrillbitEndpoint> builder().address("").port(0).id(h.id)
          .name(serviceName).build();
      discovery.unregisterService(serviceInstance);
    } catch (Exception e) {
      Throwables.throwIfUnchecked(e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public Collection<DrillbitEndpoint> getAvailableEndpoints() {
    return this.endpointsMap.values();
  }

  @Override
  public DistributedSemaphore getSemaphore(String name, int maximumLeases) {
    return new ZkDistributedSemaphore(curator, "/semaphore/" + name,
        maximumLeases);
  }

  @Override
  public <V> TransientStore<V> getOrCreateTransientStore(
      final TransientStoreConfig<V> config) {
    final ZkEphemeralStore<V> store = (ZkEphemeralStore<V>) factory
        .getOrCreateStore(config);
    return store;
  }

  private synchronized void updateEndpoints() {
    try {
      // All active bits in the Zookeeper
      final Map<String, DrillbitEndpoint> UUIDtoEndpoints = discovery.queryForInstances(serviceName).stream()
        .collect(Collectors.toConcurrentMap(ServiceInstance::getId, ServiceInstance::getPayload));

      // set of newly dead bits : original bits - new set of active bits.
      Map<String, DrillbitEndpoint> unregisteredBits = new HashMap<>(endpointsMap);
      for (Map.Entry<String, DrillbitEndpoint> newEndpoint : UUIDtoEndpoints.entrySet()) {
        unregisteredBits.remove(newEndpoint.getKey());
      }

      // Set of newly live bits : new set of active bits - original bits.
      Map<String, DrillbitEndpoint> registeredBits = new HashMap<>(UUIDtoEndpoints);
      for (Map.Entry<String, DrillbitEndpoint> newEndpoint : endpointsMap.entrySet()) {
        registeredBits.remove(newEndpoint.getKey());
      }

      endpointsMap = UUIDtoEndpoints;

      if (logger.isDebugEnabled()) {
        StringBuilder builder = new StringBuilder();
        builder.append("Active drillbit set changed.  Now includes ");
        builder.append(UUIDtoEndpoints.size());
        builder.append(" total bits.");
        if (!UUIDtoEndpoints.isEmpty()) {
          builder.append(" New active drillbits: \n");
        }
        for (DrillbitEndpoint bit : UUIDtoEndpoints.values()) {
          builder.append('\t');
          builder.append(bit.getAddress());
          builder.append(':');
          builder.append(bit.getUserPort());
          builder.append(':');
          builder.append(bit.getControlPort());
          builder.append(':');
          builder.append(bit.getDataPort());
          builder.append('\n');
        }
        logger.debug(builder.toString());
      }

      // Notify the drillbit listener for newly unregistered bits.
      if (!(unregisteredBits.isEmpty())) {
        drillbitUnregistered(unregisteredBits.entrySet().stream().collect(
          Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey)));
      }
      // Notify the drillbit listener for newly registered bits.
      if (!(registeredBits.isEmpty())) {
        drillbitRegistered(registeredBits.entrySet().stream().collect(
          Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey)));
      }

    } catch (Exception e) {
      logger.error("Failure while update Drillbit service location cache.", e);
    }
  }

  protected ServiceInstance<DrillbitEndpoint> newServiceInstance(
      DrillbitEndpoint endpoint) throws Exception {
    return ServiceInstance.<DrillbitEndpoint> builder().name(serviceName)
        .payload(endpoint).build();
  }

  protected ServiceDiscovery<DrillbitEndpoint> newDiscovery() {
    return ServiceDiscoveryBuilder.builder(DrillbitEndpoint.class).basePath("/")
        .client(curator).serializer(DrillServiceInstanceHelper.SERIALIZER)
        .build();
  }

  @Override
  public Collection<DrillbitEndpoint> getOnlineEndPoints() {

    // Not used in DoY

    throw new UnsupportedOperationException();
  }

  @Override
  public RegistrationHandle update(RegistrationHandle handle, State state) {

    // Not used in DoY

    throw new UnsupportedOperationException();
  }

}
