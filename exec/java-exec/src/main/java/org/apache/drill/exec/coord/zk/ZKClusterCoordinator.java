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
package org.apache.drill.exec.coord.zk;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.curator.framework.imps.DefaultACLProvider;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.x.discovery.ServiceCache;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.details.ServiceCacheListener;
import org.apache.drill.common.AutoCloseables;
import org.apache.drill.common.DrillNode;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.coord.ClusterCoordinator;
import org.apache.drill.exec.coord.DistributedSemaphore;
import org.apache.drill.exec.coord.DrillServiceInstanceHelper;
import org.apache.drill.exec.coord.store.CachingTransientStoreFactory;
import org.apache.drill.exec.coord.store.TransientStore;
import org.apache.drill.exec.coord.store.TransientStoreConfig;
import org.apache.drill.exec.coord.store.TransientStoreFactory;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint.State;
import org.apache.drill.shaded.guava.com.google.common.base.Throwables;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Manages cluster coordination utilizing zookeeper. *
 */
public class ZKClusterCoordinator extends ClusterCoordinator {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ZKClusterCoordinator.class);

  private CuratorFramework curator;
  private ServiceDiscovery<DrillbitEndpoint> discovery;
  private final String serviceName;
  private final CountDownLatch initialConnection = new CountDownLatch(1);
  private final TransientStoreFactory factory;
  private ServiceCache<DrillbitEndpoint> serviceCache;

  // endpointsMap maps String UUID to Drillbit endpoints
  private  Map<String, DrillbitEndpoint> endpointsMap = new ConcurrentHashMap<>();
  private static final Pattern ZK_COMPLEX_STRING = Pattern.compile("(^.*?)/(.*)/([^/]*)$");

  public ZKClusterCoordinator(DrillConfig config, String connect) {
    this(config, connect, new DefaultACLProvider());
  }

  public ZKClusterCoordinator(DrillConfig config, ACLProvider aclProvider) {
    this(config, null, aclProvider);
  }

  public ZKClusterCoordinator(DrillConfig config, String connect, ACLProvider aclProvider) {

    connect = connect == null || connect.isEmpty() ? config.getString(ExecConstants.ZK_CONNECTION) : connect;
    String clusterId = config.getString(ExecConstants.SERVICE_NAME);
    String zkRoot = config.getString(ExecConstants.ZK_ROOT);

    // check if this is a complex zk string.  If so, parse into components.
    Matcher m = ZK_COMPLEX_STRING.matcher(connect);
    if(m.matches()) {
      connect = m.group(1);
      zkRoot = m.group(2);
      clusterId = m.group(3);
    }

    logger.debug("Connect {}, zkRoot {}, clusterId: " + clusterId, connect, zkRoot);

    this.serviceName = clusterId;

    RetryPolicy rp = new RetryNTimes(config.getInt(ExecConstants.ZK_RETRY_TIMES),
      config.getInt(ExecConstants.ZK_RETRY_DELAY));
    curator = CuratorFrameworkFactory.builder()
      .namespace(zkRoot)
      .connectionTimeoutMs(config.getInt(ExecConstants.ZK_TIMEOUT))
      .retryPolicy(rp)
      .connectString(connect)
      .aclProvider(aclProvider)
      .build();
    curator.getConnectionStateListenable().addListener(new InitialConnectionListener());
    curator.start();
    discovery = newDiscovery();
    factory = CachingTransientStoreFactory.of(new ZkTransientStoreFactory(curator));
  }

  public CuratorFramework getCurator() {
    return curator;
  }

  @Override
  public void start(long millisToWait) throws Exception {
    logger.debug("Starting ZKClusterCoordination.");
    discovery.start();

    if(millisToWait != 0) {
      boolean success = this.initialConnection.await(millisToWait, TimeUnit.MILLISECONDS);
      if (!success) {
        throw new IOException(String.format("Failure to connect to the zookeeper cluster service within the allotted time of %d milliseconds.", millisToWait));
      }
    }else{
      this.initialConnection.await();
    }

    serviceCache = discovery
        .serviceCacheBuilder()
        .name(serviceName)
        .build();
    serviceCache.addListener(new EndpointListener());
    serviceCache.start();
    updateEndpoints();
  }

  private class InitialConnectionListener implements ConnectionStateListener{

    @Override
    public void stateChanged(CuratorFramework client, ConnectionState newState) {
      if(newState == ConnectionState.CONNECTED) {
        ZKClusterCoordinator.this.initialConnection.countDown();
        client.getConnectionStateListenable().removeListener(this);
      }
    }
  }

  private class EndpointListener implements ServiceCacheListener {
    @Override
    public void stateChanged(CuratorFramework client, ConnectionState newState) { }

    @Override
    public void cacheChanged() {
      logger.debug("Got cache changed --> updating endpoints");
      updateEndpoints();
    }
  }

  @Override
  public void close() throws Exception {
    // discovery attempts to close its caches(ie serviceCache) already. however, being good citizens we make sure to
    // explicitly close serviceCache. Not only that we make sure to close serviceCache before discovery to prevent
    // double releasing and disallowing jvm to spit bothering warnings. simply put, we are great!
    AutoCloseables.close(serviceCache, discovery, curator, factory);
  }

  @Override
  public RegistrationHandle register(DrillbitEndpoint data) {
    try {
      data = data.toBuilder().setState(State.ONLINE).build();
      ServiceInstance<DrillbitEndpoint> serviceInstance = newServiceInstance(data);
      discovery.registerService(serviceInstance);
      return new ZKRegistrationHandle(serviceInstance.getId(),data);
    } catch (Exception e) {
      Throwables.throwIfUnchecked(e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void unregister(RegistrationHandle handle) {
    if (!(handle instanceof ZKRegistrationHandle)) {
      throw new UnsupportedOperationException("Unknown handle type: " + handle.getClass().getName());
    }

    // when Drillbit is unregistered, clean all the listeners registered in CC.
    this.listeners.clear();

    ZKRegistrationHandle h = (ZKRegistrationHandle) handle;
    try {
      ServiceInstance<DrillbitEndpoint> serviceInstance = ServiceInstance.<DrillbitEndpoint>builder()
        .address("")
        .port(0)
        .id(h.id)
        .name(serviceName)
        .build();
      discovery.unregisterService(serviceInstance);
    } catch (Exception e) {
      Throwables.throwIfUnchecked(e);
      throw new RuntimeException(e);
    }
  }

  /**
   * Update drillbit endpoint state. Drillbit advertises its
   * state in Zookeeper when a shutdown request of drillbit is
   * triggered. State information is used during planning and
   * initial client connection phases.
   */
  public RegistrationHandle update(RegistrationHandle handle, State state) {
    ZKRegistrationHandle h = (ZKRegistrationHandle) handle;
    final DrillbitEndpoint endpoint;
    try {
        endpoint = h.endpoint.toBuilder().setState(state).build();
        ServiceInstance<DrillbitEndpoint> serviceInstance = ServiceInstance.<DrillbitEndpoint>builder()
                .name(serviceName)
                .id(h.id)
                .payload(endpoint).build();
        discovery.updateService(serviceInstance);
      } catch (Exception e) {
        Throwables.throwIfUnchecked(e);
        throw new RuntimeException(e);
      }
      handle.setEndPoint(endpoint);
      return handle;
  }

  @Override
  public Collection<DrillbitEndpoint> getAvailableEndpoints() {
    return getAvailableEndpointsUUID().values();
  }

  @Override
  public Map<String, DrillbitEndpoint> getAvailableEndpointsUUID() {
    return this.endpointsMap;
  }

  /*
   * Get a collection of ONLINE Drillbit endpoints by excluding the drillbits
   * that are in QUIESCENT state (drillbits shutting down). Primarily used by the planner
   * to plan queries only on ONLINE drillbits and used by the client during initial connection
   * phase to connect to a drillbit (foreman)
   * @return A collection of ONLINE endpoints
   */
  @Override
  public Collection<DrillbitEndpoint> getOnlineEndPoints() {
    return getOnlineEndpointsUUID().keySet();
  }

  @Override
  public Map<DrillbitEndpoint, String> getOnlineEndpointsUUID() {
    Map<DrillbitEndpoint, String> onlineEndpointsUUID = new HashMap<>();
    for (Map.Entry<String, DrillbitEndpoint> endpointEntry : endpointsMap.entrySet()) {
      if (isDrillbitInState(endpointEntry.getValue(), State.ONLINE)) {
        onlineEndpointsUUID.put(endpointEntry.getValue(), endpointEntry.getKey());
      }
    }
    logger.debug("Online endpoints in ZK are" + onlineEndpointsUUID.keySet().toString());
    return onlineEndpointsUUID;
  }

  @Override
  public DistributedSemaphore getSemaphore(String name, int maximumLeases) {
    return new ZkDistributedSemaphore(curator, "/semaphore/" + name, maximumLeases);
  }

  @Override
  public <V> TransientStore<V> getOrCreateTransientStore(final TransientStoreConfig<V> config) {
    final ZkEphemeralStore<V> store = (ZkEphemeralStore<V>)factory.getOrCreateStore(config);
    return store;
  }

  private synchronized void updateEndpoints() {
    try {
      // All active bits in the Zookeeper
      final Map<String, DrillbitEndpoint> UUIDtoEndpoints = discovery.queryForInstances(serviceName).stream()
        .collect(Collectors.toMap(ServiceInstance::getId, ServiceInstance::getPayload));

      final Map<DrillNode, String> activeEndpointsUUID = UUIDtoEndpoints.entrySet().stream()
        .collect(Collectors.toMap(x -> DrillNode.create(x.getValue()), Map.Entry::getKey));

      // set of newly dead bits : original bits - new set of active bits.
      final Map<DrillbitEndpoint, String> unregisteredBits = new HashMap<>();
      // Set of newly live bits : new set of active bits - original bits.
      final Map<DrillbitEndpoint, String> registeredBits = new HashMap<>();


      // Updates the endpoints map if there is a change in state of the endpoint or with the addition
      // of new drillbit endpoints. Registered endpoints is set to newly live drillbit endpoints.
      for (Map.Entry<String, DrillbitEndpoint> endpoint : UUIDtoEndpoints.entrySet()) {
        // check if this bit is newly added bit
        if (!endpointsMap.containsKey(endpoint.getKey())) {
          registeredBits.put(endpoint.getValue(), endpoint.getKey());
        }
        endpointsMap.put(endpoint.getKey(), endpoint.getValue());
      }

      // Remove all the endpoints that are newly dead
      for ( String bitUUID: endpointsMap.keySet()) {
        if (!UUIDtoEndpoints.containsKey(bitUUID)) {
          final DrillbitEndpoint unregisteredBit = endpointsMap.get(bitUUID);
          unregisteredBits.put(unregisteredBit, bitUUID);
          final DrillNode unregisteredNode = DrillNode.create(unregisteredBit);
          if (activeEndpointsUUID.containsKey(unregisteredNode)) {
            logger.info("Drillbit registered again with different UUID. [Details: Address: {}, UserPort: {}," +
              " PreviousUUID: {}, CurrentUUID: {}", unregisteredBit.getAddress(), unregisteredBit.getUserPort(),
              bitUUID, activeEndpointsUUID.get(unregisteredNode));
          }
          endpointsMap.remove(bitUUID);
        }
      }
      if (logger.isDebugEnabled()) {
        StringBuilder builder = new StringBuilder();
        builder.append("Active drillbit set changed.  Now includes ");
        builder.append(UUIDtoEndpoints.size());
        builder.append(" total bits. New active drillbits:\n");
        builder.append("Address | User Port | Control Port | Data Port | Version | State\n");
        for (DrillbitEndpoint bit: UUIDtoEndpoints.values()) {
          builder.append(bit.getAddress()).append(" | ");
          builder.append(bit.getUserPort()).append(" | ");
          builder.append(bit.getControlPort()).append(" | ");
          builder.append(bit.getDataPort()).append(" | ");
          builder.append(bit.getVersion()).append(" |");
          builder.append(bit.getState()).append(" | ");
          builder.append('\n');
        }
        logger.debug(builder.toString());
      }

      // Notify listeners of newly unregistered Drillbits.
      if (!unregisteredBits.isEmpty()) {
        drillbitUnregistered(unregisteredBits);
      }
      // Notify listeners of newly registered Drillbits.
      if (!registeredBits.isEmpty()) {
        drillbitRegistered(registeredBits);
      }
    } catch (Exception e) {
      logger.error("Failure while update Drillbit service location cache.", e);
    }
  }

  protected ServiceInstance<DrillbitEndpoint> newServiceInstance(DrillbitEndpoint endpoint) throws Exception {
    return ServiceInstance.<DrillbitEndpoint>builder()
      .name(serviceName)
      .payload(endpoint)
      .build();
  }


  protected ServiceDiscovery<DrillbitEndpoint> newDiscovery() {
    return ServiceDiscoveryBuilder
      .builder(DrillbitEndpoint.class)
      .basePath("/")
      .client(curator)
      .serializer(DrillServiceInstanceHelper.SERIALIZER)
      .build();
  }
}
