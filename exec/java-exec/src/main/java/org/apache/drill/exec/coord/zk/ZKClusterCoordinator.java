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
package org.apache.drill.exec.coord.zk;

import static com.google.common.base.Throwables.propagate;
import static com.google.common.collect.Collections2.transform;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.Lists;
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

import com.google.common.base.Function;

/**
 * Manages cluster coordination utilizing zookeeper. *
 */
public class ZKClusterCoordinator extends ClusterCoordinator {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ZKClusterCoordinator.class);

  private CuratorFramework curator;
  private ServiceDiscovery<DrillbitEndpoint> discovery;
  private volatile Collection<DrillbitEndpoint> endpoints = Collections.emptyList();
  private final String serviceName;
  private final CountDownLatch initialConnection = new CountDownLatch(1);
  private final TransientStoreFactory factory;
  private ServiceCache<DrillbitEndpoint> serviceCache;

  private static final Pattern ZK_COMPLEX_STRING = Pattern.compile("(^.*?)/(.*)/([^/]*)$");

  public ZKClusterCoordinator(DrillConfig config) throws IOException{
    this(config, null);
  }

  public ZKClusterCoordinator(DrillConfig config, String connect) throws IOException {
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

  public void close() throws Exception {
    // discovery attempts to close its caches(ie serviceCache) already. however, being good citizens we make sure to
    // explicitly close serviceCache. Not only that we make sure to close serviceCache before discovery to prevent
    // double releasing and disallowing jvm to spit bothering warnings. simply put, we are great!
    AutoCloseables.close(serviceCache, discovery, curator, factory);
  }

  @Override
  public RegistrationHandle register(DrillbitEndpoint data) {
    try {
      ServiceInstance<DrillbitEndpoint> serviceInstance = newServiceInstance(data);
      discovery.registerService(serviceInstance);
      return new ZKRegistrationHandle(serviceInstance.getId());
    } catch (Exception e) {
      throw propagate(e);
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
      propagate(e);
    }
  }

  @Override
  public Collection<DrillbitEndpoint> getAvailableEndpoints() {
    return this.endpoints;
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
      Collection<DrillbitEndpoint> newDrillbitSet =
      transform(discovery.queryForInstances(serviceName),
        new Function<ServiceInstance<DrillbitEndpoint>, DrillbitEndpoint>() {
          @Override
          public DrillbitEndpoint apply(ServiceInstance<DrillbitEndpoint> input) {
            return input.getPayload();
          }
        });

      // set of newly dead bits : original bits - new set of active bits.
      Set<DrillbitEndpoint> unregisteredBits = new HashSet<>(endpoints);
      unregisteredBits.removeAll(newDrillbitSet);

      endpoints = newDrillbitSet;

      if (logger.isDebugEnabled()) {
        StringBuilder builder = new StringBuilder();
        builder.append("Active drillbit set changed.  Now includes ");
        builder.append(newDrillbitSet.size());
        builder.append(" total bits.  New active drillbits: \n");
        for (DrillbitEndpoint bit: newDrillbitSet) {
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

      // Notify the drillbit listener for newly unregistered bits. For now, we only care when drillbits are down / unregistered.
      if (! (unregisteredBits.isEmpty()) ) {
        drillbitUnregistered(unregisteredBits);
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
