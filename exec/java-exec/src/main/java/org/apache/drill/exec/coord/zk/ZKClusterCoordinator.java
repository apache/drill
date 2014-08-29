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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.coord.ClusterCoordinator;
import org.apache.drill.exec.coord.DistributedSemaphore;
import org.apache.drill.exec.coord.DrillServiceInstanceHelper;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;

import com.google.common.base.Function;

/**
 * Manages cluster coordination utilizing zookeeper. *
 */
public class ZKClusterCoordinator extends ClusterCoordinator {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ZKClusterCoordinator.class);

  private CuratorFramework curator;
  private ServiceDiscovery<DrillbitEndpoint> discovery;
  private ServiceCache<DrillbitEndpoint> serviceCache;
  private volatile Collection<DrillbitEndpoint> endpoints = Collections.emptyList();
  private final String serviceName;
  private final CountDownLatch initialConnection = new CountDownLatch(1);

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
    if(m.matches()){
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
    discovery = getDiscovery();
    serviceCache = discovery.
      serviceCacheBuilder()
      .name(serviceName)
      .build();
  }

  public CuratorFramework getCurator(){
    return curator;
  }

  public void start(long millisToWait) throws Exception {
    logger.debug("Starting ZKClusterCoordination.");
    discovery.start();
    serviceCache.start();
    serviceCache.addListener(new ZKListener());

    if(millisToWait != 0){
      boolean success = this.initialConnection.await(millisToWait, TimeUnit.MILLISECONDS);
      if(!success) throw new IOException(String.format("Failure to connect to the zookeeper cluster service within the allotted time of %d milliseconds.", millisToWait));
    }else{
      this.initialConnection.await();
    }

    updateEndpoints();
  }

  private class InitialConnectionListener implements ConnectionStateListener{

    @Override
    public void stateChanged(CuratorFramework client, ConnectionState newState) {
      if(newState == ConnectionState.CONNECTED){
        ZKClusterCoordinator.this.initialConnection.countDown();
        client.getConnectionStateListenable().removeListener(this);
      }
    }

  }

  private class ZKListener implements ServiceCacheListener {

    @Override
    public void stateChanged(CuratorFramework client, ConnectionState newState) {
    }

    @Override
    public void cacheChanged() {
      logger.debug("Cache changed, updating.");
      updateEndpoints();
    }
  }

  public void close() throws IOException {
    serviceCache.close();
    discovery.close();
    curator.close();
  }

  @Override
  public RegistrationHandle register(DrillbitEndpoint data) {
    try {
      ServiceInstance<DrillbitEndpoint> serviceInstance = getServiceInstance(data);
      discovery.registerService(serviceInstance);
      return new ZKRegistrationHandle(serviceInstance.getId());
    } catch (Exception e) {
      throw propagate(e);
    }
  }

  @Override
  public void unregister(RegistrationHandle handle) {
    if (!(handle instanceof ZKRegistrationHandle)) throw new UnsupportedOperationException("Unknown handle type: " + handle.getClass().getName());

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

  private void updateEndpoints() {
    try {
      endpoints = transform(discovery.queryForInstances(serviceName),
        new Function<ServiceInstance<DrillbitEndpoint>, DrillbitEndpoint>() {
          @Override
          public DrillbitEndpoint apply(ServiceInstance<DrillbitEndpoint> input) {
            return input.getPayload();
          }
        });
    } catch (Exception e) {
      logger.error("Failure while update Drillbit service location cache.", e);
    }
  }

  private ServiceInstance<DrillbitEndpoint> getServiceInstance(DrillbitEndpoint endpoint) throws Exception {
    return ServiceInstance.<DrillbitEndpoint>builder()
      .name(serviceName)
      .payload(endpoint)
      .build();
  }


  public ServiceDiscovery<DrillbitEndpoint> getDiscovery() {
    return ServiceDiscoveryBuilder
      .builder(DrillbitEndpoint.class)
      .basePath("/")
      .client(curator)
      .serializer(DrillServiceInstanceHelper.SERIALIZER)
      .build();
  }
}
