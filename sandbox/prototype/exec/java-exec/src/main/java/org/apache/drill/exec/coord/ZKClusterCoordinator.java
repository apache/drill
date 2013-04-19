/*******************************************************************************
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
 ******************************************************************************/
package org.apache.drill.exec.coord;

import com.google.common.base.Function;
import com.netflix.curator.RetryPolicy;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.state.ConnectionState;
import com.netflix.curator.retry.RetryNTimes;
import com.netflix.curator.x.discovery.ServiceDiscovery;
import com.netflix.curator.x.discovery.ServiceDiscoveryBuilder;
import com.netflix.curator.x.discovery.ServiceInstance;
import com.netflix.curator.x.discovery.details.ServiceCache;
import com.netflix.curator.x.discovery.details.ServiceCacheListener;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import static com.google.common.base.Throwables.propagate;
import static com.google.common.collect.Collections2.transform;

/**
 * Manages cluster coordination utilizing zookeeper. *
 */
public class ZKClusterCoordinator extends ClusterCoordinator {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ZKClusterCoordinator.class);

  private String basePath;
  private CuratorFramework curator;
  private ServiceDiscovery<DrillbitEndpoint> discovery;
  private ServiceCache<DrillbitEndpoint> serviceCache;
  private volatile Collection<DrillbitEndpoint> endpoints = Collections.emptyList();
  private final String serviceName;

  public ZKClusterCoordinator(DrillConfig config) throws IOException {

    this.basePath = config.getString(ExecConstants.ZK_ROOT);
    this.serviceName = config.getString(ExecConstants.SERVICE_NAME);
    RetryPolicy rp = new RetryNTimes(config.getInt(ExecConstants.ZK_RETRY_TIMES),
      config.getInt(ExecConstants.ZK_RETRY_DELAY));
    curator = CuratorFrameworkFactory.builder()
      .connectionTimeoutMs(config.getInt(ExecConstants.ZK_TIMEOUT))
      .retryPolicy(rp)
      .connectString(config.getString(ExecConstants.ZK_CONNECTION))
      .build();
    discovery = getDiscovery();
    serviceCache = discovery.
      serviceCacheBuilder()
      .name(serviceName)
      .refreshPaddingMs(config.getInt(ExecConstants.ZK_REFRESH))
      .build();
  }

  public void start() throws Exception {
    logger.debug("Starting ZKClusterCoordination.");
    curator.start();
    discovery.start();
    serviceCache.start();
    serviceCache.addListener(new ZKListener());
    updateEndpoints();
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
    if (!(handle instanceof ZKRegistrationHandle)) throw new UnsupportedOperationException("Unknown handle type");

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
      .basePath(basePath)
      .client(curator)
      .serializer(DrillServiceInstanceHelper.SERIALIZER)
      .build();
  }
}
