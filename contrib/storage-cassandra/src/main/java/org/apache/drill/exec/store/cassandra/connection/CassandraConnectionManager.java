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

package org.apache.drill.exec.store.cassandra.connection;

import com.datastax.driver.core.Cluster;
import org.apache.drill.shaded.guava.com.google.common.cache.Cache;
import org.apache.drill.shaded.guava.com.google.common.cache.CacheBuilder;
import org.apache.drill.shaded.guava.com.google.common.cache.RemovalListener;
import org.apache.drill.shaded.guava.com.google.common.cache.RemovalNotification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class CassandraConnectionManager {

  private static final Logger logger = LoggerFactory.getLogger(CassandraConnectionManager.class);

  private static Cache<String, Cluster> hostConnectionMap;

  static {
    hostConnectionMap = CacheBuilder.newBuilder().maximumSize(5).expireAfterAccess(10, TimeUnit.MINUTES).removalListener(new AddressCloser()).build();
  }

  public synchronized static Cluster getCluster(List<String> hosts, int port) {
    Cluster cluster = hostConnectionMap.getIfPresent(hosts);
    if (cluster == null || cluster.isClosed()) {
      Cluster.Builder builder = Cluster.builder();
      for (String host : hosts) {
        builder = builder.addContactPoints(host);
      }
      builder = builder.withPort(port);
      builder = builder.withoutJMXReporting();
      cluster = builder.build();

      for (String host : hosts) {
        hostConnectionMap.put(host, cluster);
      }

      logger.debug("Created connection to {}.", hosts);
      logger.debug("Number of sessions opened are {}.", hostConnectionMap.size());
    }
    return cluster;
  }

  private static class AddressCloser implements RemovalListener<String, Cluster> {
    @Override
    public synchronized void onRemoval(RemovalNotification<String, Cluster> removal) {
      removal.getValue().close();
      logger.debug("Closed connection to {}.", removal.getKey());
    }
  }
}