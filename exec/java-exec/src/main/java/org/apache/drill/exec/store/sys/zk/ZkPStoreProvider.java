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
package org.apache.drill.exec.store.sys.zk;

import java.io.IOException;

import org.apache.curator.framework.CuratorFramework;
import org.apache.drill.exec.coord.ClusterCoordinator;
import org.apache.drill.exec.coord.zk.ZKClusterCoordinator;
import org.apache.drill.exec.exception.DrillbitStartupException;
import org.apache.drill.exec.store.sys.PStore;
import org.apache.drill.exec.store.sys.PStoreConfig;
import org.apache.drill.exec.store.sys.PStoreProvider;
import org.apache.drill.exec.store.sys.PStoreRegistry;

public class ZkPStoreProvider implements PStoreProvider{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ZkPStoreProvider.class);

  private final CuratorFramework curator;

  public ZkPStoreProvider(PStoreRegistry registry) throws DrillbitStartupException {
    ClusterCoordinator coord = registry.getClusterCoordinator();
    if (!(coord instanceof ZKClusterCoordinator)) {
      throw new DrillbitStartupException("A ZkPStoreProvider was created without a ZKClusterCoordinator.");
    }
    this.curator = ((ZKClusterCoordinator)registry.getClusterCoordinator()).getCurator();
  }

  public ZkPStoreProvider(CuratorFramework curator) {
    this.curator = curator;
  }

  @Override
  public void close() {
  }

  @Override
  public <V> PStore<V> getPStore(PStoreConfig<V> store) throws IOException {
    return new ZkPStore<V>(curator, store);
  }

  @Override
  public void start() {
  }

}
