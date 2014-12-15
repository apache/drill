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
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.coord.ClusterCoordinator;
import org.apache.drill.exec.coord.zk.ZKClusterCoordinator;
import org.apache.drill.exec.exception.DrillbitStartupException;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.sys.EStoreProvider;
import org.apache.drill.exec.store.sys.PStore;
import org.apache.drill.exec.store.sys.PStoreConfig;
import org.apache.drill.exec.store.sys.PStoreProvider;
import org.apache.drill.exec.store.sys.PStoreRegistry;
import org.apache.drill.exec.store.sys.local.FilePStore;
import org.apache.hadoop.fs.Path;

import com.google.common.annotations.VisibleForTesting;

public class ZkPStoreProvider implements PStoreProvider {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ZkPStoreProvider.class);

  private static final String DRILL_EXEC_SYS_STORE_PROVIDER_ZK_BLOBROOT = "drill.exec.sys.store.provider.zk.blobroot";

  private final CuratorFramework curator;

  private final DrillFileSystem fs;
  private final Path blobRoot;
  private final EStoreProvider zkEStoreProvider;

  public ZkPStoreProvider(PStoreRegistry registry) throws DrillbitStartupException {
    ClusterCoordinator coord = registry.getClusterCoordinator();
    if (!(coord instanceof ZKClusterCoordinator)) {
      throw new DrillbitStartupException("A ZkPStoreProvider was created without a ZKClusterCoordinator.");
    }
    this.curator = ((ZKClusterCoordinator)registry.getClusterCoordinator()).getCurator();

    if (registry.getConfig().hasPath(DRILL_EXEC_SYS_STORE_PROVIDER_ZK_BLOBROOT)) {
      blobRoot = new Path(registry.getConfig().getString(DRILL_EXEC_SYS_STORE_PROVIDER_ZK_BLOBROOT));
    }else{
      blobRoot = FilePStore.getLogDir();
    }

    try{
      this.fs = FilePStore.getFileSystem(registry.getConfig(), blobRoot);
    }catch(IOException e){
      throw new DrillbitStartupException("Failure while attempting to set up blob store.", e);
    }

    this.zkEStoreProvider = new ZkEStoreProvider(curator);
  }

  @VisibleForTesting
  public ZkPStoreProvider(DrillConfig config, CuratorFramework curator) throws IOException {
    this.curator = curator;
    this.blobRoot = FilePStore.getLogDir();
    this.fs = FilePStore.getFileSystem(config, blobRoot);
    this.zkEStoreProvider = new ZkEStoreProvider(curator);
  }

  @Override
  public <V> PStore<V> getStore(PStoreConfig<V> config) throws IOException {
    switch(config.getMode()){
    case BLOB_PERSISTENT:
      return new FilePStore<V>(fs, blobRoot, config);
    case EPHEMERAL:
      return zkEStoreProvider.getStore(config);
    case PERSISTENT:
      return new ZkPStore<V>(curator, config);
    default:
      throw new IllegalStateException();
    }
  }

  @Override
  public void start() {
  }

  @Override
  public void close() {
  }
}
