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
package org.apache.drill.exec.store.sys.store.provider;

import java.io.IOException;
import java.nio.file.FileStore;

import com.google.common.annotations.VisibleForTesting;
import org.apache.curator.framework.CuratorFramework;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.coord.zk.ZKClusterCoordinator;
import org.apache.drill.exec.exception.StoreException;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.sys.StoreRegistry;
import org.apache.drill.exec.store.sys.Store;
import org.apache.drill.exec.store.sys.StoreConfig;
import org.apache.drill.exec.store.sys.store.LocalPersistentStore;
import org.apache.drill.exec.store.sys.store.ZookeeperStore;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZookeeperStoreProvider extends BaseStoreProvider {
  private static final Logger logger = LoggerFactory.getLogger(ZookeeperStoreProvider.class);

  private static final String DRILL_EXEC_SYS_STORE_PROVIDER_ZK_BLOBROOT = "drill.exec.sys.store.provider.zk.blobroot";

  private final CuratorFramework curator;
  private final DrillFileSystem fs;
  private final Path blobRoot;

  public ZookeeperStoreProvider(final StoreRegistry<ZKClusterCoordinator> registry) throws StoreException {
    this(registry.getConfig(), registry.getCoordinator().getCurator());
  }

  @VisibleForTesting
  public ZookeeperStoreProvider(final DrillConfig config, final CuratorFramework curator) throws StoreException {
    this.curator = curator;

    if (config.hasPath(DRILL_EXEC_SYS_STORE_PROVIDER_ZK_BLOBROOT)) {
      blobRoot = new Path(config.getString(DRILL_EXEC_SYS_STORE_PROVIDER_ZK_BLOBROOT));
    }else{
      blobRoot = LocalPersistentStore.getLogDir();
    }

    try {
      this.fs = LocalPersistentStore.getFileSystem(config, blobRoot);
    } catch (IOException ex) {
      throw new StoreException("unable to get filesystem", ex);
    }
  }

  @Override
  public <V> Store<V> getStore(final StoreConfig<V> config) throws StoreException {
    switch(config.getMode()){
    case BLOB_PERSISTENT:
      return new LocalPersistentStore<>(fs, blobRoot, config);
    case EPHEMERAL:
    case PERSISTENT:
      return new ZookeeperStore<>(curator, config);
    default:
      throw new IllegalStateException();
    }
  }

}
