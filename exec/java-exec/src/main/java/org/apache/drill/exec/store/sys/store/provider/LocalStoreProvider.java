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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.StoreException;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.sys.StoreRegistry;
import org.apache.drill.exec.store.sys.Store;
import org.apache.drill.exec.store.sys.StoreConfig;
import org.apache.drill.exec.store.sys.store.LocalPersistentStore;
import org.apache.drill.exec.store.sys.store.LocalEphemeralStore;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A really simple provider that stores data in the local file system, one value per file.
 */
public class LocalStoreProvider extends BaseStoreProvider {
  private static final Logger logger = LoggerFactory.getLogger(LocalStoreProvider.class);

  private final Path path;
  private final boolean enableWrite;
  private final ConcurrentMap<StoreConfig<?>, Store<?>> pstores;
  private final DrillFileSystem fs;

  public LocalStoreProvider(StoreRegistry registry) throws StoreException {
    this(registry.getConfig());
  }

  public LocalStoreProvider(final DrillConfig config) throws StoreException {
    this.path = new Path(config.getString(ExecConstants.SYS_STORE_PROVIDER_LOCAL_PATH));
    this.enableWrite = config.getBoolean(ExecConstants.SYS_STORE_PROVIDER_LOCAL_ENABLE_WRITE);
    this.pstores = enableWrite ? null : new ConcurrentHashMap<StoreConfig<?>, Store<?>>();
    try {
      this.fs = LocalPersistentStore.getFileSystem(config, path);
    } catch (IOException e) {
      throw new StoreException("unable to get filesystem", e);
    }
  }

  @Override
  public <V> Store<V> getStore(StoreConfig<V> storeConfig) {
    switch(storeConfig.getMode()){
    case EPHEMERAL:
      return new LocalEphemeralStore<>();
    case BLOB_PERSISTENT:
    case PERSISTENT:
      if (enableWrite) {
        return new LocalPersistentStore<>(fs, path, storeConfig);
      } else {
        return new LocalEphemeralStore<>();
      }
    default:
      throw new IllegalStateException();
    }
  }
}
