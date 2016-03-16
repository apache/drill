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

import java.util.List;
import java.util.concurrent.ConcurrentMap;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.drill.common.AutoCloseables;
import org.apache.drill.exec.exception.StoreException;
import org.apache.drill.exec.store.sys.PersistentStore;
import org.apache.drill.exec.store.sys.PersistentStoreConfig;
import org.apache.drill.exec.store.sys.PersistentStoreProvider;

public class CachingPersistentStoreProvider extends BasePersistentStoreProvider {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CachingPersistentStoreProvider.class);

  private final ConcurrentMap<PersistentStoreConfig<?>, PersistentStore<?>> storeCache = Maps.newConcurrentMap();
  private final PersistentStoreProvider provider;

  public CachingPersistentStoreProvider(PersistentStoreProvider provider) {
    this.provider = provider;
  }

  @SuppressWarnings("unchecked")
  public <V> PersistentStore<V> getOrCreateStore(final PersistentStoreConfig<V> config) throws StoreException {
    final PersistentStore<?> store = storeCache.get(config);
    if (store == null) {
      final PersistentStore<?> newStore = provider.getOrCreateStore(config);
      final PersistentStore<?> finalStore = storeCache.putIfAbsent(config, newStore);
      if (finalStore == null) {
        return (PersistentStore<V>)newStore;
      }
      try {
        newStore.close();
      } catch (Exception ex) {
        throw new StoreException(ex);
      }
    }

    return (PersistentStore<V>) store;
  }

  @Override
  public void start() throws Exception {
    provider.start();
  }

  @Override
  public void close() throws Exception {
    final List<AutoCloseable> closeables = Lists.newArrayList();
    for (final AutoCloseable store : storeCache.values()) {
      closeables.add(store);
    }
    closeables.add(provider);
    storeCache.clear();
    AutoCloseables.close(closeables);
  }

}
