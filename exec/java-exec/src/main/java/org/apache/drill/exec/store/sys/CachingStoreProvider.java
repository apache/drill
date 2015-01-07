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
package org.apache.drill.exec.store.sys;

import java.io.IOException;
import java.util.concurrent.ConcurrentMap;

import org.apache.drill.exec.store.sys.PStoreConfig.Mode;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

public class CachingStoreProvider implements PStoreProvider, AutoCloseable {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CachingStoreProvider.class);

  private final ConcurrentMap<PStoreConfig<?>, PStore<?>> storeCache = Maps.newConcurrentMap();
  private final PStoreProvider provider;

  public CachingStoreProvider(PStoreProvider provider) {
    super();
    this.provider = provider;
  }

  @SuppressWarnings("unchecked")
  public <V> PStore<V> getStore(PStoreConfig<V> config) throws IOException {
    PStore<?> s = storeCache.get(config);
    if(s == null){
      PStore<?> newStore = provider.getStore(config);
      s = storeCache.putIfAbsent(config, newStore);
      if(s == null){
        s = newStore;
      }else{
        newStore.close();
      }
    }

    return (PStore<V>) s;

  }

  @Override
  public void start() throws IOException {
    provider.start();
  }

  @Override
  public void close() throws Exception {
    for(PStore<?> store : storeCache.values()){
      store.close();
    }
    storeCache.clear();
    provider.close();
  }

}
