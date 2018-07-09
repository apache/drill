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
package org.apache.drill.exec.coord.store;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.drill.common.AutoCloseables;

public class CachingTransientStoreFactory implements TransientStoreFactory {
  private final TransientStoreFactory delegate;
  private final Map<TransientStoreConfig, TransientStore> cache = new HashMap<>();

  public CachingTransientStoreFactory(TransientStoreFactory delegate) {
    this.delegate = Objects.requireNonNull(delegate, "delegate factory is required");
  }

  @Override
  public <V> TransientStore<V> getOrCreateStore(TransientStoreConfig<V> config) {
    TransientStore<V> store = cache.get(Objects.requireNonNull(config, "config is required"));
    if (store != null) {
      return store;
    }

    TransientStore<V> newStore = delegate.getOrCreateStore(config);
    cache.put(config, newStore);
    return newStore;
  }

  @Override
  public void close() throws Exception {
    List<AutoCloseable> closeables = new ArrayList<>(cache.values());
    closeables.add(delegate);
    cache.clear();
    AutoCloseables.close(closeables);
  }

  public static TransientStoreFactory of(TransientStoreFactory delegate) {
    return new CachingTransientStoreFactory(delegate);
  }
}
