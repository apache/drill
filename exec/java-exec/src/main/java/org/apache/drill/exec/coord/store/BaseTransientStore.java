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

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.StreamSupport;

public abstract class BaseTransientStore<V> implements TransientStore<V> {
  private final Set<TransientStoreListener> listeners = Collections.newSetFromMap(new ConcurrentHashMap<>());

  protected final TransientStoreConfig<V> config;

  protected BaseTransientStore(TransientStoreConfig<V> config) {
    this.config = Objects.requireNonNull(config, "config cannot be null");
  }

  public TransientStoreConfig<V> getConfig() {
    return config;
  }

  @Override
  public Iterator<String> keys() {
    Iterable<Map.Entry<String, V>> iterable = this::entries;
    return StreamSupport.stream(iterable.spliterator(), false)
        .map(Map.Entry::getKey)
        .iterator();
  }

  @Override
  public Iterator<V> values() {
    Iterable<Map.Entry<String, V>> iterable = this::entries;
    return StreamSupport.stream(iterable.spliterator(), false)
      .map(Map.Entry::getValue)
      .iterator();
  }

  protected void fireListeners(final TransientStoreEvent event) {
    for (final TransientStoreListener listener:listeners) {
      listener.onChange(event);
    }
  }

  @Override
  public void addListener(TransientStoreListener listener) {
    listeners.add(Objects.requireNonNull(listener, "listener cannot be null"));
  }

  @Override
  public void removeListener(TransientStoreListener listener) {
    listeners.remove(Objects.requireNonNull(listener, "listener cannot be null"));
  }
}
