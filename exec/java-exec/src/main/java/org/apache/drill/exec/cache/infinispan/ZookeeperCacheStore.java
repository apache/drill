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
package org.apache.drill.exec.cache.infinispan;

import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.persistence.spi.ExternalStore;
import org.infinispan.persistence.spi.InitializationContext;

/**
 * Stores the cached objects in zookeeper.  Objects are stored in /start/cache_name/key_name = data
 * @param <K>
 * @param <V>
 */
public class ZookeeperCacheStore<K, V> implements ExternalStore<K, V>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ZookeeperCacheStore.class);

  private String cacheName;

  @Override
  public void init(InitializationContext ctx) {
    ctx.getConfiguration();

  }

  @Override
  public MarshalledEntry<K, V> load(K key) {
    return null;
  }

  @Override
  public boolean contains(K key) {
    return false;
  }

  @Override
  public void start() {
  }

  @Override
  public void stop() {
  }

  @Override
  public void write(MarshalledEntry<K, V> entry) {
  }

  @Override
  public boolean delete(K key) {
    return false;
  }
}
