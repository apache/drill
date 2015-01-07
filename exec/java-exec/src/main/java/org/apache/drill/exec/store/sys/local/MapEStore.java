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

package org.apache.drill.exec.store.sys.local;

import org.apache.drill.exec.store.sys.EStore;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Implementation of EStore using ConcurrentHashMap.
 * @param <V>
 */
public class MapEStore<V> implements EStore<V> {
  ConcurrentHashMap<String, V> store = new ConcurrentHashMap<>();

  @Override
  public V get(String key) {
    return store.get(key);
  }

  @Override
  public void put(String key, V value) {
    store.put(key, value);
  }

  @Override
  public void delete(String key) {
    store.remove(key);
  }

  @Override
  public Iterator<Map.Entry<String, V>> iterator() {
    return store.entrySet().iterator();
  }

  @Override
  public boolean putIfAbsent(String key, V value) {
    V out = store.putIfAbsent(key, value);
    return out == null;
  }

  @Override
  public void close() {
  }
}
