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

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;

import org.apache.drill.exec.store.sys.PStore;

import com.google.common.collect.Maps;

public class NoWriteLocalPStore<V> implements PStore<V>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(NoWriteLocalPStore.class);

  private ConcurrentMap<String, V> map = Maps.newConcurrentMap();

  private ConcurrentMap<String, V> blobMap = Maps.newConcurrentMap();

  public NoWriteLocalPStore() {
    super();
  }

  @Override
  public Iterator<Entry<String, V>> iterator() {
    return map.entrySet().iterator();
  }

  @Override
  public V get(String key) {
    return map.get(key);
  }

  @Override
  public void put(String key, V value) {
    map.put(key, value);
  }

  @Override
  public boolean putIfAbsent(String key, V value) {
    return null == map.putIfAbsent(key, value);
  }

  @Override
  public void delete(String key) {
    map.remove(key);
    blobMap.remove(key);
  }

  @Override
  public void close() {
  }

}
