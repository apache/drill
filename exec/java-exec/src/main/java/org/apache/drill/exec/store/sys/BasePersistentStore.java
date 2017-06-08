/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.sys;

import org.apache.drill.exec.store.sys.store.DataChangeVersion;

import java.util.Iterator;
import java.util.Map;

public abstract class BasePersistentStore<V> implements PersistentStore<V> {

  @Override
  public Iterator<Map.Entry<String, V>> getAll() {
    return getRange(0, Integer.MAX_VALUE);
  }

  /** By default contains with version will behave the same way as without version.
   * Override this method to add version support. */
  public boolean contains(String key, DataChangeVersion version) {
    return contains(key);
  }

  /** By default get with version will behave the same way as without version.
   * Override this method to add version support. */
  @Override
  public V get(String key, DataChangeVersion version) {
    return get(key);
  }

  /** By default put with version will behave the same way as without version.
   * Override this method to add version support. */
  @Override
  public void put(String key, V value, DataChangeVersion version) {
    put(key, value);
  }

}
