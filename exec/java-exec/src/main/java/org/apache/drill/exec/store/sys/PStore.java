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

import java.util.Map;


/**
 * Interface for reading and writing values to a persistent storage provider.  Iterators are guaranteed to be returned in key order.
 * @param <V>
 */
public interface PStore<V> extends Iterable<Map.Entry<String, V>> {
  public V get(String key);
  public void put(String key, V value);
  public boolean putIfAbsent(String key, V value);
  public void delete(String key);
  public void close();
}
