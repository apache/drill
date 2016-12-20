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
package org.apache.drill.exec.store.sys.store;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.drill.common.concurrent.AutoCloseableLock;
import org.apache.drill.exec.exception.VersionMismatchException;
import org.apache.drill.exec.store.sys.BasePersistentStore;
import org.apache.drill.exec.store.sys.PersistentStoreConfig;
import org.apache.drill.exec.store.sys.PersistentStoreMode;

import com.google.common.collect.Iterables;

public class InMemoryStore<V> extends BasePersistentStore<V> {
  // private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(InMemoryPersistentStore.class);

  private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
  private final AutoCloseableLock readLock = new AutoCloseableLock(readWriteLock.readLock());
  private final AutoCloseableLock writeLock = new AutoCloseableLock(readWriteLock.writeLock());
  private final ConcurrentSkipListMap<String, V> store;
  private int version = -1;
  private final int capacity;
  private final AtomicInteger currentSize = new AtomicInteger();

  public InMemoryStore(int capacity) {
    this.capacity = capacity;
    //Allows us to trim out the oldest elements to maintain finite max size
    this.store = new ConcurrentSkipListMap<String, V>();
  }

  @Override
  public void delete(final String key) {
    try (AutoCloseableLock lock = writeLock.open()) {
      store.remove(key);
      version++;
    }
  }

  @Override
  public PersistentStoreMode getMode() {
    return PersistentStoreMode.BLOB_PERSISTENT;
  }

  @Override
  public boolean contains(final String key) {
    return contains(key, null);
  }

  @Override
  public boolean contains(final String key, final DataChangeVersion dataChangeVersion) {
    try (AutoCloseableLock lock = readLock.open()) {
      if (dataChangeVersion != null) {
        dataChangeVersion.setVersion(version);
      }
      return store.containsKey(key);
    }
  }

  @Override
  public V get(final String key) {
    return get(key, null);
  }

  @Override
  public V get(final String key, final DataChangeVersion dataChangeVersion) {
    try (AutoCloseableLock lock = readLock.open()) {
      if (dataChangeVersion != null) {
        dataChangeVersion.setVersion(version);
      }
      return store.get(key);
    }
  }

  @Override
  public void put(final String key, final V value) {
    put(key, value, null);
  }

  @Override
  public void put(final String key, final V value, final DataChangeVersion dataChangeVersion) {
    try (AutoCloseableLock lock = writeLock.open()) {
      if (dataChangeVersion != null && dataChangeVersion.getVersion() != version) {
        throw new VersionMismatchException("Version mismatch detected", dataChangeVersion.getVersion());
      }
      store.put(key, value);
      if (currentSize.incrementAndGet() > capacity) {
        //Pop Out Oldest
        store.pollLastEntry();
        currentSize.decrementAndGet();
      }

      version++;
    }
  }

  @Override
  public boolean putIfAbsent(final String key, final V value) {
    try (AutoCloseableLock lock = writeLock.open()) {
      final V old = store.putIfAbsent(key, value);
      if (old == null) {
        version++;
        return true;
      }
      return false;
    }
  }

  @Override
  public Iterator<Map.Entry<String, V>> getRange(final int skip, final int take) {
    try (AutoCloseableLock lock = readLock.open()) {
      return Iterables.limit(Iterables.skip(store.entrySet(), skip), take).iterator();
    }
  }

  @Override
  public void close() throws Exception {
    try (AutoCloseableLock lock = writeLock.open()) {
      store.clear();
      version = -1;
    }
  }
}
