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
package org.apache.drill.exec.cache;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.exception.DrillbitStartupException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.TopLevelAllocator;
import org.apache.drill.exec.proto.BitControl.PlanFragment;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;

public class LocalCache implements DistributedCache {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(LocalCache.class);

  private volatile Map<FragmentHandle, PlanFragment> handles;
  private volatile ConcurrentMap<Class<?>, DistributedMap<?>> maps;
  private volatile ConcurrentMap<Class<?>, DistributedMultiMap<?>> multiMaps;
  private volatile ConcurrentMap<String, Counter> counters;
  private static final BufferAllocator allocator = new TopLevelAllocator();

  @Override
  public void close() throws IOException {
    handles = null;
  }

  @Override
  public void run() throws DrillbitStartupException {
    handles = Maps.newConcurrentMap();
    maps = Maps.newConcurrentMap();
    multiMaps = Maps.newConcurrentMap();
    counters = Maps.newConcurrentMap();
  }

  @Override
  public PlanFragment getFragment(FragmentHandle handle) {
//    logger.debug("looking for fragment with handle: {}", handle);
    return handles.get(handle);
  }

  @Override
  public void storeFragment(PlanFragment fragment) {
//    logger.debug("Storing fragment: {}", fragment);
    handles.put(fragment.getHandle(), fragment);
  }
  
  @Override
  public <V extends DrillSerializable> DistributedMultiMap<V> getMultiMap(Class<V> clazz) {
    DistributedMultiMap<V> mmap = (DistributedMultiMap<V>) multiMaps.get(clazz);
    if (mmap == null) {
      multiMaps.putIfAbsent(clazz, new LocalDistributedMultiMapImpl<V>(clazz));
      return (DistributedMultiMap<V>) multiMaps.get(clazz);
    } else {
      return mmap;
    }
  }

  @Override
  public <V extends DrillSerializable> DistributedMap<V> getMap(Class<V> clazz) {
    DistributedMap m = maps.get(clazz);
    if (m == null) {
      maps.putIfAbsent(clazz, new LocalDistributedMapImpl<V>(clazz));
      return (DistributedMap<V>) maps.get(clazz);
    } else {
      return m;
    }
  }

  @Override
  public Counter getCounter(String name) {
    Counter c = counters.get(name);
    if (c == null) {
      counters.putIfAbsent(name, new LocalCounterImpl());
      return counters.get(name);
    } else {
      return c;
    }
  }

  public static ByteArrayDataOutput serialize(DrillSerializable obj) {
    ByteArrayDataOutput out = ByteStreams.newDataOutput();
    try {
      obj.write(out);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return out;
  }

  public static <V extends DrillSerializable> V deserialize(byte[] bytes, Class<V> clazz) {
    ByteArrayDataInput in = ByteStreams.newDataInput(bytes);
    try {
      V obj = clazz.getConstructor(BufferAllocator.class).newInstance(allocator);
      obj.read(in);
      return obj;
    } catch (InstantiationException | IllegalAccessException | IOException | NoSuchMethodException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }

  public static class LocalDistributedMultiMapImpl<V extends DrillSerializable> implements DistributedMultiMap<V> {
    private ArrayListMultimap<String, ByteArrayDataOutput> mmap;
    private Class<V> clazz;

    public LocalDistributedMultiMapImpl(Class<V> clazz) {
      mmap = ArrayListMultimap.create();
      this.clazz = clazz;
    }

    @Override
    public Collection<V> get(String key) {
      List<V> list = Lists.newArrayList();
      for (ByteArrayDataOutput o : mmap.get(key)) {
        list.add(deserialize(o.toByteArray(), this.clazz));
      }
      return list;
    }

    @Override
    public void put(String key, DrillSerializable value) {
      mmap.put(key, serialize(value));
    }
  }

  public static class LocalDistributedMapImpl<V extends DrillSerializable> implements DistributedMap<V> {
    private ConcurrentMap<String, ByteArrayDataOutput> m;
    private Class<V> clazz;

    public LocalDistributedMapImpl(Class<V> clazz) {
      m = Maps.newConcurrentMap();
      this.clazz = clazz;
    }

    @Override
    public V get(String key) {
      if (m.get(key) == null) return null;
      return (V) deserialize(m.get(key).toByteArray(), this.clazz);
    }

    @Override
    public void put(String key, V value) {
      m.put(key, serialize(value));
    }

    @Override
    public void putIfAbsent(String key, V value) {
      m.putIfAbsent(key, serialize(value));
    }

    @Override
    public void putIfAbsent(String key, V value, long ttl, TimeUnit timeUnit) {
      m.putIfAbsent(key, serialize(value));
      logger.warn("Expiration not implemented in local map cache");
    }
  }

  public static class LocalCounterImpl implements Counter {
    private AtomicLong al = new AtomicLong();

    @Override
    public long get() {
      return al.get();
    }

    @Override
    public long incrementAndGet() {
      return al.incrementAndGet();
    }

    @Override
    public long decrementAndGet() {
      return al.decrementAndGet();
    }
  }
}
