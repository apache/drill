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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import org.apache.drill.exec.exception.DrillbitStartupException;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.ExecProtos.PlanFragment;

import com.google.common.collect.Maps;

public class LocalCache implements DistributedCache {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(LocalCache.class);

  private volatile Map<FragmentHandle, PlanFragment> handles;
  private volatile ConcurrentMap<Class, DistributedMap> maps;
  private volatile ConcurrentMap<Class, DistributedMultiMap> multiMaps;
  private volatile ConcurrentMap<String, Counter> counters;
  
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
    logger.debug("looking for fragment with handle: {}", handle);
    return handles.get(handle);
  }

  @Override
  public void storeFragment(PlanFragment fragment) {
    logger.debug("Storing fragment: {}", fragment);
    handles.put(fragment.getHandle(), fragment);
  }
  
  @Override
  public <V extends DrillSerializable> DistributedMultiMap<V> getMultiMap(Class<V> clazz) {
    DistributedMultiMap mmap = multiMaps.get(clazz);
    if (mmap == null) {
      multiMaps.putIfAbsent(clazz, new LocalDistributedMultiMapImpl<V>(clazz));
      return multiMaps.get(clazz);
    } else {
      return mmap;
    }
  }

  @Override
  public <V extends DrillSerializable> DistributedMap<V> getMap(Class<V> clazz) {
    DistributedMap m = maps.get(clazz);
    if (m == null) {
      maps.putIfAbsent(clazz, new LocalDistributedMapImpl(clazz));
      return maps.get(clazz);
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

  public static DrillSerializable deserialize(byte[] bytes, Class clazz) {
    ByteArrayDataInput in = ByteStreams.newDataInput(bytes);
    try {
      DrillSerializable obj = (DrillSerializable)clazz.newInstance();
      obj.read(in);
      return obj;
    } catch (InstantiationException | IllegalAccessException | IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static class LocalDistributedMultiMapImpl<V> implements DistributedMultiMap<V> {
    private ArrayListMultimap<String, ByteArrayDataOutput> mmap;
    private Class<DrillSerializable> clazz;

    public LocalDistributedMultiMapImpl(Class clazz) {
      mmap = ArrayListMultimap.create();
      this.clazz = clazz;
    }

    @Override
    public Collection<DrillSerializable> get(String key) {
      List<DrillSerializable> list = Lists.newArrayList();
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

  public static class LocalDistributedMapImpl<V> implements DistributedMap<V> {
    private ConcurrentMap<String, ByteArrayDataOutput> m;
    private Class<DrillSerializable> clazz;

    public LocalDistributedMapImpl(Class clazz) {
      m = Maps.newConcurrentMap();
      this.clazz = clazz;
    }

    @Override
    public DrillSerializable get(String key) {
      if (m.get(key) == null) return null;
      return deserialize(m.get(key).toByteArray(), this.clazz);
    }

    @Override
    public void put(String key, DrillSerializable value) {
      m.put(key, serialize(value));
    }

    @Override
    public void putIfAbsent(String key, DrillSerializable value) {
      m.putIfAbsent(key, serialize(value));
    }

    @Override
    public void putIfAbsent(String key, DrillSerializable value, long ttl, TimeUnit timeUnit) {
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
