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
package org.apache.drill.exec.cache.local;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.util.DataOutputOutputStream;
import org.apache.drill.exec.cache.Counter;
import org.apache.drill.exec.cache.DistributedCache;
import org.apache.drill.exec.cache.DistributedMap;
import org.apache.drill.exec.cache.DistributedMultiMap;
import org.apache.drill.exec.cache.DrillSerializable;
import org.apache.drill.exec.exception.DrillbitStartupException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.TopLevelAllocator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimaps;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;

public class LocalCache implements DistributedCache {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(LocalCache.class);

  private volatile ConcurrentMap<CacheConfig<?, ?>, DistributedMap<?, ?>> maps;
  private volatile ConcurrentMap<CacheConfig<?, ?>, DistributedMultiMap<?, ?>> multiMaps;
  private volatile ConcurrentMap<String, Counter> counters;
  private static final BufferAllocator allocator = new TopLevelAllocator(DrillConfig.create());

  private static final ObjectMapper mapper = DrillConfig.create().getMapper();

  @Override
  public void close() throws IOException {

  }

  @Override
  public void run() throws DrillbitStartupException {
    maps = Maps.newConcurrentMap();
    multiMaps = Maps.newConcurrentMap();
    counters = Maps.newConcurrentMap();
  }

  @Override
  public <K, V> DistributedMultiMap<K, V> getMultiMap(CacheConfig<K, V> config) {
    DistributedMultiMap<K, V> mmap = (DistributedMultiMap<K, V>) multiMaps.get(config);
    if (mmap == null) {
      multiMaps.putIfAbsent(config, new LocalDistributedMultiMapImpl<K, V>(config));
      return (DistributedMultiMap<K, V>) multiMaps.get(config);
    } else {
      return mmap;
    }
  }

  @Override
  public <K, V> DistributedMap<K, V> getMap(CacheConfig<K, V> config) {
    DistributedMap<K, V> m = (DistributedMap<K, V>) maps.get(config);
    if (m == null) {
      maps.putIfAbsent(config, new LocalDistributedMapImpl<K, V>(config));
      return (DistributedMap<K, V>) maps.get(config);
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

  private static BytesHolder serialize(Object obj, SerializationMode mode) {
    if (obj instanceof String) {
      return new BytesHolder( ((String)obj).getBytes(Charsets.UTF_8));
    }
    try{
      switch (mode) {
      case DRILL_SERIALIZIABLE: {
        ByteArrayDataOutput out = ByteStreams.newDataOutput();
        OutputStream outputStream = DataOutputOutputStream.constructOutputStream(out);
        ((DrillSerializable)obj).writeToStream(outputStream);
        outputStream.flush();
        return new BytesHolder(out.toByteArray());
      }

      case JACKSON: {
        ByteArrayDataOutput out = ByteStreams.newDataOutput();
        out.write(mapper.writeValueAsBytes(obj));
        return new BytesHolder(out.toByteArray());
      }

      case PROTOBUF:
        return new BytesHolder(( (Message) obj).toByteArray());

      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    throw new UnsupportedOperationException();
  }

  private static <V> V deserialize(BytesHolder b, SerializationMode mode, Class<V> clazz) {
    byte[] bytes = b.bytes;
    try {

      if (clazz == String.class) {
        return (V) new String(bytes, Charsets.UTF_8);
      }

      switch (mode) {
      case DRILL_SERIALIZIABLE: {
        InputStream inputStream = new ByteArrayInputStream(bytes);
        V obj = clazz.getConstructor(BufferAllocator.class).newInstance(allocator);
        ((DrillSerializable) obj).readFromStream(inputStream);
        return obj;
      }

      case JACKSON: {
        return (V) mapper.readValue(bytes, clazz);
      }

      case PROTOBUF: {
        Parser<V> parser = null;
        for (Field f : clazz.getFields()) {
          if (f.getName().equals("PARSER") && Modifier.isStatic(f.getModifiers())) {
            parser = (Parser<V>) f.get(null);
          }
        }
        if (parser == null) {
          throw new UnsupportedOperationException(String.format("Unable to find parser for class %s.", clazz.getName()));
        }
        InputStream inputStream = new ByteArrayInputStream(bytes);
        return parser.parseFrom(inputStream);
      }

      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    throw new UnsupportedOperationException();
  }

  private static class BytesHolder {
    final byte[] bytes;
    public BytesHolder(byte[] bytes) {
      this.bytes = bytes;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + Arrays.hashCode(bytes);
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      BytesHolder other = (BytesHolder) obj;
      if (!Arrays.equals(bytes, other.bytes)) {
        return false;
      }
      return true;
    }

  }

  static class LocalDistributedMultiMapImpl<K, V> implements DistributedMultiMap<K, V> {
    private ListMultimap<BytesHolder, BytesHolder> mmap;
    private CacheConfig<K, V> config;

    public LocalDistributedMultiMapImpl(CacheConfig<K, V> config) {
      ListMultimap<BytesHolder, BytesHolder> innerMap = ArrayListMultimap.create();
      mmap = Multimaps.synchronizedListMultimap(innerMap);
      this.config = config;
    }

    @Override
    public Collection<V> get(K key) {
      List<V> list = Lists.newArrayList();
      for (BytesHolder o : mmap.get(serialize(key, config.getMode()))) {
        list.add(deserialize(o, config.getMode(), config.getValueClass()));
      }
      return list;
    }

    @Override
    public Future<Boolean> put(K key, V value) {
      mmap.put(serialize(key, config.getMode()), serialize(value, config.getMode()));
      return new LocalCacheFuture(true);
    }
  }

  public static class LocalCacheFuture<V> implements Future<V> {

    V value;

    public LocalCacheFuture(V value) {
      this.value = value;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      return false;
    }

    @Override
    public boolean isCancelled() {
      return false;
    }

    @Override
    public boolean isDone() {
      return true;
    }

    @Override
    public V get() throws InterruptedException, ExecutionException {
      return value;
    }

    @Override
    public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
      return value;
    }
  }

  public static class LocalDistributedMapImpl<K, V> implements DistributedMap<K, V> {
    protected ConcurrentMap<BytesHolder, BytesHolder> m;
    protected CacheConfig<K, V> config;

    public LocalDistributedMapImpl(CacheConfig<K, V> config) {
      m = Maps.newConcurrentMap();
      this.config = config;
    }

    @Override
    public V get(K key) {
      BytesHolder b = m.get(serialize(key, config.getMode()));
      if (b == null) {
        return null;
      }
      return (V) deserialize(b, config.getMode(), config.getValueClass());
    }

    @Override
    public Iterable<Entry<K, V>> getLocalEntries() {
      return new Iterable<Entry<K, V>>() {
        @Override
        public Iterator<Entry<K, V>> iterator() {
          return new DeserializingTransformer(m.entrySet().iterator());
        }
      };

    }

    @Override
    public Future<V> put(K key, V value) {
      m.put(serialize(key, config.getMode()), serialize(value, config.getMode()));
      return new LocalCacheFuture(value);
    }


    @Override
    public Future<V> putIfAbsent(K key, V value) {
      m.putIfAbsent(serialize(key, config.getMode()), serialize(value, config.getMode()));
      return new LocalCacheFuture(value);
    }

    @Override
    public Future<V> delete(K key) {
      V value = get(key);
      m.remove(serialize(key, config.getMode()));
      return new LocalCacheFuture(value);
    }

    @Override
    public Future<V> putIfAbsent(K key, V value, long ttl, TimeUnit timeUnit) {
      m.putIfAbsent(serialize(key, config.getMode()), serialize(value, config.getMode()));
      logger.warn("Expiration not implemented in local map cache");
      return new LocalCacheFuture<V>(value);
    }

    private class DeserializingTransformer implements Iterator<Map.Entry<K, V>> {
      private Iterator<Map.Entry<BytesHolder, BytesHolder>> inner;

      public DeserializingTransformer(Iterator<Entry<BytesHolder, BytesHolder>> inner) {
        super();
        this.inner = inner;
      }

      @Override
      public boolean hasNext() {
        return inner.hasNext();
      }

      @Override
      public Entry<K, V> next() {
        return newEntry(inner.next());
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }

      public Entry<K, V> newEntry(final Entry<BytesHolder, BytesHolder> input) {
        return new Map.Entry<K, V>() {

          @Override
          public K getKey() {
            return deserialize(input.getKey(), config.getMode(), config.getKeyClass());
          }

          @Override
          public V getValue() {
            return deserialize(input.getValue(), config.getMode(), config.getValueClass());
          }

          @Override
          public V setValue(V value) {
            throw new UnsupportedOperationException();
          }

        };
      }

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
