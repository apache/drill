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

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Maps;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.cache.Counter;
import org.apache.drill.exec.cache.DistributedCache;
import org.apache.drill.exec.cache.DistributedMap;
import org.apache.drill.exec.cache.DistributedMultiMap;
import org.apache.drill.exec.cache.SerializationDefinition;
import org.apache.drill.exec.cache.local.LocalCache.LocalCounterImpl;
import org.apache.drill.exec.exception.DrillbitStartupException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.BitControl.FragmentStatus;
import org.apache.drill.exec.proto.BitControl.PlanFragment;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.infinispan.Cache;
import org.infinispan.atomic.Delta;
import org.infinispan.atomic.DeltaAware;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.jgroups.blocks.atomic.CounterService;
import org.jgroups.fork.ForkChannel;
import org.jgroups.protocols.COUNTER;
import org.jgroups.protocols.FRAG2;
import org.jgroups.stack.ProtocolStack;


public class ICache implements DistributedCache{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ICache.class);

  private EmbeddedCacheManager manager;
  private ForkChannel cacheChannel;
  private final CounterService counters;
  private final boolean local;
  private volatile ConcurrentMap<String, Counter> localCounters;

  public ICache(DrillConfig config, BufferAllocator allocator, boolean local) throws Exception {
    String clusterName = config.getString(ExecConstants.SERVICE_NAME);
    this.local = local;

    final CacheMode mode = local ? CacheMode.LOCAL : CacheMode.DIST_SYNC;
    GlobalConfigurationBuilder gcb = new GlobalConfigurationBuilder();

    if(!local){
      gcb.transport() //
        .defaultTransport() //
        .clusterName(clusterName);
    }

    gcb.serialization() //
        .addAdvancedExternalizer(new VAAdvancedExternalizer(allocator)) //
        .addAdvancedExternalizer(new JacksonAdvancedExternalizer<>(SerializationDefinition.OPTION, config.getMapper())) //
        .addAdvancedExternalizer(new JacksonAdvancedExternalizer<>(SerializationDefinition.STORAGE_PLUGINS, config.getMapper())) //
        .addAdvancedExternalizer(new ProtobufAdvancedExternalizer<>(SerializationDefinition.FRAGMENT_STATUS, FragmentStatus.PARSER)) //
        .addAdvancedExternalizer(new ProtobufAdvancedExternalizer<>(SerializationDefinition.FRAGMENT_HANDLE, FragmentHandle.PARSER)) //
        .addAdvancedExternalizer(new ProtobufAdvancedExternalizer<>(SerializationDefinition.PLAN_FRAGMENT, PlanFragment.PARSER)) //
      .build();

    Configuration c = new ConfigurationBuilder() //
      .clustering() //

      .cacheMode(mode) //
      .storeAsBinary().enable() //
      .build();
    this.manager = new DefaultCacheManager(gcb.build(), c);

    if(!local){
      JGroupsTransport transport = (JGroupsTransport) manager.getCache("first").getAdvancedCache().getRpcManager().getTransport();
      this.cacheChannel = new ForkChannel(transport.getChannel(), "drill-stack", "drill-hijacker", true, ProtocolStack.ABOVE, FRAG2.class, new COUNTER());
      this.counters = new CounterService(this.cacheChannel);
    }else{
      this.cacheChannel = null;
      this.counters = null;
    }
  }


//  @Override
//  public <K, V> Map<K, V> getSmallAtomicMap(CacheConfig<K, V> config) {
//    Cache<String, ?> cache = manager.getCache("atomic-maps");
//    return AtomicMapLookup.getAtomicMap(cache, config.getName());
//  }


  @Override
  public void close() throws IOException {
    manager.stop();
  }

  @Override
  public void run() throws DrillbitStartupException {
    try {
      if(local){
        localCounters = Maps.newConcurrentMap();
        manager.start();
      }else{
        cacheChannel.connect("c1");
      }

    } catch (Exception e) {
      throw new DrillbitStartupException("Failure while trying to set up JGroups.");
    }
  }

  @Override
  public <K, V> DistributedMultiMap<K, V> getMultiMap(CacheConfig<K, V> config) {
    Cache<K, DeltaList<V>> cache = manager.getCache(config.getName());
    return new IMulti<K, V>(cache, config);
  }

  @Override
  public <K, V> DistributedMap<K, V> getMap(CacheConfig<K, V> config) {
    Cache<K, V> c = manager.getCache(config.getName());
    return new IMap<K, V>(c, config);
  }

  @Override
  public Counter getCounter(String name) {
    if(local){
        Counter c = localCounters.get(name);
        if (c == null) {
          localCounters.putIfAbsent(name, new LocalCounterImpl());
          return localCounters.get(name);
        } else {
          return c;
        }

    }else{
      return new JGroupsCounter(counters.getOrCreateCounter(name, 0));
    }

  }

  private class JGroupsCounter implements Counter{
    final org.jgroups.blocks.atomic.Counter inner;

    public JGroupsCounter(org.jgroups.blocks.atomic.Counter inner) {
      super();
      this.inner = inner;
    }

    @Override
    public long get() {
      return inner.get();
    }

    @Override
    public long incrementAndGet() {
      return inner.incrementAndGet();
    }

    @Override
    public long decrementAndGet() {
      return inner.decrementAndGet();
    }

  }

  private class IMap<K, V> implements DistributedMap<K, V>{

    private Cache<K, V> cache;
    private CacheConfig<K, V> config;

    public IMap(Cache<K, V> cache, CacheConfig<K, V> config) {
      super();
      this.cache = cache;
      this.config = config;
    }

    @Override
    public Iterable<Entry<K, V>> getLocalEntries() {
      return cache.entrySet();
    }

    @Override
    public V get(K key) {
      return cache.get(key);
    }

    @Override
    public Future<V> delete(K key) {
      return cache.removeAsync(key);
    }

    @Override
    public Future<V> put(K key, V value) {
      return cache.putAsync(key, value);
    }

    @Override
    public Future<V> putIfAbsent(K key, V value) {
      return cache.putIfAbsentAsync(key, value);
    }

    @Override
    public Future<V> putIfAbsent(K key, V value, long ttl, TimeUnit timeUnit) {
      return cache.putIfAbsentAsync(key, value, ttl, timeUnit);
    }

  }

  private class IMulti<K, V> implements DistributedMultiMap<K, V>{

    private Cache<K, DeltaList<V>> cache;
    private CacheConfig<K, V> config;

    public IMulti(Cache<K, DeltaList<V>> cache, CacheConfig<K, V> config) {
      super();
      this.cache = cache;
      this.config = config;
    }

    @Override
    public Collection<V> get(K key) {
      return cache.get(key);
    }

    @Override
    public Future<Boolean> put(K key, V value) {
      return new ICacheFuture(cache.putAsync(key, new DeltaList(value)));
    }

  }

  public static class ICacheFuture implements Future<Boolean> {

    Future future;

    public ICacheFuture(Future future) {
      this.future = future;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      return future.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled() {
      return future.isCancelled();
    }

    @Override
    public boolean isDone() {
      return future.isDone();
    }

    @Override
    public Boolean get() throws InterruptedException, ExecutionException {
      future.get();
      return true;
    }

    @Override
    public Boolean get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
      future.get(timeout, unit);
      return true;
    }
  }




  private static class DeltaList<V> extends LinkedList<V> implements DeltaAware, Delta, List<V> {

    /** The serialVersionUID */
    private static final long serialVersionUID = 2176345973026460708L;

    public DeltaList(Collection<? extends V> c) {
       super(c);
    }

    public DeltaList(V obj) {
       super();
       add(obj);
    }

    @Override
    public Delta delta() {
       return new DeltaList<V>(this);
    }

    @Override
    public void commit() {
       this.clear();
    }

    @SuppressWarnings("unchecked")
    @Override
    public DeltaAware merge(DeltaAware d) {
       List<V> other = null;
       if (d != null && d instanceof DeltaList) {
          other = (List<V>) d;
          for (V e : this) {
             other.add(e);
          }
          return (DeltaAware) other;
       } else {
          return this;
       }
    }
 }

}
