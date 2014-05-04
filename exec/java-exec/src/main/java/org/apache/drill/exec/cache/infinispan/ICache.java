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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.cache.Counter;
import org.apache.drill.exec.cache.DistributedCache;
import org.apache.drill.exec.cache.DistributedMap;
import org.apache.drill.exec.cache.DistributedMultiMap;
import org.apache.drill.exec.cache.DrillSerializable;
import org.apache.drill.exec.cache.SerializationDefinition;
import org.apache.drill.exec.cache.ProtoSerializable.FragmentHandleSerializable;
import org.apache.drill.exec.cache.ProtoSerializable.PlanFragmentSerializable;
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
import org.infinispan.configuration.global.GlobalConfiguration;
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
  private final Cache<FragmentHandleSerializable, PlanFragmentSerializable> fragments;

  public ICache(DrillConfig config, BufferAllocator allocator) throws Exception {
    String clusterName = config.getString(ExecConstants.SERVICE_NAME);
    GlobalConfiguration gc = new GlobalConfigurationBuilder() //
      .transport() //
        .defaultTransport() //
        .clusterName(clusterName) //;
        //
      .serialization() //
        .addAdvancedExternalizer(new VAAdvancedExternalizer(allocator)) //
        .addAdvancedExternalizer(new JacksonAdvancedExternalizer<>(SerializationDefinition.OPTION, config.getMapper())) //
        .addAdvancedExternalizer(new JacksonAdvancedExternalizer<>(SerializationDefinition.STORAGE_PLUGINS, config.getMapper())) //
        .addAdvancedExternalizer(new ProtobufAdvancedExternalizer<>(SerializationDefinition.FRAGMENT_STATUS, FragmentStatus.PARSER)) //
      .build();

    Configuration c = new ConfigurationBuilder() //
      .clustering() //
      .cacheMode(CacheMode.DIST_ASYNC) //
      .storeAsBinary() //
      .build();
    this.manager = new DefaultCacheManager(gc, c);
    JGroupsTransport transport = (JGroupsTransport) manager.getCache("first").getAdvancedCache().getRpcManager().getTransport();
    this.cacheChannel = new ForkChannel(transport.getChannel(), "drill-stack", "drill-hijacker", true, ProtocolStack.ABOVE, FRAG2.class, new COUNTER());
    this.fragments = manager.getCache(PlanFragment.class.getName());
    this.counters = new CounterService(this.cacheChannel);
  }

  @Override
  public void close() throws IOException {
    manager.stop();
  }

  @Override
  public void run() throws DrillbitStartupException {
    try {
      cacheChannel.connect("c1");
    } catch (Exception e) {
      throw new DrillbitStartupException("Failure while trying to set up JGroups.");
    }
  }

  @Override
  public PlanFragment getFragment(FragmentHandle handle) {
    PlanFragmentSerializable pfs = fragments.get(new FragmentHandleSerializable(handle));
    if(pfs == null) return null;
    return pfs.getObject();
  }

  @Override
  public void storeFragment(PlanFragment fragment) {
    fragments.put(new FragmentHandleSerializable(fragment.getHandle()), new PlanFragmentSerializable(fragment));
  }

  @Override
  public <V extends DrillSerializable> DistributedMultiMap<V> getMultiMap(Class<V> clazz) {
    Cache<String, DeltaList<V>> cache = manager.getCache(clazz.getName());
    return new IMulti<V>(cache, clazz);
  }


  @Override
  public <V extends DrillSerializable> DistributedMap<V> getNamedMap(String name, Class<V> clazz) {
    Cache<String, V> c = manager.getCache(name);
    return new IMap<V>(c);
  }

  @Override
  public <V extends DrillSerializable> DistributedMap<V> getMap(Class<V> clazz) {
    return getNamedMap(clazz.getName(), clazz);
  }

  @Override
  public Counter getCounter(String name) {
    return new JGroupsCounter(counters.getOrCreateCounter(name, 0));
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

  private class IMap<V extends DrillSerializable> implements DistributedMap<V>{

    private Cache<String, V> cache;


    public IMap(Cache<String, V> cache) {
      super();
      this.cache = cache;
    }

    @Override
    public V get(String key) {
      return cache.get(key);
    }

    @Override
    public void put(String key, V value) {
      cache.put(key,  value);
    }

    @Override
    public void putIfAbsent(String key, V value) {
      cache.putIfAbsent(key,  value);
    }

    @Override
    public void putIfAbsent(String key, V value, long ttl, TimeUnit timeUnit) {
      cache.putIfAbsent(key, value, ttl, timeUnit);
    }

    @Override
    public Iterator<Entry<String, V>> iterator() {
      return cache.entrySet().iterator();
    }

  }

  private class IMulti<V extends DrillSerializable> implements DistributedMultiMap<V>{

    private Cache<String, DeltaList<V>> cache;
    private Class<V> clazz;

    public IMulti(Cache<String, DeltaList<V>> cache, Class<V> clazz) {
      super();
      this.cache = cache;
      this.clazz = clazz;
    }

    @Override
    public Collection<V> get(String key) {
      return cache.get(key);
    }

    @Override
    public void put(String key, V value) {
      cache.put(key, new DeltaList<V>(value));
//      cache.getAdvancedCache().applyDelta(key, new DeltaList<V>(value), key);
    }

  }




  private static class DeltaList<V extends DrillSerializable> extends LinkedList<V> implements DeltaAware, Delta{

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


//  public void run() {
//    Config c = new Config();
//    SerializerConfig sc = new SerializerConfig() //
//      .setImplementation(new HCVectorAccessibleSerializer(allocator)) //
//      .setTypeClass(VectorAccessibleSerializable.class);
//    c.setInstanceName(instanceName);
//    c.getSerializationConfig().addSerializerConfig(sc);
//    instance = getInstanceOrCreateNew(c);
//    workQueueLengths = instance.getTopic("queue-length");
//    fragments = new HandlePlan(instance);
//    endpoints = CacheBuilder.newBuilder().maximumSize(2000).build();
//    workQueueLengths.addMessageListener(new Listener());
//  }
}
