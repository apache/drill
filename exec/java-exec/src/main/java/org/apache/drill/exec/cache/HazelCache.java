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
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.cache.ProtoBufImpl.HWorkQueueStatus;
import org.apache.drill.exec.cache.ProtoBufImpl.HandlePlan;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.BitControl.PlanFragment;
import org.apache.drill.exec.proto.BitControl.WorkQueueStatus;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.DuplicateInstanceNameException;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IMap;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;

public class HazelCache implements DistributedCache {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HazelCache.class);

  private final String instanceName;
  private HazelcastInstance instance;
  private ITopic<HWorkQueueStatus> workQueueLengths;
  private HandlePlan fragments;
  private Cache<WorkQueueStatus, Integer>  endpoints;
  private BufferAllocator allocator;

  public HazelCache(DrillConfig config, BufferAllocator allocator) {
    this.instanceName = config.getString(ExecConstants.SERVICE_NAME);
    this.allocator = allocator;
  }

  private class Listener implements MessageListener<HWorkQueueStatus>{

    @Override
    public void onMessage(Message<HWorkQueueStatus> wrapped) {
      logger.debug("Received new queue length message.");
      endpoints.put(wrapped.getMessageObject().get(), 0);
    }
    
  }
  
  public void run() {
    Config c = new Config();
    SerializerConfig sc = new SerializerConfig() // 
      .setImplementation(new HCVectorAccessibleSerializer(allocator)) //
      .setTypeClass(VectorAccessibleSerializable.class);
    c.setInstanceName(instanceName);
    c.getSerializationConfig().addSerializerConfig(sc);
    instance = getInstanceOrCreateNew(c);
    workQueueLengths = instance.getTopic("queue-length");
    fragments = new HandlePlan(instance);
    endpoints = CacheBuilder.newBuilder().maximumSize(2000).build();
    workQueueLengths.addMessageListener(new Listener());
  }

  private HazelcastInstance getInstanceOrCreateNew(Config c) {
    for (HazelcastInstance instance : Hazelcast.getAllHazelcastInstances()){
      if (instance.getName().equals(this.instanceName))
        return instance;
    }
    try {
    return Hazelcast.newHazelcastInstance(c);
    } catch (DuplicateInstanceNameException e) {
      return getInstanceOrCreateNew(c);
    }
  }

//  @Override
//  public void updateLocalQueueLength(int length) {
//    workQueueLengths.publish(new HWorkQueueStatus(WorkQueueStatus.newBuilder().setEndpoint(endpoint)
//        .setQueueLength(length).setReportTime(System.currentTimeMillis()).build()));
//  }
//
//  @Override
//  public List<WorkQueueStatus> getQueueLengths() {
//    return Lists.newArrayList(endpoints.asMap().keySet());
//  }

  @Override
  public void close() throws IOException {
    this.instance.getLifecycleService().shutdown();
  }

  @Override
  public PlanFragment getFragment(FragmentHandle handle) {
    return this.fragments.get(handle);
  }

  @Override
  public void storeFragment(PlanFragment fragment) {
    fragments.put(fragment.getHandle(), fragment);
  }
  

  @Override
  public <V extends DrillSerializable> DistributedMultiMap<V> getMultiMap(Class<V> clazz) {
    com.hazelcast.core.MultiMap<String, V> mmap = this.instance.getMultiMap(clazz.toString());
    return new HCDistributedMultiMapImpl<V>(mmap, clazz);
  }

  @Override
  public <V extends DrillSerializable> DistributedMap<V> getMap(Class<V> clazz) {
    IMap<String, V> imap = this.instance.getMap(clazz.toString());
    MapConfig myMapConfig = new MapConfig();
    myMapConfig.setBackupCount(0);
    myMapConfig.setReadBackupData(true);
    instance.getConfig().getMapConfigs().put(clazz.toString(), myMapConfig);
    return new HCDistributedMapImpl<V>(imap, clazz);
  }

  @Override
  public Counter getCounter(String name) {
    return new HCCounterImpl(this.instance.getAtomicLong(name));
  }

  public static class HCDistributedMapImpl<V extends DrillSerializable> implements DistributedMap<V> {
    private IMap<String, V> m;

    public HCDistributedMapImpl(IMap<String, V> m, Class<V> clazz) {
      this.m = m;
    }

    public V get(String key) {
      return m.get(key);
    }

    public void put(String key, V value) {
      m.put(key, value);
    }

    public void putIfAbsent(String key, V value) {
      m.putIfAbsent(key, value);
    }

    public void putIfAbsent(String key, V value, long ttl, TimeUnit timeunit) {
      m.putIfAbsent(key, value, ttl, timeunit);
    }
  }

  public static class HCDistributedMultiMapImpl<V extends DrillSerializable> implements DistributedMultiMap<V> {
    private com.hazelcast.core.MultiMap<String, V> mmap;

    public HCDistributedMultiMapImpl(com.hazelcast.core.MultiMap<String, V> mmap, Class<V> clazz) {
      this.mmap = mmap;
    }

    public Collection<V> get(String key) {
      List<V> list = Lists.newArrayList();
      for (V v : mmap.get(key)) {
        list.add(v);
      }
      return list;
    }

    @Override
    public void put(String key, V value) {
      mmap.put(key, value);
    }
  }

  public static class HCCounterImpl implements Counter {
    private IAtomicLong n;

    public HCCounterImpl(IAtomicLong n) {
      this.n = n;
    }

    public long get() {
      return n.get();
    }

    public long incrementAndGet() {
      return n.incrementAndGet();
    }

    public long decrementAndGet() {
      return n.decrementAndGet();
    }
  }

}
