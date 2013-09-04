/*******************************************************************************
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
 ******************************************************************************/
package org.apache.drill.exec.cache;

import java.io.IOException;
import java.util.List;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.cache.ProtoBufImpl.HWorkQueueStatus;
import org.apache.drill.exec.cache.ProtoBufImpl.HandlePlan;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.ExecProtos.PlanFragment;
import org.apache.drill.exec.proto.ExecProtos.WorkQueueStatus;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.protobuf.InvalidProtocolBufferException;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
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
  
  public HazelCache(DrillConfig config) {
    this.instanceName = config.getString(ExecConstants.SERVICE_NAME);
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
    c.setInstanceName(instanceName);
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
    return Hazelcast.newHazelcastInstance(c);
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
  

  

}
