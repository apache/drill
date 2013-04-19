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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.CoordinationProtos.WorkQueueStatus;

import com.beust.jcommander.internal.Lists;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import com.hazelcast.nio.DataSerializable;

public class HazelCache implements DistributedCache {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HazelCache.class);

  private final String instanceName;
  private HazelcastInstance instance;
  private ITopic<WrappedWorkQueueStatus> workQueueLengths;
  private DrillbitEndpoint endpoint;
  private Cache<WorkQueueStatus, Integer>  endpoints;
  private IMap<TemplatizedLogicalPlan, TemplatizedPhysicalPlan> optimizedPlans;
  
  public HazelCache(DrillConfig config) {
    this.instanceName = config.getString(ExecConstants.SERVICE_NAME);
  }

  private class Listener implements MessageListener<WrappedWorkQueueStatus>{

    @Override
    public void onMessage(Message<WrappedWorkQueueStatus> wrapped) {
      logger.debug("Received new queue length message.");
      endpoints.put(wrapped.getMessageObject().status, 0);
    }
    
  }
  
  public void run(DrillbitEndpoint endpoint) {
    Config c = new Config();
    c.setInstanceName(instanceName);
    instance = getInstanceOrCreateNew(c);
    workQueueLengths = instance.getTopic("queue-length");
    optimizedPlans = instance.getMap("plan-optimizations");
    this.endpoint = endpoint;
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

  @Override
  public void saveOptimizedPlan(TemplatizedLogicalPlan logical, TemplatizedPhysicalPlan physical) {
    optimizedPlans.put(logical, physical);
  }

  @Override
  public TemplatizedPhysicalPlan getOptimizedPlan(TemplatizedLogicalPlan logical) {
    return optimizedPlans.get(logical);
  }

  @Override
  public void updateLocalQueueLength(int length) {
    workQueueLengths.publish(new WrappedWorkQueueStatus(WorkQueueStatus.newBuilder().setEndpoint(endpoint)
        .setQueueLength(length).setReportTime(System.currentTimeMillis()).build()));
  }

  @Override
  public List<WorkQueueStatus> getQueueLengths() {
    return Lists.newArrayList(endpoints.asMap().keySet());
  }

  public class WrappedWorkQueueStatus implements DataSerializable {

    public WorkQueueStatus status;

    public WrappedWorkQueueStatus(WorkQueueStatus status) {
      this.status = status;
    }

    @Override
    public void readData(DataInput arg0) throws IOException {
      int len = arg0.readShort();
      byte[] b = new byte[len];
      arg0.readFully(b);
      this.status = WorkQueueStatus.parseFrom(b);
    }

    @Override
    public void writeData(DataOutput arg0) throws IOException {
      byte[] b = status.toByteArray();
      if (b.length > Short.MAX_VALUE) throw new IOException("Unexpectedly long value.");
      arg0.writeShort(b.length);
      arg0.write(b);
    }

  }

  @Override
  public void close() throws IOException {
    this.instance.getLifecycleService().shutdown();
  }
  

  

}
