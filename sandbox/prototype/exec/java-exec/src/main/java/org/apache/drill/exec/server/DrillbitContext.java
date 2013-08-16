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
package org.apache.drill.exec.server;

import io.netty.channel.nio.NioEventLoopGroup;

import java.util.Collection;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.logical.StorageEngineConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.cache.DistributedCache;
import org.apache.drill.exec.coord.ClusterCoordinator;
import org.apache.drill.exec.exception.SetupException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.planner.PhysicalPlanReader;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.rpc.NamedThreadFactory;
import org.apache.drill.exec.rpc.bit.BitCom;
import org.apache.drill.exec.store.StorageEngine;

import com.google.common.base.Preconditions;
import com.yammer.metrics.MetricRegistry;

import org.apache.drill.exec.store.StorageEngineRegistry;

public class DrillbitContext {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillbitContext.class);

  private BootStrapContext context;

  private PhysicalPlanReader reader;
  private final ClusterCoordinator coord;
  private final BitCom com;
  private final DistributedCache cache;
  private final DrillbitEndpoint endpoint;
  private final StorageEngineRegistry storageEngineRegistry;
  
  public DrillbitContext(DrillbitEndpoint endpoint, BootStrapContext context, ClusterCoordinator coord, BitCom com, DistributedCache cache) {
    super();
    Preconditions.checkNotNull(endpoint);
    Preconditions.checkNotNull(context);
    Preconditions.checkNotNull(com);

    this.context = context;
    this.coord = coord;
    this.com = com;
    this.cache = cache;
    this.endpoint = endpoint;
    this.storageEngineRegistry = new StorageEngineRegistry(this);
    this.reader = new PhysicalPlanReader(context.getConfig(), context.getConfig().getMapper(), endpoint, storageEngineRegistry);
  }
  
  public DrillbitEndpoint getEndpoint(){
    return endpoint;
  }
  
  public DrillConfig getConfig() {
    return context.getConfig();
  }
  
  public Collection<DrillbitEndpoint> getBits(){
    return coord.getAvailableEndpoints();
  }

  public BufferAllocator getAllocator(){
    return context.getAllocator();
  }
  
  public StorageEngine getStorageEngine(StorageEngineConfig config) throws ExecutionSetupException {
    return storageEngineRegistry.getEngine(config);
  }
  
  public NioEventLoopGroup getBitLoopGroup(){
    return context.getBitLoopGroup();
  }
  
  public BitCom getBitCom(){
    return com;
  }
  
  public MetricRegistry getMetrics(){
    return context.getMetrics();
  }
  
  public DistributedCache getCache(){
    return cache;
  }
  
  public PhysicalPlanReader getPlanReader(){
    return reader;
  }
  
  
}
