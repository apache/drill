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
import org.apache.drill.common.logical.StorageEngineConfig;
import org.apache.drill.common.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.cache.DistributedCache;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.rpc.NamedThreadFactory;
import org.apache.drill.exec.rpc.bit.BitCom;
import org.apache.drill.exec.store.StorageEngine;

import com.yammer.metrics.MetricRegistry;

public class DrillbitContext {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillbitContext.class);
  
  private final DrillConfig config;
  private final Drillbit underlyingBit;
  private final NioEventLoopGroup loop;
  private final MetricRegistry metrics;
  
  public DrillbitContext(DrillConfig config, Drillbit underlyingBit) {
    super();
    this.config = config;
    this.underlyingBit = underlyingBit;
    this.loop = new NioEventLoopGroup(1, new NamedThreadFactory("BitServer-"));
    this.metrics = new MetricRegistry(config.getString(ExecConstants.METRICS_CONTEXT_NAME));
  }
  
  public DrillConfig getConfig() {
    return config;
  }
  
  public Collection<DrillbitEndpoint> getBits(){
    return underlyingBit.coord.getAvailableEndpoints();
  }

  public BufferAllocator getAllocator(){
    return underlyingBit.pool;
  }
  
  public StorageEngine getStorageEngine(StorageEngineConfig config){
    throw new UnsupportedOperationException();
  }
  
  public NioEventLoopGroup getBitLoopGroup(){
    return loop;
  }
  
  public BitCom getBitCom(){
    return underlyingBit.engine.getBitCom();
  }
  
  public MetricRegistry getMetrics(){
    return metrics;
  }
  
  public DistributedCache getCache(){
    return underlyingBit.cache;
  }
  
  
}
