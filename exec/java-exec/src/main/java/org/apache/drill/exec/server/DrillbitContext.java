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
package org.apache.drill.exec.server;

import io.netty.channel.nio.NioEventLoopGroup;

import java.util.Collection;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.FunctionRegistry;
import org.apache.drill.exec.cache.DistributedCache;
import org.apache.drill.exec.coord.ClusterCoordinator;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.physical.impl.OperatorCreatorRegistry;
import org.apache.drill.exec.planner.PhysicalPlanReader;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.rpc.control.Controller;
import org.apache.drill.exec.rpc.control.WorkEventBus;
import org.apache.drill.exec.rpc.data.DataConnectionCreator;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.StoragePluginRegistry.DrillSchemaFactory;
import org.apache.drill.exec.store.StoragePlugin;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Preconditions;

public class DrillbitContext {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillbitContext.class);

  private BootStrapContext context;

  private PhysicalPlanReader reader;
  private final ClusterCoordinator coord;
  private final DataConnectionCreator connectionsPool;
  private final DistributedCache cache;
  private final DrillbitEndpoint endpoint;
  private final StoragePluginRegistry storagePlugins;
  private final OperatorCreatorRegistry operatorCreatorRegistry;
  private final Controller controller;
  private final WorkEventBus workBus;
  private final FunctionImplementationRegistry functionRegistry;
  private final FunctionRegistry functionRegistryX;
  
  public DrillbitContext(DrillbitEndpoint endpoint, BootStrapContext context, ClusterCoordinator coord, Controller controller, DataConnectionCreator connectionsPool, DistributedCache cache, WorkEventBus workBus) {
    super();
    Preconditions.checkNotNull(endpoint);
    Preconditions.checkNotNull(context);
    Preconditions.checkNotNull(controller);
    Preconditions.checkNotNull(connectionsPool);
    this.workBus = workBus;
    this.controller = controller;
    this.context = context;
    this.coord = coord;
    this.connectionsPool = connectionsPool;
    this.cache = cache;
    this.endpoint = endpoint;
    this.storagePlugins = new StoragePluginRegistry(this);
    this.reader = new PhysicalPlanReader(context.getConfig(), context.getConfig().getMapper(), endpoint, storagePlugins);
    this.operatorCreatorRegistry = new OperatorCreatorRegistry(context.getConfig());
    this.functionRegistry = new FunctionImplementationRegistry(context.getConfig());
    this.functionRegistryX = new FunctionRegistry(context.getConfig());
  }

  public FunctionRegistry getFunctionRegistry(){
    return functionRegistryX;
  }
  
  public FunctionImplementationRegistry getFunctionImplementationRegistry() {
    return functionRegistry;
  }

  public WorkEventBus getWorkBus(){
    return workBus;
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

  public OperatorCreatorRegistry getOperatorCreatorRegistry() {
    return operatorCreatorRegistry;
  }

  public StoragePluginRegistry getStorage(){
    return this.storagePlugins;
  }
  
  public NioEventLoopGroup getBitLoopGroup(){
    return context.getBitLoopGroup();
  }
  
  
  public DataConnectionCreator getDataConnectionsPool(){
    return connectionsPool;
  }
  
  public Controller getController(){
    return controller;
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
  
  public DrillSchemaFactory getSchemaFactory(){
    return storagePlugins.getSchemaFactory();
  }
  
}
