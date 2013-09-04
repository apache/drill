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
package org.apache.drill.exec.ops;

import java.util.Collection;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.logical.StorageEngineConfig;
import org.apache.drill.exec.cache.DistributedCache;
import org.apache.drill.exec.planner.PhysicalPlanReader;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.rpc.bit.BitCom;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.StorageEngine;

public class QueryContext {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(QueryContext.class);
  
  private QueryId queryId;
  private DrillbitContext drillbitContext;
  
  public QueryContext(QueryId queryId, DrillbitContext drllbitContext) {
    super();
    this.queryId = queryId;
    this.drillbitContext = drllbitContext;
  }
  
  public DrillbitEndpoint getCurrentEndpoint(){
    return drillbitContext.getEndpoint();
  }
  
  public QueryId getQueryId() {
    return queryId;
  }

  public StorageEngine getStorageEngine(StorageEngineConfig config) throws ExecutionSetupException {
    return drillbitContext.getStorageEngine(config);
  }

  public DistributedCache getCache(){
    return drillbitContext.getCache();
  }
  
  public Collection<DrillbitEndpoint> getActiveEndpoints(){
    return drillbitContext.getBits();
  }
  
  public PhysicalPlanReader getPlanReader(){
    return drillbitContext.getPlanReader();
  }
  
  public BitCom getBitCom(){
    return drillbitContext.getBitCom();
  }
  
  public DrillConfig getConfig(){
    return drillbitContext.getConfig();
  }
  
}
