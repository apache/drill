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

import java.io.Closeable;
import java.util.List;

import org.apache.drill.common.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.exception.DrillbitStartupException;
import org.apache.drill.exec.proto.ExecProtos.PlanFragment;
import org.apache.drill.exec.proto.ExecProtos.WorkQueueStatus;


public interface DistributedCache extends Closeable{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DistributedCache.class);
  
  public void run(DrillbitEndpoint endpoint) throws DrillbitStartupException;
  
  public void saveOptimizedPlan(TemplatizedLogicalPlan logical, TemplatizedPhysicalPlan physical);
  public TemplatizedPhysicalPlan getOptimizedPlan(TemplatizedLogicalPlan logical);
  
  public void updateLocalQueueLength(int length);
  public List<WorkQueueStatus> getQueueLengths(); 
  
  public PlanFragment getFragment(long fragmentId);
  public void storeFragment(PlanFragment fragment);
}
