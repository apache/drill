/*
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
package org.apache.drill.exec.resourcemgr;

import com.typesafe.config.Config;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.resourcemgr.exception.RMConfigException;
import org.apache.drill.exec.resourcemgr.selectors.ResourcePoolSelector;

import java.util.List;
import java.util.Map;

public interface ResourcePool {
  String getPoolName();

  boolean isLeafPool();

  boolean isDefaultPool();

  // Only valid for leaf pool since it will have a queue assigned to it with this configuration
  long getMaxQueryMemoryPerNode();

  boolean visitAndSelectPool(QueueAssignmentResult assignmentResult, QueryContext queryContext);

  double getPoolMemoryShare();

  long getPoolMemoryInMB(int numClusterNodes);

  void parseAndCreateChildPools(Config poolConfig, NodeResources parentResource, Map<String, QueryQueueConfig>
    leadQueueCollector) throws RMConfigException;

  QueryQueueConfig getQueuryQueue();

  String getFullPath();

  ResourcePool getParentPool();

  List<ResourcePool> getChildPools();

  ResourcePoolSelector getSelector();
}
