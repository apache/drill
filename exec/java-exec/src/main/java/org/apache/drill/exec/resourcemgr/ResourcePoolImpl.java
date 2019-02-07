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
import org.apache.drill.exec.resourcemgr.selectors.DefaultSelector;
import org.apache.drill.exec.resourcemgr.selectors.ResourcePoolSelector;
import org.apache.drill.exec.resourcemgr.selectors.ResourcePoolSelectorFactory;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ResourcePoolImpl implements ResourcePool {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ResourcePoolImpl.class);

  private String poolName;

  private List<ResourcePool> childPools;

  private double parentResourceShare;

  private double poolResourceShare;

  private QueryQueueConfig assignedQueue;

  private ResourcePoolSelector assignedSelector;

  private NodeResources poolResourcePerNode;

  private static final String POOL_NAME_KEY = "pool_name";

  private static final String POOL_MEMORY_SHARE_KEY = "memory";

  private static final String POOL_CHILDREN_POOLS_KEY = "child_pools";

  private static final String POOL_SELECTOR_KEY = "selector";

  private static final String POOL_QUEUE_KEY = "queue";

  ResourcePoolImpl(Config poolConfig, double poolAbsResourceShare, double parentResourceShare,
                   NodeResources parentNodeResource, Map<String, QueryQueueConfig> leafQueueCollector) {
    this.poolName = poolConfig.getString(POOL_NAME_KEY);
    this.parentResourceShare = parentResourceShare;
    this.poolResourceShare = poolAbsResourceShare * this.parentResourceShare;
    assignedSelector = ResourcePoolSelectorFactory.createSelector(poolConfig.hasPath(POOL_SELECTOR_KEY) ?
      poolConfig.getConfig(POOL_SELECTOR_KEY) : null);
    parseAndCreateChildPools(poolConfig, parentNodeResource, leafQueueCollector);
  }

  @Override
  public String getPoolName() {
    return poolName;
  }

  @Override
  public boolean isLeafPool() {
    return childPools == null;
  }

  @Override
  public boolean isDefaultPool() {
    return (assignedSelector instanceof DefaultSelector);
  }

  @Override
  public long getMaxQueryMemoryPerNode() {
    Preconditions.checkState(isLeafPool() && assignedQueue != null, "max_query_memory_per_node is " +
      "only valid for leaf level pools which has a queue assigned to it [Details: PoolName: %s]", poolName);
    return assignedQueue.getMaxQueryMemoryInMBPerNode();
  }

  @Override
  public boolean checkAndSelectPool(QueueAssignmentResult assignmentResult) {
    return false;
  }

  @Override
  public double getPoolMemoryShare() {
    return poolResourceShare;
  }

  @Override
  public long getPoolMemoryInMB(int numClusterNodes) {
    return poolResourcePerNode.getMemoryInMB() * numClusterNodes;
  }

  @Override
  public void parseAndCreateChildPools(Config poolConfig, NodeResources parentResource,
                                       Map<String, QueryQueueConfig> leafQueueCollector) {
    this.poolResourcePerNode = new NodeResources(Math.round(parentResource.getMemoryInBytes() * poolResourceShare),
      parentResource.getNumVirtualCpu());
    if (poolConfig.hasPath(POOL_CHILDREN_POOLS_KEY)) {
      // TODO: create the child pools here
      childPools = new ArrayList<>();
      List<? extends Config> childPoolsConfig = poolConfig.getConfigList(POOL_CHILDREN_POOLS_KEY);
      logger.debug("Creating {} child pools for parent pool {}", childPoolsConfig.size(), poolName);
      for (Config childConfig : childPoolsConfig) {
        childPools.add(new ResourcePoolImpl(childConfig, childConfig.getDouble(POOL_MEMORY_SHARE_KEY),
          poolResourceShare, poolResourcePerNode, leafQueueCollector));
      }
    } else {
      logger.info("Resource Pool {} is a leaf level pool with queue assigned to it", poolName);
      assignedQueue = new QueryQueueConfigImpl(poolConfig.getConfig(POOL_QUEUE_KEY), poolName, poolResourcePerNode);
    }
  }

  @Override
  public QueryQueueConfig getQueuryQueue() {
    Preconditions.checkState(isLeafPool() && assignedQueue != null, "QueryQueue is only " +
        "valid for leaf level pools.[Details: PoolName: %s]", poolName);
    return assignedQueue;
  }
}
