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

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.resourcemgr.exception.QueueSelectionException;
import org.apache.drill.exec.resourcemgr.selectionpolicy.QueueSelectionPolicy;
import org.apache.drill.exec.resourcemgr.selectionpolicy.QueueSelectionPolicyFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ResourcePoolTreeImpl implements ResourcePoolTree {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ResourcePoolTreeImpl.class);

  private ResourcePool rootPool;

  private DrillConfig rmConfig;

  private NodeResources totalNodeResources;

  private double resourceShare;

  private Map<String, QueryQueueConfig> leafQueues = new HashMap<>();

  private QueueSelectionPolicy selectionPolicy;

  private static final String ROOT_POOL_MEMORY_SHARE_KEY = "memory";

  private static final String ROOT_POOL_QUEUE_SELECTION_POLICY_KEY = "queue_selection_policy";

  ResourcePoolTreeImpl(DrillConfig rmConfig, NodeResources totalNodeResources) {
    this.rmConfig = rmConfig;
    this.totalNodeResources = totalNodeResources;
    this.resourceShare = rmConfig.hasPath(ROOT_POOL_MEMORY_SHARE_KEY) ?
      rmConfig.getDouble(ROOT_POOL_MEMORY_SHARE_KEY) : 1.0;
    this.selectionPolicy = QueueSelectionPolicyFactory.createSelectionPolicy(
      rmConfig.hasPath(ROOT_POOL_QUEUE_SELECTION_POLICY_KEY) ? rmConfig.getString(ROOT_POOL_QUEUE_SELECTION_POLICY_KEY)
        : "");
    rootPool = new ResourcePoolImpl(rmConfig, resourceShare, 1.0, totalNodeResources, leafQueues);
  }

  @Override
  public ResourcePool getRootPool() {
    return rootPool;
  }

  @Override
  public Map<String, QueryQueueConfig> getAllLeafQueues() {
    return leafQueues;
  }

  @Override
  public double getResourceShare() {
    return resourceShare;
  }

  @Override
  public QueueAssignmentResult selectAllQueues(QueryContext queryContext) {
    return null;
  }

  @Override
  public QueryQueueConfig selectOneQueue(QueryContext queryContext) throws QueueSelectionException {
    final QueueAssignmentResult assignmentResult = selectAllQueues(queryContext);
    final List<ResourcePool> selectedPools = assignmentResult.getSelectedLeafPools();
    if (selectedPools.size() == 0) {
      throw new QueueSelectionException(String.format("No resource pool to choose from for this query. [Details: " +
          "QueryId: {}", queryContext.getQueryId()));
    } else if (selectedPools.size() == 1) {
      return selectedPools.get(0).getQueuryQueue();
    }

    return selectionPolicy.selectQueue(selectedPools, queryContext).getQueuryQueue();
  }

  @Override
  public QueueSelectionPolicy getSelectionPolicyInUse() {
    return selectionPolicy;
  }
}
