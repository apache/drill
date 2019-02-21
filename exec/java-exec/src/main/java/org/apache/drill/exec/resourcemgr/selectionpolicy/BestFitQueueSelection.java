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
package org.apache.drill.exec.resourcemgr.selectionpolicy;

import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.resourcemgr.NodeResources;
import org.apache.drill.exec.resourcemgr.QueryQueueConfig;
import org.apache.drill.exec.resourcemgr.ResourcePool;
import org.apache.drill.exec.resourcemgr.exception.QueueSelectionException;

import java.util.Comparator;
import java.util.List;

/**
 * Helps to select a queue whose {@link QueryQueueConfig#getMaxQueryMemoryInMBPerNode()} is nearest to the max memory
 * on a node required by the given query. Nearest is found by following rule:
 * <ul>
 *   <li>
 *     Queue whose {@link org.apache.drill.exec.resourcemgr.QueryQueueConfigImpl#MAX_QUERY_MEMORY_PER_NODE_KEY} is
 *     equal to max memory per node of given query
 *   </li>
 *   <li>
 *     Queue whose {@link org.apache.drill.exec.resourcemgr.QueryQueueConfigImpl#MAX_QUERY_MEMORY_PER_NODE_KEY} is
 *     just greater than max memory per node of given query. From all queues whose max_query_memory_per_node is
 *     greater than what is needed by the query, the queue with minimum value is chosen.
 *   </li>
 *   <li>
 *     Queue whose {@link org.apache.drill.exec.resourcemgr.QueryQueueConfigImpl#MAX_QUERY_MEMORY_PER_NODE_KEY} is
 *     just less than max memory per node of given query. From all queues whose max_query_memory_per_node is
 *     less than what is needed by the query, the queue with maximum value is chosen.
 *   </li>
 * </ul>
 */
public class BestFitQueueSelection extends AbstractQueueSelectionPolicy {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BestFitQueueSelection.class);

  public BestFitQueueSelection() {
    super("bestfit");
  }

  /**
   * Comparator used to sort the leaf ResourcePool lists based on
   * {@link org.apache.drill.exec.resourcemgr.QueryQueueConfigImpl#MAX_QUERY_MEMORY_PER_NODE_KEY}
   */
  private static class BestFitComparator implements Comparator<ResourcePool> {
    @Override
    public int compare(ResourcePool o1, ResourcePool o2) {
      long pool1Value = o1.getQueuryQueue().getMaxQueryMemoryInMBPerNode();
      long pool2Value = o2.getQueuryQueue().getMaxQueryMemoryInMBPerNode();
      if (pool1Value == pool2Value) {
        return 0;
      } else if (pool1Value < pool2Value) {
        return -1;
      } else {
        return 1;
      }
    }
  }

  @Override
  public ResourcePool selectQueue(List<ResourcePool> allPools, QueryContext queryContext,
                                  NodeResources maxResourcePerNode) throws QueueSelectionException {
    if (allPools.isEmpty()) {
      throw new QueueSelectionException(String.format("There is no pools to select the best fit pool for the query: %s",
        queryContext.getQueryId()));
    }

    allPools.sort(new BestFitComparator());
    final long queryMaxNodeMemory = maxResourcePerNode.getMemoryInMB();
    ResourcePool selectedPool = null;
    for (ResourcePool pool : allPools) {
      selectedPool = pool;
      long poolMaxNodeMem = pool.getQueuryQueue().getMaxQueryMemoryInMBPerNode();
      if (poolMaxNodeMem == queryMaxNodeMemory) {
        break;
      } else if (poolMaxNodeMem > queryMaxNodeMemory) {
        break;
      }
    }
    return selectedPool;
  }
}
