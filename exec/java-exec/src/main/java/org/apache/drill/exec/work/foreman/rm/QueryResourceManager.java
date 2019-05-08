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
package org.apache.drill.exec.work.foreman.rm;

import org.apache.drill.exec.planner.fragment.QueryParallelizer;
import org.apache.drill.exec.resourcemgr.NodeResources;
import org.apache.drill.exec.resourcemgr.config.QueryQueueConfig;
import org.apache.drill.exec.resourcemgr.config.exception.QueueSelectionException;
import org.apache.drill.exec.work.foreman.rm.QueryQueue.QueryQueueException;
import org.apache.drill.exec.work.foreman.rm.QueryQueue.QueueTimeoutException;

import java.util.Map;

public interface QueryResourceManager {

  /**
   * Responses which {@link QueryResourceManager#admit()} method can return to the caller
   */
  enum QueryAdmitResponse {
    UNKNOWN,
    ADMITTED,
    WAIT_FOR_RESPONSE;

    @Override
    public String toString() {
      return name().toLowerCase();
    }
  }

  /**
   * State machine of the QueryResourceManager
   */
  enum QueryRMState {
    STARTED,
    ENQUEUED,
    ADMITTED,
    RESERVED_RESOURCES,
    RELEASED_RESOURCES,
    DEQUEUED,
    FAILED,
    COMPLETED;

    @Override
    public String toString() {
      return name().toLowerCase();
    }
  }

  /**
   * Hint that this resource manager queues. Allows the Foreman
   * to short-circuit expensive logic if no queuing will actually
   * be done. This is a static attribute per Drillbit run.
   */
  boolean hasQueue();

  /**
   * For some cases the foreman does not have a full plan, just a cost. In
   * this case, this object will not plan memory, but still needs the cost
   * to place the job into the correct queue.
   * @param cost
   */
  void setCost(double cost);

  /**
   * Cost of query in terms of DrillbitEndpoint UUID to resources required by all minor fragments of this query
   * which will run on that DrillbitEndpoint. {@link QueryParallelizer} calculates this costs based on it's own
   * heuristics for each query and sets it for the ResourceManager.
   * @param costOnAssignedEndpoints map of DrillbitEndpointUUID to resources required by this query on that node
   */
  void setCost(Map<String, NodeResources> costOnAssignedEndpoints);

  /**
   * Create a parallelizer to parallelize each major fragment of the query into
   * many minor fragments. The parallelizer encapsulates the logic of how much
   * memory and parallelism is required for the query.
   * @param memoryPlanning memory planning needs to be done during parallelization
   * @return {@link QueryParallelizer} to use
   */
  QueryParallelizer getParallelizer(boolean memoryPlanning);

  /**
   * Admit the query into the cluster. Can be sync or async call which depends upon the implementation. Caller should
   * use returned response to take necessary action
   * @return {@link QueryAdmitResponse} response for the admit call
   * @throws QueryQueueException if something goes wrong with the queue mechanism
   * @throws QueueTimeoutException if admit requests times out
   */
  QueryAdmitResponse admit() throws QueueTimeoutException, QueryQueueException;

  /**
   * Returns the name of the queue (if any) on which the query was placed.
   * @return queue name, or null if queue is not supported
   */
  String queueName();

  /**
   * @return max memory a query can use on a node
   */
  long queryMemoryPerNode();

  /**
   * TODO: Try to deprecate this api since it's only used by ThrottledResourceManager. It can be replaced by per
   * operator minimum memory which will be added with DistributedResourceManager support.
   * @return minimum memory required by buffered operator
   */
  long minimumOperatorMemory();

  /**
   * Updates the state machine of queryRM
   * @param newState new target state
   */
  void updateState(QueryRMState newState);

  /**
   * Called to reserve resources required by query in a state store. This will help to make decisions for future queries
   * based on the information in state store about the available resources in the cluster.
   * @return true successfully reserved resources, false failure while reserving resources
   * @throws Exception in case of non transient failure
   */
  boolean reserveResources() throws Exception;

  /**
   * Select a queue out of all configured ones for this query. The selected queue config will be later used to make
   * decisions about resource assignment to this query.
   * @param maxNodeResource maximum resources which this query needs across all assigned endpoints
   * @return configuration of selected queue for this query
   * @throws QueueSelectionException
   */
  QueryQueueConfig selectQueue(NodeResources maxNodeResource) throws QueueSelectionException;

  /**
   * TODO: Check if this api is needed ?
   * @return leader of selected queue to which admit request will be sent
   */
  String getLeaderId();
  /**
   * Mark the query as completing, giving up its slot in the
   * cluster. Releases any lease that may be held for a system with queues.
   */

  void exit();
}
