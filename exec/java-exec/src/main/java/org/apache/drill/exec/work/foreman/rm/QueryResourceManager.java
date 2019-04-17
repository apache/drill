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
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.resourcemgr.NodeResources;
import org.apache.drill.exec.resourcemgr.config.QueryQueueConfig;
import org.apache.drill.exec.resourcemgr.config.exception.QueueSelectionException;
import org.apache.drill.exec.work.foreman.rm.QueryQueue.QueryQueueException;
import org.apache.drill.exec.work.foreman.rm.QueryQueue.QueueTimeoutException;

import java.util.Map;

/**
 * Extends a {@link QueryResourceAllocator} to provide queueing support.
 */

public interface QueryResourceManager {

  enum QueryAdmitResponse {
    UNKNOWN,
    ADMITTED,
    WAIT_FOR_RESPONSE;

    @Override
    public String toString() {
      return name().toLowerCase();
    }
  }

  enum QueryRMState {
    STARTED,
    ENQUEUED,
    ADMITTED,
    RESERVED_RESOURCES,
    RELEASED_RESOURCES,
    DEQUEUED,
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

  void setCost(Map<String, NodeResources> costOnAssignedEndpoints);
  /**
   * Create a parallelizer to parallelize each major fragment of the query into
   * many minor fragments. The parallelizer encapsulates the logic of how much
   * memory and parallelism is required for the query.
   * @param memoryPlanning memory planning needs to be done during parallelization
   * @return
   */
  QueryParallelizer getParallelizer(boolean memoryPlanning);

  /**
   * Admit the query into the cluster. Blocks until the query
   * can run. (Later revisions may use a more thread-friendly
   * approach.)
   * @throws QueryQueueException if something goes wrong with the
   * queue mechanism
   */

  QueryAdmitResponse admit() throws QueueTimeoutException, QueryQueueException;


  /**
   * Returns the name of the queue (if any) on which the query was
   * placed. Valid only after the query is admitted.
   *
   * @return queue name, or null if queuing is not enabled.
   */

  String queueName();


  long queryMemoryPerNode();

  long minimumOperatorMemory();

  /**
   * Updates the state machine of queryRM
   * @param newState new target state
   */
  void updateState(QueryRMState newState);

  /**
   * Called to reserve resources required by query. Updates the queryRM state to RESERVED_RESOURCES if successful
   */
  boolean reserveResources(QueryQueueConfig selectedQueue, UserBitShared.QueryId queryId) throws Exception;

  QueryQueueConfig selectQueue(NodeResources maxNodeResource) throws QueueSelectionException;

  String getLeaderId();
  /**
   * Mark the query as completing, giving up its slot in the
   * cluster. Releases any lease that may be held for a system with queues.
   */

  void exit();
}
