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

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.planner.fragment.DefaultParallelizer;
import org.apache.drill.exec.planner.fragment.QueryParallelizer;
import org.apache.drill.exec.resourcemgr.NodeResources;
import org.apache.drill.exec.resourcemgr.config.QueryQueueConfig;
import org.apache.drill.exec.resourcemgr.config.exception.QueueSelectionException;
import org.apache.drill.exec.work.foreman.Foreman;

import java.util.Map;

/**
 * Represents a default resource manager for clusters that do not provide query
 * queues. Without queues to provide a hard limit on the query admission rate,
 * the number of active queries must be estimated and the resulting resource
 * allocations will be rough estimates.
 */

public class DefaultResourceManager implements ResourceManager {

  public final long memoryPerNode;

  public final int cpusPerNode;

  public DefaultResourceManager() {
    memoryPerNode = DrillConfig.getMaxDirectMemory();

    // Note: CPUs are not yet used, they will be used in a future
    // enhancement.

    cpusPerNode = Runtime.getRuntime().availableProcessors();
  }

  @Override
  public long memoryPerNode() { return memoryPerNode; }

  @Override
  public int cpusPerNode() { return cpusPerNode; }

  @Override
  public QueryResourceManager newQueryRM(final Foreman foreman) {
    return new DefaultQueryResourceManager(this, foreman);
  }

  public void addToWaitingQueue(final QueryResourceManager queryRM) {
    throw new UnsupportedOperationException("For Default ResourceManager there shouldn't be any query in waiting " +
      "queue");
  }

  @Override
  public void close() { }

  public static class DefaultQueryResourceManager implements QueryResourceManager {
    private final DefaultResourceManager rm;
    private final QueryContext queryContext;

    public DefaultQueryResourceManager(final DefaultResourceManager rm, final Foreman foreman) {
      this.rm = rm;
      this.queryContext = foreman.getQueryContext();
    }

    @Override
    public void setCost(double cost) {
      // no-op
    }

    @Override
    public void setCost(Map<String, NodeResources> costOnAssignedEndpoints) {
      throw new UnsupportedOperationException("DefaultResourceManager doesn't support setting up cost");
    }

    @Override
    public QueryParallelizer getParallelizer(boolean memoryPlanning){
      return new DefaultParallelizer(memoryPlanning, queryContext);
    }

    public QueryAdmitResponse admit() {
      // No queueing by default
      return QueryAdmitResponse.ADMITTED;
    }

    public boolean reserveResources() throws Exception {
      // Resource reservation is not done in this case only estimation is assigned to operator during planning time
      return true;
    }

    @Override
    public QueryQueueConfig selectQueue(NodeResources maxNodeResource)  throws QueueSelectionException {
      throw new UnsupportedOperationException("DefaultResourceManager doesn't support any queues");
    }

    @Override
    public String getLeaderId() {
      throw new UnsupportedOperationException("DefaultResourceManager doesn't support leaders");
    }

    @Override
    public void updateState(QueryRMState newState) {
      // no op since Default QueryRM doesn't have any state machine
    }

    @Override
    public void exit() {
      // No queueing by default
    }

    @Override
    public boolean hasQueue() { return false; }

    @Override
    public String queueName() { return null; }

    @Override
    public long queryMemoryPerNode() {
      return rm.memoryPerNode;
    }

    @Override
    public long minimumOperatorMemory() {
      return 0;
    }
  }
}
