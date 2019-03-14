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

import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.planner.fragment.ZKQueueParallelizer;
import org.apache.drill.exec.planner.fragment.QueryParallelizer;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.work.foreman.Foreman;
import org.apache.drill.exec.work.foreman.rm.QueryQueue.QueryQueueException;
import org.apache.drill.exec.work.foreman.rm.QueryQueue.QueueLease;
import org.apache.drill.exec.work.foreman.rm.QueryQueue.QueueTimeoutException;

/**
 * Global resource manager that provides basic admission control (AC) via a
 * configured queue: either the Zookeeper-based distributed queue or the
 * in-process embedded Drillbit queue. The queue places an upper limit on the
 * number of running queries. This limit then "slices" memory and CPU between
 * queries: each gets the same share of resources.
 * <p>
 * This is a "basic" implementation. Clearly, a more advanced implementation
 * could look at query cost to determine whether to give a given query more or
 * less than the "standard" share. That is left as a future exercise; in this
 * version we just want to get the basics working.
 * <p>
 * This is the resource manager level. This resource manager is paired with a
 * queue implementation to produce a complete solution. This composition-based
 * approach allows sharing of functionality across queue implementations.
 */

public class ThrottledResourceManager extends AbstractResourceManager {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ThrottledResourceManager.class);

  /**
   * Per-query resource manager. Handles resources and optional queue lease for
   * a single query. As such, this is a non-shared resource: it is associated
   * with a Foreman: a single thread at plan time, and a single event (in some
   * thread) at query completion time. Because of these semantics, no
   * synchronization is needed within this class.
   */

  public static class QueuedQueryResourceManager implements QueryResourceManager {

    private final Foreman foreman;
    private QueueLease lease;
    private final QueryContext queryContext;
    private double queryCost;
    private final ThrottledResourceManager rm;

    public QueuedQueryResourceManager(final ThrottledResourceManager rm,
        final Foreman foreman) {
      this.foreman = foreman;
      this.queryContext = foreman.getQueryContext();
      this.rm = rm;
    }

    @Override
    public void setCost(double cost) {
      this.queryCost = cost;
    }

    @Override
    public QueryParallelizer getParallelizer(boolean planHasMemory) {
      // currently memory planning is disabled. Enable it once the RM functionality is fully implemented.
      return new ZKQueueParallelizer(planHasMemory, this, this.queryContext);
    }

    @Override
    public void admit() throws QueueTimeoutException, QueryQueueException {
      lease = rm.queue().enqueue(foreman.getQueryId(), queryCost);
    }

    public long queryMemoryPerNode() {

      if (lease == null) {
        return rm.defaultQueryMemoryPerNode(queryCost);
      }

      // Use actual memory assigned to this query.

      return lease.queryMemoryPerNode();
    }

    @Override
    public long minimumOperatorMemory() {
      return rm.minimumOperatorMemory();
    }

    @Override
    public void exit() {
      if (lease != null) {
        lease.release();
      }
      lease = null;
    }

    @Override
    public boolean hasQueue() { return true; }

    @Override
    public String queueName() {
      return lease == null ? null : lease.queueName();
    }
  }

  private final QueryQueue queue;

  public ThrottledResourceManager(final DrillbitContext drillbitContext,
      final QueryQueue queue) {
    super(drillbitContext);
    this.queue = queue;
    queue.setMemoryPerNode(memoryPerNode());
  }

  public long minimumOperatorMemory() {
    return queue.minimumOperatorMemory();
  }

  public long defaultQueryMemoryPerNode(double cost) {
    return queue.defaultQueryMemoryPerNode(cost);
  }

  public QueryQueue queue() { return queue; }

  @Override
  public QueryResourceManager newQueryRM(Foreman foreman) {
    return new QueuedQueryResourceManager(this, foreman);
  }

  @Override
  public void close() {
    queue.close();
  }
}
