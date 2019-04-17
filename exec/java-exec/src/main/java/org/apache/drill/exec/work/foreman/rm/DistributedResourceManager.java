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
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.StoreException;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.planner.fragment.DistributedQueueParallelizer;
import org.apache.drill.exec.planner.fragment.QueryParallelizer;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.resourcemgr.NodeResources;
import org.apache.drill.exec.resourcemgr.config.QueryQueueConfig;
import org.apache.drill.exec.resourcemgr.config.RMCommonDefaults;
import org.apache.drill.exec.resourcemgr.config.ResourcePoolTree;
import org.apache.drill.exec.resourcemgr.config.ResourcePoolTreeImpl;
import org.apache.drill.exec.resourcemgr.config.exception.QueueSelectionException;
import org.apache.drill.exec.resourcemgr.config.exception.RMConfigException;
import org.apache.drill.exec.resourcemgr.exception.QueueWaitTimeoutExpired;
import org.apache.drill.exec.resourcemgr.rmblobmgr.RMBlobStoreManager;
import org.apache.drill.exec.resourcemgr.rmblobmgr.RMConsistentBlobStoreManager;
import org.apache.drill.exec.resourcemgr.rmblobmgr.exception.ResourceUnavailableException;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.work.foreman.Foreman;
import org.apache.drill.exec.work.foreman.rm.QueryQueue.QueryQueueException;
import org.apache.drill.exec.work.foreman.rm.QueryQueue.QueueTimeoutException;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.drill.shaded.guava.com.google.common.base.Stopwatch;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.drill.exec.ExecConstants.RM_WAIT_THREAD_INTERVAL;


public class DistributedResourceManager implements ResourceManager {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DistributedResourceManager.class);

  private final ResourcePoolTree rmPoolTree;

  private final DrillbitContext context;

  private final DrillConfig rmConfig;

  public final long memoryPerNode;

  public final int cpusPerNode;

  private final WaitQueueThread waitQueueThread;

  private final RMBlobStoreManager rmBlobStoreManager;

  // Wait queues for each queue which holds queries that are admitted by leader but not yet executed because resource
  // is unavailable
  private final Map<String, PriorityQueue<DistributedQueryRM>> waitingQueuesForAdmittedQuery = new HashMap<>();

  // Comparator used in priority max-wait queues for each queue such that query which came in first is at the top of
  // the queue. Query which came first will have highest elapsed time
  private static final Comparator<DistributedQueryRM> waitTimeComparator = (DistributedQueryRM d1, DistributedQueryRM
  d2) -> Long.compare(d2.elapsedWaitTime(), d1.elapsedWaitTime());

  public DistributedResourceManager(DrillbitContext context) throws DrillRuntimeException {
    memoryPerNode = DrillConfig.getMaxDirectMemory();
    cpusPerNode = Runtime.getRuntime().availableProcessors();
    try {
      this.context = context;
      this.rmConfig = DrillConfig.createForRM();
      rmPoolTree = new ResourcePoolTreeImpl(rmConfig, DrillConfig.getMaxDirectMemory(),
        Runtime.getRuntime().availableProcessors(), 1);
      logger.debug("Successfully parsed RM config \n{}", rmConfig.getConfig(ResourcePoolTreeImpl.ROOT_POOL_CONFIG_KEY));
      Set<String> leafQueues = rmPoolTree.getAllLeafQueues().keySet();
      for (String leafQueue : leafQueues) {
        waitingQueuesForAdmittedQuery.put(leafQueue, new PriorityQueue<>(waitTimeComparator));
      }
      this.rmBlobStoreManager = new RMConsistentBlobStoreManager(context, rmPoolTree.getAllLeafQueues().values());

      // start the wait thread
      final int waitThreadInterval = calculateWaitInterval(rmConfig, rmPoolTree.getAllLeafQueues().values());
      logger.debug("Wait thread refresh interval is set as {}", waitThreadInterval);
      this.waitQueueThread = new WaitQueueThread(waitThreadInterval);
      this.waitQueueThread.setDaemon(true);
    } catch (RMConfigException ex) {
      throw new DrillRuntimeException(String.format("Failed while parsing Drill RM Configs. Drillbit won't be started" +
        " unless config is fixed or RM is disabled by setting %s to false", ExecConstants.RM_ENABLED), ex);
    } catch (StoreException ex) {
      throw new DrillRuntimeException("Failed while creating the blob store manager for managing RM state blobs", ex);
    }
  }

  private int calculateWaitInterval(DrillConfig rmConfig, Collection<QueryQueueConfig> leafQueues) {
    if (rmConfig.hasPath(RM_WAIT_THREAD_INTERVAL)) {
      return rmConfig.getInt(RM_WAIT_THREAD_INTERVAL);
    }

    // Otherwise out of all the configured queues use half of the minimum positive wait time as the interval
    int minWaitInterval = RMCommonDefaults.MAX_WAIT_TIMEOUT_IN_MS;
    for (QueryQueueConfig leafQueue : leafQueues) {
      int queueWaitTime = leafQueue.getWaitTimeoutInMs();
      if (queueWaitTime > 0) {
        minWaitInterval = Math.min(minWaitInterval, queueWaitTime);
      }
    }
    return minWaitInterval;
  }

  @Override
  public long memoryPerNode() {
    return memoryPerNode;
  }

  @Override
  public int cpusPerNode() {
    return cpusPerNode;
  }

  @Override
  public QueryResourceManager newQueryRM(Foreman foreman) {
    return new DistributedQueryRM(this, foreman);
  }

  @Override
  public void addToWaitingQueue(final QueryResourceManager queryRM) {
    final DistributedQueryRM distributedQueryRM = (DistributedQueryRM)queryRM;
    final String queueName = distributedQueryRM.queueName();
    final PriorityQueue<DistributedQueryRM> waitingQueue = waitingQueuesForAdmittedQuery.get(queueName);
    waitingQueue.add(distributedQueryRM);
  }

  private void reserveResources(Map<String, NodeResources> queryResourceAssignment,
                                       QueryQueueConfig selectedQueue, String leaderId, String queryId,
                                       String foremanNode) throws Exception {
    //rmBlobStoreManager.reserveResources();
  }

  private void freeResources(Map<String, NodeResources> queryResourceAssignment, QueryQueueConfig selectedQueue,
                     String leaderId, String queryId, String foremanNode) throws Exception {

  }

  public ResourcePoolTree getRmPoolTree() {
    return rmPoolTree;
  }

  @Override
  public void close() {
  }

  public class DistributedQueryRM implements QueryResourceManager {

    private final DistributedResourceManager drillRM;

    private final QueryContext context;

    private final Foreman foreman;

    private QueryRMState currentState;

    private Stopwatch waitStartTime;

    private Map<String, NodeResources> assignedEndpointsCost;

    DistributedQueryRM(ResourceManager resourceManager, Foreman queryForeman) {
      this.drillRM = (DistributedResourceManager) resourceManager;
      this.context = queryForeman.getQueryContext();
      this.foreman = queryForeman;
      currentState = QueryRMState.STARTED;
    }

    @Override
    public boolean hasQueue() {
      return true;
    }

    @Override
    public void setCost(double cost) {
      throw new UnsupportedOperationException("DistributedQueryRM doesn't support cost in double format");
    }

    public void setCost(Map<String, NodeResources> costOnAssignedEndpoints) {
      // Should be called when queryRM is in STARTED state
      Preconditions.checkState(currentState == QueryRMState.STARTED,
        "Cost is being set when queryRM is in %s state", currentState.toString());
      assignedEndpointsCost = costOnAssignedEndpoints;
    }

    public long queryMemoryPerNode() {
      return 0;
    }

    @Override
    public long minimumOperatorMemory() {
      return 0;
    }

    @Override
    public QueryParallelizer getParallelizer(boolean planHasMemory) {
      // currently memory planning is disabled. Enable it once the RM functionality is fully implemented.
      return new DistributedQueueParallelizer(true || planHasMemory, this.context, this);
    }

    @Override
    public QueryAdmitResponse admit() throws QueueTimeoutException, QueryQueueException {
      // TODO: for now it will just return since leader election is not available
      // Once leader election support is there we will throw exception in case of error
      // otherwise just return
      updateState(QueryRMState.ENQUEUED);
      return QueryAdmitResponse.ADMITTED;
    }

    @Override
    public String queueName() {
      return "";
    }

    @Override
    public QueryQueueConfig selectQueue(NodeResources maxNodeResource)  throws QueueSelectionException {
      return drillRM.rmPoolTree.selectOneQueue(context, maxNodeResource);
      //TODO: based on selected queue store the leader UUID as well
    }

    @Override
    public String getLeaderId() {
      // TODO: Return emoty string for now
      return "";
    }

    public boolean reserveResources(QueryQueueConfig selectedQueue, UserBitShared.QueryId queryId) throws Exception {
      try {
        Preconditions.checkState(assignedEndpointsCost != null,
          "Cost of the query is not set before calling reserve resources");
        // TODO: pass the correct parameter values to function below
        drillRM.reserveResources(null, null, null, null, null);
        updateState(QueryRMState.RESERVED_RESOURCES);
        return true;
      } catch (ResourceUnavailableException ex) {
        // add the query to the waiting queue for retry
        // set the wait time if not already done
        if (waitStartTime == null) {
          waitStartTime = Stopwatch.createStarted();
        }
        // Check if wait time has expired before adding in waiting queue
        final long timeElapsedWaiting = elapsedWaitTime();
        if (timeElapsedWaiting >= selectedQueue.getWaitTimeoutInMs()) {
          // timeout has expired so don't put in waiting queue
          throw new QueueWaitTimeoutExpired(String.format("Failed to reserve resources for the query and the wait " +
            "timeout is also expired. [Details: QueryId: %s, Queue: %s, ElapsedTime: %d",
            queryId, selectedQueue.getQueueName(), timeElapsedWaiting), ex);
        }
        drillRM.addToWaitingQueue(this);
        return false;
      } catch (Exception ex) {
        logger.error("Failed while reserving resources for this query", ex);
        throw ex;
      }
    }

    private long elapsedWaitTime() {
      return waitStartTime.elapsed(TimeUnit.MILLISECONDS);
    }

    public void updateState(QueryRMState newState) {
      // no op since Default QueryRM doesn't have any state machine
      // for now we are just overwriting the currentState. May be we can add logic for handling incorrect
      // state transitions and allowed state transitions
      this.currentState = newState;
    }

    @Override
    public void exit() {
      // 1. if queryRM is in admitted state: That means exit is called either when query is failed. When query is
      // cancelled then exit will never be called in ADMITTED state of queryRM. Since then query will be in STARTING
      // state and cancellation_requested event will be queued until query moves to running state.
      //
      // Because of above even though queryRM can be in waiting queue of Resource Pool, it doesn't have any race
      // condition with cancel request. Since cancel will not be processed until queryRM moves to RESERVED_RESOURCES
      // STATE as part of query moving to Running state
      //
      // In the failure case queryRM just needs to send message back to leader to release it's reserved slot. Message
      // will be sent back to leader who admitted the query. So we don't have to read the blob as resources
      // are not reserved yet and running count is not incremented. Also as part of failure since exit will be called
      // on queryRM from failure handling thread, then waiting thread should always check the queryState before
      // trying to reserve resource for it. If query is in terminal state then it should just log it and remove
      // queryRM for that query from the waiting queue.
      //
      // 2. if query is in reserved resources state then update zookeeper to release resources and send message back to
      // current leader to release the slot.
    }
  }

  public static class WaitQueueThread extends Thread {
    private final int refreshInterval;

    public WaitQueueThread(int waitInterval) {
      setName("DistributedResourceManager.WaitThread");
      refreshInterval = waitInterval;
    }

    @Override
    public void run() {
      while (true) {
        try {
          Thread.sleep(refreshInterval);
        } catch (InterruptedException ex) {
          logger.error("Thread {} is interrupted", getName());
          Thread.currentThread().interrupt();
          break;
        }
      }
    }
  }
}
