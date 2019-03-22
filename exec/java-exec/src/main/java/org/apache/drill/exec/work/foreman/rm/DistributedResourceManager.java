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

import avro.shaded.com.google.common.annotations.VisibleForTesting;
import org.apache.drill.common.DrillNode;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.coord.zk.ZKClusterCoordinator;
import org.apache.drill.exec.exception.StoreException;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.planner.fragment.DistributedQueueParallelizer;
import org.apache.drill.exec.planner.fragment.QueryParallelizer;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.UserBitShared.QueryResult.QueryState;
import org.apache.drill.exec.proto.helper.QueryIdHelper;
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
import org.apache.drill.exec.work.foreman.DrillbitStatusListener;
import org.apache.drill.exec.work.foreman.Foreman;
import org.apache.drill.exec.work.foreman.rm.QueryQueue.QueryQueueException;
import org.apache.drill.exec.work.foreman.rm.QueryQueue.QueueTimeoutException;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.drill.shaded.guava.com.google.common.base.Stopwatch;

import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.apache.drill.exec.ExecConstants.RM_WAIT_THREAD_INTERVAL;


public class DistributedResourceManager implements ResourceManager {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DistributedResourceManager.class);

  private final ResourcePoolTree rmPoolTree;

  private final DrillbitContext context;

  public final long memoryPerNode;

  private final int cpusPerNode;

  private final Thread waitQueueThread;

  private volatile AtomicBoolean exitDaemonThreads = new AtomicBoolean(false);

  private final RMBlobStoreManager rmBlobStoreManager;

  // Wait queues for each queue which holds queries that are admitted by leader but not yet executed because resource
  // is unavailable
  private final Map<String, PriorityQueue<DistributedQueryRM>> waitingQueuesForAdmittedQuery = new ConcurrentHashMap<>();

  // Comparator used in priority max-wait queues for each queue such that query which came in first is at the top of
  // the queue. Query which came first will have highest elapsed time
  private static final Comparator<DistributedQueryRM> waitTimeComparator = (DistributedQueryRM d1, DistributedQueryRM
  d2) -> Long.compare(d2.elapsedWaitTime(), d1.elapsedWaitTime());

  private final Queue<DistributedQueryRM> queryRMCleanupQueue = new ConcurrentLinkedQueue<>();

  private final Thread queryRMCleanupThread;

  public DistributedResourceManager(DrillbitContext context) throws DrillRuntimeException {
    try {
      memoryPerNode = DrillConfig.getMaxDirectMemory();
      cpusPerNode = Runtime.getRuntime().availableProcessors();
      this.context = context;
      final DrillConfig rmConfig = DrillConfig.createForRM();
      rmPoolTree = new ResourcePoolTreeImpl(rmConfig, DrillConfig.getMaxDirectMemory(),
        Runtime.getRuntime().availableProcessors(), 1);
      logger.debug("Successfully parsed RM config \n{}", rmConfig.getConfig(ResourcePoolTreeImpl.ROOT_POOL_CONFIG_KEY));
      Set<String> leafQueues = rmPoolTree.getAllLeafQueues().keySet();
      for (String leafQueue : leafQueues) {
        waitingQueuesForAdmittedQuery.put(leafQueue, new PriorityQueue<>(waitTimeComparator));
      }
      this.rmBlobStoreManager = new RMConsistentBlobStoreManager(context, rmPoolTree);

      // Register the DrillbitStatusListener which registers the localBitResourceShare
      context.getClusterCoordinator().addDrillbitStatusListener(
        new RegisterLocalBitResources(context, rmPoolTree, rmBlobStoreManager));

      // calculate wait interval
      final int waitThreadInterval = calculateWaitInterval(rmConfig, rmPoolTree.getAllLeafQueues().values());
      logger.debug("Wait thread refresh interval is set as {}", waitThreadInterval);
      // start the wait thread
      this.waitQueueThread = startDaemonThreads(WaitQueueThread.class, waitThreadInterval);
      // start the cleanup thread
      queryRMCleanupThread = startDaemonThreads(CleanupThread.class, waitThreadInterval);
    } catch (RMConfigException ex) {
      throw new DrillRuntimeException(String.format("Failed while parsing Drill RM Configs. Drillbit won't be started" +
        " unless config is fixed or RM is disabled by setting %s to false", ExecConstants.RM_ENABLED), ex);
    } catch (StoreException ex) {
      throw new DrillRuntimeException("Failed while creating the blob store manager for managing RM state blobs", ex);
    }
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

  @VisibleForTesting
  public ResourcePoolTree getRmPoolTree() {
    return rmPoolTree;
  }

  @Override
  public void close() {
    // interrupt the wait thread
    exitDaemonThreads.set(true);
    waitQueueThread.interrupt();
    queryRMCleanupThread.interrupt();

    // Clear off the QueryRM for admitted queries which are in waiting state. This should be fine even in case of
    // graceful shutdown since other bits will get notification as bit going down and will update the cluster state
    // accordingly
    // TODO: Fix race condition between wait thread completing to process waitQueryRM and again putting back the
    // object in the queue. In parallel close thread clearing off the queue
    waitingQueuesForAdmittedQuery.clear();
  }

  /**
   * Calculates the refresh interval for the wait thread which process all the admitted queries by leader but are
   * waiting on Foreman node for resource availability. If all the queues wait timeout is set to 0 then there won't
   * be any queries in the wait queue and refresh interval will be half of MAX_WAIT_TIMEOUT. Otherwise it will be
   * half of minimum of waiting time across all queues.
   * @param rmConfig rm configurations
   * @param leafQueues configured collection of leaf pools or queues
   * @return refresh interval for wait thread
   */
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
    final int halfMinWaitInterval = minWaitInterval / 2;
    return (halfMinWaitInterval == 0) ? minWaitInterval : halfMinWaitInterval;
  }

  private Thread startDaemonThreads(Class<? extends Thread> threadClass, Integer interval) {
    try {
      final Constructor threadConstructor = threadClass.getConstructor(DistributedResourceManager.class, Integer.class);
      final Thread threadToCreate = (Thread) threadConstructor.newInstance(this, interval);
      threadToCreate.setDaemon(true);
      threadToCreate.start();
      return threadToCreate;
    } catch (Exception ex) {
      throw new DrillRuntimeException(String.format("Failed to create %s daemon thread for Distributed RM",
        threadClass.getName()), ex);
    }
  }

  private void addToWaitingQueue(final QueryResourceManager queryRM) {
    final DistributedQueryRM distributedQueryRM = (DistributedQueryRM)queryRM;
    final String queueName = distributedQueryRM.queueName();
    synchronized (waitingQueuesForAdmittedQuery) {
      final PriorityQueue<DistributedQueryRM> waitingQueue = waitingQueuesForAdmittedQuery.get(queueName);
      waitingQueue.add(distributedQueryRM);
      logger.info("Count of times queryRM for the query {} is added in the wait queue is {}",
        ((DistributedQueryRM) queryRM).queryIdString, distributedQueryRM.incrementAndGetWaitRetryCount());
    }
  }

  private void reserveResources(Map<String, NodeResources> queryResourceAssignment,
                                QueryQueueConfig selectedQueue, String leaderId, String queryId,
                                String foremanUUID) throws Exception {
    logger.info("Reserving resources for query {}. [Details: ResourceMap: {}]", queryId,
      queryResourceAssignment.toString());
    rmBlobStoreManager.reserveResources(queryResourceAssignment, selectedQueue, leaderId, queryId, foremanUUID);
  }

  private String freeResources(Map<String, NodeResources> queryResourceAssignment, QueryQueueConfig selectedQueue,
                             String leaderId, String queryId, String foremanUUID) throws Exception {
    logger.info("Free resources for query {}. [Details: ResourceMap: {}]", queryId, queryResourceAssignment.toString());
    return rmBlobStoreManager.freeResources(queryResourceAssignment, selectedQueue, leaderId, queryId, foremanUUID);
  }

  public static class DistributedQueryRM implements QueryResourceManager {

    private final DistributedResourceManager drillRM;

    private final QueryContext queryContext;

    private final Foreman foreman;

    private final String foremanUUID;

    private final String queryIdString;

    private QueryRMState currentState;

    private Stopwatch waitStartTime;

    private Map<String, NodeResources> assignedEndpointsCost;

    private QueryQueueConfig selectedQueue;

    private String admittedLeaderUUID;

    private String currentQueueLeader;

    private int cleanupTryCount;

    private int retryCountAfterWaitQueue;

    DistributedQueryRM(ResourceManager resourceManager, Foreman queryForeman) {
      Preconditions.checkArgument(resourceManager instanceof DistributedResourceManager);
      this.drillRM = (DistributedResourceManager) resourceManager;
      this.queryContext = queryForeman.getQueryContext();
      this.foreman = queryForeman;
      this.queryIdString = QueryIdHelper.getQueryId(queryContext.getQueryId());
      currentState = QueryRMState.STARTED;
      foremanUUID = queryContext.getOnlineEndpointNodeUUIDs().get(queryContext.getCurrentEndpointNode());
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
      Preconditions.checkState(selectedQueue != null, "Queue is not yet selected for this query");
      return selectedQueue.getMaxQueryMemoryInMBPerNode();
    }

    @Override
    public long minimumOperatorMemory() {
      return queryContext.getOption(ExecConstants.MIN_MEMORY_PER_BUFFERED_OP_KEY).num_val;
    }

    @Override
    public QueryParallelizer getParallelizer(boolean planHasMemory) {
      // currently memory planning is disabled. Enable it once the RM functionality is fully implemented.
      return new DistributedQueueParallelizer(planHasMemory, this.queryContext, this);
    }

    @Override
    public QueryAdmitResponse admit() throws QueueTimeoutException, QueryQueueException {
      // TODO: for now it will just return ADMITTED since leader election is not available
      // Once leader election support is there we will throw exception in case of error
      // otherwise just return
      Preconditions.checkState(selectedQueue != null, "Query is being admitted before selecting " +
        "a queue for it");
      updateState(QueryRMState.ENQUEUED);
      return QueryAdmitResponse.ADMITTED;
    }

    @Override
    public String queueName() {
      Preconditions.checkState(selectedQueue != null, "Queue is not selected yet");
      return selectedQueue.getQueueName();
    }

    @Override
    public QueryQueueConfig selectQueue(NodeResources maxNodeResource)  throws QueueSelectionException {
      if (selectedQueue != null) {
        return selectedQueue;
      }

      selectedQueue = drillRM.rmPoolTree.selectOneQueue(queryContext, maxNodeResource);
      // TODO: Set the LeaderUUID based on the selected queue
      admittedLeaderUUID = foremanUUID;
      currentQueueLeader = admittedLeaderUUID;
      logger.info("Selected queue {} for query {} with leader {}", selectedQueue.getQueueName(), queryIdString,
        admittedLeaderUUID);
      return selectedQueue;
    }

    @Override
    public String getLeaderId() {
      return admittedLeaderUUID;
    }

    public boolean reserveResources() throws Exception {
      try {
        Preconditions.checkState(selectedQueue != null, "A queue is not selected for the query " +
          "before trying to reserve resources for this query");
        Preconditions.checkState(assignedEndpointsCost != null,
          "Cost of the query is not set before calling reserve resources");
        drillRM.reserveResources(assignedEndpointsCost, selectedQueue, admittedLeaderUUID, queryIdString, foremanUUID);
        updateState(QueryRMState.RESERVED_RESOURCES);
        return true;
      } catch (ResourceUnavailableException ex) {
        // add the query to the waiting queue for retry and set the wait time if not already done
        if (waitStartTime == null) {
          waitStartTime = Stopwatch.createStarted();
        }
        // Check if wait time has expired before adding in waiting queue
        final long timeElapsedWaiting = elapsedWaitTime();
        if (timeElapsedWaiting >= selectedQueue.getWaitTimeoutInMs()) {
          // timeout has expired so don't put in waiting queue
          throw new QueueWaitTimeoutExpired(String.format("Failed to reserve resources for the query and the wait " +
            "timeout is also expired. [Details: QueryId: %s, Queue: %s, ElapsedTime: %d",
            queryIdString, selectedQueue.getQueueName(), timeElapsedWaiting), ex);
        }
        drillRM.addToWaitingQueue(this);
        return false;
      } catch (Exception ex) {
        logger.error("Failed while reserving resources for this query", ex);
        throw ex;
      }
    }

    @Override
    public void updateState(QueryRMState newState) {
      boolean isSuccessful = false;
      switch (currentState) {
        case STARTED:
          isSuccessful = (newState == QueryRMState.ENQUEUED || newState == QueryRMState.FAILED);
          break;
        case ENQUEUED:
          isSuccessful = (newState == QueryRMState.ADMITTED || newState == QueryRMState.FAILED);
          break;
        case ADMITTED:
          isSuccessful = (newState == QueryRMState.RESERVED_RESOURCES || newState == QueryRMState.DEQUEUED);
          break;
        case RESERVED_RESOURCES:
          isSuccessful = (newState == QueryRMState.RELEASED_RESOURCES);
          break;
        case RELEASED_RESOURCES:
          isSuccessful = (newState == QueryRMState.DEQUEUED);
          break;
        case DEQUEUED:
          isSuccessful = (newState == QueryRMState.COMPLETED);
          break;
      }

      final String logString = String.format("QueryRM state transition from %s --> %s is %s",
        currentState.toString(), newState.toString(), isSuccessful ? "successful" : "failed");
      if (isSuccessful) {
        this.currentState = newState;
        logger.info(logString);
        return;
      }

      throw new IllegalStateException(logString);
    }

    @VisibleForTesting
    public QueryRMState getCurrentState() {
      return currentState;
    }

    /**
     * Exit on queryRM will only be called from ForemanResult::close() in case when query either fails or completes
     * or is cancelled.
     * When query fails/completes/cancel then it will never be in the wait queue. Hence exit should not worry about
     * removing the queryRM object from wait queue.
     * TODO: Incomplete because protocol to send message to leader is unavailable
     */
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

      switch (currentState) {
        case ADMITTED:
          // send message to admittedQueueLeader about completion of this query so that it can release it's local queue
          // slot. This send should be a sync call. If send of message fails then add this query back to
          // queryRMCleanupQueue. If send failure happens because of leader change then ignore the failure
          updateState(QueryRMState.DEQUEUED);
          break;
        case RESERVED_RESOURCES:
          // try to release the resources and update state on Zookeeper
          try {
            currentQueueLeader = drillRM.freeResources(assignedEndpointsCost, selectedQueue, admittedLeaderUUID,
              queryIdString, foremanUUID);
            // successfully released resources so update the state
            updateState(QueryRMState.RELEASED_RESOURCES);
          } catch (Exception ex) {
            logger.info("Failed while freeing resources for this query {} in queryRM exit for {} time", queryIdString,
              incrementAndGetCleanupCount());
            drillRM.queryRMCleanupQueue.add(this);
            return;
          }
        case RELEASED_RESOURCES:
          // send message to currentQueueLeader about completion of this query so that it can release it's local queue
          // slot. This send should be a sync call. If send of message fails then add this query back to
          // queryRMCleanupQueue. If send failure happens because of leader change then ignore the failure
          updateState(QueryRMState.DEQUEUED);
          break;
        case STARTED:
        case ENQUEUED:
          Preconditions.checkState(foreman.getState() == QueryState.FAILED, "QueryRM exit is " +
            "called in an unexpected query state. [Details: QueryRM state: %s, Query State: %s]",
            currentState, foreman.getState());
          updateState(QueryRMState.FAILED);
          return;
        default:
          throw new IllegalStateException("QueryRM exit is called in unexpected state. Looks like something is wrong " +
            "with internal state!!");
      }
      updateState(QueryRMState.COMPLETED);
    }

    private long elapsedWaitTime() {
      return waitStartTime.elapsed(TimeUnit.MILLISECONDS);
    }

    private int incrementAndGetCleanupCount() {
      ++cleanupTryCount;
      return cleanupTryCount;
    }

    private int incrementAndGetWaitRetryCount() {
      ++retryCountAfterWaitQueue;
      return retryCountAfterWaitQueue;
    }
  }

  /**
   * All queries which are in admitted state but are not able to reserve resources will be in this queue and process
   * by the wait thread. When query is in wait thread in can never fail since it's not running and cancellation will
   * wait for it to go in running state.
   */
  private class WaitQueueThread extends Thread {
    private final int refreshInterval;

    public WaitQueueThread(Integer waitInterval) {
      refreshInterval = waitInterval;
      setName("DistributedResourceManager.WaitThread");
    }

    // TODO: Incomplete
    @Override
    public void run() {
      while (!exitDaemonThreads.get()) {
        try {
          synchronized (waitingQueuesForAdmittedQuery) {
           for (PriorityQueue<DistributedQueryRM> queue : waitingQueuesForAdmittedQuery.values()) {
             // get the initial queue count such that we only try to dequeue that many query only since newly dequeued
             // query can also meanwhile come back to this queue.
             final int queueSize = queue.size();
             while(queueSize > 0) {
               final DistributedQueryRM queryRM = queue.poll();
               context.getExecutor().submit(queryRM.foreman);
             }
           }
          }
          Thread.sleep(refreshInterval);
        } catch (InterruptedException ex) {
          logger.error("Thread {} is interrupted", getName());
        }
      }
    }
  }

  /**
   * All the completed queries whose result is sent back to client but during cleanup encountered some issues will be
   * present in the queryRMCleanupQueue for the lifetime of this Drillbit. These queryRM object will be tried for
   * cleanup since that affect the state of the cluster
   */
  private class CleanupThread extends Thread {
    private final int refreshTime;

    public CleanupThread(Integer refreshInterval) {
      this.refreshTime = refreshInterval;
      setName("DistributedResourceManager.CleanupThread");
    }

    @Override
    public void run() {
      while(!exitDaemonThreads.get()) {
        try {
          int queryRMCount = queryRMCleanupQueue.size();

          while (queryRMCount > 0) {
            --queryRMCount;
            final DistributedQueryRM queryRM = queryRMCleanupQueue.poll();
            queryRM.exit();
          }

          // wait here for some time
          Thread.sleep(refreshTime);
        } catch (InterruptedException ex) {
          logger.error("Thread {} is interrupted", getName());
        }
      }
    }
  }

  public static class RegisterLocalBitResources implements DrillbitStatusListener {

    private final DrillNode localEndpointNode;

    private final RMBlobStoreManager storeManager;

    private final NodeResources localBitResourceShare;

    private Set<String> leafQueues;

    private final ZKClusterCoordinator coord;

    private final DrillbitContext context;

    public RegisterLocalBitResources(DrillbitContext context, ResourcePoolTree rmPoolTree,
                                     RMBlobStoreManager storeManager) {
      this.localEndpointNode = DrillNode.create(context.getEndpoint());
      this.localBitResourceShare = rmPoolTree.getRootPoolResources();
      this.storeManager = storeManager;
      this.coord = (ZKClusterCoordinator) context.getClusterCoordinator();
      this.context = context;
      this.leafQueues = rmPoolTree.getAllLeafQueues().keySet();
    }

    @Override
    public void drillbitUnregistered(Map<DrillbitEndpoint, String> unregisteredDrillbitsUUID) {
      // no-op for now. May be we can use this to handler failure scenarios of bit going down
    }

    @Override
    public void drillbitRegistered(Map<DrillbitEndpoint, String> registeredDrillbitsUUID) {
      // Check if in registeredDrillbits local drillbit is present and with state as ONLINE since this listener
      // will be invoked for every state change as well
      // TODO: This can be improved once DrillNode is used everywhere instead of DrillbitEndpoint
      final Map<DrillNode, String> registeredNodeUUID = registeredDrillbitsUUID.entrySet().stream()
        .collect(Collectors.toMap(x -> DrillNode.create(x.getKey()), Map.Entry::getValue));
      final Map<String, DrillbitEndpoint> uuidToEndpoint = registeredDrillbitsUUID.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));

      try {
        if (registeredNodeUUID.containsKey(localEndpointNode)) {
          final String localBitUUID = registeredNodeUUID.get(localEndpointNode);
          final DrillbitEndpoint localEndpoint = uuidToEndpoint.get(localBitUUID);

          if (localEndpoint.getState() == DrillbitEndpoint.State.ONLINE) {
            storeManager.registerResource(localBitUUID, localBitResourceShare);
            logger.info("Registering local bit resource share");

            // TODO: Temp update queue leaders as self
            for (String queueName : leafQueues) {
              storeManager.updateLeadershipInformation(queueName, localBitUUID);
            }
          }
        }
      } catch (Exception ex) {
        // fails to register local bit resources to zookeeper
        logger.error("Fails to register local bit resource share so unregister local Drillbit");
        // below getRegistrationHandle blocks until registration handle is set, if already set then return immediately
        coord.unregister(context.getRegistrationHandle());
      }
    }
  }
}
