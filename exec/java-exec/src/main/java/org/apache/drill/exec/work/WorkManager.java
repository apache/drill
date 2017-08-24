/**
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
package org.apache.drill.exec.work;

import com.codahale.metrics.Gauge;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.drill.common.SelfCleaningRunnable;
import org.apache.drill.common.concurrent.ExtendedLatch;
import org.apache.drill.exec.coord.ClusterCoordinator;
import org.apache.drill.exec.metrics.DrillMetrics;
import org.apache.drill.exec.proto.BitControl.FragmentStatus;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.GeneralRPCProtos.Ack;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.helper.QueryIdHelper;
import org.apache.drill.exec.rpc.DrillRpcFuture;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.control.Controller;
import org.apache.drill.exec.rpc.control.WorkEventBus;
import org.apache.drill.exec.rpc.data.DataConnectionCreator;
import org.apache.drill.exec.server.BootStrapContext;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.sys.PersistentStoreProvider;
import org.apache.drill.exec.work.batch.ControlMessageHandler;
import org.apache.drill.exec.work.foreman.Foreman;
import org.apache.drill.exec.work.foreman.QueryManager;
import org.apache.drill.exec.work.fragment.FragmentExecutor;
import org.apache.drill.exec.work.fragment.FragmentManager;
import org.apache.drill.exec.work.user.UserWorker;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;

/**
 * Manages the running fragments in a Drillbit. Periodically requests run-time stats updates from fragments
 * running elsewhere.
 */
public class WorkManager implements AutoCloseable {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(WorkManager.class);

  /*
   * We use a {@see java.util.concurrent.ConcurrentHashMap} because it promises never to throw a
   * {@see java.util.ConcurrentModificationException}; we need that because the statusThread may
   * iterate over the map while other threads add FragmentExecutors via the {@see #WorkerBee}.
   */
  private final Map<FragmentHandle, FragmentExecutor> runningFragments = new ConcurrentHashMap<>();

  private final ConcurrentMap<QueryId, Foreman> queries = Maps.newConcurrentMap();

  private final BootStrapContext bContext;
  private DrillbitContext dContext;

  private final ControlMessageHandler controlMessageWorker;
  private final UserWorker userWorker;
  private final WorkerBee bee;
  private final WorkEventBus workBus;
  private final Executor executor;
  private final StatusThread statusThread;
  private long numOfRunningQueries;
  private long numOfRunningFragments;

  /**
   * How often the StatusThread collects statistics about running fragments.
   */
  private final static int STATUS_PERIOD_SECONDS = 5;

  public WorkManager(final BootStrapContext context) {
    this.bContext = context;
    bee = new WorkerBee(); // TODO should this just be an interface?
    workBus = new WorkEventBus(); // TODO should this just be an interface?
    executor = context.getExecutor();

    // TODO references to this escape here (via WorkerBee) before construction is done
    controlMessageWorker = new ControlMessageHandler(bee); // TODO getFragmentRunner(), getForemanForQueryId()
    userWorker = new UserWorker(bee); // TODO should just be an interface? addNewForeman(), getForemanForQueryId()
    statusThread = new StatusThread();
  }

  public void start(
      final DrillbitEndpoint endpoint,
      final Controller controller,
      final DataConnectionCreator data,
      final ClusterCoordinator coord,
      final PersistentStoreProvider provider,
      final PersistentStoreProvider profilesProvider) {
    dContext = new DrillbitContext(endpoint, bContext, coord, controller, data, workBus, provider, profilesProvider);
    statusThread.start();

    DrillMetrics.register("drill.fragments.running",
        new Gauge<Integer>() {
          @Override
          public Integer getValue() {
            return runningFragments.size();
          }
        });
  }

  public Executor getExecutor() {
    return executor;
  }

  public WorkEventBus getWorkBus() {
    return workBus;
  }

  public ControlMessageHandler getControlMessageHandler() {
    return controlMessageWorker;
  }

  public UserWorker getUserWorker() {
    return userWorker;
  }

  public WorkerBee getBee() {
    return bee;
  }

  @Override
  public void close() throws Exception {
    statusThread.interrupt();

    final long numRunningFragments = runningFragments.size();
    if (numRunningFragments != 0) {
      logger.warn("Closing WorkManager but there are {} running fragments.", numRunningFragments);
      if (logger.isDebugEnabled()) {
        for (final FragmentHandle handle : runningFragments.keySet()) {
          logger.debug("Fragment still running: {} status: {}", QueryIdHelper.getQueryIdentifier(handle),
            runningFragments.get(handle).getStatus());
        }
      }
    }

    if (getContext() != null) {
      getContext().close();
    }
  }

  public DrillbitContext getContext() {
    return dContext;
  }

  private ExtendedLatch exitLatch = null; // used to wait to exit when things are still running

  /**
   * Waits until it is safe to exit. Blocks until all currently running fragments have completed.
   *
   * <p>This is intended to be used by {@link org.apache.drill.exec.server.Drillbit#close()}.</p>
   */
  public void waitToExit(Drillbit bit, boolean forcefulShutdown) {
    synchronized(this) {
      numOfRunningQueries = queries.size();
      numOfRunningFragments = runningFragments.size();
      if ( queries.isEmpty() && runningFragments.isEmpty()) {
        return;
      }
      logger.info("Draining " + queries +" queries and "+ runningFragments+" fragments.");
      exitLatch = new ExtendedLatch();
    }
    // Wait uninterruptibly until all the queries and running fragments on that drillbit goes down
    // to zero
    if( forcefulShutdown ) {
      exitLatch.awaitUninterruptibly(5000);
    } else {
      exitLatch.awaitUninterruptibly();
    }
  }

  /**
   * If it is safe to exit, and the exitLatch is in use, signals it so that waitToExit() will
   * unblock. Logs the number of pending fragments and queries that are running on that
   * drillbit to track the progress of shutdown process.
   */
  private void indicateIfSafeToExit() {
    synchronized(this) {
      if (exitLatch != null) {
        logger.info("Waiting for "+ queries.size() +" queries to complete before shutting down");
        logger.info("Waiting for "+ runningFragments.size() +" running fragments to complete before shutting down");
        if(runningFragments.size() > numOfRunningFragments|| queries.size() > numOfRunningQueries) {
          logger.info("New Fragments or queries are added while drillbit is Shutting down");
        }
        if (queries.isEmpty() && runningFragments.isEmpty()) {
          // Both Queries and Running fragments are empty.
          // So its safe for the drillbit to exit.
          exitLatch.countDown();
        }
      }
    }
  }
  /**
   *  Get the number of queries that are running on a drillbit.
   *  Primarily used to monitor the number of running queries after a
   *  shutdown request is triggered.
   */
  public synchronized Map<String, Integer> getRemainingQueries() {
        Map<String, Integer> queriesInfo = new HashMap<String, Integer>();
        queriesInfo.put("queriesCount", queries.size());
        queriesInfo.put("fragmentsCount", runningFragments.size());
        return queriesInfo;
  }

  /**
   * Narrowed interface to WorkManager that is made available to tasks it is managing.
   */
  public class WorkerBee {
    public void addNewForeman(final Foreman foreman) {
      queries.put(foreman.getQueryId(), foreman);

      // We're relying on the Foreman to clean itself up with retireForeman().
      executor.execute(foreman);
    }

    /**
     * Add a self contained runnable work to executor service.
     * @param runnable
     */
    public void addNewWork(final Runnable runnable) {
      executor.execute(runnable);
    }

    /**
     * Remove the given Foreman from the running query list.
     *
     * <p>The running query list is a bit of a misnomer, because it doesn't
     * necessarily mean that {@link org.apache.drill.exec.work.foreman.Foreman#run()}
     * is executing. That only lasts for the duration of query setup, after which
     * the Foreman instance survives as a state machine that reacts to events
     * from the local root fragment as well as RPC responses from remote Drillbits.</p>
     *
     * @param foreman the Foreman to retire
     */
    public void retireForeman(final Foreman foreman) {
      Preconditions.checkNotNull(foreman);

      final QueryId queryId = foreman.getQueryId();
      final boolean wasRemoved = queries.remove(queryId, foreman);
      if (!wasRemoved) {
        logger.warn("Couldn't find retiring Foreman for query " + queryId);
//        throw new IllegalStateException("Couldn't find retiring Foreman for query " + queryId);
      }

      indicateIfSafeToExit();
    }

    public Foreman getForemanForQueryId(final QueryId queryId) {
      return queries.get(queryId);
    }

    public DrillbitContext getContext() {
      return dContext;
    }

    /**
     * Currently used to start a root fragment that is not blocked on data, and leaf fragments.
     * @param fragmentExecutor the executor to run
     */
    public void addFragmentRunner(final FragmentExecutor fragmentExecutor) {
      final FragmentHandle fragmentHandle = fragmentExecutor.getContext().getHandle();
      runningFragments.put(fragmentHandle, fragmentExecutor);
      executor.execute(new SelfCleaningRunnable(fragmentExecutor) {
        @Override
        protected void cleanup() {
          runningFragments.remove(fragmentHandle);
          indicateIfSafeToExit();
        }
      });
    }

    /**
     * Currently used to start a root fragment that is blocked on data, and intermediate fragments. This method is
     * called, when the first batch arrives.
     *
     * @param fragmentManager the manager for the fragment
     */
    public void startFragmentPendingRemote(final FragmentManager fragmentManager) {
      final FragmentHandle fragmentHandle = fragmentManager.getHandle();
      final FragmentExecutor fragmentExecutor = fragmentManager.getRunnable();
      if (fragmentExecutor == null) {
        // the fragment was most likely cancelled
        return;
      }
      runningFragments.put(fragmentHandle, fragmentExecutor);
      executor.execute(new SelfCleaningRunnable(fragmentExecutor) {
        @Override
        protected void cleanup() {
          runningFragments.remove(fragmentHandle);
          workBus.removeFragmentManager(fragmentHandle);
          indicateIfSafeToExit();
        }
      });
    }

    public FragmentExecutor getFragmentRunner(final FragmentHandle handle) {
      return runningFragments.get(handle);
    }
  }

  /**
   * Periodically gather current statistics. {@link QueryManager} uses a FragmentStatusListener to
   * maintain changes to state, and should be current. However, we want to collect current statistics
   * about RUNNING queries, such as current memory consumption, number of rows processed, and so on.
   * The FragmentStatusListener only tracks changes to state, so the statistics kept there will be
   * stale; this thread probes for current values.
   *
   * For each running fragment if the Foreman is the local Drillbit then status is updated locally bypassing the Control
   * Tunnel, whereas for remote Foreman it is sent over the Control Tunnel.
   */
  private class StatusThread extends Thread {
    public StatusThread() {
      // assume this thread is created by a non-daemon thread
      setName("WorkManager.StatusThread");
    }

    @Override
    public void run() {

      // Get the controller and localBitEndPoint outside the loop since these will not change once a Drillbit and
      // StatusThread is started
      final Controller controller = dContext.getController();
      final DrillbitEndpoint localBitEndPoint = dContext.getEndpoint();

      while (true) {
        final List<DrillRpcFuture<Ack>> futures = Lists.newArrayList();
        for (final FragmentExecutor fragmentExecutor : runningFragments.values()) {
          final FragmentStatus status = fragmentExecutor.getStatus();
          if (status == null) {
            continue;
          }

          final DrillbitEndpoint foremanEndpoint = fragmentExecutor.getContext().getForemanEndpoint();

          // If local endpoint is the Foreman for this running fragment, then submit the status locally bypassing the
          // Control Tunnel
          if (localBitEndPoint.equals(foremanEndpoint)) {
            workBus.statusUpdate(status);
          } else { // else send the status to remote Foreman over Control Tunnel
            futures.add(controller.getTunnel(foremanEndpoint).sendFragmentStatus(status));
          }
        }

        for (final DrillRpcFuture<Ack> future : futures) {
          try {
            future.checkedGet();
          } catch (final RpcException ex) {
            logger.info("Failure while sending intermediate fragment status to Foreman", ex);
          }
        }

        try {
          Thread.sleep(STATUS_PERIOD_SECONDS * 1000);
        } catch (final InterruptedException e) {
          // Preserve evidence that the interruption occurred so that code higher up on the call stack can learn of the
          // interruption and respond to it if it wants to.
          Thread.currentThread().interrupt();

          // exit status thread on interrupt.
          break;
        }
      }
    }
  }
}
