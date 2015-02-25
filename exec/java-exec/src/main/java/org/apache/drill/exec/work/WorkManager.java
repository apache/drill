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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.drill.common.SelfCleaningRunnable;
import org.apache.drill.exec.coord.ClusterCoordinator;
import org.apache.drill.exec.proto.BitControl.FragmentStatus;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.GeneralRPCProtos.Ack;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.rpc.DrillRpcFuture;
import org.apache.drill.exec.rpc.NamedThreadFactory;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.control.Controller;
import org.apache.drill.exec.rpc.control.WorkEventBus;
import org.apache.drill.exec.rpc.data.DataConnectionCreator;
import org.apache.drill.exec.rpc.data.DataResponseHandler;
import org.apache.drill.exec.rpc.data.DataResponseHandlerImpl;
import org.apache.drill.exec.server.BootStrapContext;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.sys.PStoreProvider;
import org.apache.drill.exec.work.batch.ControlHandlerImpl;
import org.apache.drill.exec.work.batch.ControlMessageHandler;
import org.apache.drill.exec.work.foreman.Foreman;
import org.apache.drill.exec.work.fragment.FragmentExecutor;
import org.apache.drill.exec.work.fragment.FragmentManager;
import org.apache.drill.exec.work.user.UserWorker;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

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
  private final DataResponseHandler dataHandler;
  private final UserWorker userWorker;
  private final WorkerBee bee;
  private final WorkEventBus workBus;
  private final ExecutorService executor;
  private final StatusThread statusThread;

  /**
   * How often the StatusThread collects statistics about running fragments.
   */
  private final static int STATUS_PERIOD_SECONDS = 5;

  public WorkManager(final BootStrapContext context) {
    this.bContext = context;
    bee = new WorkerBee(); // TODO should this just be an interface?
    workBus = new WorkEventBus(); // TODO should this just be an interface?

    /*
     * TODO
     * This executor isn't bounded in any way and could create an arbitrarily large number of
     * threads, possibly choking the machine. We should really put an upper bound on the number of
     * threads that can be created. Ideally, this might be computed based on the number of cores or
     * some similar metric; ThreadPoolExecutor can impose an upper bound, and might be a better choice.
     */
    executor = Executors.newCachedThreadPool(new NamedThreadFactory("WorkManager-"));

    // TODO references to this escape here (via WorkerBee) before construction is done
    controlMessageWorker = new ControlHandlerImpl(bee); // TODO getFragmentRunner(), getForemanForQueryId()
    userWorker = new UserWorker(bee); // TODO should just be an interface? addNewForeman(), getForemanForQueryId()
    statusThread = new StatusThread();
    dataHandler = new DataResponseHandlerImpl(bee); // TODO only uses startFragmentPendingRemote()
  }

  public void start(final DrillbitEndpoint endpoint, final Controller controller,
      final DataConnectionCreator data, final ClusterCoordinator coord, final PStoreProvider provider) {
    dContext = new DrillbitContext(endpoint, bContext, coord, controller, data, workBus, provider, executor);
    statusThread.start();

    // TODO remove try block once metrics moved from singleton, For now catch to avoid unit test failures
    try {
      dContext.getMetrics().register(
          MetricRegistry.name("drill.exec.work.running_fragments." + dContext.getEndpoint().getUserPort()),
              new Gauge<Integer>() {
                @Override
                public Integer getValue() {
                  return runningFragments.size();
                }
          });
    } catch (IllegalArgumentException e) {
      logger.warn("Exception while registering metrics", e);
    }
  }

  public ExecutorService getExecutor() {
    return executor;
  }

  public WorkEventBus getWorkBus() {
    return workBus;
  }

  public DataResponseHandler getDataHandler() {
    return dataHandler;
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
    try {
      if (executor != null) {
        executor.awaitTermination(1, TimeUnit.SECONDS);
      }
    } catch (InterruptedException e) {
      logger.warn("Executor interrupted while awaiting termination");
    }
  }

  public DrillbitContext getContext() {
    return dContext;
  }

  /**
   * Narrowed interface to WorkManager that is made available to tasks it is managing.
   */
  public class WorkerBee {
    public void addNewForeman(final Foreman foreman) {
      queries.put(foreman.getQueryId(), foreman);
      executor.execute(new SelfCleaningRunnable(foreman) {
        @Override
        protected void cleanup() {
          queries.remove(foreman.getQueryId(), foreman);
        }
      });
    }

    public Foreman getForemanForQueryId(final QueryId queryId) {
      return queries.get(queryId);
    }

    public DrillbitContext getContext() {
      return dContext;
    }

    public void startFragmentPendingRemote(final FragmentManager handler) {
      executor.execute(handler.getRunnable());
    }

    public void addFragmentRunner(final FragmentExecutor fragmentExecutor) {
      final FragmentHandle fragmentHandle = fragmentExecutor.getContext().getHandle();
      runningFragments.put(fragmentHandle, fragmentExecutor);
      executor.execute(new SelfCleaningRunnable(fragmentExecutor) {
        @Override
        protected void cleanup() {
          runningFragments.remove(fragmentHandle);
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
   */
  private class StatusThread extends Thread {
    public StatusThread() {
      setDaemon(true);
      setName("WorkManager.StatusThread");
    }

    @Override
    public void run() {
      while(true) {
        final List<DrillRpcFuture<Ack>> futures = Lists.newArrayList();
        for(FragmentExecutor fragmentExecutor : runningFragments.values()) {
          final FragmentStatus status = fragmentExecutor.getStatus();
          if (status == null) {
            continue;
          }

          final DrillbitEndpoint ep = fragmentExecutor.getContext().getForemanEndpoint();
          futures.add(dContext.getController().getTunnel(ep).sendFragmentStatus(status));
        }

        for(DrillRpcFuture<Ack> future : futures) {
          try {
            future.checkedGet();
          } catch(RpcException ex) {
            logger.info("Failure while sending intermediate fragment status to Foreman", ex);
          }
        }

        try {
          Thread.sleep(STATUS_PERIOD_SECONDS * 1000);
        } catch(InterruptedException e) {
          // exit status thread on interrupt.
          break;
        }
      }
    }
  }
}
