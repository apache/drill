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

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.drill.exec.coord.ClusterCoordinator;
import org.apache.drill.exec.proto.BitControl.FragmentStatus;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.GeneralRPCProtos.Ack;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.helper.QueryIdHelper;
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
import org.apache.drill.exec.work.foreman.QueryStatus;
import org.apache.drill.exec.work.fragment.FragmentExecutor;
import org.apache.drill.exec.work.fragment.FragmentManager;
import org.apache.drill.exec.work.user.UserWorker;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;

public class WorkManager implements Closeable {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(WorkManager.class);

  private Set<FragmentManager> incomingFragments = Collections.newSetFromMap(Maps
      .<FragmentManager, Boolean> newConcurrentMap());

  private LinkedBlockingQueue<RunnableWrapper> pendingTasks = Queues.newLinkedBlockingQueue();

  private Map<FragmentHandle, FragmentExecutor> runningFragments = Maps.newConcurrentMap();

  private ConcurrentMap<QueryId, Foreman> queries = Maps.newConcurrentMap();

  private ConcurrentMap<QueryId, QueryStatus> status = Maps.newConcurrentMap();

  private BootStrapContext bContext;
  private DrillbitContext dContext;

  private final ControlMessageHandler controlMessageWorker;
  private final DataResponseHandler dataHandler;
  private final UserWorker userWorker;
  private final WorkerBee bee;
  private final WorkEventBus workBus;
  private ExecutorService executor;
  private final EventThread eventThread;
  private final StatusThread statusThread;
  private Controller controller;

  public WorkManager(BootStrapContext context) {
    this.bee = new WorkerBee();
    this.workBus = new WorkEventBus(bee);
    this.bContext = context;
    this.controlMessageWorker = new ControlHandlerImpl(bee);
    this.userWorker = new UserWorker(bee);
    this.eventThread = new EventThread();
    this.statusThread = new StatusThread();
    this.dataHandler = new DataResponseHandlerImpl(bee);
  }

  public void start(DrillbitEndpoint endpoint, Controller controller,
      DataConnectionCreator data, ClusterCoordinator coord, PStoreProvider provider) {
    this.dContext = new DrillbitContext(endpoint, bContext, coord, controller, data, workBus, provider);
    // executor = Executors.newFixedThreadPool(dContext.getConfig().getInt(ExecConstants.EXECUTOR_THREADS)
    this.executor = Executors.newCachedThreadPool(new NamedThreadFactory("WorkManager-"));
    this.controller = controller;
    this.eventThread.start();
    this.statusThread.start();
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
      dContext.getMetrics().register(
              MetricRegistry.name("drill.exec.work.pendingTasks" + dContext.getEndpoint().getUserPort()),
              new Gauge<Integer>() {
                @Override
                public Integer getValue() {
                  return pendingTasks.size();
                }
              });
    } catch (IllegalArgumentException e) {
      logger.warn("Exception while registering metrics", e);
    }
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
  public void close() throws IOException {
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

  private static String getId(FragmentHandle handle){
    return "FragmentExecutor: " + QueryIdHelper.getQueryId(handle.getQueryId()) + ':' + handle.getMajorFragmentId() + ':' + handle.getMinorFragmentId();
  }

  // create this so items can see the data here whether or not they are in this package.
  public class WorkerBee {



    public void addFragmentRunner(FragmentExecutor runner) {
      logger.debug("Adding pending task {}", runner);
      RunnableWrapper wrapper = new RunnableWrapper(runner, getId(runner.getContext().getHandle()));
      pendingTasks.add(wrapper);
    }

    public void addNewForeman(Foreman foreman) {
      String id = "Foreman: " + QueryIdHelper.getQueryId(foreman.getQueryId());
      RunnableWrapper wrapper = new RunnableWrapper(foreman, id);
      pendingTasks.add(wrapper);
      queries.put(foreman.getQueryId(), foreman);
    }

    public void addFragmentPendingRemote(FragmentManager handler) {
      incomingFragments.add(handler);
    }

    public void startFragmentPendingRemote(FragmentManager handler) {
      incomingFragments.remove(handler);
      FragmentExecutor runner = handler.getRunnable();
      RunnableWrapper wrapper = new RunnableWrapper(runner, getId(runner.getContext().getHandle()));
      pendingTasks.add(wrapper);
    }

    public FragmentExecutor getFragmentRunner(FragmentHandle handle) {
      return runningFragments.get(handle);
    }

    public void removeFragment(FragmentHandle handle) {
      runningFragments.remove(handle);
    }

    public Foreman getForemanForQueryId(QueryId queryId) {
      return queries.get(queryId);
    }

    public void retireForeman(Foreman foreman) {
      queries.remove(foreman.getQueryId(), foreman);
    }

    public DrillbitContext getContext() {
      return dContext;
    }

  }

  private class StatusThread extends Thread {
    public StatusThread() {
      this.setDaemon(true);
      this.setName("WorkManager Status Reporter");
    }

    @Override
    public void run() {
      while(true){
        List<DrillRpcFuture<Ack>> futures = Lists.newArrayList();
        for(FragmentExecutor e : runningFragments.values()){
          FragmentStatus status = e.getStatus();
          if(status == null){
            continue;
          }
          DrillbitEndpoint ep = e.getContext().getForemanEndpoint();
          futures.add(controller.getTunnel(ep).sendFragmentStatus(status));
        }

        for(DrillRpcFuture<Ack> future : futures){
          try{
            future.checkedGet();
          }catch(RpcException ex){
            logger.info("Failure while sending intermediate fragment status to Foreman", ex);
          }
        }

        try{
          Thread.sleep(5000);
        }catch(InterruptedException e){
          // exit status thread on interrupt.
          break;
        }
      }
    }

  }

  private class EventThread extends Thread {
    public EventThread() {
      this.setDaemon(true);
      this.setName("WorkManager Event Thread");
    }

    @Override
    public void run() {
      try {
        while (true) {
          // logger.debug("Polling for pending work tasks.");
          RunnableWrapper r = pendingTasks.take();
          if (r != null) {
            logger.debug("Starting pending task {}", r);
            if (r.inner instanceof FragmentExecutor) {
              FragmentExecutor fragmentExecutor = (FragmentExecutor) r.inner;
              runningFragments.put(fragmentExecutor.getContext().getHandle(), fragmentExecutor);
            }
            executor.execute(r);
          }

        }
      } catch (InterruptedException e) {
        logger.info("Work Manager stopping as it was interrupted.");
      }
    }

  }

  private class RunnableWrapper implements Runnable {

    final Runnable inner;
    private final String id;

    public RunnableWrapper(Runnable r, String id){
      this.inner = r;
      this.id = id;
    }

    @Override
    public void run() {
      try{
        inner.run();
      }catch(Exception | Error e){
        logger.error("Failure while running wrapper [{}]", id, e);
      }
    }

  }

}
