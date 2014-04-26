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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.cache.DistributedCache;
import org.apache.drill.exec.coord.ClusterCoordinator;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.rpc.NamedThreadFactory;
import org.apache.drill.exec.rpc.control.Controller;
import org.apache.drill.exec.rpc.control.WorkEventBus;
import org.apache.drill.exec.rpc.data.DataConnectionCreator;
import org.apache.drill.exec.rpc.data.DataResponseHandler;
import org.apache.drill.exec.rpc.data.DataResponseHandlerImpl;
import org.apache.drill.exec.server.BootStrapContext;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.work.batch.ControlHandlerImpl;
import org.apache.drill.exec.work.batch.ControlMessageHandler;
import org.apache.drill.exec.work.foreman.Foreman;
import org.apache.drill.exec.work.fragment.FragmentExecutor;
import org.apache.drill.exec.work.fragment.FragmentManager;
import org.apache.drill.exec.work.user.UserWorker;

import com.google.common.collect.Maps;
import com.google.common.collect.Queues;

public class WorkManager implements Closeable{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(WorkManager.class);
  
  private Set<FragmentManager> incomingFragments = Collections.newSetFromMap(Maps.<FragmentManager, Boolean> newConcurrentMap());

  private PriorityBlockingQueue<Runnable> pendingTasks = Queues.newPriorityBlockingQueue();
  
  private Map<FragmentHandle, FragmentExecutor> runningFragments = Maps.newConcurrentMap();
  
  private ConcurrentMap<QueryId, Foreman> queries = Maps.newConcurrentMap();

  private BootStrapContext bContext;
  private DrillbitContext dContext;

  private final ControlMessageHandler controlMessageWorker;
  private final DataResponseHandler dataHandler;
  private final UserWorker userWorker;
  private final WorkerBee bee;
  private final WorkEventBus workBus;
  private Executor executor;
  private final EventThread eventThread;
  
  public WorkManager(BootStrapContext context){
    this.bee = new WorkerBee();
    this.workBus = new WorkEventBus(bee);
    this.bContext = context;
    this.controlMessageWorker = new ControlHandlerImpl(bee);
    this.userWorker = new UserWorker(bee);
    this.eventThread = new EventThread();
    this.dataHandler = new DataResponseHandlerImpl(bee);
  }
  
  public void start(DrillbitEndpoint endpoint, DistributedCache cache, Controller controller, DataConnectionCreator data, ClusterCoordinator coord){
    this.dContext = new DrillbitContext(endpoint, bContext, coord, controller, data, cache, workBus);
    executor = Executors.newFixedThreadPool(dContext.getConfig().getInt(ExecConstants.EXECUTOR_THREADS), new NamedThreadFactory("WorkManager-"));
    eventThread.start();
  }
  
  public WorkEventBus getWorkBus(){
    return workBus;
  }
  
  public DataResponseHandler getDataHandler() {
    return dataHandler;
  }
  
  public ControlMessageHandler getControlMessageHandler(){
    return controlMessageWorker;
  }

  public UserWorker getUserWorker(){
    return userWorker;
  }
  
  @Override
  public void close() throws IOException {
  }
  

  public DrillbitContext getContext() {
    return dContext;
  }

  // create this so items can see the data here whether or not they are in this package.
  public class WorkerBee{

    public void addFragmentRunner(FragmentExecutor runner){
      logger.debug("Adding pending task {}", runner);
      pendingTasks.add(runner);
    }
    
    public void addNewForeman(Foreman foreman){
      pendingTasks.add(foreman);
    }


    public void addFragmentPendingRemote(FragmentManager handler){
      incomingFragments.add(handler);
    }
    
    public void startFragmentPendingRemote(FragmentManager handler){
      incomingFragments.remove(handler);
      pendingTasks.add(handler.getRunnable());
    }
    
    public FragmentExecutor getFragmentRunner(FragmentHandle handle){
      return runningFragments.get(handle);
    }
    
    public Foreman getForemanForQueryId(QueryId queryId){
      return queries.get(queryId);
    }
    
    public void retireForeman(Foreman foreman){
      queries.remove(foreman.getQueryId(), foreman);
    }

    public DrillbitContext getContext() {
      return dContext;
    }

  }



 private class EventThread extends Thread{
   public EventThread(){
     this.setDaemon(true);
     this.setName("WorkManager Event Thread");
   }

  @Override
  public void run() {
    try {
    while(true){
//      logger.debug("Polling for pending work tasks.");
      Runnable r = pendingTasks.take();
      if(r != null){
        logger.debug("Starting pending task {}", r);
        executor.execute(r);  
      }
      
    }
    } catch (InterruptedException e) {
      logger.info("Work Manager stopping as it was interrupted.");
    }
  }
   
   
 }

  
  
}
