/*******************************************************************************
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
 ******************************************************************************/
package org.apache.drill.exec.ops;

import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.metrics.SingleThreadNestedCounter;
import org.apache.drill.exec.planner.FragmentRunnable;
import org.apache.drill.exec.proto.ExecProtos.PlanFragment;
import org.apache.drill.exec.rpc.bit.BitCom;
import org.apache.drill.exec.server.DrillbitContext;

import com.yammer.metrics.MetricRegistry;
import com.yammer.metrics.Timer;

/**
 * Contextual objects required for execution of a particular fragment.  
 */
public class FragmentContext {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FragmentContext.class);

  private final static String METRIC_TIMER_FRAGMENT_TIME = MetricRegistry.name(FragmentRunnable.class, "completionTimes");
  private final static String METRIC_BATCHES_COMPLETED = MetricRegistry.name(FragmentRunnable.class, "batchesCompleted");
  private final static String METRIC_RECORDS_COMPLETED = MetricRegistry.name(FragmentRunnable.class, "recordsCompleted");
  private final static String METRIC_DATA_PROCESSED = MetricRegistry.name(FragmentRunnable.class, "dataProcessed");

  private final DrillbitContext context;
  private final PlanFragment fragment;
  public final SingleThreadNestedCounter batchesCompleted;
  public final SingleThreadNestedCounter recordsCompleted;
  public final SingleThreadNestedCounter dataProcessed;
  public final Timer fragmentTime;

  public FragmentContext(DrillbitContext dbContext, PlanFragment fragment) {
    this.fragmentTime = dbContext.getMetrics().timer(METRIC_TIMER_FRAGMENT_TIME);
    this.batchesCompleted = new SingleThreadNestedCounter(dbContext, METRIC_BATCHES_COMPLETED);
    this.recordsCompleted = new SingleThreadNestedCounter(dbContext, METRIC_RECORDS_COMPLETED);
    this.dataProcessed = new SingleThreadNestedCounter(dbContext, METRIC_DATA_PROCESSED);
    this.context = dbContext;
    this.fragment = fragment;
  }

  public void fail(Throwable cause) {

  }

  public DrillbitContext getDrillbitContext(){
    return context;
  }
  
  public PlanFragment getFragment() {
    return fragment;
  }
  
  public BufferAllocator getAllocator(){
    // TODO: A local query allocator to ensure memory limits and accurately gauge memory usage.
    return context.getAllocator();
  }

  
  public FilteringRecordBatchTransformer getFilteringExpression(LogicalExpression expr){
    return null;
  }
  
  
  public BitCom getCommunicator(){
    return null;
  }
}
