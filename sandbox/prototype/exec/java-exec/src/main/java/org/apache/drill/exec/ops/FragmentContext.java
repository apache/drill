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

import java.io.IOException;

import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.exec.compile.ClassTransformer;
import org.apache.drill.exec.compile.QueryClassLoader;
import org.apache.drill.exec.compile.TemplateClassDefinition;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.metrics.SingleThreadNestedCounter;
import org.apache.drill.exec.physical.impl.FilteringRecordBatchTransformer;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.ExecProtos.FragmentStatus;
import org.apache.drill.exec.rpc.bit.BitCom;
import org.apache.drill.exec.rpc.user.UserServer.UserClientConnection;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.work.FragmentRunner;
import org.apache.drill.exec.work.batch.IncomingBuffers;

import com.yammer.metrics.MetricRegistry;
import com.yammer.metrics.Timer;

/**
 * Contextual objects required for execution of a particular fragment.  
 */
public class FragmentContext {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FragmentContext.class);

  private final static String METRIC_TIMER_FRAGMENT_TIME = MetricRegistry.name(FragmentRunner.class, "completionTimes");
  private final static String METRIC_BATCHES_COMPLETED = MetricRegistry.name(FragmentRunner.class, "batchesCompleted");
  private final static String METRIC_RECORDS_COMPLETED = MetricRegistry.name(FragmentRunner.class, "recordsCompleted");
  private final static String METRIC_DATA_PROCESSED = MetricRegistry.name(FragmentRunner.class, "dataProcessed");

  private final DrillbitContext context;
  public final SingleThreadNestedCounter batchesCompleted;
  public final SingleThreadNestedCounter recordsCompleted;
  public final SingleThreadNestedCounter dataProcessed;
  public final Timer fragmentTime;
  private final FragmentHandle handle;
  private final UserClientConnection connection;
  private final IncomingBuffers buffers;
  private volatile Throwable failureCause;
  private volatile boolean failed = false;
  private final FunctionImplementationRegistry funcRegistry;
  private final QueryClassLoader loader;
  private final ClassTransformer transformer;
  
  public FragmentContext(DrillbitContext dbContext, FragmentHandle handle, UserClientConnection connection, IncomingBuffers buffers, FunctionImplementationRegistry funcRegistry) {
    this.loader = new QueryClassLoader(true);
    this.transformer = new ClassTransformer();
    this.fragmentTime = dbContext.getMetrics().timer(METRIC_TIMER_FRAGMENT_TIME);
    this.batchesCompleted = new SingleThreadNestedCounter(dbContext, METRIC_BATCHES_COMPLETED);
    this.recordsCompleted = new SingleThreadNestedCounter(dbContext, METRIC_RECORDS_COMPLETED);
    this.dataProcessed = new SingleThreadNestedCounter(dbContext, METRIC_DATA_PROCESSED);
    this.context = dbContext;
    this.connection = connection;
    this.handle = handle;
    this.buffers = buffers;
    this.funcRegistry = funcRegistry;
  }

  public void fail(Throwable cause) {
    logger.debug("Fragment Context received failure. {}", cause);
    failed = true;
    failureCause = cause;
  }
  
  public DrillbitContext getDrillbitContext(){
    return context;
  }

  public DrillbitEndpoint getIdentity(){
    return context.getEndpoint();
  }
  
  public FragmentHandle getHandle() {
    return handle;
  }

  public BufferAllocator getAllocator(){
    // TODO: A local query allocator to ensure memory limits and accurately gauge memory usage.
    return context.getAllocator();
  }

  public <T> T getImplementationClass(CodeGenerator<T> cg) throws ClassTransformationException, IOException{
    long t1 = System.nanoTime();
    T t= transformer.getImplementationClass(this.loader, cg.getDefinition(), cg.generate(), cg.getMaterializedClassName());
    logger.debug("Compile time: {} micros.", (System.nanoTime() - t1)/1000/1000 );
    return t;
    
  }
  
  public void addMetricsToStatus(FragmentStatus.Builder stats){
    stats.setBatchesCompleted(batchesCompleted.get());
    stats.setDataProcessed(dataProcessed.get());
    stats.setRecordsCompleted(recordsCompleted.get());
  }
  
  public UserClientConnection getConnection() {
    return connection;
  }

  public BitCom getCommunicator(){
    return context.getBitCom();
  }
  
  public IncomingBuffers getBuffers(){
    return buffers;
  }

  public Throwable getFailureCause() {
    return failureCause;
  }
  
  public boolean isFailed(){
    return failed;
  }
  
  public FunctionImplementationRegistry getFunctionRegistry(){
    return funcRegistry;
  }
  
  public QueryClassLoader getClassLoader(){
    return loader;
  }
}
