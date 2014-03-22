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
package org.apache.drill.exec.ops;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import net.hydromatic.optiq.SchemaPlus;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.compile.ClassTransformer;
import org.apache.drill.exec.compile.QueryClassLoader;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.expr.ClassGenerator;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.OutOfMemoryException;
import org.apache.drill.exec.proto.BitControl.PlanFragment;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.rpc.control.ControlTunnel;
import org.apache.drill.exec.rpc.data.DataTunnel;
import org.apache.drill.exec.rpc.user.UserServer.UserClientConnection;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.work.batch.IncomingBuffers;

import com.beust.jcommander.internal.Lists;
import com.beust.jcommander.internal.Maps;

/**
 * Contextual objects required for execution of a particular fragment.
 */
public class FragmentContext implements Closeable {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FragmentContext.class);


  private Map<FragmentHandle, DataTunnel> tunnels = Maps.newHashMap();

  private final DrillbitContext context;
  private final UserClientConnection connection;
  private final FragmentStats stats;
  private final FunctionImplementationRegistry funcRegistry;
  private final QueryClassLoader loader;
  private final ClassTransformer transformer;
  private final BufferAllocator allocator;
  private final PlanFragment fragment;
  private List<Thread> daemonThreads = Lists.newLinkedList();
  private IncomingBuffers buffers;

  private volatile Throwable failureCause;
  private volatile boolean failed = false;

  public FragmentContext(DrillbitContext dbContext, PlanFragment fragment, UserClientConnection connection,
      FunctionImplementationRegistry funcRegistry) throws OutOfMemoryException {
    this.loader = new QueryClassLoader(true);
    this.transformer = new ClassTransformer();
    this.stats = new FragmentStats(dbContext.getMetrics());
    this.context = dbContext;
    this.connection = connection;
    this.fragment = fragment;
    this.funcRegistry = funcRegistry;
    logger.debug("Getting initial memory allocation of {}", fragment.getMemInitial());
    this.allocator = context.getAllocator().getChildAllocator(fragment.getHandle(), fragment.getMemInitial(), fragment.getMemMax());
  }

  public void setBuffers(IncomingBuffers buffers) {
    this.buffers = buffers;
  }

  public void fail(Throwable cause) {
    logger.debug("Fragment Context received failure. {}", cause);
    failed = true;
    failureCause = cause;
  }

  public DrillbitContext getDrillbitContext() {
    return context;
  }
  
  public SchemaPlus getRootSchema(){
    return context.getStorage().getSchemaFactory().getOrphanedRootSchema();
  }

  /**
   * Get this node's identity.
   * @return A DrillbitEndpoint object.
   */
  public DrillbitEndpoint getIdentity() {
    return context.getEndpoint();
  }

  public FragmentStats getStats(){
    return this.stats;
  }
  
  /**
   * The FragmentHandle for this Fragment
   * @return FragmentHandle
   */
  public FragmentHandle getHandle() {
    return fragment.getHandle();
  }

  /**
   * Get this fragment's allocator.
   * @return
   */
  public BufferAllocator getAllocator() {
    return allocator;
  }

  public <T> T getImplementationClass(ClassGenerator<T> cg) throws ClassTransformationException, IOException {
    return getImplementationClass(cg.getCodeGenerator());
  }
  
  public <T> T getImplementationClass(CodeGenerator<T> cg) throws ClassTransformationException, IOException {
    long t1 = System.nanoTime();
    
    T t = transformer.getImplementationClass(this.loader, cg.getDefinition(), cg.generate(),
        cg.getMaterializedClassName());
    logger.debug("Compile time: {} millis.", (System.nanoTime() - t1) / 1000 / 1000);
    return t;

  }

  /**
   * Get the user connection associated with this fragment.  This return null unless this is a root fragment.
   * @return The RPC connection to the query submitter.
   */
  public UserClientConnection getConnection() {
    return connection;
  }

  public ControlTunnel getControlTunnel(DrillbitEndpoint endpoint) {
    return context.getController().getTunnel(endpoint);
  }

  public DataTunnel getDataTunnel(DrillbitEndpoint endpoint, FragmentHandle remoteHandle) {
    DataTunnel tunnel = tunnels.get(remoteHandle);
    if (tunnel == null) {
      tunnel = context.getDataConnectionsPool().getTunnel(endpoint, remoteHandle);
      tunnels.put(remoteHandle, tunnel);
    }
    return tunnel;
  }

  /**
   * Add a new thread to this fragment's context. This thread will likely run for the life of the fragment but should be
   * terminated when the fragment completes. When the fragment completes, the threads will be interrupted.
   *
   * @param thread
   */
  public void addDaemonThread(Thread thread) {
    daemonThreads.add(thread);
    thread.start();
    
  }

  public IncomingBuffers getBuffers() {
    return buffers;
  }

  public Throwable getFailureCause() {
    return failureCause;
  }

  public boolean isFailed() {
    return failed;
  }

  public FunctionImplementationRegistry getFunctionRegistry() {
    return funcRegistry;
  }

  public QueryClassLoader getClassLoader() {
    return loader;
  }

  public DrillConfig getConfig() {
    return context.getConfig();
  }

  @Override
  public void close() {
    for(Thread thread: daemonThreads){
     thread.interrupt();
    }
    allocator.close();
  }
}
