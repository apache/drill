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

import io.netty.buffer.DrillBuf;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import net.hydromatic.optiq.SchemaPlus;
import net.hydromatic.optiq.jdbc.SimpleOptiqSchema;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.ExecutionSetupException;
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
import org.apache.drill.exec.proto.UserBitShared.UserCredentials;
import org.apache.drill.exec.rpc.control.ControlTunnel;
import org.apache.drill.exec.rpc.data.DataTunnel;
import org.apache.drill.exec.rpc.user.UserServer.UserClientConnection;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.options.FragmentOptionManager;
import org.apache.drill.exec.server.options.OptionList;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.work.batch.IncomingBuffers;

import com.carrotsearch.hppc.LongObjectOpenHashMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

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
  private final BufferAllocator allocator;
  private final PlanFragment fragment;
  private List<Thread> daemonThreads = Lists.newLinkedList();
  private IncomingBuffers buffers;
  private final long queryStartTime;
  private final int rootFragmentTimeZone;
  private final OptionManager fragmentOptions;
  private final UserCredentials credentials;
  private LongObjectOpenHashMap<DrillBuf> managedBuffers = new LongObjectOpenHashMap<>();

  private volatile Throwable failureCause;
  private volatile FragmentContextState state = FragmentContextState.OK;

  private static enum FragmentContextState {
    OK,
    FAILED,
    CANCELED
  }

  public FragmentContext(DrillbitContext dbContext, PlanFragment fragment, UserClientConnection connection,
      FunctionImplementationRegistry funcRegistry) throws OutOfMemoryException, ExecutionSetupException {

    this.context = dbContext;
    this.connection = connection;
    this.fragment = fragment;
    this.funcRegistry = funcRegistry;
    this.queryStartTime = fragment.getQueryStartTime();
    this.rootFragmentTimeZone = fragment.getTimeZone();
    this.credentials = fragment.getCredentials();
    logger.debug("Getting initial memory allocation of {}", fragment.getMemInitial());
    logger.debug("Fragment max allocation: {}", fragment.getMemMax());
    try {
      OptionList list;
      if (!fragment.hasOptionsJson() || fragment.getOptionsJson().isEmpty()) {
        list = new OptionList();
      } else {
        list = dbContext.getConfig().getMapper().readValue(fragment.getOptionsJson(), OptionList.class);
      }
      this.fragmentOptions = new FragmentOptionManager(context.getOptionManager(), list);
    } catch (Exception e) {
      throw new ExecutionSetupException("Failure while reading plan options.", e);
    }

    // Add the fragment context to the root allocator.
    // The QueryManager will call the root allocator to recalculate all the memory limits for all the fragments
    try {
      this.allocator = dbContext.getAllocator().getChildAllocator(this, fragment.getMemInitial(), fragment.getMemMax(), true);
      assert (allocator != null);
    }catch(Throwable e){
      throw new ExecutionSetupException("Failure while getting memory allocator for fragment.", e);
    }
    this.stats = new FragmentStats(allocator, dbContext.getMetrics(), fragment.getAssignment());

    this.loader = new QueryClassLoader(dbContext.getConfig(), fragmentOptions);
  }

  public OptionManager getOptions() {
    return fragmentOptions;
  }

  public void setBuffers(IncomingBuffers buffers) {
    this.buffers = buffers;
  }

  public void fail(Throwable cause) {
    logger.error("Fragment Context received failure.", cause);
    setState(FragmentContextState.FAILED);
    failureCause = cause;
  }

  public void cancel() {
    setState(FragmentContextState.CANCELED);
  }

  /**
   * Allowed transitions from left to right: OK -> FAILED -> CANCELED
   * @param newState
   */
  private synchronized void setState(FragmentContextState newState) {
    if (state == FragmentContextState.OK) {
      state = newState;
    } else if (newState == FragmentContextState.CANCELED) {
      state = newState;
    }
  }

  public DrillbitContext getDrillbitContext() {
    return context;
  }

  public SchemaPlus getRootSchema() {
    if (connection == null) {
      fail(new UnsupportedOperationException("Schema tree can only be created in root fragment. " +
          "This is a non-root fragment."));
      return null;
    } else {
      SchemaPlus root = SimpleOptiqSchema.createRootSchema(false);
      context.getStorage().getSchemaFactory().registerSchemas(connection.getSession(), root);
      return root;
    }
  }

  /**
   * Get this node's identity.
   * @return A DrillbitEndpoint object.
   */
  public DrillbitEndpoint getIdentity() {
    return context.getEndpoint();
  }

  public FragmentStats getStats() {
    return this.stats;
  }

  public long getQueryStartTime() {
    return this.queryStartTime;
  }

  public int getRootFragmentTimeZone() {
    return this.rootFragmentTimeZone;
  }

  public DrillbitEndpoint getForemanEndpoint() {return fragment.getForeman();}

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
  @Deprecated
  public BufferAllocator getAllocator() {
    if(allocator == null){
      FragmentHandle handle=getHandle();
      String frag=handle!=null?handle.getMajorFragmentId()+":"+handle.getMinorFragmentId():"0:0";
      logger.debug("Fragment:"+frag+" Allocator is NULL");
    }
    return allocator;
  }

  public BufferAllocator getNewChildAllocator(long initialReservation,
                                              long maximumReservation,
                                              boolean applyFragmentLimit) throws OutOfMemoryException {
    return allocator.getChildAllocator(this, initialReservation, maximumReservation, applyFragmentLimit);
  }

  public <T> T getImplementationClass(ClassGenerator<T> cg) throws ClassTransformationException, IOException {
    return getImplementationClass(cg.getCodeGenerator());
  }

  public <T> T getImplementationClass(CodeGenerator<T> cg) throws ClassTransformationException, IOException {
    return context.getCompiler().getImplementationClass(cg);
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
    return state == FragmentContextState.FAILED;
  }

  public boolean isCancelled() {
    return state == FragmentContextState.CANCELED;
  }

  public FunctionImplementationRegistry getFunctionRegistry() {
    return funcRegistry;
  }

  public UserCredentials getCredentials() {
    return credentials;
  }

  public QueryClassLoader getClassLoader() {
    return loader;
  }

  public DrillConfig getConfig() {
    return context.getConfig();
  }

  public void setFragmentLimit(long limit) {
    this.allocator.setFragmentLimit(limit);
  }

  @Override
  public void close() {
    for (Thread thread: daemonThreads) {
     thread.interrupt();
    }
    Object[] mbuffers = ((LongObjectOpenHashMap<Object>)(Object)managedBuffers).values;
    for (int i =0; i < mbuffers.length; i++) {
      if (managedBuffers.allocated[i]) {
        ((DrillBuf)mbuffers[i]).release();
      }
    }

    if (buffers != null) {
      buffers.close();
    }

    FragmentHandle handle=getHandle();
    String frag=handle!=null?handle.getMajorFragmentId()+":"+handle.getMinorFragmentId():"0:0";
    allocator.close();
    logger.debug("Fragment:"+frag+" After close allocator is: "+allocator!=null?"OK":"NULL");
  }

  public DrillBuf replace(DrillBuf old, int newSize) {
    if (managedBuffers.remove(old.memoryAddress()) == null) {
      throw new IllegalStateException("Tried to remove unmanaged buffer.");
    }
    old.release();
    return getManagedBuffer(newSize);
  }

  public DrillBuf getManagedBuffer() {
    return getManagedBuffer(256);
  }

  public DrillBuf getManagedBuffer(int size) {
    DrillBuf newBuf = allocator.buffer(size);
    managedBuffers.put(newBuf.memoryAddress(), newBuf);
    newBuf.setFragmentContext(this);
    return newBuf;
  }

}
