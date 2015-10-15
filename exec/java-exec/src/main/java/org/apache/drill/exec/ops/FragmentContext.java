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

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.calcite.schema.SchemaPlus;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.expr.ClassGenerator;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.OutOfMemoryException;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.memory.OutOfMemoryRuntimeException;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.proto.BitControl.PlanFragment;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.GeneralRPCProtos.Ack;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.RpcOutcomeListener;
import org.apache.drill.exec.rpc.control.ControlTunnel;
import org.apache.drill.exec.rpc.user.UserServer.UserClientConnection;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.options.FragmentOptionManager;
import org.apache.drill.exec.server.options.OptionList;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.store.PartitionExplorer;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.testing.ExecutionControls;
import org.apache.drill.exec.util.ImpersonationUtil;
import org.apache.drill.exec.work.batch.IncomingBuffers;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Contextual objects required for execution of a particular fragment.
 */
public class FragmentContext implements AutoCloseable, UdfUtilities {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FragmentContext.class);

  private final Map<DrillbitEndpoint, AccountingDataTunnel> tunnels = Maps.newHashMap();
  private final List<OperatorContextImpl> contexts = Lists.newLinkedList();

  private final DrillbitContext context;
  private final UserClientConnection connection; // is null if this context is for non-root fragment
  private final QueryContext queryContext; // is null if this context is for non-root fragment
  private final FragmentStats stats;
  private final FunctionImplementationRegistry funcRegistry;
  private final BufferAllocator allocator;
  private final PlanFragment fragment;
  private final ContextInformation contextInformation;
  private IncomingBuffers buffers;
  private final OptionManager fragmentOptions;
  private final BufferManager bufferManager;
  private ExecutorState executorState;
  private final ExecutionControls executionControls;

  private final SendingAccountor sendingAccountor = new SendingAccountor();
  private final Consumer<RpcException> exceptionConsumer = new Consumer<RpcException>() {
    @Override
    public void accept(final RpcException e) {
      fail(e);
    }

    @Override
    public void interrupt(final InterruptedException e) {
      if (shouldContinue()) {
        logger.error("Received an unexpected interrupt while waiting for the data send to complete.", e);
        fail(e);
      }
    }
  };

  private final RpcOutcomeListener<Ack> statusHandler = new StatusHandler(exceptionConsumer, sendingAccountor);
  private final AccountingUserConnection accountingUserConnection;

  /**
   * Create a FragmentContext instance for non-root fragment.
   *
   * @param dbContext DrillbitContext.
   * @param fragment Fragment implementation.
   * @param funcRegistry FunctionImplementationRegistry.
   * @throws ExecutionSetupException
   */
  public FragmentContext(final DrillbitContext dbContext, final PlanFragment fragment,
      final FunctionImplementationRegistry funcRegistry) throws ExecutionSetupException {
    this(dbContext, fragment, null, null, funcRegistry);
  }

  /**
   * Create a FragmentContext instance for root fragment.
   *
   * @param dbContext DrillbitContext.
   * @param fragment Fragment implementation.
   * @param queryContext QueryContext.
   * @param connection UserClientConnection.
   * @param funcRegistry FunctionImplementationRegistry.
   * @throws ExecutionSetupException
   */
  public FragmentContext(final DrillbitContext dbContext, final PlanFragment fragment, final QueryContext queryContext,
      final UserClientConnection connection, final FunctionImplementationRegistry funcRegistry)
    throws ExecutionSetupException {
    this.context = dbContext;
    this.queryContext = queryContext;
    this.connection = connection;
    this.accountingUserConnection = new AccountingUserConnection(connection, sendingAccountor, statusHandler);
    this.fragment = fragment;
    this.funcRegistry = funcRegistry;
    contextInformation = new ContextInformation(fragment.getCredentials(), fragment.getContext());

    logger.debug("Getting initial memory allocation of {}", fragment.getMemInitial());
    logger.debug("Fragment max allocation: {}", fragment.getMemMax());

    final OptionList list;
    if (!fragment.hasOptionsJson() || fragment.getOptionsJson().isEmpty()) {
      list = new OptionList();
    } else {
      try {
        list = dbContext.getLpPersistence().getMapper().readValue(fragment.getOptionsJson(), OptionList.class);
      } catch (final Exception e) {
        throw new ExecutionSetupException("Failure while reading plan options.", e);
      }
    }
    fragmentOptions = new FragmentOptionManager(context.getOptionManager(), list);

    executionControls = new ExecutionControls(fragmentOptions, dbContext.getEndpoint());

    // Add the fragment context to the root allocator.
    // The QueryManager will call the root allocator to recalculate all the memory limits for all the fragments
    try {
      allocator = context.getAllocator().getChildAllocator(this, fragment.getMemInitial(), fragment.getMemMax(), true);
      Preconditions.checkNotNull(allocator, "Unable to acuqire allocator");
    } catch(final OutOfMemoryException | OutOfMemoryRuntimeException e) {
      throw UserException.memoryError(e)
        .addContext("Fragment", getHandle().getMajorFragmentId() + ":" + getHandle().getMinorFragmentId())
        .build(logger);
    } catch(final Throwable e) {
      throw new ExecutionSetupException("Failure while getting memory allocator for fragment.", e);
    }

    stats = new FragmentStats(allocator, dbContext.getMetrics(), fragment.getAssignment());
    bufferManager = new BufferManager(this.allocator, this);
  }

  /**
   * TODO: Remove this constructor when removing the SimpleRootExec (DRILL-2097). This is kept only to avoid modifying
   * the long list of test files.
   */
  public FragmentContext(DrillbitContext dbContext, PlanFragment fragment, UserClientConnection connection,
      FunctionImplementationRegistry funcRegistry) throws ExecutionSetupException {
    this(dbContext, fragment, null, connection, funcRegistry);
  }

  public OptionManager getOptions() {
    return fragmentOptions;
  }

  public void setBuffers(final IncomingBuffers buffers) {
    Preconditions.checkArgument(this.buffers == null, "Can only set buffers once.");
    this.buffers = buffers;
  }

  public void setExecutorState(final ExecutorState executorState) {
    Preconditions.checkArgument(this.executorState == null, "ExecutorState can only be set once.");
    this.executorState = executorState;
  }

  public void fail(final Throwable cause) {
    executorState.fail(cause);
  }

  /**
   * Tells individual operations whether they should continue. In some cases, an external event (typically cancellation)
   * will mean that the fragment should prematurely exit execution. Long running operations should check this every so
   * often so that Drill is responsive to cancellation operations.
   *
   * @return false if the action should terminate immediately, true if everything is okay.
   */
  public boolean shouldContinue() {
    return executorState.shouldContinue();
  }

  public DrillbitContext getDrillbitContext() {
    return context;
  }

  public SchemaPlus getRootSchema() {
    if (queryContext == null) {
      fail(new UnsupportedOperationException("Schema tree can only be created in root fragment. " +
          "This is a non-root fragment."));
      return null;
    }

    final boolean isImpersonationEnabled = isImpersonationEnabled();
    // If impersonation is enabled, we want to view the schema as query user and suppress authorization errors. As for
    // InfoSchema purpose we want to show tables the user has permissions to list or query. If  impersonation is
    // disabled view the schema as Drillbit process user and throw authorization errors to client.
    SchemaConfig schemaConfig = SchemaConfig
        .newBuilder(
            isImpersonationEnabled ? queryContext.getQueryUserName() : ImpersonationUtil.getProcessUserName(),
            queryContext)
        .setIgnoreAuthErrors(isImpersonationEnabled)
        .build();

    return queryContext.getRootSchema(schemaConfig);
  }

  /**
   * Get this node's identity.
   * @return A DrillbitEndpoint object.
   */
  public DrillbitEndpoint getIdentity() {
    return context.getEndpoint();
  }

  public FragmentStats getStats() {
    return stats;
  }

  @Override
  public ContextInformation getContextInformation() {
    return contextInformation;
  }

  public DrillbitEndpoint getForemanEndpoint() {
    return fragment.getForeman();
  }

  /**
   * The FragmentHandle for this Fragment
   * @return FragmentHandle
   */
  public FragmentHandle getHandle() {
    return fragment.getHandle();
  }

  private String getFragIdString() {
    final FragmentHandle handle = getHandle();
    final String frag = handle != null ? handle.getMajorFragmentId() + ":" + handle.getMinorFragmentId() : "0:0";
    return frag;
  }



  /**
   * Get this fragment's allocator.
   * @return the allocator
   */
  @Deprecated
  public BufferAllocator getAllocator() {
    if (allocator == null) {
      logger.debug("Fragment: " + getFragIdString() + " Allocator is NULL");
    }
    return allocator;
  }

  public BufferAllocator getNewChildAllocator(final long initialReservation,
                                              final long maximumReservation,
                                              final boolean applyFragmentLimit) throws OutOfMemoryException {
    return allocator.getChildAllocator(this, initialReservation, maximumReservation, applyFragmentLimit);
  }

  public <T> T getImplementationClass(final ClassGenerator<T> cg)
      throws ClassTransformationException, IOException {
    return getImplementationClass(cg.getCodeGenerator());
  }

  public <T> T getImplementationClass(final CodeGenerator<T> cg)
      throws ClassTransformationException, IOException {
    return context.getCompiler().getImplementationClass(cg);
  }

  public <T> List<T> getImplementationClass(final ClassGenerator<T> cg, final int instanceCount) throws ClassTransformationException, IOException {
    return getImplementationClass(cg.getCodeGenerator(), instanceCount);
  }

  public <T> List<T> getImplementationClass(final CodeGenerator<T> cg, final int instanceCount) throws ClassTransformationException, IOException {
    return context.getCompiler().getImplementationClass(cg, instanceCount);
  }

  public AccountingUserConnection getUserDataTunnel() {
    Preconditions.checkState(connection != null, "Only Root fragment can get UserDataTunnel");
    return accountingUserConnection;
  }

  public ControlTunnel getControlTunnel(final DrillbitEndpoint endpoint) {
    return context.getController().getTunnel(endpoint);
  }

  public AccountingDataTunnel getDataTunnel(final DrillbitEndpoint endpoint) {
    AccountingDataTunnel tunnel = tunnels.get(endpoint);
    if (tunnel == null) {
      tunnel = new AccountingDataTunnel(context.getDataConnectionsPool().getTunnel(endpoint), sendingAccountor, statusHandler);
      tunnels.put(endpoint, tunnel);
    }
    return tunnel;
  }

  public IncomingBuffers getBuffers() {
    return buffers;
  }

  public OperatorContext newOperatorContext(PhysicalOperator popConfig, OperatorStats stats, boolean applyFragmentLimit)
      throws OutOfMemoryException {
    OperatorContextImpl context = new OperatorContextImpl(popConfig, this, stats, applyFragmentLimit);
    contexts.add(context);
    return context;
  }

  public OperatorContext newOperatorContext(PhysicalOperator popConfig, boolean applyFragmentLimit)
      throws OutOfMemoryException {
    OperatorContextImpl context = new OperatorContextImpl(popConfig, this, applyFragmentLimit);
    contexts.add(context);
    return context;
  }

  @VisibleForTesting
  @Deprecated
  public Throwable getFailureCause() {
    return executorState.getFailureCause();
  }

  @VisibleForTesting
  @Deprecated
  public boolean isFailed() {
    return executorState.isFailed();
  }

  public FunctionImplementationRegistry getFunctionRegistry() {
    return funcRegistry;
  }

  public DrillConfig getConfig() {
    return context.getConfig();
  }

  public void setFragmentLimit(final long limit) {
    allocator.setFragmentLimit(limit);
  }

  public ExecutionControls getExecutionControls() {
    return executionControls;
  }

  public String getQueryUserName() {
    return fragment.getCredentials().getUserName();
  }

  public boolean isImpersonationEnabled() {
    // TODO(DRILL-2097): Until SimpleRootExec tests are removed, we need to consider impersonation disabled if there is
    // no config
    if (getConfig() == null) {
      return false;
    }

    return getConfig().getBoolean(ExecConstants.IMPERSONATION_ENABLED);
  }

  @Override
  public void close() {
    waitForSendComplete();

    // close operator context
    for (OperatorContextImpl opContext : contexts) {
      suppressingClose(opContext);
    }

    suppressingClose(bufferManager);
    suppressingClose(buffers);
    suppressingClose(allocator);
  }

  private void suppressingClose(final AutoCloseable closeable) {
    try {
      if (closeable != null) {
        closeable.close();
      }
    } catch (final Exception e) {
      fail(e);
    }
  }

  public DrillBuf replace(final DrillBuf old, final int newSize) {
    return bufferManager.replace(old, newSize);
  }

  @Override
  public DrillBuf getManagedBuffer() {
    return bufferManager.getManagedBuffer();
  }

  public DrillBuf getManagedBuffer(final int size) {
    return bufferManager.getManagedBuffer(size);
  }

  @Override
  public PartitionExplorer getPartitionExplorer() {
    throw new UnsupportedOperationException(String.format("The partition explorer interface can only be used " +
        "in functions that can be evaluated at planning time. Make sure that the %s configuration " +
        "option is set to true.", PlannerSettings.CONSTANT_FOLDING.getOptionName()));
  }

  /**
   * Wait for ack that all outgoing batches have been sent
   */
  public void waitForSendComplete() {
    sendingAccountor.waitForSendComplete();
  }

  public interface ExecutorState {
    /**
     * Whether execution should continue.
     *
     * @return false if execution should stop.
     */
    public boolean shouldContinue();

    /**
     * Inform the executor if a exception occurs and fragment should be failed.
     *
     * @param t
     *          The exception that occurred.
     */
    public void fail(final Throwable t);

    @VisibleForTesting
    @Deprecated
    public boolean isFailed();

    @VisibleForTesting
    @Deprecated
    public Throwable getFailureCause();

  }

}
