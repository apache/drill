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
package org.apache.drill.exec.ops;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.compile.CodeCompiler;
import org.apache.drill.exec.coord.ClusterCoordinator;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.expr.holders.ValueHolder;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.impl.OperatorCreatorRegistry;
import org.apache.drill.exec.planner.PhysicalPlanReader;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.proto.BitControl.PlanFragment;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.GeneralRPCProtos.Ack;
import org.apache.drill.exec.proto.helper.QueryIdHelper;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.RpcOutcomeListener;
import org.apache.drill.exec.rpc.UserClientConnection;
import org.apache.drill.exec.rpc.control.Controller;
import org.apache.drill.exec.rpc.control.WorkEventBus;
import org.apache.drill.exec.rpc.user.UserServer;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.QueryProfileStoreContext;
import org.apache.drill.exec.server.options.FragmentOptionManager;
import org.apache.drill.exec.server.options.OptionList;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.server.options.OptionSet;
import org.apache.drill.exec.store.PartitionExplorer;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.testing.ExecutionControls;
import org.apache.drill.exec.util.ImpersonationUtil;
import org.apache.drill.exec.work.batch.IncomingBuffers;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.netty.buffer.DrillBuf;

/**
 * Contextual objects required for execution of a particular fragment.
 * This is the implementation; use <tt>FragmentContext</tt>
 * in code to allow tests to use test-time implementations.
 */
public class FragmentContextImpl extends BaseFragmentContext implements ExecutorFragmentContext {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FragmentContextImpl.class);

  private final Map<DrillbitEndpoint, AccountingDataTunnel> tunnels = Maps.newHashMap();
  private final List<OperatorContextImpl> contexts = Lists.newLinkedList();

  private final DrillbitContext context;
  private final UserClientConnection connection; // is null if this context is for non-root fragment
  private final QueryContext queryContext; // is null if this context is for non-root fragment
  private final FragmentStats stats;
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
      if (executorState.shouldContinue()) {
        logger.error("Received an unexpected interrupt while waiting for the data send to complete.", e);
        fail(e);
      }
    }
  };

  private final RpcOutcomeListener<Ack> statusHandler = new StatusHandler(exceptionConsumer, sendingAccountor);
  private final AccountingUserConnection accountingUserConnection;
  /** Stores constants and their holders by type */
  private final Map<String, Map<MinorType, ValueHolder>> constantValueHolderCache;

  /**
   * Create a FragmentContext instance for non-root fragment.
   *
   * @param dbContext DrillbitContext.
   * @param fragment Fragment implementation.
   * @param funcRegistry FunctionImplementationRegistry.
   * @throws ExecutionSetupException
   */
  public FragmentContextImpl(final DrillbitContext dbContext, final PlanFragment fragment,
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
  public FragmentContextImpl(final DrillbitContext dbContext, final PlanFragment fragment, final QueryContext queryContext,
                             final UserClientConnection connection, final FunctionImplementationRegistry funcRegistry)
    throws ExecutionSetupException {
    super(funcRegistry);
    this.context = dbContext;
    this.queryContext = queryContext;
    this.connection = connection;
    this.accountingUserConnection = new AccountingUserConnection(connection, sendingAccountor, statusHandler);
    this.fragment = fragment;
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
      allocator = context.getAllocator().newChildAllocator(
          "frag:" + QueryIdHelper.getFragmentId(fragment.getHandle()),
          fragment.getMemInitial(),
          fragment.getMemMax());
      Preconditions.checkNotNull(allocator, "Unable to acuqire allocator");
    } catch (final OutOfMemoryException e) {
      throw UserException.memoryError(e)
        .addContext("Fragment", getHandle().getMajorFragmentId() + ":" + getHandle().getMinorFragmentId())
        .build(logger);
    } catch(final Throwable e) {
      throw new ExecutionSetupException("Failure while getting memory allocator for fragment.", e);
    }

    stats = new FragmentStats(allocator, fragment.getAssignment());
    bufferManager = new BufferManagerImpl(this.allocator);
    constantValueHolderCache = Maps.newHashMap();
  }

  /**
   * TODO: Remove this constructor when removing the SimpleRootExec (DRILL-2097). This is kept only to avoid modifying
   * the long list of test files.
   */
  public FragmentContextImpl(DrillbitContext dbContext, PlanFragment fragment, UserClientConnection connection,
                             FunctionImplementationRegistry funcRegistry) throws ExecutionSetupException {
    this(dbContext, fragment, null, connection, funcRegistry);
  }

  @Override
  public OptionManager getOptions() {
    return fragmentOptions;
  }

  @Override
  public PhysicalPlanReader getPlanReader() {
    return context.getPlanReader();
  }

  @Override
  public ClusterCoordinator getClusterCoordinator() {
    return context.getClusterCoordinator();
  }

  @Override
  public void setBuffers(final IncomingBuffers buffers) {
    Preconditions.checkArgument(this.buffers == null, "Can only set buffers once.");
    this.buffers = buffers;
  }
  public QueryProfileStoreContext getProfileStoreContext() {
    return context.getProfileStoreContext();
  }

  @Override
  public Set<Map.Entry<UserServer.BitToUserConnection, UserServer.BitToUserConnectionConfig>> getUserConnections() {
    return context.getUserConnections();
  }

  public void setExecutorState(final ExecutorState executorState) {
    Preconditions.checkArgument(this.executorState == null, "ExecutorState can only be set once.");
    this.executorState = executorState;
  }

  public void fail(final Throwable cause) {
    executorState.fail(cause);
  }

  public SchemaPlus getFullRootSchema() {
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

    return queryContext.getFullRootSchema(schemaConfig);
  }

  public FragmentStats getStats() {
    return stats;
  }

  @Override
  public Collection<DrillbitEndpoint> getBits() {
    return context.getBits();
  }

  @Override
  public ContextInformation getContextInformation() {
    return contextInformation;
  }

  @Override
  public DrillbitEndpoint getForemanEndpoint() {
    return fragment.getForeman();
  }

  @Override
  public DrillbitEndpoint getEndpoint() {
    return context.getEndpoint();
  }

  @Override
  public Controller getController() {
    return context.getController();
  }

  @Override
  public OperatorCreatorRegistry getOperatorCreatorRegistry() {
    return context.getOperatorCreatorRegistry();
  }

  @Override
  public ExecutorService getScanDecodeExecutor() {
    return context.getScanDecodeExecutor();
  }

  @Override
  public ExecutorService getScanExecutor() {
    return context.getScanExecutor();
  }

  /**
   * The FragmentHandle for this Fragment
   * @return FragmentHandle
   */
  public FragmentHandle getHandle() {
    return fragment.getHandle();
  }

  public String getFragIdString() {
    final FragmentHandle handle = getHandle();
    final String frag = handle != null ? handle.getMajorFragmentId() + ":" + handle.getMinorFragmentId() : "0:0";
    return frag;
  }

  @Override
  public boolean isUserAuthenticationEnabled() {
    // TODO(DRILL-2097): Until SimpleRootExec tests are removed, we need to consider impersonation disabled if there is
    // no config
    if (getConfig() == null) {
      return false;
    }

    return getConfig().getBoolean(ExecConstants.USER_AUTHENTICATION_ENABLED);
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

  @Override
  public BufferAllocator getNewChildAllocator(final String operatorName,
      final int operatorId,
      final long initialReservation,
      final long maximumReservation) throws OutOfMemoryException {
    return allocator.newChildAllocator(
        "op:" + QueryIdHelper.getFragmentId(fragment.getHandle()) + ":" + operatorId + ":" + operatorName,
        initialReservation,
        maximumReservation
        );
  }

  public boolean isOverMemoryLimit() {
    return allocator.isOverLimit();
  }

  @Override
  public CodeCompiler getCompiler() {
    return context.getCompiler();
  }

  @Override
  public AccountingUserConnection getUserDataTunnel() {
    Preconditions.checkState(connection != null, "Only Root fragment can get UserDataTunnel");
    return accountingUserConnection;
  }

  @Override
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

  public OperatorContext newOperatorContext(PhysicalOperator popConfig, OperatorStats stats)
      throws OutOfMemoryException {
    OperatorContextImpl context = new OperatorContextImpl(popConfig, this, stats);
    contexts.add(context);
    return context;
  }

  public OperatorContext newOperatorContext(PhysicalOperator popConfig)
      throws OutOfMemoryException {
    OperatorContextImpl context = new OperatorContextImpl(popConfig, this);
    contexts.add(context);
    return context;
  }

  @Override
  public DrillConfig getConfig() {
    return context.getConfig();
  }

  @Override
  public ExecutorState getExecutorState() {
    return executorState;
  }

  @Override
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

  @Override
  public PartitionExplorer getPartitionExplorer() {
    throw new UnsupportedOperationException(String.format("The partition explorer interface can only be used " +
        "in functions that can be evaluated at planning time. Make sure that the %s configuration " +
        "option is set to true.", PlannerSettings.CONSTANT_FOLDING.getOptionName()));
  }

  @Override
  public ValueHolder getConstantValueHolder(String value, MinorType type, Function<DrillBuf, ValueHolder> holderInitializer) {
    if (!constantValueHolderCache.containsKey(value)) {
      constantValueHolderCache.put(value, Maps.<MinorType, ValueHolder>newHashMap());
    }

    Map<MinorType, ValueHolder> holdersByType = constantValueHolderCache.get(value);
    ValueHolder valueHolder = holdersByType.get(type);
    if (valueHolder == null) {
      valueHolder = holderInitializer.apply(getManagedBuffer());
      holdersByType.put(type, valueHolder);
    }
    return valueHolder;
  }

  public ExecutorService getExecutor(){
    return context.getExecutor();
  }

  @Override
  public void waitForSendComplete() {
    sendingAccountor.waitForSendComplete();
  }

  @Override
  public WorkEventBus getWorkEventbus() {
    return context.getWorkBus();
  }

  public boolean isBuffersDone() {
    Preconditions.checkState(this.buffers != null, "Incoming Buffers is not set in this fragment context");
    return buffers.isDone();
  }

  @Override
  protected BufferManager getBufferManager() {
    return bufferManager;
  }
}
