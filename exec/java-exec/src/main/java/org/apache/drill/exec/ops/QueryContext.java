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

import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.common.AutoCloseables;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.config.LogicalPlanPersistence;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.expr.fn.registry.RemoteFunctionRegistry;
import org.apache.drill.exec.expr.holders.ValueHolder;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.planner.sql.DrillOperatorTable;
import org.apache.drill.exec.proto.BitControl.QueryContextInformation;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.helper.QueryIdHelper;
import org.apache.drill.exec.rpc.user.UserSession;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.QueryProfileStoreContext;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.server.options.OptionValue;
import org.apache.drill.exec.server.options.QueryOptionManager;
import org.apache.drill.exec.store.PartitionExplorer;
import org.apache.drill.exec.store.PartitionExplorerImpl;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.SchemaConfig.SchemaConfigInfoProvider;
import org.apache.drill.exec.store.SchemaTreeProvider;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.testing.ExecutionControls;
import org.apache.drill.exec.util.Utilities;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.netty.buffer.DrillBuf;

// TODO - consider re-name to PlanningContext, as the query execution context actually appears
// in fragment contexts
public class QueryContext implements AutoCloseable, OptimizerRulesContext, SchemaConfigInfoProvider {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(QueryContext.class);

  private final DrillbitContext drillbitContext;
  private final UserSession session;
  private final OptionManager queryOptions;
  private final PlannerSettings plannerSettings;
  private final ExecutionControls executionControls;

  private final BufferAllocator allocator;
  private final BufferManager bufferManager;
  private final ContextInformation contextInformation;
  private final QueryContextInformation queryContextInfo;
  private final ViewExpansionContext viewExpansionContext;
  private final SchemaTreeProvider schemaTreeProvider;
  /** Stores constants and their holders by type */
  private final Map<String, Map<MinorType, ValueHolder>> constantValueHolderCache;

  /*
   * Flag to indicate if close has been called, after calling close the first
   * time this is set to true and the close method becomes a no-op.
   */
  private boolean closed = false;
  private DrillOperatorTable table;

  public QueryContext(final UserSession session, final DrillbitContext drillbitContext, QueryId queryId) {
    this.drillbitContext = drillbitContext;
    this.session = session;
    queryOptions = new QueryOptionManager(session.getOptions());
    executionControls = new ExecutionControls(queryOptions, drillbitContext.getEndpoint());
    plannerSettings = new PlannerSettings(queryOptions, getFunctionRegistry());
    plannerSettings.setNumEndPoints(drillbitContext.getBits().size());

    // If we do not need to support dynamic UDFs for this query, just use static operator table
    // built at the startup. Else, build new operator table from latest version of function registry.
    if (queryOptions.getOption(ExecConstants.USE_DYNAMIC_UDFS)) {
      this.table = new DrillOperatorTable(drillbitContext.getFunctionImplementationRegistry(), drillbitContext.getOptionManager());
    } else {
      this.table = drillbitContext.getOperatorTable();
    }

    queryContextInfo = Utilities.createQueryContextInfo(session.getDefaultSchemaPath(), session.getSessionId());
    contextInformation = new ContextInformation(session.getCredentials(), queryContextInfo);

    allocator = drillbitContext.getAllocator().newChildAllocator(
        "query:" + QueryIdHelper.getQueryId(queryId),
        PlannerSettings.getInitialPlanningMemorySize(),
        plannerSettings.getPlanningMemoryLimit());
    bufferManager = new BufferManagerImpl(this.allocator);
    viewExpansionContext = new ViewExpansionContext(this);
    schemaTreeProvider = new SchemaTreeProvider(drillbitContext);
    constantValueHolderCache = Maps.newHashMap();
  }

  @Override
  public PlannerSettings getPlannerSettings() {
    return plannerSettings;
  }

  public UserSession getSession() {
    return session;
  }

  @Override
  public BufferAllocator getAllocator() {
    return allocator;
  }

  /**
   * Return reference to default schema instance in a schema tree. Each {@link org.apache.calcite.schema.SchemaPlus}
   * instance can refer to its parent and its children. From the returned reference to default schema instance,
   * clients can traverse the entire schema tree and know the default schema where to look up the tables first.
   *
   * @return Reference to default schema instance in a schema tree.
   */
  public SchemaPlus getNewDefaultSchema() {
    final SchemaPlus rootSchema = getRootSchema();
    final SchemaPlus defaultSchema = session.getDefaultSchema(rootSchema);
    if (defaultSchema == null) {
      return rootSchema;
    }

    return defaultSchema;
  }

  /**
   * Get root schema with schema owner as the user who issued the query that is managed by this QueryContext.
   * @return Root of the schema tree.
   */
  public SchemaPlus getRootSchema() {
    return getRootSchema(getQueryUserName());
  }

  /**
   * Return root schema with schema owner as the given user.
   *
   * @param userName User who owns the schema tree.
   * @return Root of the schema tree.
   */
  @Override
  public SchemaPlus getRootSchema(final String userName) {
    return schemaTreeProvider.createRootSchema(userName, this);
  }

  /**
   *  Create and return a SchemaTree with given <i>schemaConfig</i>.
   * @param schemaConfig
   * @return
   */
  public SchemaPlus getRootSchema(SchemaConfig schemaConfig) {
    return schemaTreeProvider.createRootSchema(schemaConfig);
  }

  /**
   * Get the user name of the user who issued the query that is managed by this QueryContext.
   * @return
   */
  @Override
  public String getQueryUserName() {
    return session.getCredentials().getUserName();
  }

  public OptionManager getOptions() {
    return queryOptions;
  }

  public ExecutionControls getExecutionControls() {
    return executionControls;
  }

  public DrillbitEndpoint getCurrentEndpoint() {
    return drillbitContext.getEndpoint();
  }

  public StoragePluginRegistry getStorage() {
    return drillbitContext.getStorage();
  }

  public LogicalPlanPersistence getLpPersistence() {
    return drillbitContext.getLpPersistence();
  }

  public Collection<DrillbitEndpoint> getActiveEndpoints() {
    return drillbitContext.getBits();
  }

  public DrillConfig getConfig() {
    return drillbitContext.getConfig();
  }

  public QueryProfileStoreContext getProfileStoreContext() {
    return drillbitContext.getProfileStoreContext();
  }

  @Override
  public FunctionImplementationRegistry getFunctionRegistry() {
    return drillbitContext.getFunctionImplementationRegistry();
  }

  @Override
  public ViewExpansionContext getViewExpansionContext() {
    return viewExpansionContext;
  }

  @Override
  public OptionValue getOption(String optionKey) {
    return getOptions().getOption(optionKey);
  }

  public boolean isImpersonationEnabled() {
     return getConfig().getBoolean(ExecConstants.IMPERSONATION_ENABLED);
  }

  public boolean isUserAuthenticationEnabled() {
    return getConfig().getBoolean(ExecConstants.USER_AUTHENTICATION_ENABLED);
  }

  public DrillOperatorTable getDrillOperatorTable() {
    return table;
  }

  /**
   * Re-creates drill operator table to refresh functions list from local function registry.
   */
  public void reloadDrillOperatorTable() {
    table = new DrillOperatorTable(
        drillbitContext.getFunctionImplementationRegistry(),
        drillbitContext.getOptionManager());
  }

  public QueryContextInformation getQueryContextInfo() {
    return queryContextInfo;
  }

  public RemoteFunctionRegistry getRemoteFunctionRegistry() {
    return drillbitContext.getRemoteFunctionRegistry();
  }

  @Override
  public ContextInformation getContextInformation() {
    return contextInformation;
  }

  @Override
  public DrillBuf getManagedBuffer() {
    return bufferManager.getManagedBuffer();
  }

  @Override
  public PartitionExplorer getPartitionExplorer() {
    return new PartitionExplorerImpl(getRootSchema());
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

  @Override
  public void close() throws Exception {
    try {
      if (!closed) {
        List<AutoCloseable> toClose = Lists.newArrayList();

        // TODO(DRILL-1942) the new allocator has this capability built-in, so we can remove bufferManager and
        // allocator from the toClose list.
        toClose.add(bufferManager);
        toClose.add(allocator);
        toClose.add(schemaTreeProvider);

        AutoCloseables.close(toClose);
      }
    } finally {
      closed = true;
    }
  }
}
