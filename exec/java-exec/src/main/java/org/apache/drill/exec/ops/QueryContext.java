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

import java.util.Collection;

import io.netty.buffer.DrillBuf;
import net.hydromatic.optiq.SchemaPlus;
import net.hydromatic.optiq.jdbc.SimpleOptiqSchema;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.expr.fn.impl.DateUtility;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.OutOfMemoryException;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.planner.sql.DrillOperatorTable;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.rpc.user.UserSession;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.server.options.QueryOptionManager;
import org.apache.drill.exec.store.PartitionExplorer;
import org.apache.drill.exec.store.PartitionExplorerImpl;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.testing.ExecutionControls;

// TODO except for a couple of tests, this is only created by Foreman
// TODO the many methods that just return drillbitContext.getXxx() should be replaced with getDrillbitContext()
// TODO - consider re-name to PlanningContext, as the query execution context actually appears
// in fragment contexts
public class QueryContext implements AutoCloseable, UdfUtilities {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(QueryContext.class);

  private final DrillbitContext drillbitContext;
  private final UserSession session;
  private final OptionManager queryOptions;
  private final PlannerSettings plannerSettings;
  private final DrillOperatorTable table;
  private final ExecutionControls executionControls;

  private final BufferAllocator allocator;
  private final BufferManager bufferManager;
  private final QueryDateTimeInfo queryDateTimeInfo;
  private static final int INITIAL_OFF_HEAP_ALLOCATION_IN_BYTES = 1024 * 1024;
  private static final int MAX_OFF_HEAP_ALLOCATION_IN_BYTES = 16 * 1024 * 1024;

  /*
   * Flag to indicate if close has been called, after calling close the first
   * time this is set to true and the close method becomes a no-op.
   */
  private boolean closed = false;

  public QueryContext(final UserSession session, final DrillbitContext drillbitContext) {
    this.drillbitContext = drillbitContext;
    this.session = session;
    queryOptions = new QueryOptionManager(session.getOptions());
    executionControls = new ExecutionControls(queryOptions, drillbitContext.getEndpoint());
    plannerSettings = new PlannerSettings(queryOptions, getFunctionRegistry());
    plannerSettings.setNumEndPoints(drillbitContext.getBits().size());
    table = new DrillOperatorTable(getFunctionRegistry());

    final long queryStartTime = System.currentTimeMillis();
    final int timeZone = DateUtility.getIndex(System.getProperty("user.timezone"));
    queryDateTimeInfo = new QueryDateTimeInfo(queryStartTime, timeZone);

    try {
      allocator = drillbitContext.getAllocator().getChildAllocator(null, INITIAL_OFF_HEAP_ALLOCATION_IN_BYTES,
          MAX_OFF_HEAP_ALLOCATION_IN_BYTES, false);
    } catch (OutOfMemoryException e) {
      throw new DrillRuntimeException("Error creating off-heap allocator for planning context.",e);
    }
    // TODO(DRILL-1942) the new allocator has this capability built-in, so this can be removed once that is available
    bufferManager = new BufferManager(this.allocator, null);
  }

  public PlannerSettings getPlannerSettings() {
    return plannerSettings;
  }

  public UserSession getSession() {
    return session;
  }

  public BufferAllocator getAllocator() {
    return allocator;
  }

  public SchemaPlus getNewDefaultSchema() {
    final SchemaPlus rootSchema = getRootSchema();
    final SchemaPlus defaultSchema = session.getDefaultSchema(rootSchema);
    if (defaultSchema == null) {
      return rootSchema;
    }

    return defaultSchema;
  }

  public SchemaPlus getRootSchema() {
    final SchemaPlus rootSchema = SimpleOptiqSchema.createRootSchema(false);
    drillbitContext.getSchemaFactory().registerSchemas(session, rootSchema);
    return rootSchema;
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

  public Collection<DrillbitEndpoint> getActiveEndpoints() {
    return drillbitContext.getBits();
  }

  public DrillConfig getConfig() {
    return drillbitContext.getConfig();
  }

  public FunctionImplementationRegistry getFunctionRegistry() {
    return drillbitContext.getFunctionImplementationRegistry();
  }

  public DrillOperatorTable getDrillOperatorTable() {
    return table;
  }

  @Override
  public QueryDateTimeInfo getQueryDateTimeInfo() {
    return queryDateTimeInfo;
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
  public void close() throws Exception {
    try {
      if (!closed) {
        // TODO(DRILL-1942) the new allocator has this capability built-in, so this can be removed once that is available
        bufferManager.close();
        allocator.close();
      }
    } finally {
      closed = true;
    }
  }
}
