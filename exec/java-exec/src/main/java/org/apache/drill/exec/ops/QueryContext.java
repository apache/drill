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

import net.hydromatic.optiq.SchemaPlus;
import net.hydromatic.optiq.jdbc.SimpleOptiqSchema;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.coord.ClusterCoordinator;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.planner.PhysicalPlanReader;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.planner.sql.DrillOperatorTable;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.rpc.control.WorkEventBus;
import org.apache.drill.exec.rpc.data.DataConnectionCreator;
import org.apache.drill.exec.rpc.user.UserSession;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.server.options.QueryOptionManager;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.sys.PStoreProvider;

public class QueryContext{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(QueryContext.class);

  private final QueryId queryId;
  private final DrillbitContext drillbitContext;
  private final WorkEventBus workBus;
  private UserSession session;
  private OptionManager queryOptions;
  public final Multitimer<QuerySetup> timer;
  private final PlannerSettings plannerSettings;
  private final DrillOperatorTable table;

  public QueryContext(UserSession session, QueryId queryId, DrillbitContext drllbitContext) {
    super();
    this.queryId = queryId;
    this.drillbitContext = drllbitContext;
    this.workBus = drllbitContext.getWorkBus();
    this.session = session;
    this.timer = new Multitimer<>(QuerySetup.class);
    this.queryOptions = new QueryOptionManager(session.getOptions());
    this.plannerSettings = new PlannerSettings(queryOptions, getFunctionRegistry());
    this.plannerSettings.setNumEndPoints(this.getActiveEndpoints().size());
    this.table = new DrillOperatorTable(getFunctionRegistry());
  }

  public PStoreProvider getPersistentStoreProvider(){
    return drillbitContext.getPersistentStoreProvider();
  }

  public PlannerSettings getPlannerSettings(){
    return plannerSettings;
  }

  public UserSession getSession(){
    return session;
  }

  public SchemaPlus getNewDefaultSchema(){
    SchemaPlus rootSchema = getRootSchema();
    SchemaPlus defaultSchema = session.getDefaultSchema(rootSchema);
    if(defaultSchema == null){
      return rootSchema;
    }else{
      return defaultSchema;
    }
  }

  public SchemaPlus getRootSchema(){
    SchemaPlus rootSchema = SimpleOptiqSchema.createRootSchema(false);
    drillbitContext.getSchemaFactory().registerSchemas(session, rootSchema);
    return rootSchema;
  }

  public OptionManager getOptions() {
    return queryOptions;
  }

  public OptionManager getSessionOptions() {
    return session.getOptions();
  }

  public DrillbitEndpoint getCurrentEndpoint(){
    return drillbitContext.getEndpoint();
  }

  public QueryId getQueryId() {
    return queryId;
  }

  public StoragePluginRegistry getStorage(){
    return drillbitContext.getStorage();
  }

  public Collection<DrillbitEndpoint> getActiveEndpoints(){
    return drillbitContext.getBits();
  }

  public PhysicalPlanReader getPlanReader(){
    return drillbitContext.getPlanReader();
  }

  public DataConnectionCreator getDataConnectionsPool(){
    return drillbitContext.getDataConnectionsPool();
  }

  public DrillConfig getConfig(){
    return drillbitContext.getConfig();
  }

  public WorkEventBus getWorkBus(){
    return workBus;
  }

  public FunctionImplementationRegistry getFunctionRegistry(){
    return drillbitContext.getFunctionImplementationRegistry();
  }

  public DrillOperatorTable getDrillOperatorTable() {
    return table;
  }

  public ClusterCoordinator getClusterCoordinator() {
    return drillbitContext.getClusterCoordinator();
  }
}
