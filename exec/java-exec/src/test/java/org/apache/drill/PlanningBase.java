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
package org.apache.drill;

import java.io.IOException;
import java.net.URL;

import mockit.Mocked;
import mockit.NonStrictExpectations;
import net.hydromatic.optiq.SchemaPlus;
import net.hydromatic.optiq.jdbc.SimpleOptiqSchema;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.util.TestTools;
import org.apache.drill.exec.ExecTest;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.memory.TopLevelAllocator;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.planner.sql.DrillOperatorTable;
import org.apache.drill.exec.planner.sql.DrillSqlWorker;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.rpc.user.UserSession;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.options.QueryOptionManager;
import org.apache.drill.exec.server.options.SessionOptionManager;
import org.apache.drill.exec.server.options.SystemOptionManager;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.sys.local.LocalPStoreProvider;
import org.junit.Rule;
import org.junit.rules.TestRule;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;

public class PlanningBase extends ExecTest{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PlanningBase.class);

  @Rule public final TestRule TIMEOUT = TestTools.getTimeoutRule(10000);

  @Mocked DrillbitContext dbContext;
  @Mocked QueryContext context;
  private final DrillConfig config = DrillConfig.create();


  protected void testSqlPlanFromFile(String file) throws Exception {
    testSqlPlan(getFile(file));
  }

  protected void testSqlPlan(String sqlCommands) throws Exception {
    String[] sqlStrings = sqlCommands.split(";");


    final LocalPStoreProvider provider = new LocalPStoreProvider(config);
    provider.start();

    final SystemOptionManager systemOptions = new SystemOptionManager(config, provider);
    systemOptions.init();
    final SessionOptionManager sessionOptions = new SessionOptionManager(systemOptions);
    final QueryOptionManager queryOptions = new QueryOptionManager(sessionOptions);

    new NonStrictExpectations() {
      {
        dbContext.getMetrics();
        result = new MetricRegistry();
        dbContext.getAllocator();
        result = new TopLevelAllocator();
        dbContext.getConfig();
        result = config;
        dbContext.getOptionManager();
        result = systemOptions;
        dbContext.getPersistentStoreProvider();
        result = provider;
      }
    };

    final StoragePluginRegistry registry = new StoragePluginRegistry(dbContext);
    registry.init();
    final FunctionImplementationRegistry functionRegistry = new FunctionImplementationRegistry(config);
    final DrillOperatorTable table = new DrillOperatorTable(functionRegistry);
    final SchemaPlus root = SimpleOptiqSchema.createRootSchema(false);
    registry.getSchemaFactory().registerSchemas(UserSession.Builder.newBuilder().setSupportComplexTypes(true).build(), root);


    new NonStrictExpectations() {
      {
        context.getNewDefaultSchema();
        result = root;
        context.getStorage();
        result = registry;
        context.getFunctionRegistry();
        result = functionRegistry;
        context.getSession();
        result = UserSession.Builder.newBuilder().setSupportComplexTypes(true).build();
        context.getCurrentEndpoint();
        result = DrillbitEndpoint.getDefaultInstance();
        context.getActiveEndpoints();
        result = ImmutableList.of(DrillbitEndpoint.getDefaultInstance());
        context.getPlannerSettings();
        result = new PlannerSettings(queryOptions, functionRegistry);
        context.getOptions();
        result = queryOptions;
        context.getConfig();
        result = config;
        context.getDrillOperatorTable();
        result = table;
      }
    };

    for (String sql : sqlStrings) {
      if (sql.trim().isEmpty()) {
        continue;
      }
      DrillSqlWorker worker = new DrillSqlWorker(context);
      PhysicalPlan p = worker.getPlan(sql);
    }

  }

  protected String getFile(String resource) throws IOException{
    URL url = Resources.getResource(resource);
    if (url == null) {
      throw new IOException(String.format("Unable to find path %s.", resource));
    }
    return Resources.toString(url, Charsets.UTF_8);
  }
}
