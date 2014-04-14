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
import net.hydromatic.optiq.tools.Frameworks;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.util.TestTools;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.memory.TopLevelAllocator;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.planner.sql.DrillSqlWorker;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.rpc.user.UserSession;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.junit.Rule;
import org.junit.rules.TestRule;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;

public class PlanningBase {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PlanningBase.class);

  @Rule public final TestRule TIMEOUT = TestTools.getTimeoutRule(30000);
  @Rule public final TestRule LOG_OUTCOME = TestTools.getTestLogReporter(logger);

  @Mocked DrillbitContext dbContext;
  @Mocked QueryContext context;
  private final DrillConfig config = DrillConfig.create();


  protected void testSqlPlanFromFile(String file) throws Exception{
    testSqlPlan(getFile(file));
  }

  protected void testSqlPlan(String sqlCommands) throws Exception{
    String[] sqlStrings = sqlCommands.split(";");

    new NonStrictExpectations() {
      {
        dbContext.getMetrics();
        result = new MetricRegistry();
        dbContext.getAllocator();
        result = new TopLevelAllocator();
        dbContext.getConfig();
        result = config;
      }
    };

    StoragePluginRegistry registry = new StoragePluginRegistry(dbContext);
    final FunctionImplementationRegistry functionRegistry = new FunctionImplementationRegistry(config);
    final SchemaPlus root = Frameworks.createRootSchema();
    registry.getSchemaFactory().registerSchemas(null, root);


    new NonStrictExpectations() {
      {
        context.getNewDefaultSchema();
        result = root;
        context.getFunctionRegistry();
        result = functionRegistry;
        context.getSession();
        result = new UserSession(null, null, null);
        context.getCurrentEndpoint();
        result = DrillbitEndpoint.getDefaultInstance();
        context.getActiveEndpoints();
        result = ImmutableList.of(DrillbitEndpoint.getDefaultInstance());
        context.getConfig();
        result = config;
      }
    };

    for(String sql : sqlStrings){
      if(sql.trim().isEmpty()) continue;
      DrillSqlWorker worker = new DrillSqlWorker(context);
      PhysicalPlan p = worker.getPlan(sql);
    }

  }

  protected String getFile(String resource) throws IOException{
    URL url = Resources.getResource(resource);
    if(url == null){
      throw new IOException(String.format("Unable to find path %s.", resource));
    }
    return Resources.toString(url, Charsets.UTF_8);
  }
}
