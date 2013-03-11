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
package org.apache.drill.common.logical;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.logical.data.Scan;
import org.apache.drill.common.logical.data.Store;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.storage.MockStorageEngineConfig;
import org.junit.Test;

import java.io.IOException;

import static junit.framework.TestCase.assertEquals;

public class LogicalPlanBuilderTest {


   /**
    * Tests assembling the same plan as simple_engine_plan.json
    */
   @Test
   public void testBuildSimplePlan() throws IOException {


      PlanProperties planProperties = new PlanProperties();
      planProperties.version = 1;
      planProperties.generator = new PlanProperties.Generator();
      planProperties.generator.type = "manual";
      planProperties.generator.info = "na";

      Scan scan = new Scan("mock-engine", null, null);
      Store store = new Store("mock-engine", null, null);
      store.setInput(scan);

      LogicalPlanBuilder builder = new LogicalPlanBuilder()
              .planProperties(planProperties)
              .addStorageEngine(new MockStorageEngineConfig("mock-engine", "http://www.apache.org/"))
              .addLogicalOperator(scan)
              .addLogicalOperator(store);

      LogicalPlan fromBuilder = builder.build();

      DrillConfig config = DrillConfig.create();
      LogicalPlan fromJson = LogicalPlan.parse(config, FileUtils.getResourceAsString("/storage_engine_plan.json"));

      assertEquals(fromJson.toJsonString(config), fromBuilder.toJsonString(config));

   }
}
