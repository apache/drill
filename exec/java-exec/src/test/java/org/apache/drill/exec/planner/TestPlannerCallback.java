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
package org.apache.drill.exec.planner;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.util.CancelFlag;
import org.apache.drill.BaseTestQuery;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.SchemaConfig;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestPlannerCallback extends BaseTestQuery {

  @BeforeClass
  public static void setupDefaultTestCluster() throws Exception {
    BaseTestQuery.setupDefaultTestCluster();
    bits[0].getContext().getStorage().addPlugin("fake", new FakeStoragePlugin());
  }

  @Test
  public void ensureCallbackIsRegistered() throws Exception {
    try{
      test("select * from cp.`employee.json`");
    }catch(Exception e){
      assertTrue(e.getMessage().contains("Statement preparation aborted"));
      return;
    }

    fail("Should have seen exception in planning due to planner initialization.");
  }

  public static class FakeStoragePlugin extends AbstractStoragePlugin {

    @Override
    public StoragePluginConfig getConfig() {
      return null;
    }

    @Override
    public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
      // TODO Auto-generated method stub
    }

    @Override
    public PlannerCallback getPlannerCallback(QueryContext optimizerContext, PlannerPhase phase) {
      return new PlannerCallback(){
        @Override
        public void initializePlanner(RelOptPlanner planner) {
          CancelFlag f = new CancelFlag();
          f.requestCancel();
          planner.setCancelFlag(f);
        }

      };
    }


  }
}
