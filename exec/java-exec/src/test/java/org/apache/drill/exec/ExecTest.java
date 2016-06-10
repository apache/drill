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
package org.apache.drill.exec;

import com.codahale.metrics.MetricRegistry;
import mockit.NonStrictExpectations;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.scanner.ClassPathScanner;
import org.apache.drill.exec.compile.CodeCompilerTestFactory;
import org.apache.drill.exec.memory.RootAllocatorFactory;
import org.apache.drill.exec.metrics.DrillMetrics;
import org.apache.drill.exec.physical.impl.OperatorCreatorRegistry;
import org.apache.drill.exec.planner.PhysicalPlanReaderTestFactory;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.options.SystemOptionManager;
import org.apache.drill.exec.store.sys.store.provider.LocalPersistentStoreProvider;
import org.apache.drill.test.DrillTest;
import org.junit.After;
import org.junit.BeforeClass;


public class ExecTest extends DrillTest {

  protected static SystemOptionManager optionManager;
  private static final DrillConfig c = DrillConfig.create();

  @After
  public void clear(){
    // TODO:  (Re DRILL-1735) Check whether still needed now that
    // BootstrapContext.close() resets the metrics.
    DrillMetrics.resetMetrics();
  }


  @BeforeClass
  public static void setupOptionManager() throws Exception{
    final LocalPersistentStoreProvider provider = new LocalPersistentStoreProvider(c);
    provider.start();
    optionManager = new SystemOptionManager(PhysicalPlanReaderTestFactory.defaultLogicalPlanPersistence(c), provider);
    optionManager.init();
  }

  protected void mockDrillbitContext(final DrillbitContext bitContext) throws Exception {
    new NonStrictExpectations() {{
      bitContext.getMetrics(); result = new MetricRegistry();
      bitContext.getAllocator(); result = RootAllocatorFactory.newRoot(c);
      bitContext.getOperatorCreatorRegistry(); result = new OperatorCreatorRegistry(ClassPathScanner.fromPrescan(c));
      bitContext.getConfig(); result = c;
      bitContext.getOptionManager(); result = optionManager;
      bitContext.getCompiler(); result = CodeCompilerTestFactory.getTestCompiler(c);
    }};
  }

}
