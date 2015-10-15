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
package org.apache.drill.exec.physical.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.scanner.ClassPathScanner;
import org.apache.drill.exec.ExecTest;
import org.apache.drill.exec.compile.CodeCompilerTestFactory;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.memory.RootAllocatorFactory;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.physical.base.FragmentRoot;
import org.apache.drill.exec.planner.PhysicalPlanReader;
import org.apache.drill.exec.planner.PhysicalPlanReaderTestFactory;
import org.apache.drill.exec.proto.BitControl.PlanFragment;
import org.apache.drill.exec.rpc.user.UserServer;
import org.apache.drill.exec.server.DrillbitContext;
import org.junit.Test;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;

import mockit.Injectable;
import mockit.NonStrictExpectations;

public class TestComparisonFunctions extends ExecTest {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestComparisonFunctions.class);

  private final DrillConfig c = DrillConfig.create();
  private final String COMPARISON_TEST_PHYSICAL_PLAN = "functions/comparisonTest.json";
  private PhysicalPlanReader reader;
  private FunctionImplementationRegistry registry;

  public void runTest(@Injectable final DrillbitContext bitContext,
                      @Injectable UserServer.UserClientConnection connection, String expression, int expectedResults) throws Throwable {
    new NonStrictExpectations() {{
      bitContext.getMetrics(); result = new MetricRegistry();
      bitContext.getAllocator(); result = RootAllocatorFactory.newRoot(c);
      bitContext.getOperatorCreatorRegistry(); result = new OperatorCreatorRegistry(ClassPathScanner.fromPrescan(c));
      bitContext.getConfig(); result = c;
      bitContext.getCompiler(); result = CodeCompilerTestFactory.getTestCompiler(c);
    }};

    final String planString = Resources.toString(Resources.getResource(COMPARISON_TEST_PHYSICAL_PLAN), Charsets.UTF_8).replaceAll("EXPRESSION", expression);
    if (reader == null) {
      reader = PhysicalPlanReaderTestFactory.defaultPhysicalPlanReader(c);
    }
    if (registry == null) {
      registry = new FunctionImplementationRegistry(c);
    }
    final FragmentContext context =
        new FragmentContext(bitContext, PlanFragment.getDefaultInstance(), connection, registry);
    final PhysicalPlan plan = reader.readPhysicalPlan(planString);
    final SimpleRootExec exec = new SimpleRootExec(ImplCreator.getExec(context, (FragmentRoot) plan.getSortedOperators(false).iterator().next()));

    while(exec.next()) {
      assertEquals(String.format("Expression: %s;", expression), expectedResults,
          exec.getSelectionVector2().getCount());
//      for (ValueVector vv: exec) {
//        vv.close();
//      }
    }

    exec.close();
    context.close();

    if (context.getFailureCause() != null) {
      throw context.getFailureCause();
    }

    assertTrue(!context.isFailed());
  }

  @Test
  public void testInt(@Injectable final DrillbitContext bitContext,
                           @Injectable UserServer.UserClientConnection connection) throws Throwable {
    runTest(bitContext, connection, "intColumn == intColumn", 100);
    runTest(bitContext, connection, "intColumn != intColumn", 0);
    runTest(bitContext, connection, "intColumn > intColumn", 0);
    runTest(bitContext, connection, "intColumn < intColumn", 0);
    runTest(bitContext, connection, "intColumn >= intColumn", 100);
    runTest(bitContext, connection, "intColumn <= intColumn", 100);
  }

  @Test
  public void testBigInt(@Injectable final DrillbitContext bitContext,
                      @Injectable UserServer.UserClientConnection connection) throws Throwable {
    runTest(bitContext, connection, "bigIntColumn == bigIntColumn", 100);
    runTest(bitContext, connection, "bigIntColumn != bigIntColumn", 0);
    runTest(bitContext, connection, "bigIntColumn > bigIntColumn", 0);
    runTest(bitContext, connection, "bigIntColumn < bigIntColumn", 0);
    runTest(bitContext, connection, "bigIntColumn >= bigIntColumn", 100);
    runTest(bitContext, connection, "bigIntColumn <= bigIntColumn", 100);
  }

  @Test
  public void testFloat4(@Injectable final DrillbitContext bitContext,
                         @Injectable UserServer.UserClientConnection connection) throws Throwable {
    runTest(bitContext, connection, "float4Column == float4Column", 100);
    runTest(bitContext, connection, "float4Column != float4Column", 0);
    runTest(bitContext, connection, "float4Column > float4Column", 0);
    runTest(bitContext, connection, "float4Column < float4Column", 0);
    runTest(bitContext, connection, "float4Column >= float4Column", 100);
    runTest(bitContext, connection, "float4Column <= float4Column", 100);
  }

  @Test
  public void testFloat8(@Injectable final DrillbitContext bitContext,
                         @Injectable UserServer.UserClientConnection connection) throws Throwable {
    runTest(bitContext, connection, "float8Column == float8Column", 100);
    runTest(bitContext, connection, "float8Column != float8Column", 0);
    runTest(bitContext, connection, "float8Column > float8Column", 0);
    runTest(bitContext, connection, "float8Column < float8Column", 0);
    runTest(bitContext, connection, "float8Column >= float8Column", 100);
    runTest(bitContext, connection, "float8Column <= float8Column", 100);
  }

  @Test
  public void testIntNullable(@Injectable final DrillbitContext bitContext,
                      @Injectable UserServer.UserClientConnection connection) throws Throwable {
    runTest(bitContext, connection, "intNullableColumn == intNullableColumn", 50);
    runTest(bitContext, connection, "intNullableColumn != intNullableColumn", 0);
    runTest(bitContext, connection, "intNullableColumn > intNullableColumn", 0);
    runTest(bitContext, connection, "intNullableColumn < intNullableColumn", 0);
    runTest(bitContext, connection, "intNullableColumn >= intNullableColumn", 50);
    runTest(bitContext, connection, "intNullableColumn <= intNullableColumn", 50);
  }

  @Test
  public void testBigIntNullable(@Injectable final DrillbitContext bitContext,
                         @Injectable UserServer.UserClientConnection connection) throws Throwable {
    runTest(bitContext, connection, "bigIntNullableColumn == bigIntNullableColumn", 50);
    runTest(bitContext, connection, "bigIntNullableColumn != bigIntNullableColumn", 0);
    runTest(bitContext, connection, "bigIntNullableColumn > bigIntNullableColumn", 0);
    runTest(bitContext, connection, "bigIntNullableColumn < bigIntNullableColumn", 0);
    runTest(bitContext, connection, "bigIntNullableColumn >= bigIntNullableColumn", 50);
    runTest(bitContext, connection, "bigIntNullableColumn <= bigIntNullableColumn", 50);
  }
}
