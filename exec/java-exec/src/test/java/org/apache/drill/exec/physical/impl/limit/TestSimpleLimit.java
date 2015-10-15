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
package org.apache.drill.exec.physical.impl.limit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.scanner.ClassPathScanner;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.exec.ExecTest;
import org.apache.drill.exec.compile.CodeCompilerTestFactory;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.memory.RootAllocatorFactory;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.physical.base.FragmentRoot;
import org.apache.drill.exec.physical.impl.ImplCreator;
import org.apache.drill.exec.physical.impl.OperatorCreatorRegistry;
import org.apache.drill.exec.physical.impl.SimpleRootExec;
import org.apache.drill.exec.planner.PhysicalPlanReader;
import org.apache.drill.exec.planner.PhysicalPlanReaderTestFactory;
import org.apache.drill.exec.proto.BitControl.PlanFragment;
import org.apache.drill.exec.rpc.user.UserServer;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.vector.BigIntVector;
import org.junit.Ignore;
import org.junit.Test;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Charsets;
import com.google.common.io.Files;

import mockit.Injectable;
import mockit.NonStrictExpectations;

public class TestSimpleLimit extends ExecTest {
  private final DrillConfig c = DrillConfig.create();

  @Test
  public void testLimit(@Injectable final DrillbitContext bitContext, @Injectable UserServer.UserClientConnection connection) throws Throwable {
    new NonStrictExpectations() {{
      bitContext.getMetrics(); result = new MetricRegistry();
      bitContext.getAllocator(); result = RootAllocatorFactory.newRoot(c);
      bitContext.getOperatorCreatorRegistry(); result = new OperatorCreatorRegistry(ClassPathScanner.fromPrescan(c));
      bitContext.getConfig(); result = c;
      bitContext.getCompiler(); result = CodeCompilerTestFactory.getTestCompiler(c);
    }};

    verifyLimitCount(bitContext, connection, "test1.json", 5);
  }

  @Test
  public void testLimitNoEnd(@Injectable final DrillbitContext bitContext, @Injectable UserServer.UserClientConnection connection) throws Throwable {
    new NonStrictExpectations() {{
      bitContext.getMetrics(); result = new MetricRegistry();
      bitContext.getAllocator(); result = RootAllocatorFactory.newRoot(c);
      bitContext.getOperatorCreatorRegistry(); result = new OperatorCreatorRegistry(ClassPathScanner.fromPrescan(c));
      bitContext.getConfig(); result = c;
      bitContext.getCompiler(); result = CodeCompilerTestFactory.getTestCompiler(c);
    }};

    verifyLimitCount(bitContext, connection, "test3.json", 95);
  }

  @Test
  @Ignore
  // The testcase is not valid. "test4.json" using increasingBigInt(0) to generate a list of increasing number starting from 0, and verify the sum.
  // However, when evaluate the increasingBitInt(0), if the outgoing batch could not hold the new value, doEval() return false, and start the
  // next batch. But the value has already been increased by 1 in the prior failed try. Therefore, the sum of the generated number could be different,
  // depending on the size of each outgoing batch, and when the batch could not hold any more values.
  public void testLimitAcrossBatches(@Injectable final DrillbitContext bitContext, @Injectable UserServer.UserClientConnection connection) throws Throwable {
    new NonStrictExpectations(){{
      bitContext.getMetrics(); result = new MetricRegistry();
      bitContext.getAllocator(); result = RootAllocatorFactory.newRoot(c);
      bitContext.getOperatorCreatorRegistry(); result = new OperatorCreatorRegistry(ClassPathScanner.fromPrescan(c));
      bitContext.getConfig(); result = c;
      bitContext.getCompiler(); result = CodeCompilerTestFactory.getTestCompiler(c);
    }};

    verifyLimitCount(bitContext, connection, "test2.json", 69999);
    final long start = 30000;
    final long end = 100000;
    final long expectedSum = (end - start) * (end + start - 1) / 2; //Formula for sum of series

    verifySum(bitContext, connection, "test4.json", 70000, expectedSum);


  }

  private void verifyLimitCount(DrillbitContext bitContext, UserServer.UserClientConnection connection, String testPlan, int expectedCount) throws Throwable {
    final PhysicalPlanReader reader = PhysicalPlanReaderTestFactory.defaultPhysicalPlanReader(c);
    final PhysicalPlan plan = reader.readPhysicalPlan(Files.toString(FileUtils.getResourceAsFile("/limit/" + testPlan), Charsets.UTF_8));
    final FunctionImplementationRegistry registry = new FunctionImplementationRegistry(c);
    final FragmentContext context = new FragmentContext(bitContext, PlanFragment.getDefaultInstance(), connection, registry);
    final SimpleRootExec exec = new SimpleRootExec(ImplCreator.getExec(context, (FragmentRoot) plan.getSortedOperators(false).iterator().next()));
    int recordCount = 0;
    while(exec.next()) {
      recordCount += exec.getRecordCount();
    }

    assertEquals(expectedCount, recordCount);

    if(context.getFailureCause() != null) {
      throw context.getFailureCause();
    }

    assertTrue(!context.isFailed());
  }

  private void verifySum(DrillbitContext bitContext, UserServer.UserClientConnection connection, String testPlan, int expectedCount, long expectedSum) throws Throwable {
    final PhysicalPlanReader reader = PhysicalPlanReaderTestFactory.defaultPhysicalPlanReader(c);
    final PhysicalPlan plan = reader.readPhysicalPlan(Files.toString(FileUtils.getResourceAsFile("/limit/" + testPlan), Charsets.UTF_8));
    final FunctionImplementationRegistry registry = new FunctionImplementationRegistry(c);
    final FragmentContext context = new FragmentContext(bitContext, PlanFragment.getDefaultInstance(), connection, registry);
    final SimpleRootExec exec = new SimpleRootExec(ImplCreator.getExec(context, (FragmentRoot) plan.getSortedOperators(false).iterator().next()));
    int recordCount = 0;
    long sum = 0;
    while(exec.next()) {
      recordCount += exec.getRecordCount();
      final BigIntVector v = (BigIntVector) exec.iterator().next();
      for (int i = 0; i < v.getAccessor().getValueCount(); i++) {
        sum += v.getAccessor().get(i);
      }
    }

    assertEquals(expectedCount, recordCount);
    assertEquals(expectedSum, sum);

    if(context.getFailureCause() != null) {
      throw context.getFailureCause();
    }
    assertTrue(!context.isFailed());
  }
}
