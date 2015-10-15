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
package org.apache.drill.exec.physical.impl.sort;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.SchemaPath;
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
import org.apache.drill.exec.rpc.user.UserServer.UserClientConnection;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.vector.BigIntVector;
import org.apache.drill.exec.vector.IntVector;
import org.junit.Ignore;
import org.junit.Test;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Charsets;
import com.google.common.io.Files;

import mockit.Injectable;
import mockit.NonStrictExpectations;

@Ignore
public class TestSimpleSort extends ExecTest {
  // private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestSimpleSort.class);
  private final DrillConfig c = DrillConfig.create();

  @Test
  public void sortOneKeyAscending(@Injectable final DrillbitContext bitContext, @Injectable UserClientConnection connection) throws Throwable {
    new NonStrictExpectations() {{
      bitContext.getMetrics(); result = new MetricRegistry();
      bitContext.getAllocator(); result = RootAllocatorFactory.newRoot(c);
      bitContext.getOperatorCreatorRegistry(); result = new OperatorCreatorRegistry(ClassPathScanner.fromPrescan(c));
      bitContext.getConfig(); result = c;
      bitContext.getCompiler(); result = CodeCompilerTestFactory.getTestCompiler(c);
    }};

    final PhysicalPlanReader reader = PhysicalPlanReaderTestFactory.defaultPhysicalPlanReader(c);
    final PhysicalPlan plan = reader.readPhysicalPlan(Files.toString(FileUtils.getResourceAsFile("/sort/one_key_sort.json"), Charsets.UTF_8));
    final FunctionImplementationRegistry registry = new FunctionImplementationRegistry(c);
    final FragmentContext context = new FragmentContext(bitContext, PlanFragment.getDefaultInstance(), connection, registry);
    final SimpleRootExec exec = new SimpleRootExec(ImplCreator.getExec(context, (FragmentRoot) plan.getSortedOperators(false).iterator().next()));

    int previousInt = Integer.MIN_VALUE;
    int recordCount = 0;
    int batchCount = 0;

    while(exec.next()) {
      batchCount++;
      final IntVector c1 = exec.getValueVectorById(new SchemaPath("blue", ExpressionPosition.UNKNOWN), IntVector.class);
      final IntVector c2 = exec.getValueVectorById(new SchemaPath("green", ExpressionPosition.UNKNOWN), IntVector.class);

      final IntVector.Accessor a1 = c1.getAccessor();
      final IntVector.Accessor a2 = c2.getAccessor();

      for(int i =0; i < c1.getAccessor().getValueCount(); i++) {
        recordCount++;
        assertTrue(previousInt <= a1.get(i));
        previousInt = a1.get(i);
        assertEquals(previousInt, a2.get(i));
      }
    }

    System.out.println(String.format("Sorted %,d records in %d batches.", recordCount, batchCount));

    if(context.getFailureCause() != null) {
      throw context.getFailureCause();
    }
    assertTrue(!context.isFailed());
  }

  @Test
  public void sortTwoKeysOneAscendingOneDescending(@Injectable final DrillbitContext bitContext, @Injectable UserClientConnection connection) throws Throwable {
    new NonStrictExpectations(){{
      bitContext.getMetrics(); result = new MetricRegistry();
      bitContext.getAllocator(); result = RootAllocatorFactory.newRoot(c);
      bitContext.getOperatorCreatorRegistry(); result = new OperatorCreatorRegistry(ClassPathScanner.fromPrescan(c));
      bitContext.getConfig(); result = c;
      bitContext.getCompiler(); result = CodeCompilerTestFactory.getTestCompiler(c);
    }};

    final PhysicalPlanReader reader = PhysicalPlanReaderTestFactory.defaultPhysicalPlanReader(c);
    final PhysicalPlan plan = reader.readPhysicalPlan(Files.toString(FileUtils.getResourceAsFile("/sort/two_key_sort.json"), Charsets.UTF_8));
    final FunctionImplementationRegistry registry = new FunctionImplementationRegistry(c);
    final FragmentContext context = new FragmentContext(bitContext, PlanFragment.getDefaultInstance(), connection, registry);
    final SimpleRootExec exec = new SimpleRootExec(ImplCreator.getExec(context, (FragmentRoot) plan.getSortedOperators(false).iterator().next()));

    int previousInt = Integer.MIN_VALUE;
    long previousLong = Long.MAX_VALUE;

    int recordCount = 0;
    int batchCount = 0;

    while(exec.next()) {
      batchCount++;
      final IntVector c1 = exec.getValueVectorById(new SchemaPath("blue", ExpressionPosition.UNKNOWN), IntVector.class);
      final BigIntVector c2 = exec.getValueVectorById(new SchemaPath("alt", ExpressionPosition.UNKNOWN), BigIntVector.class);

      final IntVector.Accessor a1 = c1.getAccessor();
      final BigIntVector.Accessor a2 = c2.getAccessor();

      for(int i =0; i < c1.getAccessor().getValueCount(); i++) {
        recordCount++;
        assertTrue(previousInt <= a1.get(i));

        if(previousInt != a1.get(i)) {
          previousLong = Long.MAX_VALUE;
          previousInt = a1.get(i);
        }

        assertTrue(previousLong >= a2.get(i));
        //System.out.println(previousInt + "\t" + a2.get(i));
      }
    }

    System.out.println(String.format("Sorted %,d records in %d batches.", recordCount, batchCount));

    if(context.getFailureCause() != null) {
      throw context.getFailureCause();
    }
    assertTrue(!context.isFailed());
  }
}
