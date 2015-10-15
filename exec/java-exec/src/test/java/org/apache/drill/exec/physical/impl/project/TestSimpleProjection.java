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
package org.apache.drill.exec.physical.impl.project;

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
import org.apache.drill.exec.util.VectorUtil;
import org.apache.drill.exec.vector.NullableBigIntVector;
import org.junit.Test;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Charsets;
import com.google.common.io.Files;

import mockit.Injectable;
import mockit.NonStrictExpectations;

public class TestSimpleProjection extends ExecTest {
  //private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestSimpleProjection.class);
  private final DrillConfig c = DrillConfig.create();

  @Test
  public void project(@Injectable final DrillbitContext bitContext, @Injectable UserClientConnection connection) throws Throwable {
    new NonStrictExpectations() {{
      bitContext.getMetrics(); result = new MetricRegistry();
      bitContext.getAllocator(); result = RootAllocatorFactory.newRoot(c);
      bitContext.getOperatorCreatorRegistry(); result = new OperatorCreatorRegistry(ClassPathScanner.fromPrescan(c));
      bitContext.getConfig(); result = c;
      bitContext.getCompiler(); result = CodeCompilerTestFactory.getTestCompiler(c);
    }};

    final PhysicalPlanReader reader = PhysicalPlanReaderTestFactory.defaultPhysicalPlanReader(c);
    final PhysicalPlan plan = reader.readPhysicalPlan(Files.toString(FileUtils.getResourceAsFile("/project/test1.json"), Charsets.UTF_8));
    final FunctionImplementationRegistry registry = new FunctionImplementationRegistry(c);
    final FragmentContext context = new FragmentContext(bitContext, PlanFragment.getDefaultInstance(), connection, registry);
    final SimpleRootExec exec = new SimpleRootExec(ImplCreator.getExec(context, (FragmentRoot) plan.getSortedOperators(false).iterator().next()));

    while (exec.next()) {
      VectorUtil.showVectorAccessibleContent(exec.getIncoming(), "\t");
      final NullableBigIntVector c1 = exec.getValueVectorById(new SchemaPath("col1", ExpressionPosition.UNKNOWN), NullableBigIntVector.class);
      final NullableBigIntVector c2 = exec.getValueVectorById(new SchemaPath("col2", ExpressionPosition.UNKNOWN), NullableBigIntVector.class);
      int x = 0;
      final NullableBigIntVector.Accessor a1 = c1.getAccessor();
      final NullableBigIntVector.Accessor a2 = c2.getAccessor();

      for (int i =0; i < c1.getAccessor().getValueCount(); i++) {
        if (!a1.isNull(i)) {
          assertEquals(a1.get(i)+1, a2.get(i));
        }
        x += a1.isNull(i) ? 0 : a1.get(i);
      }
    }

    if (context.getFailureCause() != null) {
      throw context.getFailureCause();
    }
    assertTrue(!context.isFailed());
  }
}
