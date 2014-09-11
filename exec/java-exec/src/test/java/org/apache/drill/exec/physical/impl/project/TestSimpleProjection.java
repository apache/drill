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
import mockit.Injectable;
import mockit.NonStrictExpectations;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.exec.ExecTest;
import org.apache.drill.exec.compile.CodeCompiler;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.memory.TopLevelAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.physical.base.FragmentRoot;
import org.apache.drill.exec.physical.impl.ImplCreator;
import org.apache.drill.exec.physical.impl.OperatorCreatorRegistry;
import org.apache.drill.exec.physical.impl.SimpleRootExec;
import org.apache.drill.exec.planner.PhysicalPlanReader;
import org.apache.drill.exec.proto.BitControl.PlanFragment;
import org.apache.drill.exec.proto.CoordinationProtos;
import org.apache.drill.exec.rpc.user.UserServer.UserClientConnection;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.util.VectorUtil;
import org.apache.drill.exec.vector.NullableBigIntVector;
import org.junit.Test;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Charsets;
import com.google.common.io.Files;

public class TestSimpleProjection extends ExecTest {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestSimpleProjection.class);
  DrillConfig c = DrillConfig.create();

  @Test
  public void project(@Injectable final DrillbitContext bitContext, @Injectable UserClientConnection connection) throws Throwable{

    new NonStrictExpectations() {{
      bitContext.getMetrics(); result = new MetricRegistry();
      bitContext.getAllocator(); result = new TopLevelAllocator();
      bitContext.getOperatorCreatorRegistry(); result = new OperatorCreatorRegistry(c);
      bitContext.getConfig(); result = c;
      bitContext.getCompiler(); result = CodeCompiler.getTestCompiler(c);
    }};

    PhysicalPlanReader reader = new PhysicalPlanReader(c, c.getMapper(), CoordinationProtos.DrillbitEndpoint.getDefaultInstance());
    PhysicalPlan plan = reader.readPhysicalPlan(Files.toString(FileUtils.getResourceAsFile("/project/test1.json"), Charsets.UTF_8));
    FunctionImplementationRegistry registry = new FunctionImplementationRegistry(c);
    FragmentContext context = new FragmentContext(bitContext, PlanFragment.getDefaultInstance(), connection, registry);
    SimpleRootExec exec = new SimpleRootExec(ImplCreator.getExec(context, (FragmentRoot) plan.getSortedOperators(false).iterator().next()));

    while (exec.next()) {
      VectorUtil.showVectorAccessibleContent(exec.getIncoming(), "\t");
      NullableBigIntVector c1 = exec.getValueVectorById(new SchemaPath("col1", ExpressionPosition.UNKNOWN), NullableBigIntVector.class);
      NullableBigIntVector c2 = exec.getValueVectorById(new SchemaPath("col2", ExpressionPosition.UNKNOWN), NullableBigIntVector.class);
      int x = 0;
      NullableBigIntVector.Accessor a1, a2;
      a1 = c1.getAccessor();
      a2 = c2.getAccessor();

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
