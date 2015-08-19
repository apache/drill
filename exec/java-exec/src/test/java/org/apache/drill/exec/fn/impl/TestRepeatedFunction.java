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
package org.apache.drill.exec.fn.impl;

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
import org.apache.drill.exec.memory.RootAllocator;
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
import org.apache.drill.exec.vector.BitVector;
import org.apache.drill.exec.vector.IntVector;
import org.junit.Test;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Charsets;
import com.google.common.io.Files;

public class TestRepeatedFunction extends ExecTest{
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestRepeatedFunction.class);
  final DrillConfig c = DrillConfig.create();

  @Test
  public void testRepeated(@Injectable final DrillbitContext bitContext, @Injectable UserClientConnection connection) throws Throwable{
    new NonStrictExpectations(){{
      bitContext.getMetrics(); result = new MetricRegistry();
      bitContext.getOperatorCreatorRegistry(); result = new OperatorCreatorRegistry(c);
      bitContext.getConfig(); result = c;
      bitContext.getAllocator(); result = new RootAllocator(c);
      bitContext.getCompiler(); result = CodeCompiler.getTestCompiler(c);
    }};

    PhysicalPlanReader reader = new PhysicalPlanReader(c, c.getMapper(), CoordinationProtos.DrillbitEndpoint.getDefaultInstance());
    PhysicalPlan plan = reader.readPhysicalPlan(Files.toString(FileUtils.getResourceAsFile("/physical_repeated_1.json"), Charsets.UTF_8));
    FunctionImplementationRegistry registry = new FunctionImplementationRegistry(c);
    FragmentContext context = new FragmentContext(bitContext, PlanFragment.getDefaultInstance(), connection, registry);
    SimpleRootExec exec = new SimpleRootExec(ImplCreator.getExec(context, (FragmentRoot) plan.getSortedOperators(false).iterator().next()));

    boolean oneIsOne = false;
    int size = 0;
    int[] sizes = {1,2,0,6};

    while(exec.next()){
      IntVector c1 = exec.getValueVectorById(new SchemaPath("cnt", ExpressionPosition.UNKNOWN), IntVector.class);
      BitVector c2 = exec.getValueVectorById(new SchemaPath("has_min", ExpressionPosition.UNKNOWN), BitVector.class);

      for(int i =0; i < exec.getRecordCount(); i++){
        int curSize = sizes[size % sizes.length];
        assertEquals(curSize, c1.getAccessor().get(i));
        switch(curSize){
        case 1:
          assertEquals(oneIsOne, 1 == c2.getAccessor().get(i));
          oneIsOne = !oneIsOne;
          break;
        case 2:
          assertEquals(1, c2.getAccessor().get(i));
          break;
        case 0:
          assertEquals(0, c2.getAccessor().get(i));
          break;
        case 6:
          assertEquals(1, c2.getAccessor().get(i));
          break;
        }
        size++;
      }
    }

    if(context.getFailureCause() != null){
      throw context.getFailureCause();
    }
    assertTrue(!context.isFailed());
  }
}
