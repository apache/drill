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
package org.apache.drill.exec.physical.impl.agg;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import mockit.Injectable;
import mockit.NonStrictExpectations;

import org.apache.drill.common.config.DrillConfig;
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
import org.apache.drill.exec.vector.BigIntVector;
import org.apache.drill.exec.vector.IntVector;
import org.junit.Test;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Charsets;
import com.google.common.io.Files;

public class TestAgg extends ExecTest {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestAgg.class);
  DrillConfig c = DrillConfig.create();

  private SimpleRootExec doTest(final DrillbitContext bitContext, UserClientConnection connection, String file) throws Exception{
    new NonStrictExpectations(){{
      bitContext.getMetrics(); result = new MetricRegistry();
      bitContext.getAllocator(); result = new TopLevelAllocator();
      bitContext.getOperatorCreatorRegistry(); result = new OperatorCreatorRegistry(c);
      bitContext.getConfig(); result = c;
      bitContext.getCompiler(); result = CodeCompiler.getTestCompiler(c);
    }};

    PhysicalPlanReader reader = new PhysicalPlanReader(c, c.getMapper(), CoordinationProtos.DrillbitEndpoint.getDefaultInstance());
    PhysicalPlan plan = reader.readPhysicalPlan(Files.toString(FileUtils.getResourceAsFile(file), Charsets.UTF_8));
    FunctionImplementationRegistry registry = new FunctionImplementationRegistry(c);
    FragmentContext context = new FragmentContext(bitContext, PlanFragment.getDefaultInstance(), connection, registry);
    SimpleRootExec exec = new SimpleRootExec(ImplCreator.getExec(context, (FragmentRoot) plan.getSortedOperators(false).iterator().next()));
    return exec;
  }

  @Test
  public void oneKeyAgg(@Injectable final DrillbitContext bitContext, @Injectable UserClientConnection connection) throws Throwable{
    SimpleRootExec exec = doTest(bitContext, connection, "/agg/test1.json");

    while(exec.next()){
      BigIntVector cnt = exec.getValueVectorById(SchemaPath.getSimplePath("cnt"), BigIntVector.class);
      IntVector key = exec.getValueVectorById(SchemaPath.getSimplePath("blue"), IntVector.class);
      long[] cntArr = {10001, 9999};
      int[] keyArr = {Integer.MIN_VALUE, Integer.MAX_VALUE};

      for(int i =0; i < exec.getRecordCount(); i++){
        assertEquals((Long) cntArr[i], cnt.getAccessor().getObject(i));
        assertEquals((Integer) keyArr[i], key.getAccessor().getObject(i));
      }
    }

    if(exec.getContext().getFailureCause() != null){
      throw exec.getContext().getFailureCause();
    }
    assertTrue(!exec.getContext().isFailed());

  }

  @Test
  public void twoKeyAgg(@Injectable final DrillbitContext bitContext, @Injectable UserClientConnection connection) throws Throwable{
    SimpleRootExec exec = doTest(bitContext, connection, "/agg/twokey.json");

    while(exec.next()){
      IntVector key1 = exec.getValueVectorById(SchemaPath.getSimplePath("key1"), IntVector.class);
      BigIntVector key2 = exec.getValueVectorById(SchemaPath.getSimplePath("key2"), BigIntVector.class);
      BigIntVector cnt = exec.getValueVectorById(SchemaPath.getSimplePath("cnt"), BigIntVector.class);
      BigIntVector total = exec.getValueVectorById(SchemaPath.getSimplePath("total"), BigIntVector.class);
      Integer[] keyArr1 = {Integer.MIN_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE};
      long[] keyArr2 = {0,1,2,0,1,2};
      long[] cntArr = {34,34,34,34,34,34};
      long[] totalArr = {0,34,68,0,34,68};

      for(int i =0; i < exec.getRecordCount(); i++){
//        System.out.print(key1.getAccessor().getObject(i));
//        System.out.print("\t");
//        System.out.print(key2.getAccessor().getObject(i));
//        System.out.print("\t");
//        System.out.print(cnt.getAccessor().getObject(i));
//        System.out.print("\t");
//        System.out.print(total.getAccessor().getObject(i));
//        System.out.println();
        assertEquals((Long) cntArr[i], cnt.getAccessor().getObject(i));
        assertEquals(keyArr1[i], key1.getAccessor().getObject(i));
        assertEquals((Long) keyArr2[i], key2.getAccessor().getObject(i));
        assertEquals((Long) totalArr[i], total.getAccessor().getObject(i));
      }
    }

    if(exec.getContext().getFailureCause() != null){
      throw exec.getContext().getFailureCause();
    }
    assertTrue(!exec.getContext().isFailed());

  }

}
