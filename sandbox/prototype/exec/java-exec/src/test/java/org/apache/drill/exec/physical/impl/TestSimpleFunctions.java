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

package org.apache.drill.exec.physical.impl;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.yammer.metrics.MetricRegistry;
import mockit.Injectable;
import mockit.NonStrictExpectations;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.physical.base.FragmentRoot;
import org.apache.drill.exec.planner.PhysicalPlanReader;
import org.apache.drill.exec.proto.CoordinationProtos;
import org.apache.drill.exec.proto.ExecProtos;
import org.apache.drill.exec.rpc.user.UserServer;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.vector.NullableVarBinaryHolder;
import org.apache.drill.exec.vector.NullableVarBinaryVector;
import org.apache.drill.exec.vector.NullableVarCharHolder;
import org.apache.drill.exec.vector.NullableVarCharVector;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestSimpleFunctions {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestSimpleFunctions.class);

  DrillConfig c = DrillConfig.create();

  @Test
  public void testIsNull(@Injectable final DrillbitContext bitContext,
                         @Injectable UserServer.UserClientConnection connection) throws Throwable {

    new NonStrictExpectations(){{
      bitContext.getMetrics(); result = new MetricRegistry("test");
      bitContext.getAllocator(); result = BufferAllocator.getAllocator(c);
    }};

    PhysicalPlanReader reader = new PhysicalPlanReader(c, c.getMapper(), CoordinationProtos.DrillbitEndpoint.getDefaultInstance());
    PhysicalPlan plan = reader.readPhysicalPlan(Files.toString(FileUtils.getResourceAsFile("/functions/testIsNull.json"), Charsets.UTF_8));
    FunctionImplementationRegistry registry = new FunctionImplementationRegistry(c);
    FragmentContext context = new FragmentContext(bitContext, ExecProtos.FragmentHandle.getDefaultInstance(), connection, null, registry);
    SimpleRootExec exec = new SimpleRootExec(ImplCreator.getExec(context, (FragmentRoot) plan.getSortedOperators(false).iterator().next()));

    while(exec.next()){
      assertEquals(50, exec.getSelectionVector2().getCount());
    }

    if(context.getFailureCause() != null){
      throw context.getFailureCause();
    }

    assertTrue(!context.isFailed());

  }

  @Test
  public void testIsNotNull(@Injectable final DrillbitContext bitContext,
                            @Injectable UserServer.UserClientConnection connection) throws Throwable{

    new NonStrictExpectations(){{
      bitContext.getMetrics(); result = new MetricRegistry("test");
      bitContext.getAllocator(); result = BufferAllocator.getAllocator(c);
    }};

    PhysicalPlanReader reader = new PhysicalPlanReader(c, c.getMapper(), CoordinationProtos.DrillbitEndpoint.getDefaultInstance());
    PhysicalPlan plan = reader.readPhysicalPlan(Files.toString(FileUtils.getResourceAsFile("/functions/testIsNotNull.json"), Charsets.UTF_8));
    FunctionImplementationRegistry registry = new FunctionImplementationRegistry(c);
    FragmentContext context = new FragmentContext(bitContext, ExecProtos.FragmentHandle.getDefaultInstance(), connection, null, registry);
    SimpleRootExec exec = new SimpleRootExec(ImplCreator.getExec(context, (FragmentRoot) plan.getSortedOperators(false).iterator().next()));

    while(exec.next()){
      assertEquals(50, exec.getSelectionVector2().getCount());
    }

    if(context.getFailureCause() != null){
      throw context.getFailureCause();
    }

    assertTrue(!context.isFailed());

  }

  @Test
  public void testSubstring(@Injectable final DrillbitContext bitContext,
                            @Injectable UserServer.UserClientConnection connection) throws Throwable{

    new NonStrictExpectations(){{
      bitContext.getMetrics(); result = new MetricRegistry("test");
      bitContext.getAllocator(); result = BufferAllocator.getAllocator(c);
    }};

    PhysicalPlanReader reader = new PhysicalPlanReader(c, c.getMapper(), CoordinationProtos.DrillbitEndpoint.getDefaultInstance());
    PhysicalPlan plan = reader.readPhysicalPlan(Files.toString(FileUtils.getResourceAsFile("/functions/testSubstring.json"), Charsets.UTF_8));
    FunctionImplementationRegistry registry = new FunctionImplementationRegistry(c);
    FragmentContext context = new FragmentContext(bitContext, ExecProtos.FragmentHandle.getDefaultInstance(), connection, null, registry);
    SimpleRootExec exec = new SimpleRootExec(ImplCreator.getExec(context, (FragmentRoot) plan.getSortedOperators(false).iterator().next()));

    while(exec.next()){
      NullableVarCharVector c1 = exec.getValueVectorById(new SchemaPath("col3", ExpressionPosition.UNKNOWN), NullableVarCharVector.class);
      NullableVarCharVector.Accessor a1;
      a1 = c1.getAccessor();

      int count = 0;
      for(int i = 0; i < c1.getAccessor().getValueCount(); i++){
        if (!a1.isNull(i)) {
          NullableVarCharHolder holder = new NullableVarCharHolder();
          a1.get(i, holder);
          assertEquals("aaaa", holder.toString());
          ++count;
        }
      }
      assertEquals(50, count);
    }

    if(context.getFailureCause() != null){
      throw context.getFailureCause();
    }
    assertTrue(!context.isFailed());

  }

  @Test
  public void testSubstringNegative(@Injectable final DrillbitContext bitContext,
                                    @Injectable UserServer.UserClientConnection connection) throws Throwable{

    new NonStrictExpectations(){{
      bitContext.getMetrics(); result = new MetricRegistry("test");
      bitContext.getAllocator(); result = BufferAllocator.getAllocator(c);
    }};

    PhysicalPlanReader reader = new PhysicalPlanReader(c, c.getMapper(), CoordinationProtos.DrillbitEndpoint.getDefaultInstance());
    PhysicalPlan plan = reader.readPhysicalPlan(Files.toString(FileUtils.getResourceAsFile("/functions/testSubstringNegative.json"), Charsets.UTF_8));
    FunctionImplementationRegistry registry = new FunctionImplementationRegistry(c);
    FragmentContext context = new FragmentContext(bitContext, ExecProtos.FragmentHandle.getDefaultInstance(), connection, null, registry);
    SimpleRootExec exec = new SimpleRootExec(ImplCreator.getExec(context, (FragmentRoot) plan.getSortedOperators(false).iterator().next()));

    while(exec.next()){
      NullableVarCharVector c1 = exec.getValueVectorById(new SchemaPath("col3", ExpressionPosition.UNKNOWN), NullableVarCharVector.class);
      NullableVarCharVector.Accessor a1;
      a1 = c1.getAccessor();

      int count = 0;
      for(int i = 0; i < c1.getAccessor().getValueCount(); i++){
        if (!a1.isNull(i)) {
          NullableVarCharHolder holder = new NullableVarCharHolder();
          a1.get(i, holder);
          assertEquals("aa", holder.toString());
          ++count;
        }
      }
      assertEquals(50, count);
    }

    if(context.getFailureCause() != null){
      throw context.getFailureCause();
    }
    assertTrue(!context.isFailed());

  }

  @Test
  public void testSubstringBinary(@Injectable final DrillbitContext bitContext,
                                  @Injectable UserServer.UserClientConnection connection) throws Throwable{

    new NonStrictExpectations(){{
      bitContext.getMetrics(); result = new MetricRegistry("test");
      bitContext.getAllocator(); result = BufferAllocator.getAllocator(c);
    }};

    PhysicalPlanReader reader = new PhysicalPlanReader(c, c.getMapper(), CoordinationProtos.DrillbitEndpoint.getDefaultInstance());
    PhysicalPlan plan = reader.readPhysicalPlan(Files.toString(FileUtils.getResourceAsFile("/functions/testSubstringBinary.json"), Charsets.UTF_8));
    FunctionImplementationRegistry registry = new FunctionImplementationRegistry(c);
    FragmentContext context = new FragmentContext(bitContext, ExecProtos.FragmentHandle.getDefaultInstance(), connection, null, registry);
    SimpleRootExec exec = new SimpleRootExec(ImplCreator.getExec(context, (FragmentRoot) plan.getSortedOperators(false).iterator().next()));

    while(exec.next()){
      NullableVarBinaryVector c1 = exec.getValueVectorById(new SchemaPath("col3", ExpressionPosition.UNKNOWN), NullableVarBinaryVector.class);
      NullableVarBinaryVector.Accessor a1;
      a1 = c1.getAccessor();

      int count = 0;
      for(int i = 0; i < c1.getAccessor().getValueCount(); i++){
        if (!a1.isNull(i)) {
          NullableVarBinaryHolder holder = new NullableVarBinaryHolder();
          a1.get(i, holder);
          assertEquals("aa", holder.toString());
          ++count;
        }
      }
      assertEquals(50, count);
    }

    if(context.getFailureCause() != null){
      throw context.getFailureCause();
    }
    assertTrue(!context.isFailed());

  }
  
  @After
  public void tearDown() throws Exception{
    // pause to get logger to catch up.
    Thread.sleep(1000);
  }
}
