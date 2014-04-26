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

import com.sun.codemodel.JClassAlreadyExistsException;

import mockit.Injectable;
import mockit.NonStrictExpectations;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.*;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.exec.ExecTest;
import org.apache.drill.exec.expr.fn.DrillFuncHolder;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.expr.holders.NullableVarBinaryHolder;
import org.apache.drill.exec.expr.holders.NullableVarCharHolder;
import org.apache.drill.exec.memory.TopLevelAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.physical.base.FragmentRoot;
import org.apache.drill.exec.planner.PhysicalPlanReader;
import org.apache.drill.exec.proto.CoordinationProtos;
import org.apache.drill.exec.proto.BitControl.PlanFragment;
import org.apache.drill.exec.resolver.FunctionResolver;
import org.apache.drill.exec.resolver.FunctionResolverFactory;
import org.apache.drill.exec.rpc.user.UserServer;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.vector.NullableVarBinaryVector;
import org.apache.drill.exec.vector.NullableVarCharVector;
import org.junit.After;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.codahale.metrics.MetricRegistry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TestSimpleFunctions extends ExecTest {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestSimpleFunctions.class);

  DrillConfig c = DrillConfig.create();

  @Test
  public void testHashFunctionResolution(@Injectable DrillConfig config) throws JClassAlreadyExistsException, IOException {
    FunctionImplementationRegistry registry = new FunctionImplementationRegistry(config);
    // test required vs nullable Int input
    resolveHash(config,
        new TypedNullConstant(Types.optional(TypeProtos.MinorType.INT)),
        Types.optional(TypeProtos.MinorType.INT),
        Types.required(TypeProtos.MinorType.INT),
        TypeProtos.DataMode.OPTIONAL,
        registry);

    resolveHash(config,
        new ValueExpressions.IntExpression(1, ExpressionPosition.UNKNOWN),
        Types.required(TypeProtos.MinorType.INT),
        Types.required(TypeProtos.MinorType.INT),
        TypeProtos.DataMode.REQUIRED,
        registry);

    // test required vs nullable float input
    resolveHash(config,
        new TypedNullConstant(Types.optional(TypeProtos.MinorType.FLOAT4)),
        Types.optional(TypeProtos.MinorType.FLOAT4),
        Types.required(TypeProtos.MinorType.FLOAT4),
        TypeProtos.DataMode.OPTIONAL,
        registry);

    resolveHash(config,
        new ValueExpressions.FloatExpression(5.0f, ExpressionPosition.UNKNOWN),
        Types.required(TypeProtos.MinorType.FLOAT4),
        Types.required(TypeProtos.MinorType.FLOAT4),
        TypeProtos.DataMode.REQUIRED,
        registry);

    // test required vs nullable long input
    resolveHash(config,
        new TypedNullConstant(Types.optional(TypeProtos.MinorType.BIGINT)),
        Types.optional(TypeProtos.MinorType.BIGINT),
        Types.required(TypeProtos.MinorType.BIGINT),
        TypeProtos.DataMode.OPTIONAL,
        registry);

    resolveHash(config,
        new ValueExpressions.LongExpression(100L, ExpressionPosition.UNKNOWN),
        Types.required(TypeProtos.MinorType.BIGINT),
        Types.required(TypeProtos.MinorType.BIGINT),
        TypeProtos.DataMode.REQUIRED,
        registry);

    // test required vs nullable double input
    resolveHash(config,
        new TypedNullConstant(Types.optional(TypeProtos.MinorType.FLOAT8)),
        Types.optional(TypeProtos.MinorType.FLOAT8),
        Types.required(TypeProtos.MinorType.FLOAT8),
        TypeProtos.DataMode.OPTIONAL,
        registry);

    resolveHash(config,
        new ValueExpressions.DoubleExpression(100.0, ExpressionPosition.UNKNOWN),
        Types.required(TypeProtos.MinorType.FLOAT8),
        Types.required(TypeProtos.MinorType.FLOAT8),
        TypeProtos.DataMode.REQUIRED,
        registry);
  }

  public void resolveHash(DrillConfig config, LogicalExpression arg, TypeProtos.MajorType expectedArg,
                                    TypeProtos.MajorType expectedOut, TypeProtos.DataMode expectedBestInputMode,
                                    FunctionImplementationRegistry registry) throws JClassAlreadyExistsException, IOException {
    List<LogicalExpression> args = new ArrayList<>();
    args.add(arg);
    String[] registeredNames = { "hash" };
    FunctionCall call = new FunctionCall(
        "hash",
        args,
        ExpressionPosition.UNKNOWN
    );
    FunctionResolver resolver = FunctionResolverFactory.getResolver(call);
    DrillFuncHolder matchedFuncHolder = resolver.getBestMatch(registry.getDrillRegistry().getMethods().get(call.getName()), call);
    assertEquals( expectedBestInputMode, matchedFuncHolder.getParmMajorType(0).getMode());
  }

  @Test
  public void testSubstring(@Injectable final DrillbitContext bitContext,
                            @Injectable UserServer.UserClientConnection connection) throws Throwable{

    new NonStrictExpectations(){{
      bitContext.getMetrics(); result = new MetricRegistry();
      bitContext.getAllocator(); result = new TopLevelAllocator();
      bitContext.getOperatorCreatorRegistry(); result = new OperatorCreatorRegistry(c);
    }};

    PhysicalPlanReader reader = new PhysicalPlanReader(c, c.getMapper(), CoordinationProtos.DrillbitEndpoint.getDefaultInstance());
    PhysicalPlan plan = reader.readPhysicalPlan(Files.toString(FileUtils.getResourceAsFile("/functions/testSubstring.json"), Charsets.UTF_8));
    FunctionImplementationRegistry registry = new FunctionImplementationRegistry(c);
    FragmentContext context = new FragmentContext(bitContext, PlanFragment.getDefaultInstance(), connection, registry);
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
      bitContext.getMetrics(); result = new MetricRegistry();
      bitContext.getAllocator(); result = new TopLevelAllocator();
      bitContext.getOperatorCreatorRegistry(); result = new OperatorCreatorRegistry(c);
    }};

    PhysicalPlanReader reader = new PhysicalPlanReader(c, c.getMapper(), CoordinationProtos.DrillbitEndpoint.getDefaultInstance());
    PhysicalPlan plan = reader.readPhysicalPlan(Files.toString(FileUtils.getResourceAsFile("/functions/testSubstringNegative.json"), Charsets.UTF_8));
    FunctionImplementationRegistry registry = new FunctionImplementationRegistry(c);
    FragmentContext context = new FragmentContext(bitContext, PlanFragment.getDefaultInstance(), connection, registry);
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
          //when offset is negative, substring return empty string.
          assertEquals("", holder.toString());
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
  public void testByteSubstring(@Injectable final DrillbitContext bitContext,
                                  @Injectable UserServer.UserClientConnection connection) throws Throwable{

    new NonStrictExpectations(){{
      bitContext.getMetrics(); result = new MetricRegistry();
      bitContext.getAllocator(); result = new TopLevelAllocator();
      bitContext.getOperatorCreatorRegistry(); result = new OperatorCreatorRegistry(c);
    }};

    PhysicalPlanReader reader = new PhysicalPlanReader(c, c.getMapper(), CoordinationProtos.DrillbitEndpoint.getDefaultInstance());
    PhysicalPlan plan = reader.readPhysicalPlan(Files.toString(FileUtils.getResourceAsFile("/functions/testByteSubstring.json"), Charsets.UTF_8));
    FunctionImplementationRegistry registry = new FunctionImplementationRegistry(c);
    FragmentContext context = new FragmentContext(bitContext, PlanFragment.getDefaultInstance(), connection, registry);
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
