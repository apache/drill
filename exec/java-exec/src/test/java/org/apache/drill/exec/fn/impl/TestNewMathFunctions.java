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

import java.math.BigDecimal;

import org.apache.drill.categories.OperatorTest;
import org.apache.drill.categories.UnlikelyTest;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecTest;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.ops.FragmentContextImpl;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.physical.base.FragmentRoot;
import org.apache.drill.exec.physical.impl.ImplCreator;
import org.apache.drill.exec.physical.impl.SimpleRootExec;
import org.apache.drill.exec.planner.PhysicalPlanReader;
import org.apache.drill.exec.planner.PhysicalPlanReaderTestFactory;
import org.apache.drill.exec.proto.BitControl.PlanFragment;
import org.apache.drill.exec.rpc.UserClientConnection;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VarCharVector;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;

import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

@Category({UnlikelyTest.class, OperatorTest.class})
public class TestNewMathFunctions extends ExecTest {
  private final DrillConfig c = DrillConfig.create();
  private PhysicalPlanReader reader;
  private FunctionImplementationRegistry registry;
  private FragmentContextImpl context;

  public Object[] getRunResult(SimpleRootExec exec) {
    int size = 0;
    for (final ValueVector v : exec) {
      size++;
    }

    final Object[] res = new Object[size];
    int i = 0;
    for (final ValueVector v : exec) {
      if  (v instanceof VarCharVector) {
        res[i++] = new String( ((VarCharVector) v).getAccessor().get(0));
      } else {
        res[i++] =  v.getAccessor().getObject(0);
      }
    }
    return res;
  }

  public void runTest(Object[] expectedResults, String planPath) throws Throwable {

    final DrillbitContext bitContext = mockDrillbitContext();
    final UserClientConnection connection = Mockito.mock(UserClientConnection.class);

    final String planString = Resources.toString(Resources.getResource(planPath), Charsets.UTF_8);
    if (reader == null) {
      reader = PhysicalPlanReaderTestFactory.defaultPhysicalPlanReader(c);
    }
    if (registry == null) {
      registry = new FunctionImplementationRegistry(c);
    }
    if (context == null) {
      context = new FragmentContextImpl(bitContext, PlanFragment.getDefaultInstance(), connection, registry);
    }
    final PhysicalPlan plan = reader.readPhysicalPlan(planString);
    final SimpleRootExec exec = new SimpleRootExec(ImplCreator.getExec(context, (FragmentRoot) plan.getSortedOperators(false).iterator().next()));

    exec.next(); // skip schema batch
    while (exec.next()) {
      final Object [] res = getRunResult(exec);
      assertEquals("return count does not match", expectedResults.length, res.length);

      for (int i = 0; i<res.length; i++) {
        assertEquals(String.format("column %s does not match", i),  res[i], expectedResults[i]);
      }
    }

    if (context.getExecutorState().getFailureCause() != null) {
      throw context.getExecutorState().getFailureCause();
    }

    assertTrue(!context.getExecutorState().isFailed());
  }

  @Test
  public void testTrigoMathFunc() throws Throwable {
    final Object [] expected = new Object[] {Math.sin(45), Math.cos(45), Math.tan(45),Math.asin(45), Math.acos(45), Math.atan(45),Math.sinh(45), Math.cosh(45), Math.tanh(45)};
    runTest(expected, "functions/testTrigoMathFunctions.json");
  }

  @Test
  public void testExtendedMathFunc() throws Throwable {
    final BigDecimal d = new BigDecimal("100111111111111111111111111111111111.00000000000000000000000000000000000000000000000000001");
    final Object [] expected = new Object[] {Math.cbrt(1000), Math.log(10), (Math.log(64.0)/Math.log(2.0)), Math.exp(10), Math.toDegrees(0.5), Math.toRadians(45.0), Math.PI, Math.cbrt(d.doubleValue()), Math.log(d.doubleValue()), (Math.log(d.doubleValue())/Math.log(2)), Math.exp(d.doubleValue()), Math.toDegrees(d.doubleValue()), Math.toRadians(d.doubleValue())};

    runTest(expected, "functions/testExtendedMathFunctions.json");
  }

  @Test
  public void testTruncDivMod() throws Throwable{
    final Object [] expected = new Object[] {101.0, 0, 101, 1010.0, 101, 481.0, 0.001099999999931267};
    runTest(expected, "functions/testDivModTruncFunctions.json");
  }

 @Test
 public void testIsNumeric() throws Throwable{
   final Object [] expected = new Object[] {1, 1, 1, 0};
   runTest(expected, "functions/testIsNumericFunction.json");
 }
}
