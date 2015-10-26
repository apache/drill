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
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VarCharVector;
import org.junit.Test;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;

import mockit.Injectable;
import mockit.NonStrictExpectations;

public class TestStringFunctions extends ExecTest {
  //private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestStringFunctions.class);

  private final DrillConfig c = DrillConfig.create();
  private PhysicalPlanReader reader;
  private FunctionImplementationRegistry registry;
  private FragmentContext context;

  public Object[] getRunResult(SimpleRootExec exec) {
    int size = 0;
    for (final ValueVector v : exec) {
      size++;
    }

    final Object[] res = new Object [size];
    int i = 0;
    for (final ValueVector v : exec) {
      if  (v instanceof VarCharVector) {
        res[i++] = new String( ((VarCharVector) v).getAccessor().get(0), Charsets.UTF_8);
      } else {
        res[i++] =  v.getAccessor().getObject(0);
      }
    }
    return res;
 }

  public void runTest(@Injectable final DrillbitContext bitContext,
                      @Injectable UserServer.UserClientConnection connection, Object[] expectedResults, String planPath) throws Throwable {
    new NonStrictExpectations() {{
      bitContext.getMetrics(); result = new MetricRegistry();
      bitContext.getAllocator(); result = RootAllocatorFactory.newRoot(c);
      bitContext.getOperatorCreatorRegistry(); result = new OperatorCreatorRegistry(ClassPathScanner.fromPrescan(c));
      bitContext.getConfig(); result = c;
      bitContext.getCompiler(); result = CodeCompilerTestFactory.getTestCompiler(c);
    }};

    final String planString = Resources.toString(Resources.getResource(planPath), Charsets.UTF_8);
    if (reader == null) {
      reader = PhysicalPlanReaderTestFactory.defaultPhysicalPlanReader(c);
    }
    if (registry == null) {
      registry = new FunctionImplementationRegistry(c);
    }
    if (context == null) {
      context =  new FragmentContext(bitContext, PlanFragment.getDefaultInstance(), connection, registry); //new FragmentContext(bitContext, ExecProtos.FragmentHandle.getDefaultInstance(), connection, registry);
    }
    final PhysicalPlan plan = reader.readPhysicalPlan(planString);
    final SimpleRootExec exec = new SimpleRootExec(ImplCreator.getExec(context, (FragmentRoot) plan.getSortedOperators(false).iterator().next()));

    exec.next(); // skip schema batch
    while(exec.next()) {
      final Object [] res = getRunResult(exec);
      assertEquals("return count does not match", expectedResults.length, res.length);

      for (int i = 0; i<res.length; i++) {
        assertEquals(String.format("column %s does not match", i), expectedResults[i],  res[i]);
      }
    }

    if (context.getFailureCause() != null) {
      throw context.getFailureCause();
    }
    assertTrue(!context.isFailed());
  }

  @Test
  public void testCharLength(@Injectable final DrillbitContext bitContext,
                           @Injectable UserServer.UserClientConnection connection) throws Throwable {
    Object [] expected = new Object[] {new Long(8), new Long(0), new Long(5), new Long(5),
                                       new Long(8), new Long(0), new Long(5), new Long(5),
                                       new Long(8), new Long(0), new Long(5), new Long(5),};
    runTest(bitContext, connection, expected, "functions/string/testCharLength.json");
  }

  @Test
  public void testLike(@Injectable final DrillbitContext bitContext,
                           @Injectable UserServer.UserClientConnection connection) throws Throwable {
    final Object [] expected = new Object[] {Boolean.TRUE, Boolean.TRUE, Boolean.TRUE, Boolean.FALSE};
    runTest(bitContext, connection, expected, "functions/string/testLike.json");
  }

  @Test
  public void testSimilar(@Injectable final DrillbitContext bitContext,
                           @Injectable UserServer.UserClientConnection connection) throws Throwable {
    final Object [] expected = new Object[] {Boolean.TRUE, Boolean.FALSE, Boolean.TRUE, Boolean.FALSE};
    runTest(bitContext, connection, expected, "functions/string/testSimilar.json");
  }

  @Test
  public void testLtrim(@Injectable final DrillbitContext bitContext,
                           @Injectable UserServer.UserClientConnection connection) throws Throwable {
    final Object [] expected = new Object[] {"def", "abcdef", "dabc", "", "", ""};
    runTest(bitContext, connection, expected, "functions/string/testLtrim.json");
  }

  @Test
  public void testTrim(@Injectable final DrillbitContext bitContext,
                           @Injectable UserServer.UserClientConnection connection) throws Throwable {
    final Object [] expected = new Object[] {"fghI", "", "", "!", " aaa "};
    runTest(bitContext, connection, expected, "functions/string/testTrim.json");
  }

  @Test
  public void testReplace(@Injectable final DrillbitContext bitContext,
                           @Injectable UserServer.UserClientConnection connection) throws Throwable {
    final Object [] expected = new Object[] {"aABABcdf", "ABABbABbcdf", "aababcdf", "acdf", "ABCD", "abc"};
    runTest(bitContext, connection, expected, "functions/string/testReplace.json");
  }

  @Test
  public void testRtrim(@Injectable final DrillbitContext bitContext,
                           @Injectable UserServer.UserClientConnection connection) throws Throwable {
    final Object [] expected = new Object[] {"abc", "abcdef", "ABd", "", "", ""};
    runTest(bitContext, connection, expected, "functions/string/testRtrim.json");
  }

  @Test
  public void testConcat(@Injectable final DrillbitContext bitContext,
                           @Injectable UserServer.UserClientConnection connection) throws Throwable {
    final Object [] expected = new Object[] {"abcABC", "abc", "ABC", ""};
    runTest(bitContext, connection, expected, "functions/string/testConcat.json");
  }

  @Test
  public void testLower(@Injectable final DrillbitContext bitContext,
                           @Injectable UserServer.UserClientConnection connection) throws Throwable {
    final Object [] expected = new Object[] {"abcefgh", "abc", ""};
    runTest(bitContext, connection, expected, "functions/string/testLower.json");
  }

  @Test
  public void testPosition(@Injectable final DrillbitContext bitContext,
                           @Injectable UserServer.UserClientConnection connection) throws Throwable {
    final Object [] expected = new Object[] {new Long(2), new Long(0), new Long(0), new Long(0),
                                       new Long(2), new Long(0), new Long(0), new Long(0)};
    runTest(bitContext, connection, expected, "functions/string/testPosition.json");
  }

  @Test
  public void testRight(@Injectable final DrillbitContext bitContext,
                           @Injectable UserServer.UserClientConnection connection) throws Throwable {
    final Object [] expected = new Object[] {"ef", "abcdef", "abcdef", "cdef", "f", "", ""};
    runTest(bitContext, connection, expected, "functions/string/testRight.json");
  }


  @Test
  public void testSubstr(@Injectable final DrillbitContext bitContext,
                           @Injectable UserServer.UserClientConnection connection) throws Throwable {
    final Object [] expected = new Object[] {"abc", "bcd", "bcdef", "bcdef", "", "", "", "", "भारत", "वर्ष", "वर्ष", "cdef", "", "", "", "ड्रिल"};
    runTest(bitContext, connection, expected, "functions/string/testSubstr.json");
  }

  @Test
  public void testLeft(@Injectable final DrillbitContext bitContext,
                           @Injectable UserServer.UserClientConnection connection) throws Throwable {
    final Object [] expected = new Object[] {"ab", "abcdef", "abcdef", "abcd", "a", "", ""};
    runTest(bitContext, connection, expected, "functions/string/testLeft.json");
  }

  @Test
  public void testLpad(@Injectable final DrillbitContext bitContext,
                           @Injectable UserServer.UserClientConnection connection) throws Throwable {
    final Object [] expected = new Object[] {"", "", "abcdef", "ab", "ab", "abcdef", "AAAAabcdef", "ABABabcdef", "ABCAabcdef", "ABCDabcdef"};
    runTest(bitContext, connection, expected, "functions/string/testLpad.json");
  }

  @Test
  public void testRegexpReplace(@Injectable final DrillbitContext bitContext,
                           @Injectable UserServer.UserClientConnection connection) throws Throwable {
    final Object [] expected = new Object[] {"ThM", "Th", "Thomas"};
    runTest(bitContext, connection, expected, "functions/string/testRegexpReplace.json");
  }

  @Test
  public void testRpad(@Injectable final DrillbitContext bitContext,
                           @Injectable UserServer.UserClientConnection connection) throws Throwable {
    final Object [] expected = new Object[] {"", "", "abcdef", "ab", "ab", "abcdef", "abcdefAAAA", "abcdefABAB", "abcdefABCA", "abcdefABCD"};
    runTest(bitContext, connection, expected, "functions/string/testRpad.json");
  }

  @Test
  public void testUpper(@Injectable final DrillbitContext bitContext,
                           @Injectable UserServer.UserClientConnection connection) throws Throwable {
    final Object [] expected = new Object[] {"ABCEFGH", "ABC", ""};
    runTest(bitContext, connection, expected, "functions/string/testUpper.json");
  }

  @Test
  public void testNewStringFuncs(@Injectable final DrillbitContext bitContext, @Injectable UserServer.UserClientConnection connection) throws Throwable {
    final Object [] expected = new Object[] {97, 65, -32, "A", "btrim", "Peace Peace Peace ", "हकुना मताता हकुना मताता ", "katcit", "\u00C3\u00A2pple", "नदम"};
    runTest(bitContext, connection, expected, "functions/string/testStringFuncs.json");
  }
}
