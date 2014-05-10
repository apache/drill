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
import mockit.Injectable;
import mockit.NonStrictExpectations;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecTest;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.memory.TopLevelAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.physical.base.FragmentRoot;
import org.apache.drill.exec.planner.PhysicalPlanReader;
import org.apache.drill.exec.proto.BitControl.PlanFragment;
import org.apache.drill.exec.proto.CoordinationProtos;
import org.apache.drill.exec.rpc.user.UserServer.UserClientConnection;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.vector.Float4Vector;
import org.apache.drill.exec.vector.NullableBigIntVector;
import org.apache.drill.exec.vector.NullableBitVector;
import org.apache.drill.exec.vector.NullableFloat8Vector;
import org.apache.drill.exec.vector.NullableIntVector;
import org.apache.drill.exec.vector.NullableVar16CharVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.Var16CharVector;
import org.junit.Test;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;

import java.util.Iterator;

public class TestHiveUDFs extends ExecTest {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestHiveUDFs.class);

  DrillConfig c = DrillConfig.create();
  PhysicalPlanReader reader;
  FunctionImplementationRegistry registry;
  FragmentContext context;

  private void setup(final DrillbitContext bitContext, UserClientConnection connection) throws Throwable {
    if(reader == null)
      reader = new PhysicalPlanReader(c, c.getMapper(), CoordinationProtos.DrillbitEndpoint.getDefaultInstance());
    if(registry == null)
      registry = new FunctionImplementationRegistry(c);
    if(context == null)
      context = new FragmentContext(bitContext, PlanFragment.getDefaultInstance(), connection, registry);
  }

  @Test
  public void testGenericUDF(@Injectable final DrillbitContext bitContext,
                      @Injectable UserClientConnection connection) throws Throwable {
    new NonStrictExpectations(){{
      bitContext.getMetrics(); result = new MetricRegistry();
      bitContext.getAllocator(); result = new TopLevelAllocator();
      bitContext.getOperatorCreatorRegistry(); result = new OperatorCreatorRegistry(c);
    }};

    String planString = Resources.toString(Resources.getResource("functions/hive/GenericUDF.json"), Charsets.UTF_8);

    setup(bitContext, connection);
    PhysicalPlan plan = reader.readPhysicalPlan(planString);
    SimpleRootExec exec = new SimpleRootExec(ImplCreator.getExec(context, (FragmentRoot) plan.getSortedOperators(false).iterator().next()));

    int numRecords = 0;

    while(exec.next()){
      // Output columns and types
      //  1. str1 : Var16Char
      //  2. upperStr1 : NullableVar16Char
      //  3. unix_timestamp : NullableBigInt
      //  4. concat : NullableVarChar
      //  5. flt1 : Float4
      //  6. format_number : NullableFloat8
      //  7. nullableStr1 : NullableVar16Char
      //  8. upperNullableStr1 : NullableVar16Char
      Iterator<ValueVector> vv = exec.iterator();
      Var16CharVector str1V = (Var16CharVector) vv.next();
      NullableVar16CharVector upperStr1V = (NullableVar16CharVector) vv.next();
      NullableBigIntVector unix_timestampV = (NullableBigIntVector) vv.next();
      NullableVar16CharVector concatV = (NullableVar16CharVector) vv.next();
      Float4Vector flt1V = (Float4Vector) vv.next();
      NullableVar16CharVector format_numberV = (NullableVar16CharVector) vv.next();
      NullableVar16CharVector nullableStr1V = ((NullableVar16CharVector) vv.next());
      NullableVar16CharVector upperNullableStr1V = ((NullableVar16CharVector) vv.next());

      for(int i=0; i<exec.getRecordCount(); i++) {


        String in = new String(str1V.getAccessor().get(i), Charsets.UTF_16);
        String upper = new String(upperStr1V.getAccessor().get(i), Charsets.UTF_16);
        assertTrue(in.toUpperCase().equals(upper));

        long unix_timestamp = unix_timestampV.getAccessor().get(i);

        String concat = new String(concatV.getAccessor().get(i), Charsets.UTF_16);
        assertTrue(concat.equals(in+"-"+in));

        float flt1 = flt1V.getAccessor().get(i);
        String format_number = new String(format_numberV.getAccessor().get(i), Charsets.UTF_16);


        String nullableStr1 = null;
        if (!nullableStr1V.getAccessor().isNull(i))
          nullableStr1 = new String(nullableStr1V.getAccessor().get(i), Charsets.UTF_16);

        String upperNullableStr1 = null;
        if (!upperNullableStr1V.getAccessor().isNull(i))
          upperNullableStr1 = new String(upperNullableStr1V.getAccessor().get(i), Charsets.UTF_16);

        assertEquals(nullableStr1 != null, upperNullableStr1 != null);
        if (nullableStr1 != null)
          assertEquals(nullableStr1.toUpperCase(), upperNullableStr1);

        System.out.println(in + ", " + upper + ", " + unix_timestamp + ", " + concat + ", " +
          flt1 + ", " + format_number + ", " + nullableStr1 + ", " + upperNullableStr1);

        numRecords++;
      }
    }

    System.out.println("Processed " + numRecords + " records");

    if(context.getFailureCause() != null){
      throw context.getFailureCause();
    }

    assertTrue(!context.isFailed());
  }

  @Test
  public void testUDF(@Injectable final DrillbitContext bitContext,
                             @Injectable UserClientConnection connection) throws Throwable {
    new NonStrictExpectations(){{
      bitContext.getMetrics(); result = new MetricRegistry();
      bitContext.getAllocator(); result = new TopLevelAllocator();
      bitContext.getOperatorCreatorRegistry(); result = new OperatorCreatorRegistry(c);
    }};

    String planString = Resources.toString(Resources.getResource("functions/hive/UDF.json"), Charsets.UTF_8);

    setup(bitContext, connection);
    PhysicalPlan plan = reader.readPhysicalPlan(planString);
    SimpleRootExec exec = new SimpleRootExec(ImplCreator.getExec(context, (FragmentRoot) plan.getSortedOperators(false).iterator().next()));

    int numRecords = 0;
    while(exec.next()){
      // Output columns and types
      // 1. str1 : Var16Char
      // 2. str1Length : Int
      // 3. str1Ascii : Int
      // 4. flt1 : Float4
      // 5. pow : Float8
      Iterator<ValueVector> vv = exec.iterator();
      Var16CharVector str1V = (Var16CharVector) vv.next();
      NullableIntVector str1LengthV = (NullableIntVector) vv.next();
      NullableIntVector str1AsciiV = (NullableIntVector) vv.next();
      Float4Vector flt1V = (Float4Vector) vv.next();
      NullableFloat8Vector powV = (NullableFloat8Vector) vv.next();

      for(int i=0; i<exec.getRecordCount(); i++) {

        String str1 = new String(str1V.getAccessor().get(i), Charsets.UTF_16);
        int str1Length = str1LengthV.getAccessor().get(i);
        assertTrue(str1.length() == str1Length);

        int str1Ascii = str1AsciiV.getAccessor().get(i);

        float flt1 = flt1V.getAccessor().get(i);

        double pow = 0;
        if (!powV.getAccessor().isNull(i)) {
          pow = powV.getAccessor().get(i);
          assertTrue(Math.pow(flt1, 2.0) == pow);
        }

        System.out.println(str1 + ", " + str1Length + ", " + str1Ascii + ", " + flt1 + ", " + pow);
        numRecords++;
      }
    }

    System.out.println("Processed " + numRecords + " records");

    if(context.getFailureCause() != null){
      throw context.getFailureCause();
    }

    assertTrue(!context.isFailed());
  }
}
