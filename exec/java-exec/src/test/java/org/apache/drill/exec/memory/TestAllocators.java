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

package org.apache.drill.exec.memory;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import io.netty.buffer.DrillBuf;

import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import io.netty.buffer.UnsafeDirectLittleEndian;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OpProfileDef;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.ops.OperatorStats;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.planner.PhysicalPlanReader;
import org.apache.drill.exec.proto.BitControl;
import org.apache.drill.exec.proto.CoordinationProtos;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.RemoteServiceSet;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.junit.Test;

public class TestAllocators {
  private static final Properties TEST_CONFIGURATIONS = new Properties() {
    private static final long serialVersionUID = 1L;

    {
      put(ExecConstants.TOP_LEVEL_MAX_ALLOC, "14000000");
      put(ExecConstants.ENABLE_FRAGMENT_MEMORY_LIMIT, "true");
      put(ExecConstants.FRAGMENT_MEM_OVERCOMMIT_FACTOR, "1.1");
    }
  };

  private final static String planFile = "/physical_allocator_test.json";

  @Test
  public void testAllocators() throws Exception {
    // Setup a drillbit (initializes a root allocator)
    final DrillConfig config = DrillConfig.create(TEST_CONFIGURATIONS);
    final RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();
    final Drillbit bit = new Drillbit(config, serviceSet);
    bit.run();
    final DrillbitContext bitContext = bit.getContext();
    final FunctionImplementationRegistry functionRegistry = bitContext.getFunctionImplementationRegistry();
    final StoragePluginRegistry storageRegistry = new StoragePluginRegistry(bitContext);

    // Create a few Fragment Contexts

    final BitControl.PlanFragment.Builder pfBuilder1 = BitControl.PlanFragment.newBuilder();
    pfBuilder1.setMemInitial(1500000);
    final BitControl.PlanFragment pf1 = pfBuilder1.build();
    final BitControl.PlanFragment.Builder pfBuilder2 = BitControl.PlanFragment.newBuilder();
    pfBuilder2.setMemInitial(500000);
    final BitControl.PlanFragment pf2 = pfBuilder1.build();

    final FragmentContext fragmentContext1 = new FragmentContext(bitContext, pf1, null, functionRegistry);
    final FragmentContext fragmentContext2 = new FragmentContext(bitContext, pf2, null, functionRegistry);

    // Get a few physical operators. Easiest way is to read a physical plan.
    final PhysicalPlanReader planReader = new PhysicalPlanReader(config, config.getMapper(),
        CoordinationProtos.DrillbitEndpoint.getDefaultInstance(), storageRegistry);
    final PhysicalPlan plan = planReader.readPhysicalPlan(Files.toString(FileUtils.getResourceAsFile(planFile),
        Charsets.UTF_8));
    final List<PhysicalOperator> physicalOperators = plan.getSortedOperators();
    final Iterator<PhysicalOperator> physicalOperatorIterator = physicalOperators.iterator();

    final PhysicalOperator physicalOperator1 = physicalOperatorIterator.next();
    final PhysicalOperator physicalOperator2 = physicalOperatorIterator.next();
    final PhysicalOperator physicalOperator3 = physicalOperatorIterator.next();
    final PhysicalOperator physicalOperator4 = physicalOperatorIterator.next();
    final PhysicalOperator physicalOperator5 = physicalOperatorIterator.next();
    final PhysicalOperator physicalOperator6 = physicalOperatorIterator.next();

    // Create some bogus Operator profile defs and stats to create operator contexts
    OpProfileDef def;
    OperatorStats stats;

    //Use some bogus operator type to create a new operator context.
    def = new OpProfileDef(physicalOperator1.getOperatorId(), UserBitShared.CoreOperatorType.MOCK_SUB_SCAN_VALUE,
        OperatorContext.getChildCount(physicalOperator1));
    stats = fragmentContext1.getStats().getOperatorStats(def, fragmentContext1.getAllocator());

    // Add a couple of Operator Contexts
    // Initial allocation = 1000000 bytes for all operators
    final OperatorContext oContext11 = fragmentContext1.newOperatorContext(physicalOperator1, true);
    final DrillBuf b11=oContext11.getAllocator().buffer(1000000);

    final OperatorContext oContext12 = fragmentContext1.newOperatorContext(physicalOperator2, stats, true);
    final DrillBuf b12=oContext12.getAllocator().buffer(500000);

    final OperatorContext oContext21 = fragmentContext1.newOperatorContext(physicalOperator3, true);

    def = new OpProfileDef(physicalOperator4.getOperatorId(), UserBitShared.CoreOperatorType.TEXT_WRITER_VALUE,
        OperatorContext.getChildCount(physicalOperator4));
    stats = fragmentContext2.getStats().getOperatorStats(def, fragmentContext2.getAllocator());
    final OperatorContext oContext22 = fragmentContext2.newOperatorContext(physicalOperator4, stats, true);
    final DrillBuf b22=oContext22.getAllocator().buffer(2000000);

    // New Fragment begins
    final BitControl.PlanFragment.Builder pfBuilder3 = BitControl.PlanFragment.newBuilder();
    pfBuilder3.setMemInitial(1000000);
    BitControl.PlanFragment pf3=pfBuilder3.build();

    final FragmentContext fragmentContext3 = new FragmentContext(bitContext, pf3, null, functionRegistry);

    // New fragment starts an operator that allocates an amount within the limit
    def = new OpProfileDef(physicalOperator5.getOperatorId(), UserBitShared.CoreOperatorType.UNION_VALUE,
        OperatorContext.getChildCount(physicalOperator5));
    stats = fragmentContext3.getStats().getOperatorStats(def, fragmentContext3.getAllocator());
    final OperatorContext oContext31 = fragmentContext3.newOperatorContext(physicalOperator5, stats, true);

    final DrillBuf b31a = oContext31.getAllocator().buffer(200000);

    // Previously running operator completes
    b22.release();
    ((AutoCloseable) oContext22).close();

    // Fragment 3 asks for more and fails
    boolean outOfMem = false;
    try {
      oContext31.getAllocator().buffer(44000000);
      fail("Fragment 3 should fail to allocate buffer");
    } catch (OutOfMemoryRuntimeException e) {
      outOfMem = true; // Expected.
    }
    assertTrue(outOfMem);

    // Operator is Exempt from Fragment limits. Fragment 3 asks for more and succeeds
    final OperatorContext oContext32 = fragmentContext3.newOperatorContext(physicalOperator6, false);
    try {
      final DrillBuf b32 = oContext32.getAllocator().buffer(4400000);
      b32.release();
    } catch (OutOfMemoryRuntimeException e) {
      fail("Fragment 3 failed to allocate buffer");
    } finally {
      closeOp(oContext32);
    }

    b11.release();
    closeOp(oContext11);
    b12.release();
    closeOp(oContext12);
    closeOp(oContext21);
    b31a.release();
    closeOp(oContext31);

    fragmentContext1.close();
    fragmentContext2.close();
    fragmentContext3.close();

    bit.close();
    serviceSet.close();

/*
    // ---------------------------------------- DEBUG ----------------------------------
    assertEquals(0, UnsafeDirectLittleEndian.getBufferCount());
    // ---------------------------------------- DEBUG ----------------------------------
*/
  }

  private void closeOp(OperatorContext c) throws Exception {
    ((AutoCloseable) c).close();
  }
}
