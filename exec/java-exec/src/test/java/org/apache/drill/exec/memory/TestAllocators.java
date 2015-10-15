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


import static org.junit.Assert.fail;

import java.util.Iterator;
import java.util.List;
import java.util.Properties;

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
import org.apache.drill.exec.planner.PhysicalPlanReaderTestFactory;
import org.apache.drill.exec.proto.BitControl;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.RemoteServiceSet;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

import io.netty.buffer.DrillBuf;

public class TestAllocators {

  private static final Properties TEST_CONFIGURATIONS = new Properties() {
    {
      put(ExecConstants.TOP_LEVEL_MAX_ALLOC, "14000000");
      put(ExecConstants.ENABLE_FRAGMENT_MEMORY_LIMIT, "true");
      put(ExecConstants.FRAGMENT_MEM_OVERCOMMIT_FACTOR, "1.1");
    }
  };

  private final static String planFile="/physical_allocator_test.json";

  @Test
  public void testTransfer() throws Exception {
    final Properties props = new Properties() {
      {
        put(ExecConstants.TOP_LEVEL_MAX_ALLOC, "1000000");
        put(ExecConstants.ERROR_ON_MEMORY_LEAK, "true");
      }
    };
    final DrillConfig config = DrillConfig.create(props);
    BufferAllocator a = RootAllocatorFactory.newRoot(config);
    BufferAllocator b = RootAllocatorFactory.newRoot(config);

    DrillBuf buf1 = a.buffer(1_000_000);
    DrillBuf buf2 = b.buffer(1_000);
    b.takeOwnership(buf1);

    buf1.release();
    buf2.release();

    a.close();
    b.close();
  }

  @Test
  public void testAllocators() throws Exception {
    // Setup a drillbit (initializes a root allocator)
    final DrillConfig config = DrillConfig.create(TEST_CONFIGURATIONS);
    final RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();
    final Drillbit bit = new Drillbit(config, serviceSet);
    bit.run();
    final DrillbitContext bitContext = bit.getContext();
    FunctionImplementationRegistry functionRegistry = bitContext.getFunctionImplementationRegistry();
    StoragePluginRegistry storageRegistry = new StoragePluginRegistry(bitContext);

    // Create a few Fragment Contexts

    BitControl.PlanFragment.Builder pfBuilder1=BitControl.PlanFragment.newBuilder();
    pfBuilder1.setMemInitial(1500000);
    BitControl.PlanFragment pf1=pfBuilder1.build();
    BitControl.PlanFragment.Builder pfBuilder2=BitControl.PlanFragment.newBuilder();
    pfBuilder2.setMemInitial(500000);
    BitControl.PlanFragment pf2=pfBuilder1.build();

    FragmentContext fragmentContext1 = new FragmentContext(bitContext, pf1, null, functionRegistry);
    FragmentContext fragmentContext2 = new FragmentContext(bitContext, pf2, null, functionRegistry);

    // Get a few physical operators. Easiest way is to read a physical plan.
    PhysicalPlanReader planReader = PhysicalPlanReaderTestFactory.defaultPhysicalPlanReader(bitContext, storageRegistry);
    PhysicalPlan plan = planReader.readPhysicalPlan(Files.toString(FileUtils.getResourceAsFile(planFile), Charsets.UTF_8));
    List<PhysicalOperator> physicalOperators = plan.getSortedOperators();
    Iterator<PhysicalOperator> physicalOperatorIterator = physicalOperators.iterator();

    PhysicalOperator physicalOperator1 = physicalOperatorIterator.next();
    PhysicalOperator physicalOperator2 = physicalOperatorIterator.next();
    PhysicalOperator physicalOperator3 = physicalOperatorIterator.next();
    PhysicalOperator physicalOperator4 = physicalOperatorIterator.next();
    PhysicalOperator physicalOperator5 = physicalOperatorIterator.next();
    PhysicalOperator physicalOperator6 = physicalOperatorIterator.next();

    // Create some bogus Operator profile defs and stats to create operator contexts
    OpProfileDef def;
    OperatorStats stats;

    //Use some bogus operator type to create a new operator context.
    def = new OpProfileDef(physicalOperator1.getOperatorId(), UserBitShared.CoreOperatorType.MOCK_SUB_SCAN_VALUE,
        OperatorContext.getChildCount(physicalOperator1));
    stats = fragmentContext1.getStats().newOperatorStats(def, fragmentContext1.getAllocator());


    // Add a couple of Operator Contexts
    // Initial allocation = 1000000 bytes for all operators
    OperatorContext oContext11 = fragmentContext1.newOperatorContext(physicalOperator1, true);
    DrillBuf b11=oContext11.getAllocator().buffer(1000000);

    OperatorContext oContext12 = fragmentContext1.newOperatorContext(physicalOperator2, stats, true);
    DrillBuf b12=oContext12.getAllocator().buffer(500000);

    OperatorContext oContext21 = fragmentContext1.newOperatorContext(physicalOperator3, true);

    def = new OpProfileDef(physicalOperator4.getOperatorId(), UserBitShared.CoreOperatorType.TEXT_WRITER_VALUE,
        OperatorContext.getChildCount(physicalOperator4));
    stats = fragmentContext2.getStats().newOperatorStats(def, fragmentContext2.getAllocator());
    OperatorContext oContext22 = fragmentContext2.newOperatorContext(physicalOperator4, stats, true);
    DrillBuf b22=oContext22.getAllocator().buffer(2000000);

    // New Fragment begins
    BitControl.PlanFragment.Builder pfBuilder3=BitControl.PlanFragment.newBuilder();
    pfBuilder3.setMemInitial(1000000);
    BitControl.PlanFragment pf3=pfBuilder3.build();

    FragmentContext fragmentContext3 = new FragmentContext(bitContext, pf3, null, functionRegistry);

    // New fragment starts an operator that allocates an amount within the limit
    def = new OpProfileDef(physicalOperator5.getOperatorId(), UserBitShared.CoreOperatorType.UNION_VALUE,
        OperatorContext.getChildCount(physicalOperator5));
    stats = fragmentContext3.getStats().newOperatorStats(def, fragmentContext3.getAllocator());
    OperatorContext oContext31 = fragmentContext3.newOperatorContext(physicalOperator5, stats, true);

    DrillBuf b31a = oContext31.getAllocator().buffer(200000);

    //Previously running operator completes
    b22.release();
    ((AutoCloseable) oContext22).close();

    // Fragment 3 asks for more and fails
    try {
      oContext31.getAllocator().buffer(4400000);
      fail("Fragment 3 should fail to allocate buffer");
    } catch (OutOfMemoryRuntimeException e) {
      // expected
    }

    // Operator is Exempt from Fragment limits. Fragment 3 asks for more and succeeds
    OperatorContext oContext32 = fragmentContext3.newOperatorContext(physicalOperator6, false);
    try {
      DrillBuf b32 = oContext32.getAllocator().buffer(4400000);
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
  }

  private void closeOp(OperatorContext c) throws Exception {
    ((AutoCloseable) c).close();
  }
}
