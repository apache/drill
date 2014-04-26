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
package org.apache.drill.exec.physical.impl.join;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import mockit.Injectable;
import mockit.NonStrictExpectations;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.common.util.TestTools;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.TopLevelAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.physical.base.FragmentRoot;
import org.apache.drill.exec.physical.impl.OperatorCreatorRegistry;
import org.apache.drill.exec.physical.impl.ImplCreator;
import org.apache.drill.exec.physical.impl.SimpleRootExec;
import org.apache.drill.exec.planner.PhysicalPlanReader;
import org.apache.drill.exec.proto.CoordinationProtos;
import org.apache.drill.exec.proto.ExecProtos;
import org.apache.drill.exec.proto.BitControl.PlanFragment;
import org.apache.drill.exec.rpc.user.UserServer;
import org.apache.drill.exec.rpc.user.UserServer.UserClientConnection;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.vector.ValueVector;
import org.junit.AfterClass;
import org.junit.Rule;
import org.junit.Test;

import com.beust.jcommander.internal.Lists;
import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.codahale.metrics.MetricRegistry;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;
import java.util.List;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.pop.PopUnitTestBase;
import org.apache.drill.exec.proto.UserProtos;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.RemoteServiceSet;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VarCharVector;
import org.apache.drill.exec.vector.IntVector;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;

import com.google.common.base.Charsets;
import com.google.common.io.Files;


public class TestHashJoin extends PopUnitTestBase{
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestMergeJoin.class);

    @Rule public final TestRule TIMEOUT = TestTools.getTimeoutRule(100000);

    DrillConfig c = DrillConfig.create();

    private void testHJMockScanCommon(final DrillbitContext bitContext, UserServer.UserClientConnection connection, String physicalPlan, int expectedRows) throws Throwable {
        new NonStrictExpectations(){{
            bitContext.getMetrics(); result = new MetricRegistry();
            bitContext.getAllocator(); result = new TopLevelAllocator();
            bitContext.getOperatorCreatorRegistry(); result = new OperatorCreatorRegistry(c);
        }};

        PhysicalPlanReader reader = new PhysicalPlanReader(c, c.getMapper(), CoordinationProtos.DrillbitEndpoint.getDefaultInstance());
        PhysicalPlan plan = reader.readPhysicalPlan(Files.toString(FileUtils.getResourceAsFile(physicalPlan), Charsets.UTF_8));
        FunctionImplementationRegistry registry = new FunctionImplementationRegistry(c);
        FragmentContext context = new FragmentContext(bitContext, PlanFragment.getDefaultInstance(), connection, registry);
        SimpleRootExec exec = new SimpleRootExec(ImplCreator.getExec(context, (FragmentRoot) plan.getSortedOperators(false).iterator().next()));

        int totalRecordCount = 0;
        while (exec.next()) {
            totalRecordCount += exec.getRecordCount();
        }
        assertEquals(expectedRows, totalRecordCount);
        System.out.println("Total Record Count: " + totalRecordCount);
        if (context.getFailureCause() != null)
            throw context.getFailureCause();
        assertTrue(!context.isFailed());
    }

    @Test
    public void multiBatchEqualityJoin(@Injectable final DrillbitContext bitContext,
                                   @Injectable UserServer.UserClientConnection connection) throws Throwable {

        testHJMockScanCommon(bitContext, connection, "/join/hash_join_multi_batch.json", 200000);
    }

    @Test
    public void multiBatchRightOuterJoin(@Injectable final DrillbitContext bitContext,
                                         @Injectable UserServer.UserClientConnection connection) throws Throwable {

        testHJMockScanCommon(bitContext, connection, "/join/hj_right_outer_multi_batch.json", 100000);
    }

    @Test
    public void multiBatchLeftOuterJoin(@Injectable final DrillbitContext bitContext,
                                        @Injectable UserServer.UserClientConnection connection) throws Throwable {


        testHJMockScanCommon(bitContext, connection, "/join/hj_left_outer_multi_batch.json", 100000);
    }

    @Test
    public void simpleEqualityJoin(@Injectable final DrillbitContext bitContext,
                                   @Injectable UserServer.UserClientConnection connection) throws Throwable {

        // Function checks for casting from Float, Double to Decimal data types
        try (RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();
             Drillbit bit = new Drillbit(CONFIG, serviceSet);
             DrillClient client = new DrillClient(CONFIG, serviceSet.getCoordinator())) {

            // run query.
            bit.run();
            client.connect();
            List<QueryResultBatch> results = client.runQuery(UserProtos.QueryType.PHYSICAL,
                    Files.toString(FileUtils.getResourceAsFile("/join/hash_join.json"), Charsets.UTF_8)
                            .replace("#{TEST_FILE_1}", FileUtils.getResourceAsFile("/build_side_input.json").toURI().toString())
                            .replace("#{TEST_FILE_2}", FileUtils.getResourceAsFile("/probe_side_input.json").toURI().toString()));

            RecordBatchLoader batchLoader = new RecordBatchLoader(bit.getContext().getAllocator());

            QueryResultBatch batch = results.get(0);
            assertTrue(batchLoader.load(batch.getHeader().getDef(), batch.getData()));

            batchLoader.getValueAccessorById(0, IntVector.class);

            Iterator<VectorWrapper<?>> itr = batchLoader.iterator();

            // Just test the join key
            long colA[] = {1, 1, 2, 2, 1, 1};

            // Check the output of decimal9
            ValueVector.Accessor intAccessor1 = itr.next().getValueVector().getAccessor();


            for (int i = 0; i < intAccessor1.getValueCount(); i++) {
                assertEquals(intAccessor1.getObject(i), colA[i]);
            }
            assertEquals(6, intAccessor1.getValueCount());
        }
    }

    @Test
    public void multipleConditionJoin(@Injectable final DrillbitContext bitContext,
                                      @Injectable UserServer.UserClientConnection connection) throws Throwable {

        // Function checks for casting from Float, Double to Decimal data types
        try (RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();
             Drillbit bit = new Drillbit(CONFIG, serviceSet);
             DrillClient client = new DrillClient(CONFIG, serviceSet.getCoordinator())) {

            // run query.
            bit.run();
            client.connect();
            List<QueryResultBatch> results = client.runQuery(UserProtos.QueryType.PHYSICAL,
                    Files.toString(FileUtils.getResourceAsFile("/join/hj_multi_condition_join.json"), Charsets.UTF_8)
                            .replace("#{TEST_FILE_1}", FileUtils.getResourceAsFile("/build_side_input.json").toURI().toString())
                            .replace("#{TEST_FILE_2}", FileUtils.getResourceAsFile("/probe_side_input.json").toURI().toString()));

            RecordBatchLoader batchLoader = new RecordBatchLoader(bit.getContext().getAllocator());

            QueryResultBatch batch = results.get(0);
            assertTrue(batchLoader.load(batch.getHeader().getDef(), batch.getData()));

            batchLoader.getValueAccessorById(0, IntVector.class);

            Iterator<VectorWrapper<?>> itr = batchLoader.iterator();

            // Just test the join key
            long colA[] = {1, 2, 1};
            long colC[] = {100, 200, 500};

            // Check the output of decimal9
            ValueVector.Accessor intAccessor1 = itr.next().getValueVector().getAccessor();
            ValueVector.Accessor intAccessor2 = itr.next().getValueVector().getAccessor();


            for (int i = 0; i < intAccessor1.getValueCount(); i++) {
                assertEquals(intAccessor1.getObject(i), colA[i]);
                assertEquals(intAccessor2.getObject(i), colC[i]);
            }
            assertEquals(3, intAccessor1.getValueCount());
        }
    }
}