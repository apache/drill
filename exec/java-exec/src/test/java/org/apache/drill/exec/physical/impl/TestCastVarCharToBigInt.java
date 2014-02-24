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

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

import mockit.Injectable;
import mockit.NonStrictExpectations;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.exec.client.DrillClient;
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
import org.apache.drill.exec.pop.PopUnitTestBase;
import org.apache.drill.exec.proto.BitControl;
import org.apache.drill.exec.proto.CoordinationProtos;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.UserProtos;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.apache.drill.exec.rpc.user.UserServer.UserClientConnection;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.RemoteServiceSet;
import org.apache.drill.exec.vector.BigIntVector;
import org.apache.drill.exec.vector.Float8Vector;

import org.apache.drill.exec.vector.ValueVector;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.codahale.metrics.MetricRegistry;

import java.util.List;


public class TestCastVarCharToBigInt extends PopUnitTestBase {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestCastVarCharToBigInt.class);

    @Test
    public void testCastToBigInt() throws Exception {
        try (RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();
             Drillbit bit = new Drillbit(CONFIG, serviceSet);
             DrillClient client = new DrillClient(CONFIG, serviceSet.getCoordinator())) {

            // run query.
            bit.run();
            client.connect();
            List<QueryResultBatch> results = client.runQuery(UserProtos.QueryType.PHYSICAL,
                    Files.toString(FileUtils.getResourceAsFile("/functions/cast/test_cast_varchar_to_bigint.json"), Charsets.UTF_8)
                            .replace("#{TEST_FILE}", "/scan_json_test_cast.json")
            );

            RecordBatchLoader batchLoader = new RecordBatchLoader(bit.getContext().getAllocator());

            QueryResultBatch batch = results.get(0);
            assertTrue(batchLoader.load(batch.getHeader().getDef(), batch.getData()));

            batchLoader.getValueAccessorById(0, BigIntVector.class);

            for (VectorWrapper<?> v : batchLoader) {

                ValueVector.Accessor accessor = v.getValueVector().getAccessor();

                assertEquals(accessor.getObject(0), 2008L);
                assertEquals(accessor.getObject(1), 2007L);
                assertEquals(accessor.getObject(2), 2006L);
            }
        }
    }
}
