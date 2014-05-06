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
package org.apache.drill.exec.record.vector;

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

import java.util.Iterator;
import java.util.List;

/* This class tests the existing date types. Simply using date types
 * by casting from VarChar, performing basic functions and converting
 * back to VarChar.
 */
public class TestDateTypes extends PopUnitTestBase {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestDateTypes.class);

    @Test
    public void testDate() throws Exception {
        try (RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();
             Drillbit bit = new Drillbit(CONFIG, serviceSet);
             DrillClient client = new DrillClient(CONFIG, serviceSet.getCoordinator())) {

            // run query.
            bit.run();
            client.connect();
            List<QueryResultBatch> results = client.runQuery(UserProtos.QueryType.PHYSICAL,
                    Files.toString(FileUtils.getResourceAsFile("/record/vector/test_date.json"), Charsets.UTF_8)
                            .replace("#{TEST_FILE}", "/test_simple_date.json"));

            RecordBatchLoader batchLoader = new RecordBatchLoader(bit.getContext().getAllocator());

            QueryResultBatch batch = results.get(0);
            assertTrue(batchLoader.load(batch.getHeader().getDef(), batch.getData()));

            for (VectorWrapper<?> v : batchLoader) {

                ValueVector.Accessor accessor = v.getValueVector().getAccessor();

                assertTrue((accessor.getObject(0)).equals("1970-01-02"));
                assertTrue((accessor.getObject(1)).equals("2008-12-28"));
                assertTrue((accessor.getObject(2)).equals("2000-02-27"));
            }

            batchLoader.clear();
            for(QueryResultBatch b : results){
              b.release();
            }
        }
    }

    @Test
    public void testSortDate() throws Exception {
        try (RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();
             Drillbit bit = new Drillbit(CONFIG, serviceSet);
             DrillClient client = new DrillClient(CONFIG, serviceSet.getCoordinator())) {

            // run query.
            bit.run();
            client.connect();
            List<QueryResultBatch> results = client.runQuery(UserProtos.QueryType.PHYSICAL,
                    Files.toString(FileUtils.getResourceAsFile("/record/vector/test_sort_date.json"), Charsets.UTF_8)
                            .replace("#{TEST_FILE}", "/test_simple_date.json"));

            RecordBatchLoader batchLoader = new RecordBatchLoader(bit.getContext().getAllocator());

            QueryResultBatch batch = results.get(0);
            assertTrue(batchLoader.load(batch.getHeader().getDef(), batch.getData()));

            for (VectorWrapper<?> v : batchLoader) {

                ValueVector.Accessor accessor = v.getValueVector().getAccessor();

                assertEquals((accessor.getObject(0)), new String("1970-01-02"));
                assertEquals((accessor.getObject(1)), new String("2000-02-27"));
                assertEquals((accessor.getObject(2)), new String("2008-12-28"));
            }

            batchLoader.clear();
            for(QueryResultBatch b : results){
              b.release();
            }
        }
    }

    @Test
    public void testTimeStamp() throws Exception {
        try (RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();
             Drillbit bit = new Drillbit(CONFIG, serviceSet);
             DrillClient client = new DrillClient(CONFIG, serviceSet.getCoordinator())) {

            // run query.
            bit.run();
            client.connect();
            List<QueryResultBatch> results = client.runQuery(UserProtos.QueryType.PHYSICAL,
                    Files.toString(FileUtils.getResourceAsFile("/record/vector/test_timestamp.json"), Charsets.UTF_8)
                            .replace("#{TEST_FILE}", "/test_simple_date.json"));

            RecordBatchLoader batchLoader = new RecordBatchLoader(bit.getContext().getAllocator());

            QueryResultBatch batch = results.get(0);
            assertTrue(batchLoader.load(batch.getHeader().getDef(), batch.getData()));

            for (VectorWrapper<?> v : batchLoader) {

                ValueVector.Accessor accessor = v.getValueVector().getAccessor();

                assertTrue(accessor.getObject(0).equals("1970-01-02 10:20:33.000"));
                assertTrue((accessor.getObject(1)).equals("2008-12-28 11:34:00.129"));
                assertTrue((accessor.getObject(2)).equals("2000-02-27 14:24:00.000"));
            }

            batchLoader.clear();
            for(QueryResultBatch b : results){
              b.release();
            }
        }
    }

    @Test
    public void testInterval() throws Exception {
        try (RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();
             Drillbit bit = new Drillbit(CONFIG, serviceSet);
             DrillClient client = new DrillClient(CONFIG, serviceSet.getCoordinator())) {

            // run query.
            bit.run();
            client.connect();
            List<QueryResultBatch> results = client.runQuery(UserProtos.QueryType.PHYSICAL,
                    Files.toString(FileUtils.getResourceAsFile("/record/vector/test_interval.json"), Charsets.UTF_8)
                            .replace("#{TEST_FILE}", "/test_simple_interval.json"));

            RecordBatchLoader batchLoader = new RecordBatchLoader(bit.getContext().getAllocator());

            QueryResultBatch batch = results.get(0);
            assertTrue(batchLoader.load(batch.getHeader().getDef(), batch.getData()));

            Iterator<VectorWrapper<?>> itr = batchLoader.iterator();

            ValueVector.Accessor accessor = itr.next().getValueVector().getAccessor();

            // Check the interval type
            assertTrue((accessor.getObject(0)).equals("2 years 2 months 1 day 1:20:35.0"));
            assertTrue((accessor.getObject(1)).equals("2 years 2 months 0 days 0:0:0.0"));
            assertTrue((accessor.getObject(2)).equals("0 years 0 months 0 days 1:20:35.0"));
            assertTrue((accessor.getObject(3)).equals("2 years 2 months 1 day 1:20:35.897"));
            assertTrue((accessor.getObject(4)).equals("0 years 0 months 0 days 0:0:35.4"));
            assertTrue((accessor.getObject(5)).equals("1 year 10 months 1 day 0:-39:-25.0"));

            accessor = itr.next().getValueVector().getAccessor();

            // Check the interval year type
            assertTrue((accessor.getObject(0)).equals("2 years 2 months "));
            assertTrue((accessor.getObject(1)).equals("2 years 2 months "));
            assertTrue((accessor.getObject(2)).equals("0 years 0 months "));
            assertTrue((accessor.getObject(3)).equals("2 years 2 months "));
            assertTrue((accessor.getObject(4)).equals("0 years 0 months "));
            assertTrue((accessor.getObject(5)).equals("1 year 10 months "));

            accessor = itr.next().getValueVector().getAccessor();

            // Check the interval day type
            assertTrue((accessor.getObject(0)).equals("1 day 1:20:35.0"));
            assertTrue((accessor.getObject(1)).equals("0 days 0:0:0.0"));
            assertTrue((accessor.getObject(2)).equals("0 days 1:20:35.0"));
            assertTrue((accessor.getObject(3)).equals("1 day 1:20:35.897"));
            assertTrue((accessor.getObject(4)).equals("0 days 0:0:35.4"));
            assertTrue((accessor.getObject(5)).equals("1 day 0:-39:-25.0"));

            batchLoader.clear();
            for(QueryResultBatch b : results){
              b.release();
            }
        }
    }

    @Test
    public void testLiterals() throws Exception {
        try (RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();
             Drillbit bit = new Drillbit(CONFIG, serviceSet);
             DrillClient client = new DrillClient(CONFIG, serviceSet.getCoordinator())) {

            // run query.
            bit.run();
            client.connect();
            List<QueryResultBatch> results = client.runQuery(UserProtos.QueryType.PHYSICAL,
                    Files.toString(FileUtils.getResourceAsFile("/record/vector/test_all_date_literals.json"), Charsets.UTF_8)
                            .replace("#{TEST_FILE}", "/test_simple_date.json"));

            RecordBatchLoader batchLoader = new RecordBatchLoader(bit.getContext().getAllocator());

            QueryResultBatch batch = results.get(0);
            assertTrue(batchLoader.load(batch.getHeader().getDef(), batch.getData()));

            String result[] = {"2008-02-27",
                               "2008-02-27 01:02:03.000",
                               "2008-02-27 01:02:03.000 UTC",
                               "10:11:13.999",
                               "2 years 2 months 3 days 0:1:3.89"};

            int idx = 0;

            for (VectorWrapper<?> v : batchLoader) {

                ValueVector.Accessor accessor = v.getValueVector().getAccessor();

                assertTrue((accessor.getObject(0)).equals(result[idx++]));
            }

            batchLoader.clear();
            for(QueryResultBatch b : results){
              b.release();
            }
        }
    }

    @Test
    public void testDateAdd() throws Exception {
        try (RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();
             Drillbit bit = new Drillbit(CONFIG, serviceSet);
             DrillClient client = new DrillClient(CONFIG, serviceSet.getCoordinator())) {

            // run query.
            bit.run();
            client.connect();
            List<QueryResultBatch> results = client.runQuery(UserProtos.QueryType.PHYSICAL,
                    Files.toString(FileUtils.getResourceAsFile("/record/vector/test_date_add.json"), Charsets.UTF_8)
                            .replace("#{TEST_FILE}", "/test_simple_date.json"));

            RecordBatchLoader batchLoader = new RecordBatchLoader(bit.getContext().getAllocator());

            QueryResultBatch batch = results.get(0);
            assertTrue(batchLoader.load(batch.getHeader().getDef(), batch.getData()));

            for (VectorWrapper<?> v : batchLoader) {

                ValueVector.Accessor accessor = v.getValueVector().getAccessor();

                assertTrue((accessor.getObject(0)).equals("2008-03-27"));

            }

            batchLoader.clear();
            for(QueryResultBatch b : results){
              b.release();
            }
        }
    }
}
