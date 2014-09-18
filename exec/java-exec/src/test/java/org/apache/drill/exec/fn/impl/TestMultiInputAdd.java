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

import static org.junit.Assert.assertTrue;

import java.util.List;

import mockit.Injectable;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.pop.PopUnitTestBase;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.apache.drill.exec.rpc.user.UserServer;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.RemoteServiceSet;
import org.apache.drill.exec.vector.ValueVector;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

public class TestMultiInputAdd extends PopUnitTestBase {

    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestMathFunctions.class);
    DrillConfig c = DrillConfig.create();


    @Test
    public void testMultiInputAdd(@Injectable final DrillbitContext bitContext, @Injectable UserServer.UserClientConnection connection) throws Throwable
    {
        try (RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();
             Drillbit bit = new Drillbit(CONFIG, serviceSet);
             DrillClient client = new DrillClient(CONFIG, serviceSet.getCoordinator())) {

            // run query.
            bit.run();
            client.connect();
            List<QueryResultBatch> results = client.runQuery(org.apache.drill.exec.proto.UserBitShared.QueryType.PHYSICAL,
                    Files.toString(FileUtils.getResourceAsFile("/functions/multi_input_add_test.json"), Charsets.UTF_8));

            RecordBatchLoader batchLoader = new RecordBatchLoader(bit.getContext().getAllocator());

            QueryResultBatch batch = results.get(0);
            assertTrue(batchLoader.load(batch.getHeader().getDef(), batch.getData()));

            for (VectorWrapper<?> v : batchLoader) {

                ValueVector.Accessor accessor = v.getValueVector().getAccessor();

                assertTrue((accessor.getObject(0)).equals(10));
            }

            batchLoader.clear();
            for(QueryResultBatch b : results){
                b.release();
            }
        }
    }
}
