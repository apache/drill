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

import com.google.common.base.Charsets;
import com.google.common.io.Files;
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
import org.apache.drill.exec.vector.BigIntVector;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertTrue;

public class TestDateFunctions extends PopUnitTestBase {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestDateFunctions.class);

    public void testCommon(String[] expectedResults, String physicalPlan, String resourceFile) throws Exception {
        try (RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();
             Drillbit bit = new Drillbit(CONFIG, serviceSet);
             DrillClient client = new DrillClient(CONFIG, serviceSet.getCoordinator())) {

            // run query.
            bit.run();
            client.connect();
            List<QueryResultBatch> results = client.runQuery(UserProtos.QueryType.PHYSICAL,
                    Files.toString(FileUtils.getResourceAsFile(physicalPlan), Charsets.UTF_8)
                            .replace("#{TEST_FILE}", resourceFile));

            RecordBatchLoader batchLoader = new RecordBatchLoader(bit.getContext().getAllocator());

            QueryResultBatch batch = results.get(0);
            assertTrue(batchLoader.load(batch.getHeader().getDef(), batch.getData()));


            int i = 0;
            for (VectorWrapper<?> v : batchLoader) {

                ValueVector.Accessor accessor = v.getValueVector().getAccessor();
                System.out.println(accessor.getObject(0));
                assertTrue((accessor.getObject(0)).toString().equals(expectedResults[i++]));
            }

            batchLoader.clear();
            for(QueryResultBatch b : results){
                b.release();
            }
        }
    }

    @Test
    public void testDateIntervalArithmetic() throws Exception {
        String expectedResults[] = {"2009-02-23",
                                    "2008-02-24",
                                    "13:20:33",
                                    "2008-02-24 12:00:00.0",
                                    "2009-04-23 12:00:00.0",
                                    "2008-02-24 12:00:00.0",
                                    "2009-04-23 12:00:00.0",
                                    "2009-02-23",
                                    "2008-02-24",
                                    "13:20:33",
                                    "2008-02-24 12:00:00.0",
                                    "2009-04-23 12:00:00.0",
                                    "2008-02-24 12:00:00.0",
                                    "2009-04-23 12:00:00.0"};

        testCommon(expectedResults, "/functions/date/date_interval_arithmetic.json", "/test_simple_date.json");
    }

    @Test
    public void testDateDifferenceArithmetic() throws Exception {

        String[] expectedResults = {"365 days 0:0:0.0",
                                   "-366 days 0:-1:0.0",
                                    "0 days 3:0:0.0",
                                    "0 days 11:0:0.0"};
        testCommon(expectedResults, "/functions/date/date_difference_arithmetic.json", "/test_simple_date.json");
    }

    @Test
    public void testIntervalArithmetic() throws Exception {

        String[] expectedResults = {"2 years 2 months ",
                                    "2 days 1:2:3.0",
                                    "0 years 2 months ",
                                    "0 days 1:2:3.0",
                                    "2 years 4 months 0 days 0:0:0.0",
                                    "0 years 0 months 0 days 2:0:6.0",
                                    "0 years 7 months 0 days 0:0:0.0",
                                    "0 years 0 months 0 days 0:30:1.500",
                                    "2 years 9 months 18 days 0:0:0.0",
                                    "0 years 0 months 0 days 2:24:7.200",
                                    "0 years 6 months 19 days 23:59:59.999",
                                    "0 years 0 months 0 days 0:28:35.714"};
        testCommon(expectedResults, "/functions/date/interval_arithmetic.json", "/test_simple_date.json");
    }

    @Test
    public void testToChar() throws Exception {

        String expectedResults[] = {"2008-Feb-23",
                                    "12 20 30",
                                    "2008 Feb 23 12:00:00",
                                    "2008-02-23 20:00:00 America/Los_Angeles"};
        testCommon(expectedResults, "/functions/date/to_char.json", "/test_simple_date.json");
    }

    @Test
    public void testToDateType() throws Exception {
        String expectedResults[] = {"2008-02-23",
                                    "12:20:30",
                                    "2008-02-23 12:00:00.0",
                                    "2008-02-23 12:00:00.0"};

        testCommon(expectedResults, "/functions/date/to_date_type.json", "/test_simple_date.json");
    }

}
