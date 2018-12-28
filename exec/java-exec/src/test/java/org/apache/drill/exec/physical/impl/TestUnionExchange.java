/*
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

import java.util.List;

import org.apache.drill.categories.OperatorTest;
import org.apache.drill.common.util.DrillFileUtils;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.pop.PopUnitTestBase;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.RecordBatchSizer;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.RemoteServiceSet;
import org.apache.drill.exec.vector.BigIntVector;
import org.junit.Test;

import org.apache.drill.shaded.guava.com.google.common.base.Charsets;
import org.apache.drill.shaded.guava.com.google.common.io.Files;
import org.junit.experimental.categories.Category;

@Category(OperatorTest.class)
public class TestUnionExchange extends PopUnitTestBase {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestUnionExchange.class);

  @Test
  public void twoBitTwoExchangeTwoEntryRun() throws Exception {
    RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();

    try (Drillbit bit1 = new Drillbit(CONFIG, serviceSet);
         Drillbit bit2 = new Drillbit(CONFIG, serviceSet);
         DrillClient client = new DrillClient(CONFIG, serviceSet.getCoordinator())) {

      bit1.run();
      bit2.run();
      client.connect();
      List<QueryDataBatch> results = client.runQuery(org.apache.drill.exec.proto.UserBitShared.QueryType.PHYSICAL,
              Files.asCharSource(DrillFileUtils.getResourceAsFile("/sender/union_exchange.json"),
                      Charsets.UTF_8).read());
      int count = 0;
      for (QueryDataBatch b : results) {
        if (b.getHeader().getRowCount() != 0) {
          count += b.getHeader().getRowCount();
        }
        b.release();
      }
      assertEquals(150, count);
    }
  }

  // Test UnionExchange BatchSizing
  @Test
  public void twoBitUnionExchangeRunBatchSizing() throws Exception {
    twoBitUnionExchangeRunBatchSizingImpl(64 * 1024);
    twoBitUnionExchangeRunBatchSizingImpl(512 * 1024);
    twoBitUnionExchangeRunBatchSizingImpl(1024 * 1024);
  }

  /**
   * This function takes in the batchSizeLimit and configures two Drillbits with that batch limit.
   * Then runs a plan that includes a UnionExchange over some mock data.
   * The test checks for exchange batch size conformity and also checks the integrity of the exchanged data.
   * @param batchSizeLimit
   * @throws Exception
   */
  public void twoBitUnionExchangeRunBatchSizingImpl(int batchSizeLimit) throws Exception {
    RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();
    try (Drillbit bit1 = new Drillbit(CONFIG, serviceSet);
         Drillbit bit2 = new Drillbit(CONFIG, serviceSet);
         DrillClient client = new DrillClient(CONFIG, serviceSet.getCoordinator())) {

      bit1.run();
      bit2.run();
      client.connect();
      bit1.getContext().getOptionManager().setLocalOption(ExecConstants.EXCHANGE_BATCH_SIZE, batchSizeLimit);
      bit2.getContext().getOptionManager().setLocalOption(ExecConstants.EXCHANGE_BATCH_SIZE, batchSizeLimit);

      List<QueryDataBatch> results = client.runQuery(org.apache.drill.exec.proto.UserBitShared.QueryType.PHYSICAL,
              Files.asCharSource(DrillFileUtils.getResourceAsFile("/sender/union_exchange_batch_sizing.json"),
                      Charsets.UTF_8).read());
      int count = 0;
      final RecordBatchLoader loader = new RecordBatchLoader(client.getAllocator());
      boolean even = true;
      for (QueryDataBatch b : results) { //iterate over each incoming batch
        int recordCount = b.getHeader().getRowCount();
        if (recordCount != 0) {
          // convert QueryDataBatch to a VectorContainer
          loader.load(b.getHeader().getDef(), b.getData());
          final VectorContainer container = loader.getContainer();
          container.setRecordCount(recordCount);
          // check if the size is within the batchSizeLimit
          RecordBatchSizer recordBatchSizer = new RecordBatchSizer(container);
          assertTrue(recordBatchSizer.getNetBatchSize() <= batchSizeLimit);
          // get col 1 vector. This is a mocked BigInt, as defined in the json file.
          // this col has Long.MIN_VALUE for even rows and Long.MAX_VALUE for odd rows
          VectorWrapper wrapper = container.getValueVector(1);
          BigIntVector bigIntVector = (BigIntVector) (wrapper.getValueVector());
          for (int i = 0; i < recordCount; i++) {
            long value = bigIntVector.getAccessor().get(i);
            // row0 is guaranteed to be Long.MIN_VALUE (even) only in the original mock data
            // batch splitting can cause row0 to be Long.MAX_VALUE, so it has to be checked
            if (i == 0) {
              even = (value == Long.MIN_VALUE);
            }
            // check if the values alternate
            if (even) {
              assertEquals(value, Long.MIN_VALUE);
            } else {
              assertEquals(value, Long.MAX_VALUE);
            }
            even = !even;
          }
          count += recordCount;
          loader.clear();
        }
        b.release();
      }
      // check if total row count received is the same as the
      // count in the mock definition
      assertEquals(300000, count);
    }
  }
}
