/*******************************************************************************
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
 ******************************************************************************/
package org.apache.drill.exec.physical.impl;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.apache.drill.common.util.FileUtils;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.pop.PopUnitTestBase;
import org.apache.drill.exec.proto.UserProtos.QueryType;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.vector.ValueVector;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.RemoteServiceSet;
import org.junit.Test;

import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.google.common.base.Charsets;
import com.google.common.io.Files;

public class TestSimpleFragmentRun extends PopUnitTestBase {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestSimpleFragmentRun.class);

  @Test
  public void runNoExchangeFragment() throws Exception {
    try(RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet(); 
        Drillbit bit = new Drillbit(CONFIG, serviceSet); 
        DrillClient client = new DrillClient(CONFIG, serviceSet.getCoordinator());){
    
    // run query.
    bit.run();
    client.connect();
    List<QueryResultBatch> results = client.runQuery(QueryType.PHYSICAL, Files.toString(FileUtils.getResourceAsFile("/physical_test2.json"), Charsets.UTF_8));

    // look at records
    RecordBatchLoader batchLoader = new RecordBatchLoader(bit.getContext().getAllocator());
    int recordCount = 0;
    for (QueryResultBatch batch : results) {
      if(!batch.hasData()) continue;
      boolean schemaChanged = batchLoader.load(batch.getHeader().getDef(), batch.getData());
      boolean firstColumn = true;

      // print headers.
      if (schemaChanged) {
        System.out.println("\n\n========NEW SCHEMA=========\n\n");
        for (IntObjectCursor<ValueVector.Base> v : batchLoader) {

          if (firstColumn) {
            firstColumn = false;
          } else {
            System.out.print("\t");
          }
          System.out.print(v.value.getField().getName());
          System.out.print("[");
          System.out.print(v.value.getField().getType().getMinorType());
          System.out.print("]");
        }
        System.out.println();
      }


      for (int i = 0; i < batchLoader.getRecordCount(); i++) {
        boolean first = true;
        recordCount++;
        for (IntObjectCursor<ValueVector.Base> v : batchLoader) {
          if (first) {
            first = false;
          } else {
            System.out.print("\t");
          }
          System.out.print(v.value.getObject(i));
        }
        if(!first) System.out.println();
      }

    }
    logger.debug("Received results {}", results);
    assertEquals(recordCount, 200);
    }
  }

}
