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

import java.util.List;

import org.apache.drill.categories.SlowTest;
import org.apache.drill.common.util.DrillFileUtils;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.pop.PopUnitTestBase;
import org.apache.drill.exec.proto.UserBitShared.QueryType;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.RemoteServiceSet;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.junit.experimental.categories.Category;

@Category({SlowTest.class})
public class TestDistributedFragmentRun extends PopUnitTestBase{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestDistributedFragmentRun.class);

  @Test
  public void oneBitOneExchangeOneEntryRun() throws Exception{
    RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();

    try(Drillbit bit1 = new Drillbit(CONFIG, serviceSet); DrillClient client = new DrillClient(CONFIG, serviceSet.getCoordinator());){
      bit1.run();
      client.connect();
      List<QueryDataBatch> results = client.runQuery(QueryType.PHYSICAL, Files.toString(DrillFileUtils.getResourceAsFile("/physical_single_exchange.json"), Charsets.UTF_8));
      int count = 0;
      for(QueryDataBatch b : results){
        count += b.getHeader().getRowCount();
        b.release();
      }
      assertEquals(100, count);
    }


  }


  @Test
  public void oneBitOneExchangeTwoEntryRun() throws Exception{
    RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();

    try(Drillbit bit1 = new Drillbit(CONFIG, serviceSet); DrillClient client = new DrillClient(CONFIG, serviceSet.getCoordinator());){
      bit1.run();
      client.connect();
      List<QueryDataBatch> results = client.runQuery(QueryType.PHYSICAL, Files.toString(DrillFileUtils.getResourceAsFile("/physical_single_exchange_double_entry.json"), Charsets.UTF_8));
      int count = 0;
      for(QueryDataBatch b : results){
        count += b.getHeader().getRowCount();
        b.release();
      }
      assertEquals(200, count);
    }


  }

    @Test
    public void oneBitOneExchangeTwoEntryRunLogical() throws Exception{
        RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();

        try(Drillbit bit1 = new Drillbit(CONFIG, serviceSet); DrillClient client = new DrillClient(CONFIG, serviceSet.getCoordinator());){
            bit1.run();
            client.connect();
            List<QueryDataBatch> results = client.runQuery(QueryType.LOGICAL, Files.toString(DrillFileUtils.getResourceAsFile("/scan_screen_logical.json"), Charsets.UTF_8));
            int count = 0;
            for(QueryDataBatch b : results){
                count += b.getHeader().getRowCount();
                b.release();
            }
            assertEquals(100, count);
        }


    }

  @Test
    public void twoBitOneExchangeTwoEntryRun() throws Exception{
      RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();

      try(Drillbit bit1 = new Drillbit(CONFIG, serviceSet); Drillbit bit2 = new Drillbit(CONFIG, serviceSet); DrillClient client = new DrillClient(CONFIG, serviceSet.getCoordinator());){
        bit1.run();
        bit2.run();
        client.connect();
        List<QueryDataBatch> results = client.runQuery(QueryType.PHYSICAL, Files.toString(DrillFileUtils.getResourceAsFile("/physical_single_exchange_double_entry.json"), Charsets.UTF_8));
        int count = 0;
      for(QueryDataBatch b : results){
        count += b.getHeader().getRowCount();
        b.release();
      }
      assertEquals(200, count);
    }


  }
}
