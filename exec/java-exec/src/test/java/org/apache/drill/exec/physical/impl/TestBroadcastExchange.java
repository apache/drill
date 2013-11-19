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

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.pop.PopUnitTestBase;
import org.apache.drill.exec.proto.UserProtos;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.RemoteServiceSet;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestBroadcastExchange extends PopUnitTestBase {
  @Test
  public void TestSingleBroadcastExchangeWithTwoScans() throws Exception {
    RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();

    try(Drillbit bit1 = new Drillbit(CONFIG, serviceSet);
        Drillbit bit2 = new Drillbit(CONFIG, serviceSet);
        DrillClient client = new DrillClient(CONFIG, serviceSet.getCoordinator())) {

      bit1.run();
      bit2.run();
      client.connect();

      String physicalPlan = Files.toString(
              FileUtils.getResourceAsFile("/sender/broadcast_exchange.json"), Charsets.UTF_8)
              .replace("#{LEFT_FILE}", FileUtils.getResourceAsFile("/join/merge_single_batch.left.json").toURI().toString())
              .replace("#{RIGHT_FILE}", FileUtils.getResourceAsFile("/join/merge_single_batch.right.json").toURI().toString());
      List<QueryResultBatch> results = client.runQuery(UserProtos.QueryType.PHYSICAL, physicalPlan);
      int count = 0;
      for(QueryResultBatch b : results) {
        if (b.getHeader().getRowCount() != 0)
          count += b.getHeader().getRowCount();
      }
      assertEquals(25, count);
    }
  }

  @Test
  public void TestMultipleSendLocationBroadcastExchange() throws Exception {
    RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();

    try(Drillbit bit1 = new Drillbit(CONFIG, serviceSet);
        Drillbit bit2 = new Drillbit(CONFIG, serviceSet);
        DrillClient client = new DrillClient(CONFIG, serviceSet.getCoordinator())) {

      bit1.run();
      bit2.run();
      client.connect();

      String physicalPlan = Files.toString(
          FileUtils.getResourceAsFile("/sender/broadcast_exchange_long_run.json"), Charsets.UTF_8);
      List<QueryResultBatch> results = client.runQuery(UserProtos.QueryType.PHYSICAL, physicalPlan);
      int count = 0;
      for(QueryResultBatch b : results) {
        if (b.getHeader().getRowCount() != 0)
          count += b.getHeader().getRowCount();
      }
      System.out.println(count);
    }
  }
}
