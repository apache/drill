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
package org.apache.drill.exec.client;

import java.util.List;

import org.apache.drill.exec.DrillSystemTestBase;
import org.apache.drill.exec.proto.UserBitShared.QueryType;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;


@Ignore
public class DrillClientSystemTest extends DrillSystemTestBase {

  private static String plan;

  @BeforeClass
  public static void setUp() throws Exception {
    DrillSystemTestBase.setUp();
    plan = Resources.toString(Resources.getResource("simple_plan.json"), Charsets.UTF_8);

  }

  @After
  public void tearDownTest() {
    stopCluster();
  }

  @Test
  public void testSubmitPlanSingleNode() throws Exception {
    startCluster(1);
    DrillClient client = new DrillClient();
    client.connect();
    List<QueryDataBatch> results = client.runQuery(QueryType.LOGICAL, plan);
    for (QueryDataBatch result : results) {
      System.out.println(result);
      result.release();
    }
    client.close();
  }

  @Test
  public void testSubmitPlanTwoNodes() throws Exception {
    startCluster(2);
    DrillClient client = new DrillClient();
    client.connect();
    List<QueryDataBatch> results = client.runQuery(QueryType.LOGICAL, plan);
    for (QueryDataBatch result : results) {
      System.out.println(result);
      result.release();
    }
    client.close();
  }
}
