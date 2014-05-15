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

import java.util.List;

import org.apache.drill.common.util.FileUtils;
import org.apache.drill.common.util.TestTools;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.pop.PopUnitTestBase;
import org.apache.drill.exec.proto.UserProtos;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.RemoteServiceSet;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

@Ignore("Currently returns wrong result.  Stopped working when incoming became more than one result set.")
public class TestMergeJoinMulCondition extends PopUnitTestBase {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestMergeJoinMulCondition.class);

  @Rule public final TestRule TIMEOUT = TestTools.getTimeoutRule(200000);

  @Test
  //the physical plan is obtained for the following SQL query:
  //  "select l.l_partkey, l.l_suppkey, ps.ps_partkey, ps.ps_suppkey "
  //      + " from cp.`tpch/lineitem.parquet` l join "
  //      + "      cp.`tpch/partsupp.parquet` ps"
  //      + " on l.l_partkey = ps.ps_partkey and "
  //      + "    l.l_suppkey = ps.ps_suppkey";
  public void testMergeJoinMultiKeys() throws Exception {
    RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();

    try(Drillbit bit1 = new Drillbit(CONFIG, serviceSet);
        DrillClient client = new DrillClient(CONFIG, serviceSet.getCoordinator());) {

      bit1.run();
      client.connect();
      List<QueryResultBatch> results = client.runQuery(UserProtos.QueryType.PHYSICAL,
          Files.toString(FileUtils.getResourceAsFile("/join/mj_multi_condition.json"),
              Charsets.UTF_8));
      int count = 0;
      for(QueryResultBatch b : results) {
        if (b.getHeader().getRowCount() != 0){
          count += b.getHeader().getRowCount();
        }
        b.release();
      }
      assertEquals(60175, count);
    }
  }

}
