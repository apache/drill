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
package org.apache.drill.exec.store.pcapng;

import org.apache.drill.PlanTestBase;
import org.apache.drill.common.exceptions.UserRemoteException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.file.Paths;

public class TestPcapngRecordReader extends PlanTestBase {
  @BeforeClass
  public static void setupTestFiles() {
    dirTestWatcher.copyResourceToRoot(Paths.get("store", "pcapng"));
  }

  @Test
  public void testStarQuery() throws Exception {
    Assert.assertEquals(123, testSql("select * from dfs.`store/pcapng/sniff.pcapng`"));
    Assert.assertEquals(1, testSql("select * from dfs.`store/pcapng/example.pcapng`"));
  }

  @Test
  public void testProjectingByName() throws Exception {
    Assert.assertEquals(123, testSql("select `timestamp`, packet_data, type from dfs.`store/pcapng/sniff.pcapng`"));
    Assert.assertEquals(1, testSql("select src_ip, dst_ip, `timestamp` from dfs.`store/pcapng/example.pcapng`"));
  }

  @Test
  public void testDiffCaseQuery() throws Exception {
    Assert.assertEquals(123, testSql("select `timestamp`, paCket_dAta, TyPe from dfs.`store/pcapng/sniff.pcapng`"));
    Assert.assertEquals(1, testSql("select src_ip, dst_ip, `Timestamp` from dfs.`store/pcapng/example.pcapng`"));
  }

  @Test
  public void testProjectingMissColls() throws Exception {
    Assert.assertEquals(123, testSql("select `timestamp`, `name`, `color` from dfs.`store/pcapng/sniff.pcapng`"));
    Assert.assertEquals(1, testSql("select src_ip, `time` from dfs.`store/pcapng/example.pcapng`"));
  }


  @Test
  public void testCountQuery() throws Exception {
    testBuilder()
        .sqlQuery("select count(*) as ct from dfs.`store/pcapng/sniff.pcapng`")
        .ordered()
        .baselineColumns("ct")
        .baselineValues(123L)
        .build()
        .run();

    testBuilder()
        .sqlQuery("select count(*) as ct from dfs.`store/pcapng/example.pcapng`")
        .ordered()
        .baselineColumns("ct")
        .baselineValues(1L)
        .build()
        .run();
  }

  @Test
  public void testGroupBy() throws Exception {
    Assert.assertEquals(47, testSql("select src_ip, count(1), sum(packet_length) from dfs.`store/pcapng/sniff.pcapng` group by src_ip"));
  }

  @Test
  public void testDistinctQuery() throws Exception {
    Assert.assertEquals(119, testSql("select distinct `timestamp`, src_ip from dfs.`store/pcapng/sniff.pcapng`"));
    Assert.assertEquals(1, testSql("select distinct packet_data from dfs.`store/pcapng/example.pcapng`"));
  }

  @Test(expected = UserRemoteException.class)
  public void testBasicQueryWithIncorrectFileName() throws Exception {
    testSql("select * from dfs.`store/pcapng/snaff.pcapng`");
  }

  @Test
  public void testPhysicalPlanExecutionBasedOnQuery() throws Exception {
    String query = "EXPLAIN PLAN for select * from dfs.`store/pcapng/sniff.pcapng`";
    String plan = getPlanInString(query, JSON_FORMAT);
    Assert.assertEquals(123, testPhysical(plan));
  }
}
