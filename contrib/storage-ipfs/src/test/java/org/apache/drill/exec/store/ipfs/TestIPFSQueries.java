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


package org.apache.drill.exec.store.ipfs;

import io.ipfs.multihash.Multihash;
import org.apache.drill.categories.IPFSStorageTest;
import org.apache.drill.categories.SlowTest;
import org.apache.drill.exec.proto.CoordinationProtos;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.fail;

@Ignore("Requires running local IPFS daemon")
@Category({SlowTest.class, IPFSStorageTest.class})
public class TestIPFSQueries extends IPFSTestBase implements IPFSTestConstants {

  @Before
  public void checkDrillbitPorts() {
    CoordinationProtos.DrillbitEndpoint ep = cluster.drillbit().getRegistrationHandle().getEndPoint();
    int controlPort = ep.getControlPort();
    int userPort = ep.getUserPort();
    int dataPort = ep.getDataPort();
    if (controlPort != IPFSGroupScan.DEFAULT_CONTROL_PORT
        || userPort != IPFSGroupScan.DEFAULT_USER_PORT
        || dataPort != IPFSGroupScan.DEFAULT_DATA_PORT) {
      //DRILL-7754 handle non-default ports
      fail(String.format("Drill binded to non-default ports: %d, %d, %d", controlPort, userPort, dataPort));
    }
  }

  @Test
  public void testNullQuery() throws Exception {

    testBuilder()
        .sqlQuery(getSelectStar(IPFSHelper.IPFS_NULL_OBJECT))
        .unOrdered()
        .expectsNumRecords(1)
        .go();
  }

  @Test
  public void testSimple() throws Exception {
    Multihash dataset = IPFSTestDataGenerator.importSimple(IPFSTestSuit.getIpfsStoragePluginConfig());
    if (null == dataset) {
      fail();
    }

    testBuilder()
        .sqlQuery(getSelectStar(dataset))
        .unOrdered()
        .expectsNumRecords(1)
        .go();
  }

  @Test
  public void testSimpleCIDv1() throws Exception {
    Multihash dataset = IPFSTestDataGenerator.importSimple(IPFSTestSuit.getIpfsStoragePluginConfig());
    if (null == dataset) {
      fail();
    }

    testBuilder()
        .sqlQuery(getSelectStar(SIMPLE_DATASET_CID_V1))
        .unOrdered()
        .expectsNumRecords(1)
        .go();
  }

  @Test
  public void testChunked() throws Exception {
    Multihash dataset = IPFSTestDataGenerator.importChunked(IPFSTestSuit.getIpfsStoragePluginConfig());
    if (null == dataset) {
      fail();
    }

    testBuilder()
        .sqlQuery(getSelectStar(dataset))
        .unOrdered()
        .expectsNumRecords(5)
        .go();
  }

  private static String getSelectStar(Multihash dataset) {
    return String.format("SELECT * FROM ipfs.`%s`", IPFSTestConstants.getQueryPath(dataset));
  }
}
